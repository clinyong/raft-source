import { createCodexIdleRecycle } from "./codexIdleRecycle";
import { ambientProcessEnv } from "../processEnvPatch";
import { RuntimeExecutableNotFoundError } from "../spawnFailureErrors";
import { spawn, execFileSync } from "node:child_process";
import { spawnRuntimeChild } from "./runtimeChild";
import { existsSync, readFileSync, statSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ChildProcess } from "node:child_process";
import { hydrateRuntimeConfig, runtimeConfigToLaunchFields, runtimeModelSourceOutcomeFromSet, type AgentConfig, type RuntimeModelInfo, type RuntimeModelSet, type RuntimeModelSourceOutcome, type Tracer , type AxSurfaceText } from "@botiverse/raft-shared";
import type { RuntimeDriver, SpawnContext, SpawnResult, ParsedEvent, RuntimeProbeResult, RuntimeProbeUnavailableReason } from "./types";
import { buildCliTransportSystemPrompt, prepareCliTransport } from "./cliTransport";
import { codexStateRootCandidates, resolveCodexHomeRootFromEnv } from "./codexHome";
import { detectNodeHostKind, NodeHostUnavailableError, resolveNodeHostLaunch } from "./nodeHostLaunch";
import {
  firstExistingPath,
  lookupCommandOnPath,
  lookupCommandOnPathAsync,
  resolveCommandOnPath,
  resolveCommandOnPathAsync,
  withWindowsUserEnvironment,
  withWindowsUserEnvironmentAsync,
  type ProbeDeps,
} from "./probe";
import { killProbeTreeAndSettle, ProbeChildError, runProbeChild } from "./probeChild";
import { resolveWindowsDirectLaunch, resolveWindowsDirectLaunchAsync } from "./windowsLaunch";
import {
  CodexEventNormalizer,
  parseCodexJsonRpcLine,
  type JsonRpcMessage,
  type JsonRpcId,
} from "./codexEventNormalizer";
import { prepareManagedMcpRuntimeProxy } from "../managedMcpRuntimeProxy";
import {
  buildCodexInstructionShapeAttrs,
  buildCodexInstructionShapeStaticAttrs,
  type CodexInstructionObservationPhase,
  type CodexInstructionShapeStaticAttrs,
  type CodexThreadRequestMethod,
} from "./codexInstructionShape";

export { parseCodexJsonRpcLine } from "./codexEventNormalizer";

/**
 * macOS desktop-bundled Codex CLI locations that are still real install surfaces.
 * `Codex.app` is intentionally NOT listed: shipping installs put the CLI under
 * ChatGPT.app; re-introducing the dead Codex.app path would re-select a missing
 * fallback and mask PATH/override failures. Use CODEX_BIN for one-off paths.
 */
const CODEX_DESKTOP_BUNDLE_PATHS = [
  "/Applications/ChatGPT.app/Contents/Resources/codex",
] as const;
const CODEX_APP_SERVER_PROBE_ARGS = ["app-server", "--help"] as const;

/** Escape hatch: absolute path or PATH name. Checked before PATH discovery. */
const CODEX_BIN_ENV = "CODEX_BIN";
/**
 * Budget for the `npm root -g` child that locates a global npm Codex install on
 * Windows (task #1263). It ran unbounded before: a hung npm (corrupt cache,
 * slow network-backed profile) stalled the whole synchronous runtime detection
 * round, which runs before the daemon's `ready`. Same value as the other probe
 * children (`readCommandVersion`, the app-server probe, the PATH lookup steps).
 */
export const CODEX_NPM_ROOT_TIMEOUT_MS = 5000;

type CodexProbeCacheEntry = {
  mtimeMs: number;
  size: number;
  appServerOk: boolean;
  version: string | null;
  failure: string | null;
};

const codexProbeCache = new Map<string, CodexProbeCacheEntry>();

function hasNonEmptyCodexTextInput(input: unknown): boolean {
  return Array.isArray(input) && input.some((item) =>
    item &&
    typeof item === "object" &&
    (item as { type?: unknown }).type === "text" &&
    typeof (item as { text?: unknown }).text === "string" &&
    (item as { text: string }).text.length > 0
  );
}

function resultTurnId(message: JsonRpcMessage): string | null {
  const turn = message.result?.turn;
  return turn && typeof turn.id === "string" ? turn.id : null;
}

interface CodexSpawnCandidate {
  source: "explicit_bin" | "npm_global" | "path" | "desktop_bundle" | "desktop_install";
  command: string;
  argsPrefix: string[];
  /** Runtimes are never started through a shell; see windowsLaunch.ts. */
  shell: false;
  env?: NodeJS.ProcessEnv;
}

interface CodexSpawnCandidateDiscovery {
  candidates: CodexSpawnCandidate[];
  /** Credential-free reasons a discovered surface could not become a candidate. */
  rejected: string[];
  /** Why the `codex` PATH lookup resolved nothing; absent when it resolved a command. */
  pathMissReason?: RuntimeProbeUnavailableReason;
  /** Path-free observations that reject nothing and never change classification (e.g. a timed-out discovery step). */
  notes: string[];
}

type ExplicitCodexBinResolution =
  | { status: "unset" }
  | { status: "resolved"; candidate: CodexSpawnCandidate }
  | { status: "invalid"; raw: string; reason: string };

function isWindowsSandboxRunner(commandPath: string): boolean {
  return path.basename(commandPath).toLowerCase().startsWith("codex-command-runner");
}

function resolveWindowsNpmCodexEntry(deps: ProbeDeps = {}): { entry: string | null; note?: string } {
  const existsSyncFn = deps.existsSyncFn ?? existsSync;
  const execFileSyncFn = deps.execFileSyncFn ?? execFileSync;
  const env = deps.env ?? ambientProcessEnv();
  const winPath = path.win32;
  let note: string | undefined;

  try {
    const globalRoot = String(execFileSyncFn("npm", ["root", "-g"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      env,
      timeout: CODEX_NPM_ROOT_TIMEOUT_MS,
    })).trim();
    const candidate = winPath.join(globalRoot, "@openai", "codex", "bin", "codex.js");
    if (existsSyncFn(candidate)) return { entry: candidate };
  } catch (error) {
    // A missing or failing npm is an expected state and stays silent; only a
    // child killed at the budget is worth a (path-free) note, because the global
    // npm install was then not searched at all rather than found empty.
    if ((error as { code?: unknown }).code === "ETIMEDOUT") {
      note = `npm_global discovery: \`npm root -g\` did not finish within ${CODEX_NPM_ROOT_TIMEOUT_MS} ms, so the global npm install was not searched and a Codex installed only there is not ruled out`;
    }
  }

  const cmdPath = resolveCommandOnPath("codex", deps);
  if (cmdPath) {
    const candidate = winPath.join(winPath.dirname(cmdPath), "node_modules", "@openai", "codex", "bin", "codex.js");
    if (existsSyncFn(candidate)) return { entry: candidate, ...(note ? { note } : {}) };
  }

  return { entry: null, ...(note ? { note } : {}) };
}

function resolveWindowsCodexDesktopEntry(deps: ProbeDeps = {}): string | null {
  const existsSyncFn = deps.existsSyncFn ?? existsSync;
  const env = deps.env ?? ambientProcessEnv();
  const homeDir = deps.homeDir;
  const winPath = path.win32;
  const localAppDataRoots = [
    env.LOCALAPPDATA,
    env.USERPROFILE ? winPath.join(env.USERPROFILE, "AppData", "Local") : null,
    homeDir ? winPath.join(homeDir, "AppData", "Local") : null,
  ].filter((candidate): candidate is string => Boolean(candidate));
  const candidates = [...new Set(localAppDataRoots.flatMap((root) => [
    winPath.join(root, "Programs", "OpenAI", "Codex", "bin", "codex.exe"),
    winPath.join(root, "OpenAI", "Codex", "bin", "codex.exe"),
  ]))];

  for (const candidate of candidates) {
    if (existsSyncFn(candidate)) return candidate;
  }

  return null;
}

/**
 * Automatic discovery candidates only. Explicit `CODEX_BIN` is never mixed into
 * this list — it is a separate authoritative decision before version arbitration.
 */
function codexSpawnCandidates(deps: ProbeDeps = {}): CodexSpawnCandidateDiscovery {
  const platform = deps.platform ?? process.platform;
  if (platform === "win32") {
    const candidates: CodexSpawnCandidate[] = [];
    const rejected: string[] = [];

    const npm = resolveWindowsNpmCodexEntry(deps);
    const notes = npm.note ? [npm.note] : [];
    const npmEntry = npm.entry;
    if (npmEntry) {
      try {
        const nodeHost = resolveNodeHostLaunch({
          env: deps.env ?? ambientProcessEnv(),
          execPath: deps.execPath,
          execIsElectron: deps.execIsElectron,
          execIsSea: deps.execIsSea,
          hasNodeRuntime: deps.hasNodeRuntime,
        });
        candidates.push({
          source: "npm_global",
          command: nodeHost.command,
          argsPrefix: [npmEntry],
          shell: false,
          env: nodeHost.env,
        });
      } catch (error) {
        if (!(error instanceof NodeHostUnavailableError)) throw error;
        // The npm package exists, but this host cannot execute its JavaScript
        // entry. That rejects only this candidate: a cmd shim or native Codex
        // executable may still be usable. Keep the reason path-free because it
        // is emitted through runtime inventory diagnostics.
        rejected.push(
          `npm_global JavaScript entry rejected: ${error.kind} (host_kind=${error.hostKind})`,
        );
      }
    }

    const { command, missReason: pathMissReason } = lookupCommandOnPath("codex", deps);
    if (command && !isWindowsSandboxRunner(command)) {
      const direct = windowsDirectCodexCandidate("path", command, deps);
      if (typeof direct === "string") rejected.push(direct);
      else candidates.push(direct);
    }

    const desktopEntry = resolveWindowsCodexDesktopEntry(deps);
    if (desktopEntry && !candidates.some((candidate) => candidate.command === desktopEntry)) {
      candidates.push({
        source: "desktop_install",
        command: desktopEntry,
        argsPrefix: [],
        shell: false,
      });
    }

    return { candidates, rejected, notes, ...(pathMissReason ? { pathMissReason } : {}) };
  }

  const candidates: CodexSpawnCandidate[] = [];

  const { command: pathCommand, missReason: pathMissReason } = lookupCommandOnPath("codex", deps);
  if (pathCommand) {
    candidates.push({
      source: "path",
      command: pathCommand,
      argsPrefix: [],
      shell: false,
    });
  }

  if (platform === "darwin") {
    const existsSyncFn = deps.existsSyncFn ?? existsSync;
    for (const bundlePath of darwinDesktopCodexPaths(deps)) {
      if (!existsSyncFn(bundlePath)) continue;
      if (candidates.some((candidate) => candidate.command === bundlePath)) continue;
      candidates.push({
        source: "desktop_bundle",
        command: bundlePath,
        argsPrefix: [],
        shell: false,
      });
    }
  }

  return { candidates, rejected: [], notes: [], ...(pathMissReason ? { pathMissReason } : {}) };
}

export function resolveCodexCommand(deps: ProbeDeps = {}): string | null {
  const explicit = resolveExplicitCodexBin(deps);
  if (explicit.status === "resolved") return explicit.candidate.command;
  if (explicit.status === "invalid") return null;

  const platform = deps.platform ?? process.platform;
  if (platform === "win32") {
    const npmEntry = resolveWindowsNpmCodexEntry(deps).entry;
    if (npmEntry) return npmEntry;

    const command = resolveCommandOnPath("codex", deps);
    if (command && !isWindowsSandboxRunner(command)) return command;

    return resolveWindowsCodexDesktopEntry(deps);
  }

  const pathCommand = resolveCommandOnPath("codex", deps);
  if (pathCommand) return pathCommand;

  if (platform === "darwin") {
    return firstExistingPath(darwinDesktopCodexPaths(deps), deps);
  }

  return null;
}

function formatCodexCandidate(candidate: CodexSpawnCandidate): string {
  return [candidate.command, ...candidate.argsPrefix].join(" ");
}

function codexCandidateCacheKey(candidate: CodexSpawnCandidate): string {
  return `${candidate.shell ? "shell" : "exec"}\0${candidate.command}\0${candidate.argsPrefix.join("\0")}`;
}

function readCandidateStat(candidate: CodexSpawnCandidate, deps: ProbeDeps): { mtimeMs: number; size: number } | null {
  const existsSyncFn = deps.existsSyncFn ?? existsSync;
  const statSyncFn = deps.statSyncFn ?? ((filePath: string) => {
    const st = statSync(filePath);
    return { mtimeMs: st.mtimeMs, size: st.size };
  });
  const statPath = candidate.argsPrefix[0] && candidate.command !== candidate.argsPrefix[0]
    ? candidate.argsPrefix[0]
    : candidate.command;
  if (!existsSyncFn(statPath)) return null;
  try {
    return statSyncFn(statPath);
  } catch {
    return null;
  }
}

/** Parsed Codex CLI version for ordering (core triple + optional prerelease numeric trail). */
type ParsedCodexVersion = {
  core: [number, number, number];
  /** null = release build (ranks above any prerelease with the same core). */
  pre: number[] | null;
};

/**
 * Parse real Codex version strings, including:
 * - `codex-cli 0.144.6`
 * - `0.147.0-alpha.6.5` / `codex-cli 0.147.0-alpha.1`
 */
export function parseCodexVersion(raw: string | null | undefined): ParsedCodexVersion | null {
  if (!raw) return null;
  const m = raw.match(/(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?/);
  if (!m) return null;
  const core: [number, number, number] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (!m[4]) return { core, pre: null };
  // Keep only numeric segments from the prerelease trail (alpha.6.5 → [6, 5]).
  const pre = m[4]
    .split(/[.-]/)
    .map((part) => {
      const n = Number(part);
      return Number.isFinite(n) ? n : null;
    })
    .filter((n): n is number => n !== null);
  // Non-numeric-only labels (e.g. bare "alpha") still rank below release via empty pre [].
  return { core, pre };
}

/** Compare Codex versions; returns >0 if a newer than b. Unparseable → null. */
export function compareCodexVersions(a: string | null | undefined, b: string | null | undefined): number | null {
  const pa = parseCodexVersion(a);
  const pb = parseCodexVersion(b);
  if (!pa || !pb) return null;
  for (let i = 0; i < 3; i += 1) {
    if (pa.core[i]! > pb.core[i]!) return 1;
    if (pa.core[i]! < pb.core[i]!) return -1;
  }
  // Release (pre=null) > any prerelease with the same core.
  if (pa.pre === null && pb.pre === null) return 0;
  if (pa.pre === null) return 1;
  if (pb.pre === null) return -1;
  const len = Math.max(pa.pre.length, pb.pre.length);
  for (let i = 0; i < len; i += 1) {
    const av = pa.pre[i] ?? 0;
    const bv = pb.pre[i] ?? 0;
    if (av > bv) return 1;
    if (av < bv) return -1;
  }
  return 0;
}

/** Test hook: empty the process-local app-server/version probe cache. */
export function clearCodexProbeCacheForTests(): void {
  codexProbeCache.clear();
}

function describeSearchPath(deps: ProbeDeps): string {
  const env = deps.env ?? ambientProcessEnv();
  const pathValue = env.PATH ?? env.Path ?? "(unset)";
  if ((deps.platform ?? process.platform) === "win32") {
    return pathValue === "(unset)" || pathValue.trim().length === 0
      ? "PATH=(unset)"
      : "PATH=present";
  }
  const preview = pathValue.length > 240 ? `${pathValue.slice(0, 240)}…` : pathValue;
  return `PATH=${preview}`;
}

function darwinDesktopCodexPaths(deps: ProbeDeps): string[] {
  const env = deps.env ?? ambientProcessEnv();
  const home = deps.homeDir ?? env.HOME ?? os.homedir();
  return [
    ...CODEX_DESKTOP_BUNDLE_PATHS,
    path.join(home, ".codex", "plugins", ".plugin-appserver", "codex"),
  ];
}

/**
 * Authoritative override from `CODEX_BIN`. Distinct from automatic discovery:
 * when set, resolution either uses this binary (after probe) or fails closed —
 * it never competes in multi-candidate version arbitration.
 */
function resolveExplicitCodexBin(deps: ProbeDeps): ExplicitCodexBinResolution {
  const env = deps.env ?? ambientProcessEnv();
  const raw = env[CODEX_BIN_ENV]?.trim();
  if (!raw) return { status: "unset" };

  const platform = deps.platform ?? process.platform;
  const existsSyncFn = deps.existsSyncFn ?? existsSync;

  let command: string | null = null;
  if (path.isAbsolute(raw) || /^[A-Za-z]:[\\/]/.test(raw) || raw.startsWith("\\\\")) {
    command = existsSyncFn(raw) ? raw : null;
    if (!command) {
      return { status: "invalid", raw, reason: "path does not exist" };
    }
  } else {
    command = resolveCommandOnPath(raw, deps);
    if (!command) {
      return { status: "invalid", raw, reason: "not found on PATH" };
    }
  }

  if (platform === "win32" && isWindowsSandboxRunner(command)) {
    return {
      status: "invalid",
      raw,
      reason: "points at codex-command-runner sandbox helper, not the Codex CLI",
    };
  }

  if (platform === "win32") {
    const direct = windowsDirectCodexCandidate("explicit_bin", command, deps);
    if (typeof direct === "string") return { status: "invalid", raw, reason: direct };
    return { status: "resolved", candidate: direct };
  }

  return {
    status: "resolved",
    candidate: {
      source: "explicit_bin",
      command,
      argsPrefix: [],
      shell: false,
    },
  };
}

/**
 * A Windows command as a direct launch: a .cmd/.bat shim is resolved to the
 * program it runs, never started through cmd.exe. Returns a path-free
 * rejection reason when the shim cannot be resolved.
 */
function windowsDirectCodexCandidate(
  source: CodexSpawnCandidate["source"],
  command: string,
  deps: ProbeDeps,
): CodexSpawnCandidate | string {
  try {
    const launch = resolveWindowsDirectLaunch("codex", command, [], deps);
    return {
      source,
      command: launch.command,
      argsPrefix: launch.args,
      shell: false,
      ...(launch.env ? { env: launch.env } : {}),
    };
  } catch (error) {
    if (!(error instanceof RuntimeExecutableNotFoundError)) throw error;
    return `${source} batch wrapper rejected: ${error.reason ?? "unresolved"}`;
  }
}

function describeCodexProbeFailure(error: unknown): string {
  if (error && typeof error === "object") {
    const candidate = error as {
      status?: unknown;
      signal?: unknown;
      code?: unknown;
    };
    if (typeof candidate.status === "number") return `exit status ${candidate.status}`;
    if (typeof candidate.signal === "string") return `terminated by ${candidate.signal}`;
    if (typeof candidate.code === "string") return candidate.code;
  }
  return "probe failed";
}

function probeCodexCandidate(
  candidate: CodexSpawnCandidate,
  deps: ProbeDeps = {},
): { failure: string | null; version: string | null } {
  const execFileSyncFn = deps.execFileSyncFn ?? execFileSync;
  const env = withWindowsUserEnvironment(candidate.env ?? deps.env ?? ambientProcessEnv(), deps);
  const stat = readCandidateStat(candidate, deps);
  const cacheKey = codexCandidateCacheKey(candidate);
  if (stat) {
    const cached = codexProbeCache.get(cacheKey);
    if (cached && cached.mtimeMs === stat.mtimeMs && cached.size === stat.size) {
      return { failure: cached.failure, version: cached.version };
    }
  }

  let failure: string | null = null;
  try {
    execFileSyncFn(candidate.command, [...candidate.argsPrefix, ...CODEX_APP_SERVER_PROBE_ARGS], {
      stdio: ["ignore", "pipe", "pipe"],
      env,
      timeout: 5000,
      shell: candidate.shell,
    });
  } catch (error) {
    failure = describeCodexProbeFailure(error);
  }

  let version: string | null = null;
  if (!failure) {
    try {
      const output = execFileSyncFn(candidate.command, [...candidate.argsPrefix, "--version"], {
        stdio: ["ignore", "pipe", "pipe"],
        env,
        timeout: 5000,
        shell: candidate.shell,
      });
      version = (Buffer.isBuffer(output) ? output.toString("utf8") : String(output ?? "")).trim().split(/\r?\n/)[0] || null;
    } catch {
      version = null;
    }
  }

  if (stat) {
    codexProbeCache.set(cacheKey, {
      mtimeMs: stat.mtimeMs,
      size: stat.size,
      appServerOk: failure === null,
      version,
      failure,
    });
  }
  return { failure, version };
}

function validateCodexAppServer(candidate: CodexSpawnCandidate, deps: ProbeDeps = {}): string | null {
  return probeCodexCandidate(candidate, deps).failure;
}

function readCodexCandidateVersion(candidate: CodexSpawnCandidate, deps: ProbeDeps = {}): string | null {
  return probeCodexCandidate(candidate, deps).version;
}

function resolveCompatibleCodexCandidate(deps: ProbeDeps = {}): {
  candidate: CodexSpawnCandidate | null;
  rejected: string[];
  version: string | null;
  notes: string[];
  /** When CODEX_BIN was set but unusable — callers must fail closed, not fall through. */
  explicitOverrideFailed: boolean;
  /** Automatic discovery only: why the `codex` PATH lookup resolved nothing. */
  pathMissReason?: RuntimeProbeUnavailableReason;
} {
  const rejected: string[] = [];
  const notes: string[] = [];

  // 1) Explicit override is an authoritative, cross-platform decision.
  const explicit = resolveExplicitCodexBin(deps);
  if (explicit.status === "invalid") {
    return {
      candidate: null,
      rejected: [`${CODEX_BIN_ENV}=${explicit.raw} rejected: ${explicit.reason}`],
      version: null,
      notes: [],
      explicitOverrideFailed: true,
    };
  }
  if (explicit.status === "resolved") {
    const { failure, version } = probeCodexCandidate(explicit.candidate, deps);
    if (failure) {
      return {
        candidate: null,
        rejected: [
          `${CODEX_BIN_ENV}=${formatCodexCandidate(explicit.candidate)} rejected: app-server probe ${failure}`,
        ],
        version: null,
        notes: [],
        explicitOverrideFailed: true,
      };
    }
    notes.push(
      `using ${CODEX_BIN_ENV} override ${formatCodexCandidate(explicit.candidate)}` +
      (version ? ` (${version})` : "") +
      "; automatic PATH/desktop discovery skipped",
    );
    return {
      candidate: explicit.candidate,
      rejected: [],
      version,
      notes,
      explicitOverrideFailed: false,
    };
  }

  // 2) Automatic discovery only — version-arbitrate among app-server-capable candidates.
  let best: { candidate: CodexSpawnCandidate; version: string | null } | null = null;

  const discovery = codexSpawnCandidates(deps);
  rejected.push(...discovery.rejected);
  notes.push(...discovery.notes);
  for (const candidate of discovery.candidates) {
    const { failure, version } = probeCodexCandidate(candidate, deps);
    if (failure) {
      rejected.push(`${candidate.source} ${formatCodexCandidate(candidate)} rejected: app-server probe ${failure}`);
      continue;
    }
    if (!best) {
      best = { candidate, version };
      continue;
    }
    const cmp = compareCodexVersions(version, best.version);
    if (cmp !== null && cmp > 0) {
      notes.push(
        `selected ${formatCodexCandidate(candidate)} (${version ?? "unknown"}) over ` +
        `${formatCodexCandidate(best.candidate)} (${best.version ?? "unknown"}) by version`,
      );
      best = { candidate, version };
    }
  }

  return {
    candidate: best?.candidate ?? null,
    rejected,
    version: best?.version ?? null,
    notes,
    explicitOverrideFailed: false,
    ...(discovery.pathMissReason ? { pathMissReason: discovery.pathMissReason } : {}),
  };
}

export function probeCodex(deps: ProbeDeps = {}): RuntimeProbeResult {
  const { candidate, rejected, version, notes, explicitOverrideFailed, pathMissReason } = resolveCompatibleCodexCandidate(deps);
  const parts = [
    ...rejected,
    ...notes,
  ];
  if (!candidate) {
    parts.push(describeSearchPath(deps));
    parts.push("Restart the Raft daemon after changing PATH or installing Codex so the new environment is picked up.");
    // Nothing discovered at all (no CODEX_BIN override, no candidate found and
    // none rejected) carries the PATH lookup's own verdict, so an undecided
    // lookup (e.g. a timeout) is not "not installed". Any rejected candidate or
    // failed override is an installed-but-unusable Codex.
    const nothingDiscovered = !explicitOverrideFailed && rejected.length === 0;
    return {
      available: false,
      unavailableReason: (nothingDiscovered ? pathMissReason : undefined) ?? "probe_failed",
      diagnostic: parts.length > 0
        ? parts.join("; ")
        : "No Codex CLI app-server candidate was found.",
    };
  }
  return {
    available: true,
    version: version ?? undefined,
    ...(parts.length > 0 ? { diagnostic: parts.join("; ") } : {}),
  };
}

export function resolveCodexSpawn(commandArgs: string[], deps: ProbeDeps = {}): { command: string; args: string[]; shell: false; source: CodexSpawnCandidate["source"]; env?: NodeJS.ProcessEnv } {
  const { candidate, rejected, explicitOverrideFailed } = resolveCompatibleCodexCandidate(deps);
  if (candidate) {
    return {
      command: candidate.command,
      args: [...candidate.argsPrefix, ...commandArgs],
      shell: candidate.shell,
      source: candidate.source,
      ...(candidate.env ? { env: candidate.env } : {}),
    };
  }

  const search = describeSearchPath(deps);
  const rejectedNote = rejected.length > 0 ? ` Rejected candidates: ${rejected.join("; ")}.` : "";
  const restartNote = " Restart the Raft daemon after changing PATH or installing Codex so the new environment is picked up.";

  if (explicitOverrideFailed) {
    throw new Error(
      `${CODEX_BIN_ENV} is set but does not resolve to a usable Codex CLI app-server entry point.` +
      rejectedNote +
      ` (${search}).` +
      " Fix or unset CODEX_BIN; automatic PATH/desktop discovery is not used while the override is set." +
      restartNote,
    );
  }

  if ((deps.platform ?? process.platform) === "win32") {
    // Surface why a found .cmd wrapper was turned away (launch_unresolved event).
    const wrapperReason = rejected.map((note) => /batch wrapper rejected: (\w+)/.exec(note)?.[1]).find(Boolean);
    throw new RuntimeExecutableNotFoundError({ runtimeId: "codex", reason: wrapperReason, message:
      "Cannot resolve a compatible Codex CLI app-server entry point on Windows. " +
      "Install Codex Desktop or install @openai/codex globally via npm (npm i -g @openai/codex). " +
      "Ignoring .codex/.sandbox-bin/codex-command-runner because it is a sandbox helper, not the Codex CLI." +
      ` (${search}).` +
      rejectedNote +
      restartNote });
  }

  throw new RuntimeExecutableNotFoundError({ runtimeId: "codex", message:
    `Cannot resolve a compatible Codex CLI app-server entry point (${search}).` +
    rejectedNote +
    restartNote });
}

export function buildCodexAppServerArgs(managedMcp?: { name: string; url: string } | null): string[] {
  const args = ["app-server"];
  if (managedMcp) {
    args.push("-c", `mcp_servers.${managedMcp.name}.url=${JSON.stringify(managedMcp.url)}`);
  }
  args.push("--listen", "stdio://");
  return args;
}

function isCodexMissingRolloutError(message: string): boolean {
  return /\bno\s+rollout\s+found\b/i.test(message)
    || /\bmissing\s+rollout\b/i.test(message)
    || /\brollout\b.*\b(not found|missing)\b/i.test(message)
    || /\bthread\b.*\b(not found|missing)\b/i.test(message)
    || /\bmissing\s+thread\b/i.test(message);
}

function isCodexThreadWriterBusyError(message: string): boolean {
  return /\bthread\b.*\balready\s+has\s+an\s+active\s+writer\b/i.test(message)
    || /\balready\s+has\s+an\s+active\s+writer\b/i.test(message);
}

// task #14: a thread the user archived in the Codex app refuses resume with
// "session <id> is archived. Run `codex unarchive <id>` ...".
function isCodexThreadArchivedError(message: string): boolean {
  return /\bis\s+archived\b/i.test(message);
}

type CodexRecoverableResumeErrorKind = "missing_rollout" | "thread_writer_busy" | "archived";
type CodexResumeRecoverySource =
  | "codex_resume_missing_rollout"
  | "codex_resume_thread_writer_busy"
  | "codex_resume_archived";

export type CodexResumeErrorClassification =
  | {
      kind: CodexRecoverableResumeErrorKind;
      resumeErrorClass: CodexRecoverableResumeErrorKind;
      recoveryAction: "fallback_fresh_thread";
      telemetry: Extract<ParsedEvent, { kind: "telemetry" }>;
      recovery: Extract<ParsedEvent, { kind: "runtime_recovery" }>;
    }
  | {
      kind: "terminal_error";
      resumeErrorClass: "permission_denied" | "unknown";
      recoveryAction?: undefined;
    };

function codexResumeRecoverySource(kind: CodexRecoverableResumeErrorKind): CodexResumeRecoverySource {
  if (kind === "missing_rollout") return "codex_resume_missing_rollout";
  if (kind === "archived") return "codex_resume_archived";
  return "codex_resume_thread_writer_busy";
}

function codexResumeRecoveryMessage(kind: CodexRecoverableResumeErrorKind): string {
  if (kind === "thread_writer_busy") {
    return "Codex could not resume its previous thread because another writer is active; Raft started a fresh Codex thread.";
  }
  if (kind === "archived") {
    return "Codex could not resume its previous thread because it is archived and could not be unarchived; Raft started a fresh Codex thread.";
  }
  return "Codex could not resume its previous thread; Raft started a fresh Codex thread.";
}

function codexResumeRecoveryDetails(): string {
  return "Use Raft conversation history and local MEMORY.md/notes as the recovery point; do not assume prior Codex thread context is loaded.";
}

function prependCodexRecoveryNotice(prompt: string, recovery: Extract<ParsedEvent, { kind: "runtime_recovery" }>): string {
  return `${recovery.message}\n\n${recovery.details}\n\n${prompt}`;
}

function buildCodexResumeRecovery(
  kind: CodexRecoverableResumeErrorKind,
  requestedSessionId?: string,
): Extract<CodexResumeErrorClassification, { recoveryAction: "fallback_fresh_thread" }> {
  const source = codexResumeRecoverySource(kind);
  return {
    kind,
    resumeErrorClass: kind,
    recoveryAction: "fallback_fresh_thread",
    telemetry: {
      kind: "telemetry",
      name: "recovery",
      source,
      attrs: {
        resume_error_class: kind,
        recovery_action: "fallback_fresh_thread",
      },
    },
    recovery: {
      kind: "runtime_recovery",
      source,
      resumeErrorClass: kind,
      recoveryAction: "fallback_fresh_thread",
      message: codexResumeRecoveryMessage(kind),
      details: codexResumeRecoveryDetails(),
      ...(requestedSessionId ? { requestedSessionId } : {}),
    },
  };
}

export function classifyCodexResumeError(message: string, requestedSessionId?: string): CodexResumeErrorClassification {
  if (isCodexThreadArchivedError(message)) {
    return buildCodexResumeRecovery("archived", requestedSessionId);
  }

  if (isCodexMissingRolloutError(message)) {
    return buildCodexResumeRecovery("missing_rollout", requestedSessionId);
  }

  if (isCodexThreadWriterBusyError(message)) {
    return buildCodexResumeRecovery("thread_writer_busy", requestedSessionId);
  }

  if (/\b(no\s+permission|permission\s+denied|forbidden|unauthorized)\b/i.test(message)) {
    return {
      kind: "terminal_error",
      resumeErrorClass: "permission_denied",
    };
  }

  return {
    kind: "terminal_error",
    resumeErrorClass: "unknown",
  };
}

export function codexThreadName(agentName: string | null | undefined): string | null {
  const trimmed = agentName?.trim();
  return trimmed ? `Raft · ${trimmed}` : null;
}

function hasJsonRpcField(message: JsonRpcMessage, field: "result" | "error"): boolean {
  return Object.prototype.hasOwnProperty.call(message, field);
}

function isJsonRpcResponse(message: JsonRpcMessage): boolean {
  return message.id !== undefined && (hasJsonRpcField(message, "result") || hasJsonRpcField(message, "error"));
}

function isCodexServerRequest(message: JsonRpcMessage): message is JsonRpcMessage & { id: JsonRpcId; method: string } {
  return message.id !== undefined && typeof message.method === "string" && !isJsonRpcResponse(message);
}

function payloadBytes(value: unknown): number | undefined {
  try {
    return Buffer.byteLength(JSON.stringify(value), "utf8");
  } catch {
    return undefined;
  }
}

function extractCodexHome(result: unknown): string | null {
  if (!result || typeof result !== "object") return null;
  const candidate = (result as { codexHome?: unknown }).codexHome;
  return typeof candidate === "string" && candidate.trim().length > 0 ? candidate : null;
}

function isCompatibleInitializeResult(result: unknown): boolean {
  if (!result || typeof result !== "object" || Array.isArray(result)) return false;
  const userAgent = (result as { userAgent?: unknown }).userAgent;
  return typeof userAgent === "string" && userAgent.trim().length > 0;
}

function unsupportedInitializeResultMessage(): string {
  return "Codex app-server initialize response is missing the expected userAgent handshake field; upgrade Codex CLI to a compatible app-server build.";
}

export class CodexDriver implements RuntimeDriver {
  readonly idleRecycle = createCodexIdleRecycle((sessionId) => this.countBackgroundTerminals(sessionId));
  readonly id = "codex";
  readonly lifecycle = {
    kind: "persistent",
    stdin: "direct",
    inFlightWake: "steer",
  } as const;
  readonly communication = {
    chat: "slock_cli",
    runtimeControl: "none",
  } as const;
  readonly stdoutChannel = "structured_protocol" as const;
  readonly session = {
    recovery: "resume_or_fresh",
  } as const;
  readonly model = {
    detectedModelsVerifiedAs: "launchable",
    toLaunchSpec: (modelId: string) => ({ params: { model: modelId } }),
  } as const;
  readonly startupReadiness = "initial_turn" as const;
  readonly requiresSessionInitForDelivery = true as const;
  readonly supportsStdinNotification = true;
  readonly busyDeliveryMode = "direct" as const;
  readonly supportsNativeStandingPrompt = true;

  probe(): RuntimeProbeResult {
    return probeCodex();
  }

  buildThreadRequest(ctx: SpawnContext): { method: "thread/start" | "thread/resume"; params: Record<string, any> } {
    // We pass the standing prompt as `developerInstructions` (additive, injected
    // as a developer-role message per turn) rather than `baseInstructions`
    // (replaces Codex's default base). Resume works because we always re-send
    // on start/resume. Compaction/session-level persistence is less certain
    // than `baseInstructions` and needs follow-up validation.
    const threadParams: Record<string, any> = {
      cwd: ctx.workingDirectory,
      approvalPolicy: "never",
      sandbox: "danger-full-access",
      sandbox_mode: "danger-full-access",
      developerInstructions: ctx.standingPrompt,
      // Raw response items are used only as payload-free liveness signals in
      // the daemon. They replace the previous transcript-mtime heuristic.
      experimentalRawEvents: true,
    };
    const launchRuntimeFields = runtimeConfigToLaunchFields(hydrateRuntimeConfig(ctx.config));
    if (launchRuntimeFields.model) {
      threadParams.model = launchRuntimeFields.model;
    }
    if (launchRuntimeFields.reasoningEffort) {
      threadParams.config = { model_reasoning_effort: launchRuntimeFields.reasoningEffort };
    }
    if (launchRuntimeFields.mode.kind === "fast") {
      threadParams.serviceTier = "fast";
    }

    if (ctx.config.sessionId) {
      return {
        method: "thread/resume",
        params: {
          threadId: ctx.config.sessionId,
          ...threadParams,
          // Resume model context without returning history the daemon does not consume.
          excludeTurns: true,
        },
      };
    }

    return {
      method: "thread/start",
      params: threadParams,
    };
  }

  private process: ChildProcess | null = null;
  private requestId = 0;
  private pendingInitialPrompt: string | null = null;
  private initializeRequestId: JsonRpcId | null = null;
  private pendingThreadRequest: { method: "thread/start" | "thread/resume"; params: Record<string, any> } | null = null;
  private pendingThreadRequestId: JsonRpcId | null = null;
  private pendingThreadRequestMethod: "thread/start" | "thread/resume" | null = null;
  private lastThreadRequestMethod: "thread/start" | "thread/resume" | null = null;
  private pendingResumeFallbackParams: Record<string, any> | null = null;
  private pendingResumeThreadId: string | null = null;
  private pendingResumeParams: Record<string, unknown> | null = null;
  // task #14: one unarchive attempt per spawn; the retried resume (or the
  // fresh-thread fallback when unarchive fails) waits on this request.
  private archivedUnarchiveAttempted = false;
  private pendingUnarchive: {
    requestId: JsonRpcId;
    resumeParams: Record<string, unknown>;
    freshParams: Record<string, unknown>;
    classification: Extract<CodexResumeErrorClassification, { recoveryAction: "fallback_fresh_thread" }>;
  } | null = null;
  // task #14: "Raft · <agent>" so Raft threads are recognisable in the Codex
  // app sidebar; responses are ignored (older app-servers lack the method).
  private threadName: string | null = null;
  private threadNameRequestIds = new Set<JsonRpcId>();
  private pendingInitialTurnRequestId: JsonRpcId | null = null;
  private pendingInitialTurnInput: unknown = null;
  private pendingDeliveryRequests = new Map<JsonRpcId, { method: "turn/start" | "turn/steer"; input: unknown; turnId: string | null }>();
  private pendingBackgroundTerminalRequests = new Map<JsonRpcId, (result: unknown) => void>();
  private initialTurnStarted = false;
  private normalizer = new CodexEventNormalizer();
  private codexHomeRoot: string | null = null;
  private spawnWorkingDirectory: string | null = null;
  private managedMcpServerName: string | null = null;
  private managedMcpReady = true;
  private managedMcpStatus: Extract<ParsedEvent, { kind: "runtime_tooling" }>["managedMcpStatus"] = "not_configured";
  private instructionShapeTracer: Tracer | null = null;
  private instructionShapeIdentityAttrs: Record<string, unknown> = {};
  private instructionShapeConfiguredSessionId: string | null = null;
  private instructionShapeStandingInstructions: unknown = undefined;
  private instructionShapeStaticAttrs: CodexInstructionShapeStaticAttrs | null = null;
  private instructionShapeRequestAttrs: Record<string, string | number | boolean> | null = null;
  private instructionShapeCompactionStarts = 0;
  private instructionShapeCompactionFinishes = 0;
  private instructionShapePostCompactionRequestPending = false;

  get currentRuntimeHomeDir(): string | null {
    return this.codexHomeRoot;
  }

  async spawn(ctx: SpawnContext): Promise<SpawnResult> {
    const { spawnEnv } = await prepareCliTransport(ctx, { NO_COLOR: "1" });
    const managedMcp = await prepareManagedMcpRuntimeProxy({
      agentId: ctx.agentId,
      launchId: ctx.launchId,
      serverUrl: ctx.config.serverUrl,
      agentCredentialKey: ctx.config.agentCredentialKey,
    });

    this.process = null;
    this.requestId = 0;
    this.pendingInitialPrompt = ctx.prompt;
    this.initializeRequestId = null;
    this.pendingThreadRequest = null;
    this.pendingThreadRequestId = null;
    this.pendingThreadRequestMethod = null;
    this.lastThreadRequestMethod = null;
    this.pendingResumeFallbackParams = null;
    this.pendingResumeThreadId = null;
    this.pendingResumeParams = null;
    this.archivedUnarchiveAttempted = false;
    this.pendingUnarchive = null;
    this.threadName = codexThreadName(ctx.config.displayName || ctx.config.name);
    this.threadNameRequestIds.clear();
    this.pendingInitialTurnRequestId = null;
    this.pendingInitialTurnInput = null;
    this.pendingDeliveryRequests.clear();
    for (const resolve of this.pendingBackgroundTerminalRequests.values()) resolve(null);
    this.pendingBackgroundTerminalRequests.clear();
    this.initialTurnStarted = false;
    this.normalizer.reset();
    this.spawnWorkingDirectory = ctx.workingDirectory;
    this.managedMcpServerName = managedMcp?.name ?? null;
    this.managedMcpReady = !managedMcp;
    this.managedMcpStatus = managedMcp ? "pending" : "not_configured";
    this.instructionShapeTracer = ctx.tracer ?? null;
    this.instructionShapeIdentityAttrs = {
      agent_id: ctx.agentId,
      server_id: ctx.config.runtimeContext?.serverId,
      machine_id: ctx.config.runtimeContext?.machineId,
      launch_id: ctx.launchId || undefined,
      process_instance_id: ctx.processInstanceId || undefined,
      runtime: ctx.config.runtime,
      runtime_version: ctx.config.runtimeContext?.daemonVersion,
    };
    this.instructionShapeConfiguredSessionId = ctx.config.sessionId || null;
    this.instructionShapeStandingInstructions = this.instructionShapeTracer
      ? ctx.standingPrompt
      : undefined;
    this.instructionShapeStaticAttrs = null;
    this.instructionShapeRequestAttrs = null;
    this.instructionShapeCompactionStarts = 0;
    this.instructionShapeCompactionFinishes = 0;
    this.instructionShapePostCompactionRequestPending = false;
    this.codexHomeRoot = resolveCodexHomeRootFromEnv(spawnEnv, {
      defaultHomeDir: os.homedir(),
      cwd: ctx.workingDirectory,
    });

    const args = buildCodexAppServerArgs(managedMcp);

    // task #1277: candidate discovery and the app-server probes run as bounded
    // probe children under this start attempt's signal; a cancelled lookup is
    // rethrown, never a verdict, and nothing is written to the probe cache.
    const { command, args: spawnArgs, shell, source, env } = await resolveCodexSpawnAsync(args, { env: spawnEnv, signal: ctx.launchSignal });
    const launchSpan = ctx.tracer?.startSpan("daemon.runtime.node_host_launch", {
      surface: "daemon",
      kind: "internal",
      attrs: {
        agentId: ctx.agentId,
        launchId: ctx.launchId || undefined,
        runtime: this.id,
        candidate_source: source,
        // Reports the host we actually detected. The previous attribute was
        // `exec_path_is_node: !versions.electron`, which inferred "is node"
        // from "is not Electron" — so on the official SEA installs, where
        // execPath is the bundled app and cannot run a JS entry, it reported
        // that the exec path WAS node. The one shape most likely to be broken
        // was the one it called healthy.
        host_kind: detectNodeHostKind(),
        electron_run_as_node: env?.ELECTRON_RUN_AS_NODE === "1",
      },
    });
    // task #365: after the last await, right before the child exists.
    ctx.assertLaunchCurrent?.();
    // A start stopped while the candidate search ran must not create a child.
    if (ctx.launchSignal?.aborted) throw new ProbeChildError("codex launch cancelled before the child was created", { code: "ABORT_ERR" });
    let proc: ChildProcess;
    try {
      proc = spawnRuntimeChild(command, spawnArgs, {
        cwd: ctx.workingDirectory,
        stdio: ["pipe", "pipe", "pipe"],
        env: env ?? spawnEnv,
        shell,
      });
      launchSpan?.end("ok");
    } catch (error) {
      launchSpan?.end("error", {
        attrs: { error_class: error instanceof Error ? error.name : "Error" },
      });
      throw error;
    }

    this.process = proc;

    queueMicrotask(() => {
      this.initializeRequestId = this.sendRequest("initialize", {
        clientInfo: { name: "slock-daemon", version: "1.0.0" },
        capabilities: { experimentalApi: true },
      });

      this.pendingThreadRequest = this.buildThreadRequest(ctx);
    });

    return { process: proc };
  }

  parseLine(line: string): ParsedEvent[] {
    const message = parseCodexJsonRpcLine(line);
    if (!message) {
      return [];
    }

    const events: ParsedEvent[] = [];
    if (isCodexServerRequest(message)) {
      this.sendErrorResponse(
        message.id,
        `Codex app-server request "${message.method}" is not supported by the non-interactive Raft daemon`,
      );
      return events;
    }

    const isResponse = isJsonRpcResponse(message);
    if (isResponse && message.id !== undefined) {
      const resolve = this.pendingBackgroundTerminalRequests.get(message.id);
      if (resolve) {
        this.pendingBackgroundTerminalRequests.delete(message.id);
        resolve(hasJsonRpcField(message, "result") ? message.result : null);
        return events;
      }
    }
    if (isResponse && hasJsonRpcField(message, "result")) {
      if (message.id === this.initializeRequestId) {
        if (!isCompatibleInitializeResult(message.result)) {
          this.initializeRequestId = null;
          this.pendingThreadRequest = null;
          this.pendingThreadRequestId = null;
          this.pendingThreadRequestMethod = null;
          this.pendingResumeFallbackParams = null;
          this.pendingResumeThreadId = null;
          this.instructionShapeStandingInstructions = undefined;
          events.push({
            kind: "error",
            message: unsupportedInitializeResultMessage(),
            startupRequestMethod: "initialize",
          });
          return events;
        }
        const codexHome = extractCodexHome(message.result);
        if (codexHome) {
          this.codexHomeRoot = path.resolve(this.spawnWorkingDirectory ?? process.cwd(), codexHome);
        }
        this.initializeRequestId = null;
        this.sendNotification("initialized", {});
        if (this.pendingThreadRequest) {
          const appServerUserAgent = (message.result as { userAgent?: unknown }).userAgent;
          this.sendThreadRequest(
            this.pendingThreadRequest.method,
            this.pendingThreadRequest.params,
            appServerUserAgent,
          );
          this.pendingThreadRequest = null;
        }
        return events;
      }
    }

    if (isResponse && hasJsonRpcField(message, "error") && message.id === this.initializeRequestId) {
      this.initializeRequestId = null;
      this.pendingThreadRequest = null;
      this.pendingThreadRequestId = null;
      this.pendingThreadRequestMethod = null;
      this.pendingResumeFallbackParams = null;
      this.pendingResumeThreadId = null;
      this.instructionShapeStandingInstructions = undefined;
      events.push({
        kind: "error",
        message: message.error?.message || "Codex app-server request failed",
        startupRequestMethod: "initialize",
      });
      return events;
    }

    if (isResponse && message.id !== undefined && this.threadNameRequestIds.delete(message.id)) {
      return events;
    }

    if (isResponse && this.pendingUnarchive && message.id === this.pendingUnarchive.requestId) {
      const { resumeParams, freshParams, classification } = this.pendingUnarchive;
      this.pendingUnarchive = null;
      if (hasJsonRpcField(message, "error")) {
        events.push(classification.telemetry);
        events.push(classification.recovery);
        if (this.pendingInitialPrompt) {
          this.pendingInitialPrompt = prependCodexRecoveryNotice(this.pendingInitialPrompt, classification.recovery);
        }
        this.sendThreadRequest("thread/start", freshParams);
        return events;
      }
      events.push({
        kind: "telemetry",
        name: "recovery",
        source: "codex_resume_unarchived",
        attrs: {
          resume_error_class: "archived",
          recovery_action: "unarchive_resume",
        },
      });
      this.sendThreadRequest("thread/resume", resumeParams);
      return events;
    }

    if (isResponse && message.id === this.pendingThreadRequestId) {
      if (hasJsonRpcField(message, "error")) {
        const errorMessage = message.error?.message || "Codex app-server request failed";
        const requestMethod = this.pendingThreadRequestMethod;
        const resumeErrorClassification = requestMethod === "thread/resume"
          ? classifyCodexResumeError(errorMessage, this.pendingResumeThreadId || undefined)
          : null;
        if (
          resumeErrorClassification?.kind === "archived"
          && resumeErrorClassification.recoveryAction === "fallback_fresh_thread"
          && !this.archivedUnarchiveAttempted
          && this.pendingResumeThreadId
          && this.pendingResumeParams
          && this.pendingResumeFallbackParams
        ) {
          // task #14: the user archived this thread in the Codex app. Unarchive
          // and resume it so the agent keeps its context (the thread reappears
          // in the sidebar); start fresh only if unarchive fails.
          this.archivedUnarchiveAttempted = true;
          this.pendingUnarchive = {
            requestId: this.sendRequest("thread/unarchive", { threadId: this.pendingResumeThreadId }),
            resumeParams: this.pendingResumeParams,
            freshParams: this.pendingResumeFallbackParams,
            classification: resumeErrorClassification,
          };
          this.pendingThreadRequestId = null;
          return events;
        }
        if (
          this.pendingResumeFallbackParams &&
          resumeErrorClassification?.recoveryAction === "fallback_fresh_thread"
        ) {
          events.push(resumeErrorClassification.telemetry);
          events.push(resumeErrorClassification.recovery);
          if (this.pendingInitialPrompt) {
            this.pendingInitialPrompt = prependCodexRecoveryNotice(
              this.pendingInitialPrompt,
              resumeErrorClassification.recovery,
            );
          }
          this.sendThreadRequest("thread/start", this.pendingResumeFallbackParams);
          this.pendingResumeFallbackParams = null;
          return events;
        }

        this.pendingThreadRequestId = null;
        this.pendingThreadRequestMethod = null;
        this.pendingResumeFallbackParams = null;
        this.pendingResumeThreadId = null;
        events.push(requestMethod
          ? { kind: "error", message: errorMessage, startupRequestMethod: requestMethod }
          : { kind: "error", message: errorMessage });
        return events;
      }

      const requestMethod = this.pendingThreadRequestMethod;
      this.pendingThreadRequestId = null;
      this.pendingThreadRequestMethod = null;
      this.pendingResumeFallbackParams = null;
      this.pendingResumeThreadId = null;
      this.pendingResumeParams = null;
      if (requestMethod) {
        events.push(this.runtimeToolingObservation(requestMethod));
      }
      this.nameThreadIfUnnamed(message.result);
    }

    if (isResponse && message.id === this.pendingInitialTurnRequestId) {
      this.pendingInitialTurnRequestId = null;
      if (hasJsonRpcField(message, "error")) {
        events.push({
          kind: "error",
          message: message.error?.message || "Codex app-server request failed",
          startupRequestMethod: "turn/start",
        });
      } else {
        if (hasNonEmptyCodexTextInput(this.pendingInitialTurnInput)) {
          this.normalizer.markNonEmptyTurnInput(resultTurnId(message));
        }
        this.pendingInitialPrompt = null;
      }
      this.pendingInitialTurnInput = null;
      return events;
    }

    if (isResponse && message.id !== undefined && this.pendingDeliveryRequests.has(message.id)) {
      const deliveryRequest = this.pendingDeliveryRequests.get(message.id)!;
      this.pendingDeliveryRequests.delete(message.id);
      if (hasJsonRpcField(message, "error")) {
        const params = message.error ?? {};
        events.push({
          kind: "delivery_error",
          message: message.error?.message || "Codex app-server request failed",
          requestMethod: deliveryRequest.method,
          source: "codex_app_server_response",
          payloadBytes: payloadBytes(params),
        });
      } else if (hasNonEmptyCodexTextInput(deliveryRequest.input)) {
        this.normalizer.markNonEmptyTurnInput(deliveryRequest.turnId ?? resultTurnId(message));
      }
      return events;
    }

    if (
      message.method === "mcpServer/startupStatus/updated"
      && message.params?.name === this.managedMcpServerName
    ) {
      const status = message.params?.status;
      if (status === "ready") {
        this.managedMcpReady = true;
        this.managedMcpStatus = "ready";
        if (this.lastThreadRequestMethod) {
          events.push(this.runtimeToolingObservation(this.lastThreadRequestMethod));
        }
        this.startInitialTurn();
      } else if (status === "failed" || status === "cancelled") {
        this.managedMcpReady = true;
        this.managedMcpStatus = "failed";
        if (this.lastThreadRequestMethod) {
          events.push(this.runtimeToolingObservation(this.lastThreadRequestMethod));
        }
        this.startInitialTurn();
      }
      return events;
    }

    const result = this.normalizer.normalizeMessage(message);
    if (result.turnStarted) {
      this.pendingInitialPrompt = null;
    }
    if (result.threadReady) {
      this.startInitialTurn();
    }
    this.observeInstructionShapeCompactionEvents(result.events);
    return [...events, ...result.events];
  }

  get currentSessionId(): string | null {
    return this.normalizer.threadId;
  }

  get hasLiveTurn(): boolean {
    return this.normalizer.hasLiveTurn;
  }

  encodeStdinMessage(
    text: string,
    _sessionId: string | null,
    opts?: { mode?: "idle" | "busy" },
  ): string | null {
    if (!this.normalizer.threadId) return null;

    const mode = opts?.mode || "busy";
    if (mode === "busy") {
      if (!this.normalizer.canSteerBusy) return null;
      const id = this.nextRequestId();
      const input = [{ type: "text", text }];
      const turnId = this.normalizer.activeTurnId;
      this.pendingDeliveryRequests.set(id, { method: "turn/steer", input, turnId });
      return JSON.stringify({
        jsonrpc: "2.0",
        id,
        method: "turn/steer",
        params: {
          threadId: this.normalizer.threadId,
          expectedTurnId: this.normalizer.activeTurnId,
          input,
        },
      });
    }

    const id = this.nextRequestId();
    const input = [{ type: "text", text }];
    this.pendingDeliveryRequests.set(id, { method: "turn/start", input, turnId: null });
    const encoded = JSON.stringify({
      jsonrpc: "2.0",
      id,
      method: "turn/start",
      params: {
        threadId: this.normalizer.threadId,
        input,
      },
    });
    this.observePostCompactionFirstRequest("turn/start");
    return encoded;
  }

  buildSystemPrompt(config: AgentConfig, _agentId: string): AxSurfaceText {
    return buildCliTransportSystemPrompt(config, {
      extraCriticalRules: [],
    });
  }

  private nextRequestId(): number {
    this.requestId += 1;
    return this.requestId;
  }

  private startInitialTurn() {
    if (!this.managedMcpReady || this.initialTurnStarted || this.pendingInitialTurnRequestId !== null || !this.pendingInitialPrompt || !this.normalizer.threadId) return;
    this.initialTurnStarted = true;
    const prompt = this.pendingInitialPrompt;
    const input = [{ type: "text", text: prompt }];
    this.pendingInitialTurnInput = input;
    this.pendingInitialTurnRequestId = this.sendRequest("turn/start", {
      threadId: this.normalizer.threadId,
      input,
    });
  }

  /** Experimental app-server API. An incomplete or unrecognised reply is
   * unknown, not permission to stop the process. */
  async countBackgroundTerminals(expectedThreadId?: string): Promise<number | null> {
    const threadId = this.normalizer.threadId;
    const process = this.process;
    if (!threadId || (expectedThreadId && threadId !== expectedThreadId) || !process?.stdin || process.stdin.destroyed || process.exitCode !== null) return null;
    let cursor: string | null = null;
    let count = 0;
    const seen = new Set<string>();
    for (let page = 0; page < 100; page++) {
      if (this.process !== process || this.normalizer.threadId !== threadId) return null;
      const result = await new Promise<unknown>((resolve) => {
        let requestId: JsonRpcId;
        try {
          requestId = this.sendRequest("thread/backgroundTerminals/list", { threadId, ...(cursor ? { cursor } : {}) });
        } catch { resolve(null); return; }
        const timer = setTimeout(() => { this.pendingBackgroundTerminalRequests.delete(requestId); resolve(null); }, 3_000);
        timer.unref?.();
        this.pendingBackgroundTerminalRequests.set(requestId, (value) => { clearTimeout(timer); resolve(value); });
      });
      if (this.process !== process || this.normalizer.threadId !== threadId || !result || typeof result !== "object" || Array.isArray(result)) return null;
      const response = result as Record<string, unknown>;
      if (!Array.isArray(response.data)) return null;
      for (const entry of response.data) {
        if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
        const item = entry as Record<string, unknown>;
        if (typeof item.processId !== "string" || !item.processId || typeof item.itemId !== "string" || typeof item.command !== "string" || typeof item.cwd !== "string") return null;
        count++;
      }
      const next = response.nextCursor;
      if (next == null) return count;
      if (typeof next !== "string" || !next || seen.has(next)) return null;
      seen.add(next);
      cursor = next;
    }
    return null;
  }

  private sendRequest(method: string, params: Record<string, any>): JsonRpcId {
    const id = this.nextRequestId();
    this.process?.stdin?.write(JSON.stringify({
      jsonrpc: "2.0",
      id,
      method,
      params,
    }) + "\n");
    this.observePostCompactionFirstRequest(method);
    return id;
  }

  /** Names the thread unless it already has a name (the user's rename wins). */
  private nameThreadIfUnnamed(result: unknown): void {
    if (!this.threadName) return;
    const thread = (result as { thread?: { id?: unknown; name?: unknown } } | null)?.thread;
    const threadId = typeof thread?.id === "string" ? thread.id : null;
    if (!threadId) return;
    if (typeof thread?.name === "string" && thread.name.trim()) return;
    this.threadNameRequestIds.add(this.sendRequest("thread/name/set", { threadId, name: this.threadName }));
  }

  private sendErrorResponse(id: JsonRpcId, message: string) {
    this.process?.stdin?.write(JSON.stringify({
      jsonrpc: "2.0",
      id,
      error: {
        code: -32601,
        message,
      },
    }) + "\n");
  }

  private sendThreadRequest(
    method: CodexThreadRequestMethod,
    params: Record<string, any>,
    appServerUserAgent?: unknown,
  ): JsonRpcId {
    const id = this.sendRequest(method, params);
    this.recordInstructionShape(
      "thread_request_sent",
      method,
      { params, appServerUserAgent },
    );
    this.pendingThreadRequestId = id;
    this.pendingThreadRequestMethod = method;
    this.lastThreadRequestMethod = method;
    this.pendingResumeFallbackParams = null;
    this.pendingResumeThreadId = null;
    this.pendingResumeParams = null;
    if (method === "thread/resume") {
      this.pendingResumeParams = params;
      // excludeTurns only applies to resume; do not forward it to thread/start.
      const { threadId: _threadId, excludeTurns: _excludeTurns, ...freshParams } = params;
      this.pendingResumeFallbackParams = freshParams;
      this.pendingResumeThreadId = typeof _threadId === "string" && _threadId.trim()
        ? _threadId.trim()
        : null;
    }
    return id;
  }

  private recordInstructionShape(
    observationPhase: CodexInstructionObservationPhase,
    requestMethod: CodexThreadRequestMethod,
    fresh?: { params: Record<string, unknown>; appServerUserAgent?: unknown },
  ): void {
    if (!this.instructionShapeTracer) return;

    if (fresh) {
      if (!this.instructionShapeStaticAttrs) {
        this.instructionShapeStaticAttrs = buildCodexInstructionShapeStaticAttrs({
          standingInstructions: this.instructionShapeStandingInstructions,
          appServerUserAgent: fresh.appServerUserAgent,
        });
        this.instructionShapeStandingInstructions = undefined;
      }
      this.instructionShapeRequestAttrs = buildCodexInstructionShapeAttrs({
        staticAttrs: this.instructionShapeStaticAttrs,
        requestParams: fresh.params,
        requestMethod,
        observationPhase,
        compactionStarts: this.instructionShapeCompactionStarts,
        compactionFinishes: this.instructionShapeCompactionFinishes,
      });
    }
    if (!this.instructionShapeRequestAttrs) return;

    const sessionId = this.normalizer.threadId || this.instructionShapeConfiguredSessionId;
    this.instructionShapeTracer.emitEvent("daemon.codex.request_instruction_shape", {
      surface: "daemon",
      attrs: {
        ...this.instructionShapeIdentityAttrs,
        session_id: sessionId || undefined,
        session_id_present: Boolean(sessionId),
        ...this.instructionShapeRequestAttrs,
        observation_phase: observationPhase,
        session_request_method: requestMethod,
        compaction_starts_count: this.instructionShapeCompactionStarts,
        compaction_finishes_count: this.instructionShapeCompactionFinishes,
        status: "ok",
      },
    });
  }

  private observeInstructionShapeCompactionEvents(events: ParsedEvent[]): void {
    if (!this.lastThreadRequestMethod) return;
    for (const event of events) {
      if (event.kind === "compaction_started") {
        this.instructionShapeCompactionStarts += 1;
        this.recordInstructionShape("compaction_started", this.lastThreadRequestMethod);
      } else if (event.kind === "compaction_finished") {
        this.instructionShapeCompactionFinishes += 1;
        this.instructionShapePostCompactionRequestPending = true;
        this.recordInstructionShape("compaction_finished", this.lastThreadRequestMethod);
      }
    }
  }

  private observePostCompactionFirstRequest(method: string): void {
    if (
      method !== "turn/start"
      || !this.instructionShapePostCompactionRequestPending
      || !this.lastThreadRequestMethod
    ) return;
    this.instructionShapePostCompactionRequestPending = false;
    this.recordInstructionShape("post_compaction_first_request", this.lastThreadRequestMethod);
  }

  private runtimeToolingObservation(
    sessionRequestMethod: "thread/start" | "thread/resume",
  ): Extract<ParsedEvent, { kind: "runtime_tooling" }> {
    return {
      kind: "runtime_tooling",
      source: "codex_app_server",
      sessionRequestMethod,
      // The current app-server handshake does not report the built-in tool
      // inventory. Preserve that negative fact instead of inferring exposure
      // from a successful resume or from later model behavior.
      nativeToolInventoryObservation: "unreported_by_app_server",
      cliTransportConfigured: true,
      managedMcpConfigured: this.managedMcpServerName !== null,
      managedMcpStatus: this.managedMcpStatus,
    };
  }

  private sendNotification(method: string, params: Record<string, any>) {
    this.process?.stdin?.write(JSON.stringify({
      jsonrpc: "2.0",
      method,
      params,
    }) + "\n");
  }

  async detectModels(): Promise<RuntimeModelSourceOutcome> {
    return runtimeModelSourceOutcomeFromSet(
      await detectCodexModelsFromAppServer() ?? detectCodexModels(resolveCodexHomeRootFromEnv()),
    );
  }
}

interface CodexModelListDetectionOptions {
  env?: NodeJS.ProcessEnv;
  cwd?: string;
  timeoutMs?: number;
  /** task #1277 — probe seams and the operation-scoped cancel (`signal`) for the candidate search. */
  deps?: ProbeDeps;
}

function asNonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function modelListPage(result: unknown): { entries: unknown[]; nextCursor: string | null } | null {
  if (!result || typeof result !== "object") return null;
  const object = result as { data?: unknown; models?: unknown; nextCursor?: unknown };
  const entries = Array.isArray(object.data)
    ? object.data
    : Array.isArray(object.models)
      ? object.models
      : null;
  if (!entries) return null;
  return {
    entries,
    nextCursor: asNonEmptyString(object.nextCursor),
  };
}

function modelListRequestParams(cursor: string | null): Record<string, unknown> {
  return cursor ? { cursor } : {};
}

function stringArray(values: unknown[], read: (value: unknown) => string | null): string[] {
  const result: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const item = read(value);
    if (!item || seen.has(item)) continue;
    seen.add(item);
    result.push(item);
  }
  return result;
}

function codexReasoningEfforts(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return stringArray(value, (entry) => {
    if (typeof entry === "string") return asNonEmptyString(entry);
    if (!entry || typeof entry !== "object") return null;
    const object = entry as { reasoningEffort?: unknown; id?: unknown };
    return asNonEmptyString(object.reasoningEffort) ?? asNonEmptyString(object.id);
  });
}

function codexServiceTierIds(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return stringArray(value, (entry) => {
    if (typeof entry === "string") return asNonEmptyString(entry);
    if (!entry || typeof entry !== "object") return null;
    const object = entry as { id?: unknown; serviceTier?: unknown };
    return asNonEmptyString(object.id) ?? asNonEmptyString(object.serviceTier);
  });
}

function codexModelInfoFromAppServer(entry: unknown): RuntimeModelInfo | null {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
  const object = entry as Record<string, unknown>;
  if (object.hidden === true) return null;

  const id = asNonEmptyString(object.id)
    ?? asNonEmptyString(object.model)
    ?? asNonEmptyString(object.slug);
  if (!id) return null;

  const label = asNonEmptyString(object.displayName)
    ?? asNonEmptyString(object.display_name)
    ?? asNonEmptyString(object.label)
    ?? id;
  const supportedReasoningEfforts = codexReasoningEfforts(object.supportedReasoningEfforts);
  const serviceTiers = codexServiceTierIds(object.serviceTiers);
  const additionalSpeedTiers = codexServiceTierIds(object.additionalSpeedTiers);
  const runtimeServiceTiers = serviceTiers.length > 0 ? serviceTiers : additionalSpeedTiers;
  const defaultReasoningEffort = asNonEmptyString(object.defaultReasoningEffort);
  const defaultServiceTier = asNonEmptyString(object.defaultServiceTier);

  return {
    id,
    label,
    verified: "launchable",
    ...(supportedReasoningEfforts.length > 0 ? { supportedReasoningEfforts } : {}),
    ...(defaultReasoningEffort ? { defaultReasoningEffort } : {}),
    ...(runtimeServiceTiers.length > 0 ? { serviceTiers: runtimeServiceTiers } : {}),
    ...(defaultServiceTier ? { defaultServiceTier } : {}),
  };
}

function codexModelSetFromAppServerEntries(entries: unknown[]): RuntimeModelSet | null {
  const models: RuntimeModelInfo[] = [];
  let defaultModel: string | undefined;

  for (const entry of entries) {
    const model = codexModelInfoFromAppServer(entry);
    if (!model) continue;
    models.push(model);
    if (!defaultModel && typeof entry === "object" && entry && (entry as { isDefault?: unknown }).isDefault === true) {
      defaultModel = model.id;
    }
  }

  return models.length > 0 ? { models, default: defaultModel } : null;
}

// ── task #1277: asynchronous twins of the candidate discovery above ─────────
//
// Used by the form's model detection (detectCodexModelsFromAppServer): the
// same candidates, arbitration, probe cache and rejection texts as the
// synchronous resolver, with every child (`npm root -g`, the PATH lookups,
// the app-server probe, the --version read) run through runProbeChild instead
// of execFileSync. The synchronous resolver stays for the launch path and the
// detection child until their own PRs.

async function resolveWindowsNpmCodexEntryAsync(deps: ProbeDeps = {}): Promise<{ entry: string | null; note?: string }> {
  const existsSyncFn = deps.existsSyncFn ?? existsSync;
  const runChild = deps.runProbeChildFn ?? runProbeChild;
  const env = deps.env ?? ambientProcessEnv();
  const winPath = path.win32;
  let note: string | undefined;

  try {
    const globalRoot = (await runChild("npm", ["root", "-g"], {
      env,
      timeoutMs: CODEX_NPM_ROOT_TIMEOUT_MS,
      stderr: "ignore",
      platform: deps.platform ?? process.platform,
      signal: deps.signal,
    })).stdout.trim();
    const candidate = winPath.join(globalRoot, "@openai", "codex", "bin", "codex.js");
    if (existsSyncFn(candidate)) return { entry: candidate };
  } catch (error) {
    // A cancelled discovery stops here, with the runner's cleanup evidence
    // intact (an unconfirmed cleanup must not be replaced by a later, cleaner
    // error); nothing below runs on its behalf.
    if (error instanceof ProbeChildError && error.code === "ABORT_ERR") throw error;
    if ((error as { code?: unknown }).code === "ETIMEDOUT") {
      note = `npm_global discovery: \`npm root -g\` did not finish within ${CODEX_NPM_ROOT_TIMEOUT_MS} ms, so the global npm install was not searched and a Codex installed only there is not ruled out`;
    }
  }

  const cmdPath = await resolveCommandOnPathAsync("codex", deps);
  if (cmdPath) {
    const candidate = winPath.join(winPath.dirname(cmdPath), "node_modules", "@openai", "codex", "bin", "codex.js");
    if (existsSyncFn(candidate)) return { entry: candidate, ...(note ? { note } : {}) };
  }

  return { entry: null, ...(note ? { note } : {}) };
}

async function windowsDirectCodexCandidateAsync(
  source: CodexSpawnCandidate["source"],
  command: string,
  deps: ProbeDeps,
): Promise<CodexSpawnCandidate | string> {
  try {
    const launch = await resolveWindowsDirectLaunchAsync("codex", command, [], deps);
    return {
      source,
      command: launch.command,
      argsPrefix: launch.args,
      shell: false,
      ...(launch.env ? { env: launch.env } : {}),
    };
  } catch (error) {
    if (!(error instanceof RuntimeExecutableNotFoundError)) throw error;
    return `${source} batch wrapper rejected: ${error.reason ?? "unresolved"}`;
  }
}

async function resolveExplicitCodexBinAsync(deps: ProbeDeps): Promise<ExplicitCodexBinResolution> {
  const env = deps.env ?? ambientProcessEnv();
  const raw = env[CODEX_BIN_ENV]?.trim();
  if (!raw) return { status: "unset" };

  const platform = deps.platform ?? process.platform;
  const existsSyncFn = deps.existsSyncFn ?? existsSync;

  let command: string | null = null;
  if (path.isAbsolute(raw) || /^[A-Za-z]:[\\/]/.test(raw) || raw.startsWith("\\\\")) {
    command = existsSyncFn(raw) ? raw : null;
    if (!command) return { status: "invalid", raw, reason: "path does not exist" };
  } else {
    command = await resolveCommandOnPathAsync(raw, deps);
    if (!command) return { status: "invalid", raw, reason: "not found on PATH" };
  }

  if (platform === "win32" && isWindowsSandboxRunner(command)) {
    return { status: "invalid", raw, reason: "points at codex-command-runner sandbox helper, not the Codex CLI" };
  }

  if (platform === "win32") {
    const direct = await windowsDirectCodexCandidateAsync("explicit_bin", command, deps);
    if (typeof direct === "string") return { status: "invalid", raw, reason: direct };
    return { status: "resolved", candidate: direct };
  }

  return { status: "resolved", candidate: { source: "explicit_bin", command, argsPrefix: [], shell: false } };
}

async function codexSpawnCandidatesAsync(deps: ProbeDeps = {}): Promise<CodexSpawnCandidateDiscovery> {
  const platform = deps.platform ?? process.platform;
  if (platform === "win32") {
    const candidates: CodexSpawnCandidate[] = [];
    const rejected: string[] = [];

    const npm = await resolveWindowsNpmCodexEntryAsync(deps);
    const notes = npm.note ? [npm.note] : [];
    const npmEntry = npm.entry;
    if (npmEntry) {
      try {
        const nodeHost = resolveNodeHostLaunch({
          env: deps.env ?? ambientProcessEnv(),
          execPath: deps.execPath,
          execIsElectron: deps.execIsElectron,
          execIsSea: deps.execIsSea,
          hasNodeRuntime: deps.hasNodeRuntime,
        });
        candidates.push({ source: "npm_global", command: nodeHost.command, argsPrefix: [npmEntry], shell: false, env: nodeHost.env });
      } catch (error) {
        if (!(error instanceof NodeHostUnavailableError)) throw error;
        rejected.push(`npm_global JavaScript entry rejected: ${error.kind} (host_kind=${error.hostKind})`);
      }
    }

    const { command, missReason: pathMissReason } = await lookupCommandOnPathAsync("codex", deps);
    if (command && !isWindowsSandboxRunner(command)) {
      const direct = await windowsDirectCodexCandidateAsync("path", command, deps);
      if (typeof direct === "string") rejected.push(direct);
      else candidates.push(direct);
    }

    const desktopEntry = resolveWindowsCodexDesktopEntry(deps);
    if (desktopEntry && !candidates.some((candidate) => candidate.command === desktopEntry)) {
      candidates.push({ source: "desktop_install", command: desktopEntry, argsPrefix: [], shell: false });
    }

    return { candidates, rejected, notes, ...(pathMissReason ? { pathMissReason } : {}) };
  }

  const candidates: CodexSpawnCandidate[] = [];
  const { command: pathCommand, missReason: pathMissReason } = await lookupCommandOnPathAsync("codex", deps);
  if (pathCommand) candidates.push({ source: "path", command: pathCommand, argsPrefix: [], shell: false });

  if (platform === "darwin") {
    const existsSyncFn = deps.existsSyncFn ?? existsSync;
    for (const bundlePath of darwinDesktopCodexPaths(deps)) {
      if (!existsSyncFn(bundlePath)) continue;
      if (candidates.some((candidate) => candidate.command === bundlePath)) continue;
      candidates.push({ source: "desktop_bundle", command: bundlePath, argsPrefix: [], shell: false });
    }
  }

  return { candidates, rejected: [], notes: [], ...(pathMissReason ? { pathMissReason } : {}) };
}

/** Shares `codexProbeCache` with the synchronous probe: the two can never disagree on a cached verdict. */
async function probeCodexCandidateAsync(
  candidate: CodexSpawnCandidate,
  deps: ProbeDeps = {},
): Promise<{ failure: string | null; version: string | null }> {
  const runChild = deps.runProbeChildFn ?? runProbeChild;
  const platform = deps.platform ?? process.platform;
  const env = await withWindowsUserEnvironmentAsync(candidate.env ?? deps.env ?? ambientProcessEnv(), deps);
  const stat = readCandidateStat(candidate, deps);
  const cacheKey = codexCandidateCacheKey(candidate);
  if (stat) {
    const cached = codexProbeCache.get(cacheKey);
    if (cached && cached.mtimeMs === stat.mtimeMs && cached.size === stat.size) {
      return { failure: cached.failure, version: cached.version };
    }
  }

  let failure: string | null = null;
  try {
    await runChild(candidate.command, [...candidate.argsPrefix, ...CODEX_APP_SERVER_PROBE_ARGS], {
      env,
      timeoutMs: 5000,
      shell: candidate.shell,
      platform,
      signal: deps.signal,
    });
  } catch (error) {
    // A cancelled probe is not a verdict about the candidate: it must not be
    // classified, and above all not written to the shared cache, where it
    // would reject this candidate for every later resolution.
    if (error instanceof ProbeChildError && error.code === "ABORT_ERR") throw error;
    failure = describeCodexProbeFailure(error);
  }

  let version: string | null = null;
  if (!failure) {
    try {
      const { stdout } = await runChild(candidate.command, [...candidate.argsPrefix, "--version"], {
        env,
        timeoutMs: 5000,
        shell: candidate.shell,
        platform,
        signal: deps.signal,
      });
      version = stdout.trim().split(/\r?\n/)[0] || null;
    } catch (error) {
      // Same rule for the version read: a cancel is not "unknown version".
      if (error instanceof ProbeChildError && error.code === "ABORT_ERR") throw error;
      version = null;
    }
  }

  if (stat) {
    codexProbeCache.set(cacheKey, { mtimeMs: stat.mtimeMs, size: stat.size, appServerOk: failure === null, version, failure });
  }
  return { failure, version };
}

async function resolveCompatibleCodexCandidateAsync(deps: ProbeDeps = {}): Promise<ReturnType<typeof resolveCompatibleCodexCandidate>> {
  const rejected: string[] = [];
  const notes: string[] = [];

  const explicit = await resolveExplicitCodexBinAsync(deps);
  if (explicit.status === "invalid") {
    return { candidate: null, rejected: [`${CODEX_BIN_ENV}=${explicit.raw} rejected: ${explicit.reason}`], version: null, notes: [], explicitOverrideFailed: true };
  }
  if (explicit.status === "resolved") {
    const { failure, version } = await probeCodexCandidateAsync(explicit.candidate, deps);
    if (failure) {
      return {
        candidate: null,
        rejected: [`${CODEX_BIN_ENV}=${formatCodexCandidate(explicit.candidate)} rejected: app-server probe ${failure}`],
        version: null,
        notes: [],
        explicitOverrideFailed: true,
      };
    }
    notes.push(
      `using ${CODEX_BIN_ENV} override ${formatCodexCandidate(explicit.candidate)}` +
      (version ? ` (${version})` : "") +
      "; automatic PATH/desktop discovery skipped",
    );
    return { candidate: explicit.candidate, rejected: [], version, notes, explicitOverrideFailed: false };
  }

  let best: { candidate: CodexSpawnCandidate; version: string | null } | null = null;
  const discovery = await codexSpawnCandidatesAsync(deps);
  rejected.push(...discovery.rejected);
  notes.push(...discovery.notes);
  // Candidates are probed one after another, as the synchronous resolver does,
  // so version arbitration sees them in the same order.
  for (const candidate of discovery.candidates) {
    const { failure, version } = await probeCodexCandidateAsync(candidate, deps);
    if (failure) {
      rejected.push(`${candidate.source} ${formatCodexCandidate(candidate)} rejected: app-server probe ${failure}`);
      continue;
    }
    if (!best) {
      best = { candidate, version };
      continue;
    }
    const cmp = compareCodexVersions(version, best.version);
    if (cmp !== null && cmp > 0) {
      notes.push(
        `selected ${formatCodexCandidate(candidate)} (${version ?? "unknown"}) over ` +
        `${formatCodexCandidate(best.candidate)} (${best.version ?? "unknown"}) by version`,
      );
      best = { candidate, version };
    }
  }

  return {
    candidate: best?.candidate ?? null,
    rejected,
    version: best?.version ?? null,
    notes,
    explicitOverrideFailed: false,
    ...(discovery.pathMissReason ? { pathMissReason: discovery.pathMissReason } : {}),
  };
}

/** task #1277 — {@link resolveCodexSpawn} off the event loop; same selection and the same error texts. */
export async function resolveCodexSpawnAsync(commandArgs: string[], deps: ProbeDeps = {}): Promise<ReturnType<typeof resolveCodexSpawn>> {
  const { candidate, rejected, explicitOverrideFailed } = await resolveCompatibleCodexCandidateAsync(deps);
  if (candidate) {
    return {
      command: candidate.command,
      args: [...candidate.argsPrefix, ...commandArgs],
      shell: candidate.shell,
      source: candidate.source,
      ...(candidate.env ? { env: candidate.env } : {}),
    };
  }

  const search = describeSearchPath(deps);
  const rejectedNote = rejected.length > 0 ? ` Rejected candidates: ${rejected.join("; ")}.` : "";
  const restartNote = " Restart the Raft daemon after changing PATH or installing Codex so the new environment is picked up.";

  if (explicitOverrideFailed) {
    throw new Error(
      `${CODEX_BIN_ENV} is set but does not resolve to a usable Codex CLI app-server entry point.` +
      rejectedNote +
      ` (${search}).` +
      " Fix or unset CODEX_BIN; automatic PATH/desktop discovery is not used while the override is set." +
      restartNote,
    );
  }

  if ((deps.platform ?? process.platform) === "win32") {
    const wrapperReason = rejected.map((note) => /batch wrapper rejected: (\w+)/.exec(note)?.[1]).find(Boolean);
    throw new RuntimeExecutableNotFoundError({ runtimeId: "codex", reason: wrapperReason, message:
      "Cannot resolve a compatible Codex CLI app-server entry point on Windows. " +
      "Install Codex Desktop or install @openai/codex globally via npm (npm i -g @openai/codex). " +
      "Ignoring .codex/.sandbox-bin/codex-command-runner because it is a sandbox helper, not the Codex CLI." +
      ` (${search}).` +
      rejectedNote +
      restartNote });
  }

  throw new RuntimeExecutableNotFoundError({ runtimeId: "codex", message:
    `Cannot resolve a compatible Codex CLI app-server entry point (${search}).` +
    rejectedNote +
    restartNote });
}

export async function detectCodexModelsFromAppServer(
  options: CodexModelListDetectionOptions = {},
): Promise<RuntimeModelSet | null> {
  const env = options.env ?? ambientProcessEnv();
  const signal = options.deps?.signal;
  let launch: { command: string; args: string[]; shell: boolean; env?: NodeJS.ProcessEnv };
  try {
    // task #1277: the candidate search and probes run as bounded probe
    // children, so a form request no longer blocks the daemon on them.
    launch = await resolveCodexSpawnAsync(["app-server", "--listen", "stdio://"], { ...options.deps, env });
  } catch (error) {
    // A cancelled detection is not a verdict: the abort travels up unchanged.
    if (error instanceof ProbeChildError && error.code === "ABORT_ERR") throw error;
    return null;
  }
  if (signal?.aborted) throw new ProbeChildError("codex model detection aborted before the app-server started", { code: "ABORT_ERR" });

  return await new Promise<RuntimeModelSet | null>((resolve, reject) => {
    const timeoutMs = options.timeoutMs ?? 5000;
    const proc = spawn(launch.command, launch.args, {
      cwd: options.cwd ?? process.cwd(),
      stdio: ["pipe", "pipe", "ignore"],
      env: launch.env ?? env,
      shell: launch.shell,
      // task #1277: its own process group, so a cancel can end the whole
      // app-server tree, not only the direct child.
      detached: process.platform !== "win32",
    });
    let settled = false;
    const onAbort = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      // Cancel holds the operation until the bounded tree cleanup settles; the
      // rejection says whether the exit was observed.
      void killProbeTreeAndSettle(proc).then((cleanup) => {
        reject(new ProbeChildError("codex model detection aborted", { code: "ABORT_ERR", cleanup }));
      });
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    let buffer = "";
    let requestId = 0;
    let initializeRequestId: JsonRpcId | null = null;
    let modelListRequestId: JsonRpcId | null = null;
    let pageCount = 0;
    const entries: unknown[] = [];

    const finish = (result: RuntimeModelSet | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      proc.kill();
      resolve(result);
    };
    const timer = setTimeout(() => finish(null), timeoutMs);

    const sendRequest = (method: string, params: Record<string, unknown>): JsonRpcId => {
      requestId += 1;
      const id = requestId;
      proc.stdin?.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
      return id;
    };
    const sendNotification = (method: string, params: Record<string, unknown>) => {
      proc.stdin?.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");
    };
    const requestModelPage = (cursor: string | null) => {
      pageCount += 1;
      modelListRequestId = sendRequest("model/list", modelListRequestParams(cursor));
    };

    proc.once("error", () => finish(null));
    proc.once("exit", () => finish(null));
    proc.stdout?.on("data", (chunk: Buffer | string) => {
      if (settled) return;
      buffer += chunk.toString();
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline === -1) break;
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (!line) continue;

        const message = parseCodexJsonRpcLine(line);
        if (!message || !isJsonRpcResponse(message)) continue;
        if (message.id === initializeRequestId) {
          if (!hasJsonRpcField(message, "result") || !isCompatibleInitializeResult(message.result)) {
            finish(null);
            return;
          }
          sendNotification("initialized", {});
          requestModelPage(null);
          continue;
        }

        if (message.id === modelListRequestId) {
          if (!hasJsonRpcField(message, "result")) {
            finish(null);
            return;
          }
          const page = modelListPage(message.result);
          if (!page) {
            finish(null);
            return;
          }
          entries.push(...page.entries);
          if (page.nextCursor && pageCount < 5) {
            requestModelPage(page.nextCursor);
            continue;
          }
          finish(codexModelSetFromAppServerEntries(entries));
          return;
        }
      }
    });

    initializeRequestId = sendRequest("initialize", {
      clientInfo: { name: "slock-daemon", version: "1.0.0" },
      capabilities: { experimentalApi: true },
    });
  });
}

/**
 * Codex caches its model catalog at `<CODEX_HOME>/models_cache.json` and
 * persists the selected default in `<CODEX_HOME>/config.toml`; without
 * CODEX_HOME the root is `~/.codex`. Legacy callers may still pass the OS home
 * and we fall back to `<home>/.codex`.
 */
export function detectCodexModels(home: string = resolveCodexHomeRootFromEnv()): RuntimeModelSet | null {
  let cachePath: string | null = null;
  let configPath: string | null = null;
  for (const root of codexStateRootCandidates(home)) {
    const candidate = path.join(root, "models_cache.json");
    if (existsSync(candidate)) {
      cachePath = candidate;
      configPath = path.join(root, "config.toml");
      break;
    }
  }

  if (!cachePath || !configPath) return null;

  let models: RuntimeModelInfo[] = [];
  try {
    const raw = readFileSync(cachePath, "utf8");
    const parsed = JSON.parse(raw);
    const entries = Array.isArray(parsed?.models) ? parsed.models : [];
    for (const entry of entries) {
      const slug = typeof entry?.slug === "string" ? entry.slug : null;
      if (!slug) continue;
      if (entry?.visibility && entry.visibility !== "public" && entry.visibility !== "list") continue;
      if (entry?.supported_in_api === false) continue;
      const label = typeof entry?.display_name === "string" && entry.display_name.length > 0
        ? entry.display_name
        : slug;
      models.push({ id: slug, label, verified: "launchable" });
    }
  } catch {
    return null;
  }

  if (models.length === 0) return null;

  let defaultModel: string | undefined;
  try {
    const raw = readFileSync(configPath, "utf8");
    // Minimal TOML lookup — `model = "..."` at top level (not inside a table).
    const match = raw.match(/^\s*model\s*=\s*"([^"]+)"/m);
    if (match) defaultModel = match[1];
  } catch {
    // Missing config.toml is fine; we just skip the default hint.
  }

  return { models, default: defaultModel };
}
