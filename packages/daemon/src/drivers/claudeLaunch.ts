import { ambientProcessEnv } from "../processEnvPatch";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { hydrateRuntimeConfig, runtimeConfigToLaunchFields, type AgentConfig } from "@botiverse/raft-shared";
import { isClaudeCustomProviderConfig } from "./claudeProviderIsolation";
import { raftSessionName } from "./sessionName";
import type { RuntimeProbeResult } from "./types";
import { firstExistingPath, formatCommandLookupTiming, lookupCommandOnPath, lookupCommandOnPathAsync, readCommandVersion, resolveHomePath, type CommandLookup, type ProbeDeps } from "./probe";
import { resolveRuntimeLaunch, resolveWindowsDirectLaunch, type DirectLaunch, type WindowsLaunchDeps } from "./windowsLaunch";

export const CLAUDE_DESKTOP_CLI_RELATIVE_PATH = path.join("Applications", "Claude Code URL Handler.app", "Contents", "MacOS", "claude");
export const CLAUDE_DESKTOP_CLI_SYSTEM_PATH = "/Applications/Claude Code URL Handler.app/Contents/MacOS/claude";
const CLAUDE_SYSTEM_PROMPT_FILE = "claude-system-prompt.md";
export const CLAUDE_DISALLOWED_TOOLS = [
  "EnterPlanMode",
  "ExitPlanMode",
  "ScheduleWakeup",
  "CronCreate",
  "CronList",
  "CronDelete",
].join(",");

export function resolveClaudeCommand(deps: ProbeDeps = {}): string | null {
  return lookupClaudeCommand(deps).command;
}

function lookupClaudeCommand(deps: ProbeDeps): CommandLookup {
  const lookup = lookupCommandOnPath("claude", deps);
  if (lookup.command) return lookup;

  if ((deps.platform ?? process.platform) !== "darwin") return lookup;
  const desktop = firstExistingPath([
    resolveHomePath(CLAUDE_DESKTOP_CLI_RELATIVE_PATH, deps),
    CLAUDE_DESKTOP_CLI_SYSTEM_PATH,
  ], deps);
  return desktop ? { command: desktop } : lookup;
}

export function resolveClaudeLaunchCommand(config: AgentConfig, deps: ProbeDeps = {}): string | null {
  const launchRuntimeFields = runtimeConfigToLaunchFields(hydrateRuntimeConfig(config));
  return launchRuntimeFields.command?.trim() || resolveClaudeCommand(deps);
}

/** task #1277 — {@link lookupClaudeCommand} off the event loop: the PATH lookup as a bounded probe child, the darwin desktop fallback as before. */
async function lookupClaudeCommandAsync(deps: ProbeDeps): Promise<CommandLookup> {
  const lookup = await lookupCommandOnPathAsync("claude", deps);
  if (lookup.command) return lookup;
  if ((deps.platform ?? process.platform) !== "darwin") return lookup;
  const desktop = firstExistingPath([
    resolveHomePath(CLAUDE_DESKTOP_CLI_RELATIVE_PATH, deps),
    CLAUDE_DESKTOP_CLI_SYSTEM_PATH,
  ], deps);
  return desktop ? { command: desktop } : lookup;
}

/** task #1277 — {@link resolveClaudeLaunchCommand} off the event loop; a configured command needs no lookup. */
export async function resolveClaudeLaunchCommandAsync(config: AgentConfig, deps: ProbeDeps = {}): Promise<string | null> {
  const launchRuntimeFields = runtimeConfigToLaunchFields(hydrateRuntimeConfig(config));
  const configured = launchRuntimeFields.command?.trim();
  if (configured) return configured;
  return (await lookupClaudeCommandAsync(deps)).command;
}

export function probeClaude(deps: ProbeDeps = {}): RuntimeProbeResult {
  const { command, missReason, timing } = lookupClaudeCommand(deps);
  // task #1240: the lookup's per-child timing (phase, ms, budget, outcome
  // only) always travels as `lookupTiming` for the detection trace; when the
  // lookup left the runtime unavailable it is also the diagnostic, so the
  // diagnostics bundle shows how long the Windows lookup took, not just that
  // it timed out. A healthy runtime carries no diagnostic (that line warns).
  const lookupTiming = formatCommandLookupTiming(timing);
  const timingFields = lookupTiming ? { lookupTiming } : {};
  if (!command) {
    return { available: false, unavailableReason: missReason, ...timingFields, ...(lookupTiming ? { diagnostic: `lookup: ${lookupTiming}` } : {}) };
  }
  return {
    available: true,
    version: readCommandVersion(command, [], deps) ?? undefined,
    ...timingFields,
  };
}

export function probeClaudeLaunch(config: AgentConfig, deps: ProbeDeps = {}): RuntimeProbeResult {
  const launchDeps = {
    ...deps,
    env: { ...(deps.env ?? ambientProcessEnv()) },
  };
  delete launchDeps.env.CLAUDECODE;
  const command = resolveClaudeLaunchCommand(config, launchDeps);
  if (!command) return { available: false };
  return {
    available: true,
    version: readCommandVersion(command, [], launchDeps) ?? undefined,
  };
}

export function buildClaudeArgs(
  config: AgentConfig,
  opts: { standingPromptFilePath: string; managedMcpConfigPath?: string | null; sessionId?: string | null },
): string[] {
  const launchRuntimeFields = runtimeConfigToLaunchFields(hydrateRuntimeConfig(config));
  const args = [
    "--allow-dangerously-skip-permissions",
    "--dangerously-skip-permissions",
    "--verbose",
    "--permission-mode", "bypassPermissions",
    "--output-format", "stream-json",
    "--input-format", "stream-json",
    "--include-partial-messages",
    "--model", launchRuntimeFields.model || "sonnet",
    "--disallowed-tools", CLAUDE_DISALLOWED_TOOLS,
    "--append-system-prompt-file", opts.standingPromptFilePath,
  ];

  if (launchRuntimeFields.reasoningEffort) {
    args.push("--effort", launchRuntimeFields.reasoningEffort);
  }

  if (isClaudeCustomProviderConfig(config)) {
    args.push("--setting-sources", "project,local");
  }

  if (launchRuntimeFields.mode.kind === "fast") {
    args.push("--settings", JSON.stringify({ fastMode: true }));
  }

  if (config.sessionId) {
    args.push("--resume", config.sessionId);
  } else if (opts.sessionId) {
    // A new session: the daemon picks the id so the display name can carry its
    // distinguishing suffix, and Claude Code honours the id we pass in (its
    // init message then reports the very same id).
    args.push("--session-id", opts.sessionId);
    const sessionName = raftSessionName(config.displayName || config.name, opts.sessionId);
    if (sessionName) {
      args.push("--name", sessionName);
    }
  }

  if (opts.managedMcpConfigPath) {
    args.push("--mcp-config", opts.managedMcpConfigPath);
  }

  return args;
}

export function buildClaudeManagedMcpConfig(server: {
  name: string;
  url: string;
}): Record<string, unknown> {
  return {
    mcpServers: {
      [server.name]: {
        type: "http",
        url: server.url,
      },
    },
  };
}

export function writeClaudeSystemPromptFile(standingPrompt: string, slockDir: string): string {
  const systemPromptPath = path.join(slockDir, CLAUDE_SYSTEM_PROMPT_FILE);
  writeFileSync(systemPromptPath, standingPrompt, { mode: 0o600 });
  return systemPromptPath;
}

export function buildClaudeSpawnSpec(
  claudeCommand: string | null,
  args: string[],
  deps: WindowsLaunchDeps = {},
): DirectLaunch {
  const platform = deps.platform ?? process.platform;
  if (platform !== "win32") return { command: claudeCommand ?? "claude", args, shell: false };
  // A bare configured name is looked up on PATH like the default; a path is
  // used as given. Either way a .cmd/.bat shim is resolved, never run via cmd.exe.
  if (!claudeCommand || !/[\\/]/.test(claudeCommand)) {
    return resolveRuntimeLaunch("claude", claudeCommand ?? "claude", args, deps);
  }
  return resolveWindowsDirectLaunch("claude", claudeCommand, args, deps);
}
