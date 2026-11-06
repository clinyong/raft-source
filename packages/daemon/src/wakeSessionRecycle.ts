import { readdir, stat } from "node:fs/promises";
import type { AgentConfig } from "@botiverse/raft-shared";
import { getIdleRecycleStrategy } from "./drivers/index";
export { readClaudeSessionResumeFacts, transcriptOutstandingBackgroundTasks } from "./drivers/claudeRecycleTranscript";
import type { IdleRecycleState, IdleRecycleStrategy } from "./drivers/types";
import { logger } from "./logger";
import type { AxSurfaceText } from "@botiverse/raft-shared";
import { axSurface, composeAxSurfaces } from "./agentRuntimeInput";
import { WAKE_SECTION_BREAK, buildStartupMemoryBlock, resolveStartupMemoryBlockConfig } from "./startupMemoryBlock";

/**
 * At-wake session recycling (RFC 070, phase 1).
 *
 * When an agent wakes after its prompt cache has certainly expired AND its
 * previous runtime session carries a large context, resuming that session
 * makes the provider re-write the whole context at cache-write prices and
 * re-read it on every subsequent step. Starting a fresh session seeded with a
 * short reconstructed briefing is strictly cheaper and faster at the wake
 * boundary; the agent's durable state (MEMORY.md, notes/, Raft channels and
 * tasks) is unaffected.
 *
 * This module owns the decision and the briefing text. The only caller is
 * AgentProcessManager.startAgentNow, which applies the plan by clearing
 * `sessionId` before spawning (so the driver does not pass `--resume`) while
 * keeping the resume-shaped wake prompt ladder (catch-up messages and unread
 * summaries still deliver — they are the v0 constructed panel).
 *
 * Deliberately NOT here (deferred per RFC 070): at-cap recycling (compaction
 * stays the mid-turn backstop), the cold-idle process reaper, and the
 * block-provider panel architecture.
 */

/** Matches the 1h prompt-cache TTL measured across the fleet (RFC 070 §1):
 * past this gap the next request re-writes the resumed context in full, so
 * resuming a large session buys nothing that a fresh session does not. */
const DEFAULT_MIN_COLD_GAP_MS = 60 * 60_000;

/** RFC 070 §4: cost is insensitive to this threshold anywhere in 48k–256k
 * (cold-wake contexts are top-heavy); 128k keeps small, cheap-to-resume
 * sessions their continuity while capturing ~98.6% of the cold rewrite. */
const DEFAULT_MIN_CONTEXT_TOKENS = 128_000;

export interface WakeRecycleConfig {
  enabled: boolean;
  minContextTokens: number;
  minColdGapMs: number;
}

export interface SessionResumeFacts {
  transcriptPath: string | null;
  lastActivityAtMs: number | null;
  lastContextTokens: number | null;
}

export type WakeRecyclePlan =
  | { action: "resume"; reason: string }
  | {
      action: "recycle";
      reason: string;
      priorSessionId: string;
      idleMs: number;
      priorContextTokens: number;
      thresholds: { minContextTokens: number; minColdGapMs: number };
      briefing: AxSurfaceText;
    };

function parseBooleanFlag(value: string | undefined): boolean | undefined {
  if (value === undefined) return undefined;
  const normalized = value.trim().toLowerCase();
  if (normalized === "1" || normalized === "true" || normalized === "on") return true;
  if (normalized === "0" || normalized === "false" || normalized === "off" || normalized === "") return false;
  return undefined;
}

function parsePositiveInteger(value: string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number.parseInt(value.trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

/**
 * Rollout knobs. Recycling runs only when the server turned the
 * `constructed_wake_context` flag on for this agent's server
 * (`AgentConfig.constructedWakeContext`). Per-agent `agents.env_vars` win over
 * daemon process env for the tuning knobs, and RAFT_WAKE_RECYCLE=0 stays a
 * local kill switch; no env value can turn recycling on without the server flag.
 */
export function resolveWakeRecycleConfig(
  serverEnabled: boolean | null | undefined,
  agentEnvVars: Record<string, string> | null | undefined,
  processEnv: NodeJS.ProcessEnv = process.env,
): WakeRecycleConfig {
  const pick = (key: string): string | undefined => agentEnvVars?.[key] ?? processEnv[key];
  return {
    enabled: serverEnabled === true && parseBooleanFlag(pick("RAFT_WAKE_RECYCLE")) !== false,
    minContextTokens: parsePositiveInteger(pick("RAFT_WAKE_RECYCLE_MIN_CONTEXT_TOKENS")) ?? DEFAULT_MIN_CONTEXT_TOKENS,
    minColdGapMs: parsePositiveInteger(pick("RAFT_WAKE_RECYCLE_MIN_COLD_GAP_MS")) ?? DEFAULT_MIN_COLD_GAP_MS,
  };
}

export interface WakeRecycleDecisionInput {
  config: WakeRecycleConfig;
  runtime: string | undefined;
  sessionId: string | null | undefined;
  hasResumePrompt: boolean;
  hasRuntimeProfileControl: boolean;
  facts: SessionResumeFacts;
  nowMs: number;
  idleRecycle?: IdleRecycleStrategy | null;
}

/** A pending runtime-profile control blocks recycling only when it is a real
 * profile transition. A `daemon_release_notice` is just a message for the
 * agent (it re-delivers on the fresh session's ladder) — after every daemon
 * release the whole fleet carries one, and pinning giant transcripts for it
 * would suppress recycling exactly when it matters most. */
export function runtimeProfileControlBlocksRecycle(
  control: { kind: string } | null | undefined,
): boolean {
  return Boolean(control && control.kind !== "daemon_release_notice");
}

/**
 * Pure decision core. Every gate that fails resolves to a plain resume with a
 * reason string; the transcript-derived facts must POSITIVELY establish both
 * "cache is certainly cold" and "context is large" before a session is
 * recycled. Unknown facts always mean resume.
 */
export function decideWakeRecycle(input: WakeRecycleDecisionInput): WakeRecyclePlan {
  const { config, facts } = input;
  if (!config.enabled) return { action: "resume", reason: "disabled" };
  const strategy = input.idleRecycle === undefined ? getIdleRecycleStrategy(input.runtime) : input.idleRecycle;
  if (!strategy) return { action: "resume", reason: "unsupported_runtime" };
  if (!input.sessionId) return { action: "resume", reason: "no_prior_session" };
  if (input.hasResumePrompt) return { action: "resume", reason: "explicit_resume_prompt" };
  if (input.hasRuntimeProfileControl) return { action: "resume", reason: "runtime_profile_control" };
  if (facts.lastActivityAtMs === null || facts.lastContextTokens === null) {
    return { action: "resume", reason: "session_facts_unavailable" };
  }
  const idleMs = input.nowMs - facts.lastActivityAtMs;
  if (idleMs < 0) return { action: "resume", reason: "session_facts_unavailable" };
  const gapReason = strategy.wakeGapReason(idleMs, config.minColdGapMs);
  if (gapReason) return { action: "resume", reason: gapReason };
  if (facts.lastContextTokens <= config.minContextTokens) return { action: "resume", reason: "context_below_threshold" };
  return {
    action: "recycle",
    reason: strategy.wakeReason,
    priorSessionId: input.sessionId,
    idleMs,
    priorContextTokens: facts.lastContextTokens,
    thresholds: { minContextTokens: config.minContextTokens, minColdGapMs: config.minColdGapMs },
    briefing: buildRecycleBriefing({ idleMs, priorContextTokens: facts.lastContextTokens }),
  };
}

/**
 * The recycled-wake first input carries NO announcement and NO instructions
 * (v2, 2026-09-11). The v1 chrome ("your session was retired… consult
 * MEMORY.md first, then re-establish state…") measurably framed every wake as
 * a resume-protocol: in the 13-recycle canary batch, 13/13 fresh sessions
 * opened with state-re-establishment rituals instead of the message that woke
 * them. A chat window never announces that its context was rebuilt — history
 * sits quietly above, the new message sits at the bottom, and the thread
 * itself carries any commitments. The constructed input now has that shape:
 * ambient blocks (memory, recent messages, in-flight facts) above, the
 * ordinary wake prompt last, nothing in between telling the agent what kind
 * of start this is or what to do first.
 */
export const buildRecycleBriefing = axSurface(
  "Recycled-wake briefing: intentionally EMPTY — the context blocks are XML-delimited and self-labeling, and any announcement or recap line reads as a protocol to follow",
  (args: { idleMs: number; priorContextTokens: number }): string => {
    void args;
    return "";
  },
  { examples: [{ title: "any recycle", args: [{ idleMs: 3 * 3_600_000, priorContextTokens: 500_000 }] }] },
);

/** Compose ax-surface pieces with the section break, skipping empty pieces so
 * an empty briefing contributes no leading blank lines. */
function composeWakeSections(...pieces: AxSurfaceText[]): AxSurfaceText {
  const nonEmpty = pieces.filter((piece) => String(piece).length > 0);
  let composed = nonEmpty[0] ?? pieces[pieces.length - 1]!;
  for (const piece of nonEmpty.slice(1)) composed = composeAxSurfaces(composed, WAKE_SECTION_BREAK, piece);
  return composed;
}

/** Trace payload + log line for an applied recycle, kept here so the
 * AgentProcessManager call site stays a thin hook. */
export function describeAppliedWakeRecycle(
  agentId: string,
  plan: Extract<WakeRecyclePlan, { action: "recycle" }>,
): { traceAttrs: Record<string, unknown>; logLine: string } {
  return {
    traceAttrs: {
      agentId,
      reason: plan.reason,
      prior_session_id: plan.priorSessionId,
      idle_ms: plan.idleMs,
      prior_context_tokens: plan.priorContextTokens,
      min_context_tokens: plan.thresholds.minContextTokens,
      min_cold_gap_ms: plan.thresholds.minColdGapMs,
    },
    logLine: `Retiring runtime session ${plan.priorSessionId} at wake (idle ${Math.round(plan.idleMs / 60_000)}m, ~${Math.round(plan.priorContextTokens / 1000)}k context); starting fresh with briefing`,
  };
}

/** How often the cold-idle sweep runs. The in-memory progress clock gates
 * candidates for free; the transcript is only read for processes already past
 * the cold gap, so a sweep over an idle fleet costs a handful of stats. */
export const COLD_IDLE_SWEEP_MS = 10 * 60_000;

export interface ColdIdleRuntimeView {
  agentId: string;
  config: AgentConfig;
  /** APM-idle AND nothing untold queued — never select a process with work
   * pending. Already-notified unread messages are the agent's own deferral and
   * do not count (see AgentProcessManager.hasUntoldInboxWork). */
  idle: boolean;
  /** Daemon's in-memory runtime-progress clock (RuntimeProgressState.lastEventAt). */
  lastEventAtMs: number;
  /** Live session id latched from the runtime (falls back to config.sessionId). */
  liveSessionId: string | null;
  idleRecycle?: IdleRecycleStrategy | null;
  runtimeState?: IdleRecycleState;
}

export interface ColdIdleRecycleStop {
  agentId: string;
  sessionId: string;
  idleMs: number;
  priorContextTokens: number;
  observedLastEventAtMs: number;
}

/**
 * Pick idle runtime processes whose prompt cache has certainly expired and
 * whose context is past the recycle threshold. Keeping such a process alive
 * buys nothing (its cache is gone; only ~1s of respawn is saved), while
 * stopping it routes the next wake through the process-start path where the
 * at-wake recycle applies — this is how the recycle reaches the fleet's
 * dominant cold-wake shape (long-lived processes woken via stdin).
 */
/** Why the sweep left a running process alone (one per view per sweep). */
export type ColdIdleSkipReason =
  | "disabled"
  | "not_idle"
  | "unsupported_runtime"
  | "no_session"
  | "recent_event"
  | "session_facts_unavailable"
  | "recent_activity"
  | "context_below_threshold"
  | "no_transcript"
  | "background_tasks_unknown"
  | "background_tasks_outstanding"
  | (string & {});

export async function selectColdIdleRecycleStops(
  views: readonly ColdIdleRuntimeView[],
  opts: {
    nowMs?: number;
    homeDir?: string;
    processEnv?: NodeJS.ProcessEnv;
    /** Called once for every view the sweep does not stop, with the gate that held it. */
    onSkip?: (agentId: string, reason: ColdIdleSkipReason) => void;
  } = {},
): Promise<ColdIdleRecycleStop[]> {
  const nowMs = opts.nowMs ?? Date.now();
  const out: ColdIdleRecycleStop[] = [];
  const skip = (agentId: string, reason: ColdIdleSkipReason) => opts.onSkip?.(agentId, reason);
  for (const view of views) {
    const cfg = resolveWakeRecycleConfig(view.config.constructedWakeContext, view.config.envVars, opts.processEnv ?? process.env);
    if (!cfg.enabled) { skip(view.agentId, "disabled"); continue; }
    const strategy = view.idleRecycle === undefined ? getIdleRecycleStrategy(view.config.runtime) : view.idleRecycle;
    if (!strategy) { skip(view.agentId, "unsupported_runtime"); continue; }
    const settings = strategy.configure(cfg, view.config.envVars ?? {});
    if (settings.skip) { skip(view.agentId, "disabled"); continue; }
    Object.assign(cfg, settings.config);
    if (view.runtimeState && !strategy.stateSafe(view.runtimeState)) { skip(view.agentId, "not_idle"); continue; }
    if (!view.idle) { skip(view.agentId, "not_idle"); continue; }
    const sessionId = view.liveSessionId ?? view.config.sessionId;
    if (!sessionId) { skip(view.agentId, "no_session"); continue; }
    if (nowMs - view.lastEventAtMs <= cfg.minColdGapMs) { skip(view.agentId, "recent_event"); continue; }
    const facts = await strategy.readFacts(sessionId, opts.homeDir);
    if (facts.lastActivityAtMs === null || facts.lastContextTokens === null) { skip(view.agentId, "session_facts_unavailable"); continue; }
    // A turn-idle runtime can still host live background work (subagents,
    // monitors) whose activity lands in the session's sidecar directory, not
    // the main transcript. Stopping mid-subagent loses that work — count the
    // sidecar's freshest mtime as activity. (Observed live: the sweep killed a
    // runtime whose Explore subagent was mid-flight.)
    const sidecarActivityAtMs = facts.transcriptPath
      ? await latestMtimeUnder(facts.transcriptPath.replace(/\.jsonl$/, ""))
      : null;
    const lastActive = Math.max(facts.lastActivityAtMs, view.lastEventAtMs, sidecarActivityAtMs ?? 0);
    if (nowMs - lastActive <= cfg.minColdGapMs) { skip(view.agentId, "recent_activity"); continue; }
    if (facts.lastContextTokens <= cfg.minContextTokens) { skip(view.agentId, "context_below_threshold"); continue; }
    // Sweep only sessions with DEFINITELY zero outstanding background tasks
    // right now: a runtime holding an armed monitor/background task may have
    // silent pending intent the daemon cannot see; leave it alone.
    if (!facts.transcriptPath) { skip(view.agentId, "no_transcript"); continue; }
    let work: Awaited<ReturnType<IdleRecycleStrategy["checkWork"]>>;
    try {
      work = await strategy.checkWork(facts.transcriptPath, sessionId, view.runtimeState);
    } catch {
      skip(view.agentId, "background_tasks_unknown");
      continue;
    }
    if (work.status !== "clear") {
      skip(view.agentId, work.reason ?? (work.status === "busy" ? "background_tasks_outstanding" : "background_tasks_unknown"));
      continue;
    }
    out.push({
      agentId: view.agentId,
      sessionId,
      idleMs: nowMs - lastActive,
      priorContextTokens: facts.lastContextTokens,
      observedLastEventAtMs: view.lastEventAtMs,
    });
  }
  return out;
}

/** Freshest mtime under a directory tree (one level of nesting is enough for
 * Claude session sidecars: subagents/, tasks/). Null when absent. */
async function latestMtimeUnder(dir: string): Promise<number | null> {
  let latest: number | null = null;
  const visit = async (target: string, depth: number): Promise<void> => {
    let info;
    try {
      info = await stat(target);
    } catch {
      return;
    }
    latest = Math.max(latest ?? 0, info.mtimeMs);
    if (!info.isDirectory() || depth >= 2) return;
    let entries: string[];
    try {
      entries = await readdir(target);
    } catch {
      return;
    }
    for (const entry of entries) {
      await visit(`${target}/${entry}`, depth + 1);
    }
  };
  await visit(dir, 0);
  return latest;
}

/** Prepend the briefing to the ladder-selected wake prompt and mark the
 * prompt source, as one atomic application so the call site stays one line. */
export function applyWakeRecycleBriefing(
  plan: WakeRecyclePlan,
  prompt: AxSurfaceText,
  promptSource: string,
): { prompt: AxSurfaceText; promptSource: string } {
  if (plan.action !== "recycle") return { prompt, promptSource };
  return { prompt: composeWakeSections(plan.briefing, prompt), promptSource: `recycled_${promptSource}` };
}

export interface PlanWakeSessionRecycleArgs {
  config: AgentConfig;
  hasResumePrompt: boolean;
  nowMs?: number;
  homeDir?: string;
  processEnv?: NodeJS.ProcessEnv;
  idleRecycle?: IdleRecycleStrategy | null;
}

/** Async entry point for AgentProcessManager: resolve knobs, read transcript
 * facts, decide. Never throws — any failure is a plain resume. */
export async function planWakeSessionRecycle(args: PlanWakeSessionRecycleArgs): Promise<WakeRecyclePlan> {
  const recycleConfig = resolveWakeRecycleConfig(args.config.constructedWakeContext, args.config.envVars, args.processEnv ?? process.env);
  if (!recycleConfig.enabled || !args.config.sessionId) {
    return decideWakeRecycle({
      config: recycleConfig,
      runtime: args.config.runtime,
      sessionId: args.config.sessionId,
      hasResumePrompt: args.hasResumePrompt,
      hasRuntimeProfileControl: runtimeProfileControlBlocksRecycle(args.config.runtimeProfileControl),
      facts: { transcriptPath: null, lastActivityAtMs: null, lastContextTokens: null },
      nowMs: args.nowMs ?? Date.now(),
    });
  }
  const strategy = args.idleRecycle === undefined ? getIdleRecycleStrategy(args.config.runtime) : args.idleRecycle;
  if (!strategy) return { action: "resume", reason: "unsupported_runtime" };
  const settings = strategy.configure(recycleConfig, args.config.envVars ?? {});
  if (settings.skip) return { action: "resume", reason: settings.skip };
  Object.assign(recycleConfig, settings.config);
  const facts = await strategy.readFacts(args.config.sessionId, args.homeDir);
  const plan = decideWakeRecycle({
    config: recycleConfig,
    runtime: args.config.runtime,
    sessionId: args.config.sessionId,
    hasResumePrompt: args.hasResumePrompt,
    hasRuntimeProfileControl: runtimeProfileControlBlocksRecycle(args.config.runtimeProfileControl),
    idleRecycle: strategy,
    facts,
    nowMs: args.nowMs ?? Date.now(),
  });
  if (plan.action === "resume") {
    // Enabled-but-resumed is the observable a canary needs: it says why the
    // recycle held its fire (warm cache, small context, missing facts, ...).
    logger.info(`[WakeRecycle] agent session ${args.config.sessionId} resumed: ${plan.reason} (lastActivityAtMs=${facts.lastActivityAtMs}, lastContextTokens=${facts.lastContextTokens}, path=${facts.transcriptPath ?? "none"})`);
    return plan;
  }
  // Constructed briefing order: briefing chrome, then the durable memory
  // index, then session-specific working state (panel). Both extras degrade
  // to absence, never block the recycle.
  let briefing = plan.briefing;

  // Startup memory block (RFC 070 §6 follow-up): a recycled start is a fresh
  // session, so the MEMORY.md head is pushed here instead of relying on the
  // standing-prompt read.
  const memoryConfig = resolveStartupMemoryBlockConfig(args.config.constructedWakeContext, args.config.envVars, args.processEnv ?? process.env);
  const workspacePath = args.config.runtimeContext?.workspacePath;
  if (memoryConfig.enabled && workspacePath) {
    const memoryBlock = await buildStartupMemoryBlock({ workspacePath, budgetTokens: memoryConfig.budgetTokens });
    if (memoryBlock) {
      briefing = composeWakeSections(briefing, memoryBlock);
      logger.info(`[WakeRecycle] startup memory block injected for recycled session ${plan.priorSessionId}`);
    }
  }

  // Constructed panel (RFC 070 §9 "v3"): open loops + objects in play (live
  // re-read where possible) + last actions, built from the retired session's
  // transcript. Failure to build degrades to the bare briefing, never blocks.
  const panelBudget = parsePositiveInteger(
    (args.config.envVars ?? undefined)?.RAFT_WAKE_RECYCLE_PANEL_TOKENS
      ?? (args.processEnv ?? process.env).RAFT_WAKE_RECYCLE_PANEL_TOKENS,
  ) ?? 8_000;
  // Recent-messages section knob: same env-family semantics, `=0` disables
  // just this section (the panel's other sections are unaffected).
  const recentMessagesRaw = (args.config.envVars ?? undefined)?.RAFT_WAKE_RECYCLE_RECENT_MESSAGES
    ?? (args.processEnv ?? process.env).RAFT_WAKE_RECYCLE_RECENT_MESSAGES;
  const recentMessagesMax = recentMessagesRaw?.trim() === "0" ? 0 : parsePositiveInteger(recentMessagesRaw);
  if (facts.transcriptPath && panelBudget > 0) {
    try {
      const panel = await strategy.buildPanel({
        transcriptPath: facts.transcriptPath,
        workspacePath: workspacePath ?? undefined,
        budgetTokens: panelBudget,
        recentMessagesMax,
      });
      if (panel) return { ...plan, briefing: composeWakeSections(briefing, panel) };
    } catch (err) {
      logger.warn(`[WakeRecycle] panel construction failed, using bare briefing: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  if (strategy.panelUnavailableReason) return { action: "resume", reason: strategy.panelUnavailableReason };
  return plan.briefing === briefing ? plan : { ...plan, briefing };
}
