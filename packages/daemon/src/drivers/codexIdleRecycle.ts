import { buildConstructedPanel } from "../wakeBriefingPanel";
import type { IdleRecycleStrategy } from "./types";
import { codexOutstandingToolCalls, readCodexSessionResumeFacts } from "./codexRecycleTranscript";

const positive = (value: string | undefined): number | undefined => {
  if (value === undefined) return undefined;
  const parsed = Number.parseInt(value.trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
};

export function createCodexIdleRecycle(countTerminals: (sessionId: string) => Promise<number | null>): IdleRecycleStrategy {
  return {
    configure(config, env) {
      if (!["1", "true", "on"].includes((env.RAFT_CODEX_WAKE_RECYCLE ?? "").trim().toLowerCase())) {
        return { config, skip: "codex_opt_in_required" };
      }
      return { config: {
        ...config,
        minContextTokens: positive(env.RAFT_CODEX_WAKE_RECYCLE_MIN_CONTEXT_TOKENS) ?? 150_000,
        minColdGapMs: positive(env.RAFT_CODEX_WAKE_RECYCLE_MIN_IDLE_MS) ?? 600_000,
      } };
    },
    readFacts: readCodexSessionResumeFacts,
    logSkip: (reason) => ["background_tasks_unknown", "background_tasks_outstanding", "codex_background_query_unavailable"].includes(reason),
    stateSafe(state, expected) {
      return (!expected || (state.sessionId === expected.sessionId && state.lastEventAtMs === expected.lastEventAtMs))
        && state.backgroundTasks === 0 && state.outstandingToolUses === 0 && !state.compacting && !state.reviewing;
    },
    async checkWork(path, sessionId, state) {
      if (state && state.liveSessionId !== sessionId) return { status: "unknown", reason: "codex_background_query_unavailable" };
      if (state && !this.stateSafe(state)) return { status: "busy", reason: "background_tasks_outstanding" };
      // The terminal API does not cover yielded outer code-mode cells.
      const outstanding = await codexOutstandingToolCalls(path);
      if (outstanding === null) return { status: "unknown", reason: "background_tasks_unknown" };
      if (outstanding > 0) return { status: "busy", reason: "background_tasks_outstanding" };
      // Tool-output text alone may omit exit status, so query the live driver.
      let count: number | null = null;
      try { count = await countTerminals(sessionId); } catch { /* unknown */ }
      if (count === null || !Number.isSafeInteger(count) || count < 0) return { status: "unknown", reason: "codex_background_query_unavailable" };
      return count > 0 ? { status: "busy", reason: "background_tasks_outstanding" } : { status: "clear" };
    },
    wakeGapReason: (idle, threshold) => idle < threshold ? "codex_idle_below_threshold" : null,
    wakeReason: "large_context",
    buildPanel: (args) => buildConstructedPanel({ ...args, codex: true }),
    panelUnavailableReason: "codex_panel_unavailable",
  };
}
