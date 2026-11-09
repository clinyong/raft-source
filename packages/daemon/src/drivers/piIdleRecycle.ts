import { stat } from "node:fs/promises";
import type { IdleRecycleStrategy } from "./types";

type PiObservation = {
  sessionId: string | null;
  tokens: number | null;
  threshold: number | null;
  transcriptPath?: string;
  reason?: string;
  /** The Pi session's own idle state, read from the live session. */
  isIdle?: boolean;
};

/**
 * Pi cannot stop while background work is unknown, or recycle without a handoff panel.
 * To enable recycling, implement checkWork and buildPanel; the safety tests will then fail by design.
 */
export function createPiIdleRecycle(snapshot: () => PiObservation): IdleRecycleStrategy {
  return {
    configure: (config) => ({ config: { ...config, minColdGapMs: 600_000 } }),
    async readFacts(sessionId) {
      const unavailable = (reason: string) => ({ transcriptPath: null, lastActivityAtMs: null, lastContextTokens: null, unavailableReason: reason });
      const first = snapshot();
      if (first.sessionId !== sessionId) return unavailable("observation_session_mismatch");
      if (first.reason) return unavailable(first.reason);
      if (!first.transcriptPath || first.tokens === null || first.threshold === null) return unavailable("observation_unavailable");
      try {
        const timestamp = (await stat(first.transcriptPath)).mtimeMs;
        const latest = snapshot();
        if (latest.sessionId !== sessionId || latest.transcriptPath !== first.transcriptPath) return unavailable("observation_session_mismatch");
        if (latest.reason) return unavailable(latest.reason);
        if (latest.tokens === null || latest.threshold === null) return unavailable("observation_unavailable");
        return { transcriptPath: latest.transcriptPath, lastActivityAtMs: timestamp, lastContextTokens: latest.tokens, minContextTokens: latest.threshold };
      } catch {
        return unavailable("observation_transcript_unavailable");
      }
    },
    stateSafe: (state) => state.backgroundTasks === 0 && state.outstandingToolUses === 0 && !state.compacting && !state.reviewing,
    // The live session is the authority on "is Pi working right now". The
    // daemon keeps its own copy of that state and it can be stale: the
    // daemon believed Pi was busy long after the session had settled
    // (2026-11-07 and 2026-11-09 stalls), and a delivery decision that
    // trusts the stale copy never recovers.
    probeIdle: () => {
      const observation = snapshot();
      if (observation.reason) return null;
      if (typeof observation.isIdle !== "boolean") return null;
      return { isIdle: observation.isIdle };
    },
    checkWork: async () => ({ status: "unknown", reason: "pi_background_work_unverified" }),
    logSkip: () => false,
    wakeGapReason: (idleMs, thresholdMs) => idleMs < thresholdMs ? "pi_idle_below_threshold" : null,
    wakeReason: "large_context",
    buildPanel: async () => null,
    panelUnavailableReason: "pi_panel_unavailable",
  };
}
