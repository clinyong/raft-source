import { stat } from "node:fs/promises";
import type { IdleRecycleStrategy } from "./types";

type PiObservation = {
  sessionId: string | null;
  tokens: number | null;
  threshold: number | null;
  transcriptPath?: string;
  reason?: string;
};

/** Observation only: background work and handoff are deliberately unimplemented. */
export function createPiIdleRecycleObservation(snapshot: () => PiObservation): IdleRecycleStrategy {
  return {
    observeOnly: true,
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
    checkWork: async () => ({ status: "unknown", reason: "pi_background_work_unverified" }),
    logSkip: () => false,
    wakeGapReason: () => "observe_only",
    wakeReason: "observe_only",
    buildPanel: async () => null,
    panelUnavailableReason: "observe_only",
  };
}
