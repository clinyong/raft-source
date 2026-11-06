import type { IdleRecycleStrategy } from "./types";

/** Observation only: background work and handoff are deliberately unimplemented. */
export function createPiIdleRecycleObservation(observeSnapshot: NonNullable<IdleRecycleStrategy["observeSnapshot"]>): IdleRecycleStrategy {
 return {
  observeSnapshot,
  observeOnly: true,
  configure: (config) => ({ config: { ...config, minColdGapMs: 600_000 } }),
  readFacts: async () => ({ transcriptPath: null, lastActivityAtMs: null, lastContextTokens: null }),
  stateSafe: (state) => state.backgroundTasks === 0 && state.outstandingToolUses === 0 && !state.compacting && !state.reviewing,
  checkWork: async () => ({ status: "unknown", reason: "pi_background_work_unverified" }),
  logSkip: () => false,
  wakeGapReason: () => "observe_only",
  wakeReason: "observe_only",
  buildPanel: async () => null,
  panelUnavailableReason: "observe_only",
 };
}
