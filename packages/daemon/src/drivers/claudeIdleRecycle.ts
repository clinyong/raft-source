import { buildConstructedPanel } from "../wakeBriefingPanel";
import type { IdleRecycleStrategy } from "./types";
import { readClaudeSessionResumeFacts, transcriptOutstandingBackgroundTasks } from "./claudeRecycleTranscript";

/** Preserve the existing Claude transcript and background-task checks. */
export const claudeIdleRecycle: IdleRecycleStrategy = {
  configure: (config) => ({ config }),
  readFacts: readClaudeSessionResumeFacts,
  stateSafe: () => true,
  logSkip: () => false,
  wakeGapReason: (idle, threshold) => idle <= threshold ? "cache_possibly_warm" : null,
  wakeReason: "cold_gap_large_context",
  buildPanel: (args) => buildConstructedPanel(args),
  panelUnavailableReason: null,
  async checkWork(path) {
    const count = await transcriptOutstandingBackgroundTasks(path);
    return { status: count === null ? "unknown" : count > 0 ? "busy" : "clear" };
  },
};
