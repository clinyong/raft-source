import type { IdleRecycleStrategy } from "./types";
import { readClaudeSessionResumeFacts, transcriptOutstandingBackgroundTasks } from "./claudeRecycleTranscript";

/** Preserve the existing Claude transcript and background-task checks. */
export const claudeIdleRecycle: IdleRecycleStrategy = {
  readFacts: readClaudeSessionResumeFacts,
  async checkWork(path) {
    const count = await transcriptOutstandingBackgroundTasks(path);
    return count === null ? "unknown" : count > 0 ? "busy" : "clear";
  },
};
