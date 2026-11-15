/**
 * One display name for a Raft-managed session, shared by every runtime driver.
 *
 * Why a shared helper: the format is a user-facing convention, and it has to stay identical
 * across runtimes (a Codex thread and a Pi session of the same agent should read the same way).
 * Changing the format in one place keeps both sides aligned.
 *
 * The suffix is the first 8 characters of the runtime session id. Both runtimes already carry
 * that id in their own identifiers — Pi's session file is `<timestamp>_<sessionId>.jsonl` and
 * Codex's thread id is the same id — so the name points straight at the underlying session
 * instead of needing a second timestamp (and a second timezone convention).
 *
 * The name is a *display* name. Neither runtime lets the daemon rename its own session files.
 */

/** How many characters of the session id to append as the distinguishing suffix. */
export const RAFT_SESSION_NAME_SUFFIX_LENGTH = 8;

/**
 * Build the display name for a session.
 *
 * @param agentName The agent's display name (falls back to its id when the server has no
 *   display name). When it is empty the session stays unnamed — an unnamed session is better
 *   than a name like `Raft ·  · 1a2b3c4d`.
 * @param sessionId The runtime session id. When it is missing the name degrades to the
 *   agent-only form, which is what the session looked like before the suffix existed.
 */
export function raftSessionName(
  agentName: string | null | undefined,
  sessionId: string | null | undefined,
): string | null {
  const trimmed = agentName?.trim();
  if (!trimmed) return null;
  const suffix = sessionId?.trim().slice(0, RAFT_SESSION_NAME_SUFFIX_LENGTH);
  return suffix ? `Raft · ${trimmed} · ${suffix}` : `Raft · ${trimmed}`;
}
