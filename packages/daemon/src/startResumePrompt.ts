/**
 * A start can carry a prompt that is rendered into the runtime's first input.
 *
 * Two kinds exist, and only one of them means "a human asked to continue this
 * session":
 *
 * - `caller` — the Server sent a resume prompt (a human resuming an agent).
 *   This one means "keep this session going": the wake recycle must not retire
 *   the session underneath the caller.
 * - `notice` — the daemon produced the prompt itself, to tell the agent that
 *   something is waiting in its App Inbox (a reminder that came due, for
 *   example). The notice says nothing about the session's fate, so it must not
 *   pin a large, cold session in place: the ordinary recycle rules still apply.
 *
 * The distinction is daemon-internal. The Server protocol is unchanged: a
 * resume prompt is still a plain string on the wire.
 */
export type StartResumePromptKind = "caller" | "notice";

export type StartResumePrompt = {
  text: string;
  kind: StartResumePromptKind;
};

/** A prompt the Server sent (a human resuming an agent). */
export function callerResumePrompt(text: string): StartResumePrompt {
  return { text, kind: "caller" };
}

/** A prompt the daemon produced for itself (an App Inbox notice). */
export function noticeResumePrompt(text: string): StartResumePrompt {
  return { text, kind: "notice" };
}

/**
 * Only a caller's prompt counts as "somebody asked to continue this session".
 * A notice must not, so a cold wake with a large prior context can still
 * recycle and open a fresh session.
 */
export function isCallerResumePrompt(prompt: StartResumePrompt | undefined): boolean {
  return prompt?.kind === "caller";
}

/** The text to render into the runtime's first input, whatever the kind is. */
export function resumePromptText(prompt: StartResumePrompt | undefined): string | undefined {
  return prompt?.text;
}
