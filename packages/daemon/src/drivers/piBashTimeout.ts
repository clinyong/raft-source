import { isToolCallEventType, type ExtensionFactory } from "@earendil-works/pi-coding-agent";

/**
 * Gives every bash tool call a default timeout.
 *
 * Pi's bash tool has no default timeout: the schema says "Timeout in seconds (optional, no
 * default timeout)", and the timer is only armed when the MODEL passes one. A command that never
 * returns therefore blocks the agent forever: the runtime stays inside the tool call, the daemon
 * sees a busy runtime that is not making progress, and nothing recovers it.
 *
 * That is not hypothetical. On 2026-11-08 a Pi agent spent 10 hours inside one headless Chrome
 * call, and the daemon's 15-minute no-progress check never even looked at it: that check only
 * runs while the last activity was "working" or "thinking", never while a tool call is in flight
 * (packages/daemon/src/agentProcessManager.ts, markRuntimeProgressStaleIfNeeded).
 *
 * Pi already knows how to end a timed-out command: it kills the process tree and returns the
 * timeout as the tool result, so the agent continues with the next turn. This only supplies the
 * timeout the model did not. A timeout the model chose is never overwritten.
 */

/** 30 minutes: long enough for a build or a big download, short enough to unblock the agent. */
export const PI_DEFAULT_BASH_TIMEOUT_SECONDS = 30 * 60;

export function piBashTimeoutSeconds(input: { timeout?: unknown }): number | undefined {
  const timeout = input.timeout;
  if (typeof timeout === "number" && Number.isFinite(timeout) && timeout > 0) return timeout;
  return undefined;
}

/** Inline Pi extension: fills in the timeout for bash calls that do not carry one. */
export function createPiBashTimeoutExtension(): ExtensionFactory {
  return (pi) => {
    pi.on("tool_call", (event) => {
      if (!isToolCallEventType("bash", event)) return undefined;
      const input = event.input as { timeout?: unknown };
      if (piBashTimeoutSeconds(input) !== undefined) return undefined;
      input.timeout = PI_DEFAULT_BASH_TIMEOUT_SECONDS;
      return undefined;
    });
  };
}
