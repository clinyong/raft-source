/** State for a reset requested by the current launch. Effects (delivery,
 * server calls and timers) belong to the caller. A clear request whose outcome
 * is unknown is treated as potentially committed and never resumes the old launch. */
export type SelfSessionResetState =
  | { kind: "none" }
  | { kind: "requested"; launchId: string }
  | { kind: "paused"; launchId: string }
  | { kind: "clearing"; launchId: string }
  | { kind: "cleared"; launchId: string; retries: 0 | 1 }
  | { kind: "uncertain"; launchId: string; retries: 0 | 1 }
  | { kind: "failed"; launchId: string; sessionCleared: boolean | "unknown" };

export type SelfSessionResetEvent =
  | { kind: "request"; launchId: string }
  | { kind: "turn_end"; launchId: string }
  | { kind: "clear_started"; launchId: string }
  | { kind: "clear_rejected"; launchId: string }
  | { kind: "clear_confirmed"; launchId: string }
  | { kind: "retry_start"; launchId: string }
  | { kind: "completed"; launchId: string }
  | { kind: "abort"; launchId: string }
  | { kind: "launch_changed"; launchId: string };

export function reduceSelfSessionReset(
  state: SelfSessionResetState,
  event: SelfSessionResetEvent,
): SelfSessionResetState {
  switch (event.kind) {
    case "request":
      return state.kind === "none" ? { kind: "requested", launchId: event.launchId } : state;
    case "turn_end":
      return state.kind === "requested" && state.launchId === event.launchId
        ? { kind: "paused", launchId: state.launchId }
        : state;
    case "clear_started":
      return state.kind === "paused" && state.launchId === event.launchId
        ? { kind: "clearing", launchId: state.launchId }
        : state;
    case "clear_rejected":
      return state.kind === "clearing" && state.launchId === event.launchId
        ? { kind: "failed", launchId: state.launchId, sessionCleared: false }
        : state;
    case "clear_confirmed":
      return state.kind === "clearing" && state.launchId === event.launchId
        ? { kind: "cleared", launchId: state.launchId, retries: 0 }
        : state;
    case "retry_start":
      return (state.kind === "cleared" || state.kind === "uncertain") && state.launchId === event.launchId && state.retries === 0
        ? { ...state, retries: 1 }
        : state;
    // The caller must verify that a fresh session actually started before
    // signalling completion; acceptance of a start request is not enough.
    case "completed":
      return (state.kind === "cleared" || state.kind === "uncertain") && state.launchId === event.launchId ? { kind: "none" } : state;
    case "abort":
      if (state.kind === "clearing" && state.launchId === event.launchId) {
        return { kind: "uncertain", launchId: state.launchId, retries: 0 };
      }
      return state.kind !== "none" && state.launchId === event.launchId && state.kind !== "failed"
        ? { kind: "failed", launchId: state.launchId, sessionCleared: state.kind === "uncertain" ? "unknown" : state.kind === "cleared" }
        : state;
    case "launch_changed":
      // A launch change before clearing cancels the request. After clearing,
      // a new launch is expected; keep the state until its outcome is known.
      if (state.kind === "none" || state.launchId === event.launchId) return state;
      if (state.kind === "cleared" || state.kind === "uncertain") return state;
      if (state.kind === "clearing") return { kind: "uncertain", launchId: state.launchId, retries: 0 };
      return state.kind === "failed" && state.sessionCleared !== false ? state : { kind: "none" };
  }
}

/** Block writes to the old launch until the reset outcome permits resumption.
 * Block as soon as requested so arrivals before turn_end stay queued.
 * A definitive pre-clear failure permits the caller to resume it. Unknown
 * outcomes fail closed, since the server may already have cleared the session. */
export function blocksOldLaunchDelivery(state: SelfSessionResetState, launchId: string): boolean {
  if (state.kind === "none" || state.launchId !== launchId) return false;
  if (state.kind === "failed") return state.sessionCleared !== false;
  return true;
}
