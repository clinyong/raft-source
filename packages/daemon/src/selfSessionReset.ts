import type { MachineToServerMessage } from "@botiverse/raft-shared";

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

/** Whether a reset may have retired the old session and must fail closed. */
export function oldSessionRetired(state: SelfSessionResetState | undefined): boolean {
  return Boolean(state && state.kind !== "none" && state.kind !== "requested"
    && !(state.kind === "failed" && state.sessionCleared === false));
}

type ResetFailureReason = "restart_timeout" | "launch_exited";
type ResetRecord<T> = {
  state: SelfSessionResetState;
  oldSessionId: string | null;
  failureReason: ResetFailureReason | null;
  timer: ReturnType<typeof setTimeout> | null;
  cancelPending: boolean;
  transient: T[];
  heldInbox: T[];
  pendingBodies: { launchId: string; ids: Set<string> } | null;
  /** The stop the reset itself triggered is expected next: the daemon asked
   * the server to clear the session, and the server stops the agent to do it.
   * That one stop is a HANDOFF (the read position survives it); any other
   * stop — the user stopping the agent — is an ending (everything is
   * forgotten). The reason distinguishes them, not the reset window. */
  handoffStopExpected: boolean;
};

/** All reset-owned data is scoped to one record per agent. */
export class SelfSessionResetCoordinator<T extends { message_id?: string }> {
  private readonly records = new Map<string, ResetRecord<T>>();
  constructor(private readonly effects: {
    send: (message: MachineToServerMessage) => void;
    error: (agentId: string, launchId: string, step: string) => void;
    resume: (agentId: string, launchId: string) => void;
    replacementActive: (agentId: string, launchId: string, oldSessionId: string) => boolean;
  }) {}

  request(agentId: string, launchId: string, sessionId: string): "accepted" | "stale" {
    let state = this.state(agentId) ?? { kind: "none" };
    if (state.kind === "failed" && state.sessionCleared === false && state.launchId !== launchId) {
      state = reduceSelfSessionReset(state, { kind: "launch_changed", launchId });
    }
    if (state.kind !== "none" && (state.launchId !== launchId || state.kind === "failed")) return "stale";
    this.setOldSession(agentId, sessionId);
    this.setState(agentId, reduceSelfSessionReset(state, { kind: "request", launchId }));
    return "accepted";
  }

  requestingLaunchExited(agentId: string, launchId: string, sessionId: string): void {
    const state = this.state(agentId);
    if (state?.kind !== "requested" || state.launchId !== launchId) return;
    this.setState(agentId, reduceSelfSessionReset(state, { kind: "abort", launchId }));
    this.setFailureReason(agentId, "launch_exited");
    this.effects.error(agentId, launchId, "requesting_launch_exited");
    this.effects.send({ type: "agent:self_session_reset:failed", agentId, launchId, sessionId, reason: "launch_exited" });
  }

  arm(agentId: string, launchId: string, sessionId: string, delay = 120_000): void {
    this.armTimer(agentId, () => this.expire(agentId, launchId, sessionId), delay);
  }

  expire(agentId: string, launchId: string, sessionId: string): void {
    this.clearTimer(agentId);
    const state = this.state(agentId);
    if (!state || state.kind === "none" || state.launchId !== launchId) return;
    // Session initialization, not the duration of its first task, proves a
    // replacement started. Keep the held inbox until that task completes.
    if (this.effects.replacementActive(agentId, launchId, sessionId)) {
      this.arm(agentId, launchId, sessionId);
      return;
    }
    if (state.kind === "clearing" && !this.cancelPending(agentId)) {
      this.setCancelPending(agentId, true);
      this.effects.send({ type: "agent:self_session_reset:cancel", agentId, launchId, sessionId });
      this.arm(agentId, launchId, sessionId, 5_000);
    } else if (state.kind === "clearing") {
      this.setCancelPending(agentId, false);
      const unknown = reduceSelfSessionReset(state, { kind: "abort", launchId });
      this.setState(agentId, reduceSelfSessionReset(unknown, { kind: "retry_start", launchId }));
      // The retry clears the session too, so it stops the agent the same way.
      this.setHandoffStopExpected(agentId, true);
      this.effects.send({ type: "agent:self_session_reset:recover", agentId, launchId, sessionId });
      this.arm(agentId, launchId, sessionId);
    } else if (state.kind === "uncertain" && state.retries === 1) {
      this.setState(agentId, reduceSelfSessionReset(state, { kind: "abort", launchId }));
      this.setFailureReason(agentId, "restart_timeout");
      this.effects.error(agentId, launchId, "replacement_start_timeout");
      this.effects.send({ type: "agent:self_session_reset:failed", agentId, launchId, sessionId, reason: "restart_timeout" });
    }
  }

  cancelResult(agentId: string, launchId: string, notStarted: boolean): void {
    const state = this.state(agentId);
    const sessionId = this.oldSession(agentId);
    if (!state || state.kind !== "clearing" || state.launchId !== launchId || !sessionId || !this.cancelPending(agentId)) return;
    this.clearTimer(agentId);
    if (notStarted) {
      this.setCancelPending(agentId, false);
      this.reject(agentId, launchId);
    } else this.expire(agentId, launchId, sessionId);
  }

  reject(agentId: string, launchId: string): void {
    const state = this.state(agentId);
    if (!state || state.kind !== "clearing" || state.launchId !== launchId) return;
    this.setState(agentId, reduceSelfSessionReset(state, { kind: "clear_rejected", launchId }));
    this.effects.error(agentId, launchId, "clear_not_started");
    this.clearTimer(agentId);
    this.setCancelPending(agentId, false);
    // The clear never started, so no stop is coming: nothing is handed off.
    this.setHandoffStopExpected(agentId, false);
    this.setOldSession(agentId, null);
    this.effects.resume(agentId, launchId);
  }

  replay(): void {
    for (const [agentId, record] of this.records) {
      const { state, oldSessionId: sessionId, failureReason } = record;
      if (!sessionId || state.kind === "none") continue;
      if (state.kind === "uncertain" && state.retries === 1) {
        this.effects.send({ type: "agent:self_session_reset:recover", agentId, launchId: state.launchId, sessionId });
      } else if (state.kind === "failed" && failureReason) {
        this.effects.send({ type: "agent:self_session_reset:failed", agentId, launchId: state.launchId, sessionId, reason: failureReason });
      }
    }
  }

  blocksInput(agentId: string, launchId: string | null, sessionId: string | null): boolean {
    const state = this.state(agentId);
    if (!state || state.kind === "none") return false;
    if (launchId && blocksOldLaunchDelivery(state, launchId)) return true;
    return Boolean(this.oldSession(agentId) && sessionId === this.oldSession(agentId) && oldSessionRetired(state));
  }

  blocksOldLaunch(agentId: string, launchId: string | null): boolean {
    const state = this.state(agentId);
    return Boolean(state && launchId && blocksOldLaunchDelivery(state, launchId));
  }

  holdsTransient(agentId: string, launchId: string | null, sessionId: string | null, deliverable: boolean): boolean {
    const state = this.state(agentId);
    return Boolean(state && state.kind !== "none" && !(state.kind === "failed" && state.sessionCleared === false)
      && (!launchId || blocksOldLaunchDelivery(state, launchId) || sessionId === this.oldSession(agentId) || !deliverable));
  }

  holdsInbox(agentId: string): boolean { return oldSessionRetired(this.state(agentId)); }

  blocksStart(agentId: string, launchId: string | undefined, sessionId: string | null | undefined, currentLaunchId: string | null | undefined): boolean {
    const state = this.state(agentId);
    if (!state || state.kind === "none" || !oldSessionRetired(state) || launchId === state.launchId) return false;
    return Boolean((this.oldSession(agentId) && sessionId === this.oldSession(agentId)) || currentLaunchId === state.launchId);
  }

  turnEnded(agentId: string, launchId: string, sessionId: string | null | undefined, successful: boolean): boolean {
    const state = this.state(agentId);
    if (!state || state.kind === "none") return false;
    const oldSession = this.oldSession(agentId);
    if (launchId !== state.launchId && oldSession && sessionId && sessionId !== oldSession && successful) {
      this.complete(agentId);
      return false;
    }
    const next = reduceSelfSessionReset(state, { kind: "turn_end", launchId });
    this.setState(agentId, next);
    return state.kind === "requested" && next.kind === "paused";
  }

  beginClear(agentId: string, launchId: string, sessionId: string | null): void {
    const state = this.state(agentId);
    if (state) this.setState(agentId, reduceSelfSessionReset(state, { kind: "clear_started", launchId }));
    if (sessionId) {
      this.setOldSession(agentId, sessionId);
      this.arm(agentId, launchId, sessionId);
    }
    // The server clears the session by stopping the agent: the stop that
    // follows this call is the reset's own, and it is a handoff.
    this.setHandoffStopExpected(agentId, true);
    this.effects.send({ type: "agent:self_session_reset:ready", agentId, launchId, sessionId });
  }

  /**
   * Whether the stop being handled now is the one the reset triggered. True
   * exactly once per expected stop: the expectation is consumed here, so a
   * later stop — the user stopping the replacement before its first turn
   * ended, say — is not a handoff and may forget everything.
   */
  consumesHandoffStop(agentId: string): boolean {
    const record = this.records.get(agentId);
    if (!record || !record.handoffStopExpected) return false;
    record.handoffStopExpected = false;
    return true;
  }

  private setHandoffStopExpected(agentId: string, expected: boolean): void {
    this.ensure(agentId).handoffStopExpected = expected;
  }


  private ensure(agentId: string): ResetRecord<T> {
    let record = this.records.get(agentId);
    if (!record) {
      record = { state: { kind: "none" }, oldSessionId: null, failureReason: null, timer: null, cancelPending: false, transient: [], heldInbox: [], pendingBodies: null, handoffStopExpected: false };
      this.records.set(agentId, record);
    }
    return record;
  }

  state(agentId: string): SelfSessionResetState | undefined { return this.records.get(agentId)?.state; }
  setState(agentId: string, state: SelfSessionResetState): void { this.ensure(agentId).state = state; }
  oldSession(agentId: string): string | null { return this.records.get(agentId)?.oldSessionId ?? null; }
  setOldSession(agentId: string, id: string | null): void { this.ensure(agentId).oldSessionId = id; }
  setFailureReason(agentId: string, reason: ResetFailureReason): void { this.ensure(agentId).failureReason = reason; }
  cancelPending(agentId: string): boolean { return this.records.get(agentId)?.cancelPending ?? false; }
  setCancelPending(agentId: string, pending: boolean): void { this.ensure(agentId).cancelPending = pending; }
  clearTimer(agentId: string): void {
    const record = this.records.get(agentId);
    if (record?.timer) clearTimeout(record.timer);
    if (record) record.timer = null;
  }
  armTimer(agentId: string, callback: () => void, delay: number): void {
    this.clearTimer(agentId);
    const timer = setTimeout(callback, delay);
    timer.unref?.();
    this.ensure(agentId).timer = timer;
  }
  clearTimers(): void { for (const id of this.records.keys()) this.clearTimer(id); }
  held(agentId: string): readonly T[] { return this.records.get(agentId)?.heldInbox ?? []; }
  suppressHeld(agentId: string, consumed: (message: T) => boolean): number {
    const record = this.records.get(agentId);
    if (!record) return 0;
    const before = record.heldInbox.length;
    record.heldInbox = record.heldInbox.filter((message) => !consumed(message));
    return before - record.heldInbox.length;
  }
  stageBodies(agentId: string, launchId: string | null, messages: readonly T[]): void {
    const record = this.records.get(agentId);
    if (!record) return;
    const ids = new Set(messages.flatMap((message) => message.message_id ? [message.message_id] : []));
    record.pendingBodies = launchId && ids.size ? { launchId, ids } : null;
  }
  launchRebound(agentId: string, previousLaunchId: string | null, nextLaunchId: string | null): void {
    const record = this.records.get(agentId);
    if (record && previousLaunchId !== nextLaunchId) record.pendingBodies = null;
  }
  confirmBodyProgress(agentId: string, launchId: string | null): void {
    const record = this.records.get(agentId);
    const staged = record?.pendingBodies;
    if (!record || !staged || staged.launchId !== launchId) return;
    this.suppressHeld(agentId, (message) => Boolean(message.message_id && staged.ids.has(message.message_id)));
    record.pendingBodies = null;
  }
  hold(agentId: string, messages: readonly T[]): void {
    const held = this.ensure(agentId).heldInbox;
    const ids = new Set(held.map((message) => message.message_id).filter(Boolean));
    for (const message of messages) {
      if (message.message_id && ids.has(message.message_id)) continue;
      held.push(message);
      if (message.message_id) ids.add(message.message_id);
    }
  }
  holdTransient(agentId: string, message: T): void {
    const queue = this.ensure(agentId).transient;
    if (!queue.includes(message)) queue.push(message);
  }
  shiftTransient(agentId: string): T | undefined { return this.records.get(agentId)?.transient.shift(); }
  restoreTransient(agentId: string, message: T): void { this.ensure(agentId).transient.unshift(message); }
  complete(agentId: string): void {
    const record = this.records.get(agentId);
    if (!record) return;
    this.clearTimer(agentId);
    // A replacement turn proves the old session was cleared. Exercise the
    // same confirmation/completion transitions used by the lifecycle model.
    if (record.state.kind === "clearing") record.state = reduceSelfSessionReset(record.state, { kind: "clear_confirmed", launchId: record.state.launchId });
    if (record.state.kind === "cleared" || record.state.kind === "uncertain") record.state = reduceSelfSessionReset(record.state, { kind: "completed", launchId: record.state.launchId });
    const transient = record.transient;
    this.records.delete(agentId);
    if (transient.length) this.ensure(agentId).transient = transient;
  }
}
