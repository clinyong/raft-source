import { describe, expect, it } from "vitest";
import { SelfSessionResetCoordinator, blocksOldLaunchDelivery, reduceSelfSessionReset } from "./selfSessionReset";

describe("self session reset lifecycle", () => {
  it("waits for the requesting launch's turn end and deduplicates", () => {
    const pending = reduceSelfSessionReset({ kind: "none" }, { kind: "request", launchId: "a" });
    expect(reduceSelfSessionReset(pending, { kind: "request", launchId: "a" })).toEqual(pending);
    expect(reduceSelfSessionReset(pending, { kind: "turn_end", launchId: "b" })).toEqual(pending);
    const paused = reduceSelfSessionReset(pending, { kind: "turn_end", launchId: "a" });
    expect(paused).toEqual({ kind: "paused", launchId: "a" });
    expect(reduceSelfSessionReset(paused, { kind: "request", launchId: "a" })).toEqual(paused);
  });

  it("cancels on launch replacement and ignores unrelated cancellation", () => {
    const pending = { kind: "requested", launchId: "a" } as const;
    expect(reduceSelfSessionReset(pending, { kind: "abort", launchId: "b" })).toEqual(pending);
    expect(reduceSelfSessionReset(pending, { kind: "launch_changed", launchId: "b" })).toEqual({ kind: "none" });
    const failed = reduceSelfSessionReset(pending, { kind: "abort", launchId: "a" });
    expect(failed).toEqual({ kind: "failed", launchId: "a", sessionCleared: false });
    expect(reduceSelfSessionReset(failed, { kind: "request", launchId: "a" })).toEqual(failed);
  });

  it("distinguishes failure before and after clearing and limits retry", () => {
    const paused = { kind: "paused", launchId: "a" } as const;
    expect(reduceSelfSessionReset(paused, { kind: "abort", launchId: "a" }))
      .toEqual({ kind: "failed", launchId: "a", sessionCleared: false });
    const clearing = reduceSelfSessionReset(paused, { kind: "clear_started", launchId: "a" });
    const cleared = reduceSelfSessionReset(clearing, { kind: "clear_confirmed", launchId: "a" });
    expect(cleared).toEqual({ kind: "cleared", launchId: "a", retries: 0 });
    const retry = reduceSelfSessionReset(cleared, { kind: "retry_start", launchId: "a" });
    expect(retry).toEqual({ kind: "cleared", launchId: "a", retries: 1 });
    expect(reduceSelfSessionReset(retry, { kind: "retry_start", launchId: "a" })).toEqual(retry);
    expect(reduceSelfSessionReset(retry, { kind: "abort", launchId: "a" }))
      .toEqual({ kind: "failed", launchId: "a", sessionCleared: true });
  });

  it("ignores stale confirmation and keeps the cleared state during replacement", () => {
    const paused = { kind: "paused", launchId: "a" } as const;
    expect(reduceSelfSessionReset(paused, { kind: "clear_confirmed", launchId: "b" })).toEqual(paused);
    const clearing = reduceSelfSessionReset(paused, { kind: "clear_started", launchId: "a" });
    const cleared = reduceSelfSessionReset(clearing, { kind: "clear_confirmed", launchId: "a" });
    expect(reduceSelfSessionReset(cleared, { kind: "launch_changed", launchId: "b" })).toEqual(cleared);
    expect(reduceSelfSessionReset(cleared, { kind: "completed", launchId: "a" })).toEqual({ kind: "none" });
  });

  it("does not assume a timed-out clear failed", () => {
    const paused = { kind: "paused", launchId: "a" } as const;
    const clearing = reduceSelfSessionReset(paused, { kind: "clear_started", launchId: "a" });
    const uncertain = reduceSelfSessionReset(clearing, { kind: "abort", launchId: "a" });
    expect(uncertain).toEqual({ kind: "uncertain", launchId: "a", retries: 0 });
    expect(reduceSelfSessionReset(uncertain, { kind: "launch_changed", launchId: "b" })).toEqual(uncertain);
    const retry = reduceSelfSessionReset(uncertain, { kind: "retry_start", launchId: "a" });
    expect(retry).toEqual({ kind: "uncertain", launchId: "a", retries: 1 });
    expect(reduceSelfSessionReset(retry, { kind: "retry_start", launchId: "a" })).toEqual(retry);
    expect(reduceSelfSessionReset(retry, { kind: "abort", launchId: "a" }))
      .toEqual({ kind: "failed", launchId: "a", sessionCleared: "unknown" });
    expect(reduceSelfSessionReset(clearing, { kind: "clear_rejected", launchId: "a" }))
      .toEqual({ kind: "failed", launchId: "a", sessionCleared: false });
  });

  it("blocks the old launch across clearing, unknown outcome, and failed restart", () => {
    expect(blocksOldLaunchDelivery({ kind: "requested", launchId: "a" }, "a")).toBe(true);
    expect(blocksOldLaunchDelivery({ kind: "paused", launchId: "a" }, "a")).toBe(true);
    expect(blocksOldLaunchDelivery({ kind: "clearing", launchId: "a" }, "a")).toBe(true);
    expect(blocksOldLaunchDelivery({ kind: "uncertain", launchId: "a", retries: 1 }, "a")).toBe(true);
    expect(blocksOldLaunchDelivery({ kind: "failed", launchId: "a", sessionCleared: "unknown" }, "a")).toBe(true);
    expect(blocksOldLaunchDelivery({ kind: "failed", launchId: "a", sessionCleared: true }, "a")).toBe(true);
    expect(blocksOldLaunchDelivery({ kind: "failed", launchId: "a", sessionCleared: false }, "a")).toBe(false);
    expect(blocksOldLaunchDelivery({ kind: "paused", launchId: "a" }, "b")).toBe(false);
  });

});

describe("SelfSessionResetCoordinator.request rejection reasons", () => {
  const makeCoordinator = (overrides: {
    replacementActive?: () => boolean;
    replacementSessionObserved?: (agentId: string, launchId: string, retiredSessionIds: readonly string[]) => boolean;
  } = {}) => {
    const debug: Array<{ event: string; details: Record<string, unknown> }> = [];
    const coordinator = new SelfSessionResetCoordinator<never>({
      send: () => {},
      error: () => {},
      resume: () => {},
      replacementActive: overrides.replacementActive ?? (() => false),
      replacementSessionObserved: overrides.replacementSessionObserved ?? (() => false),
      debug: (event, details) => debug.push({ event, details }),
    });
    return { coordinator, debug };
  };

  it("reports reset_in_progress while the previous reset is still finishing", () => {
    const { coordinator, debug } = makeCoordinator();
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("reset_in_progress");
    expect(debug.at(-1)).toEqual({
      event: "request_rejected",
      details: { reason: "reset_in_progress", requestedLaunchId: "launch-b", stateKind: "requested", stateLaunchId: "launch-a" },
    });
  });

  it("reports stale_launch when the previous reset failed for the requesting launch", () => {
    const { coordinator, debug } = makeCoordinator();
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    coordinator.requestingLaunchExited("agent", "launch-a", "session-1");
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("stale_launch");
    expect(debug.at(-1)).toEqual({
      event: "request_rejected",
      details: { reason: "stale_launch", requestedLaunchId: "launch-a", stateKind: "failed", stateLaunchId: "launch-a", sessionCleared: false },
    });
  });
});

describe("SelfSessionResetCoordinator.request with an unresolved older record", () => {
  it("treats a failed record from an older launch as reset_in_progress", () => {
    const coordinator = new SelfSessionResetCoordinator<never>({
      send: () => {}, error: () => {}, resume: () => {}, replacementActive: () => false,
      replacementSessionObserved: () => false,
    });
    coordinator.setState("agent", { kind: "failed", launchId: "launch-a", sessionCleared: "unknown" });
    // The current launch is the replacement; the record clears when its first
    // turn ends, so "Launch is no longer current" would be misleading.
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("reset_in_progress");
  });
});

// M1: the wait for the replacement's first turn was the only reason a second
// request was refused. Once the replacement reported a session of its own,
// the previous clear is proven and the new request may take the record over.
describe("SelfSessionResetCoordinator.request with a replacement that reported its session", () => {
  const makeCoordinator = (replacementSessionObserved: (agentId: string, launchId: string, retiredSessionIds: readonly string[]) => boolean) => {
    const debug: Array<{ event: string; details: Record<string, unknown> }> = [];
    const coordinator = new SelfSessionResetCoordinator<{ message_id?: string }>({
      send: () => {}, error: () => {}, resume: () => {}, replacementActive: () => false,
      replacementSessionObserved,
      debug: (event, details) => debug.push({ event, details }),
    });
    return { coordinator, debug };
  };

  it("accepts a second request while the replacement's first turn is still running", () => {
    const { coordinator, debug } = makeCoordinator((_agentId, launchId, retired) => launchId === "launch-b" && retired.includes("session-1"));
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    coordinator.setState("agent", { kind: "clearing", launchId: "launch-a" });
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("accepted");
    // The record is taken over, not completed: it ends with the new
    // replacement's first turn.
    expect(coordinator.state("agent")).toEqual({ kind: "requested", launchId: "launch-b" });
    expect(debug.at(-1)).toEqual({
      event: "request_accepted",
      details: {
        reason: "replacement_session_observed",
        requestedLaunchId: "launch-b",
        stateKind: "clearing",
        stateLaunchId: "launch-a",
      },
    });
  });

  it("keeps the messages the previous reset held when a second request takes the record over", () => {
    const { coordinator } = makeCoordinator(() => true);
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    // The first reset asked the server to clear the session, so the session is
    // retired and the second request may take the record over.
    coordinator.setState("agent", { kind: "clearing", launchId: "launch-a" });
    coordinator.hold("agent", [{ message_id: "held-1" }]);
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("accepted");
    expect(coordinator.held("agent")).toEqual([{ message_id: "held-1" }]);
  });

  it("still refuses a second request while the replacement has no session of its own", () => {
    const { coordinator } = makeCoordinator(() => false);
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    coordinator.setState("agent", { kind: "clearing", launchId: "launch-a" });
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("reset_in_progress");
  });

  it("refuses a start on a session an earlier reset retired", () => {
    const { coordinator } = makeCoordinator(() => true);
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    coordinator.setState("agent", { kind: "clearing", launchId: "launch-a" });
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("accepted");
    coordinator.setState("agent", { kind: "clearing", launchId: "launch-b" });
    // Both the session the first reset retired and the one the second reset
    // retired are refused: neither may be resumed.
    expect(coordinator.blocksStart("agent", "launch-c", "session-1", null)).toBe(true);
    expect(coordinator.blocksStart("agent", "launch-c", "session-2", null)).toBe(true);
    expect(coordinator.blocksStart("agent", "launch-c", "session-3", null)).toBe(false);
  });

  // The guards that protect a retired session must not depend on the current
  // state: a takeover puts the state back to `requested`, but the sessions the
  // earlier resets retired stay retired.
  it("keeps the retired-session guards during the takeover's requested window", () => {
    const { coordinator } = makeCoordinator(() => true);
    expect(coordinator.request("agent", "launch-a", "session-1")).toBe("accepted");
    coordinator.setState("agent", { kind: "clearing", launchId: "launch-a" });
    expect(coordinator.request("agent", "launch-b", "session-2")).toBe("accepted");
    // The takeover is in its requested window: the replacement has not started
    // clearing its own session yet.
    expect(coordinator.state("agent")).toEqual({ kind: "requested", launchId: "launch-b" });
    // A start on the session the first reset retired is refused, even though
    // the state says nothing is retired yet.
    expect(coordinator.blocksStart("agent", "launch-c", "session-1", null)).toBe(true);
    // The replacement's own session is not retired yet: it may keep running
    // and finish the turn it is running.
    expect(coordinator.blocksStart("agent", "launch-c", "session-2", null)).toBe(false);
    // Input to a retired session is refused as well.
    expect(coordinator.blocksInput("agent", null, "session-1")).toBe(true);
    expect(coordinator.blocksInput("agent", null, "session-2")).toBe(false);
    // The record still owns the inbox: the messages of the session it reset
    // belong to the next session, not to the stop.
    expect(coordinator.holdsInbox("agent")).toBe(true);
  });
});
