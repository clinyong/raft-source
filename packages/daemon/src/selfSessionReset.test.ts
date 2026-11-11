import { describe, expect, it } from "vitest";
import { blocksOldLaunchDelivery, reduceSelfSessionReset } from "./selfSessionReset";

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
