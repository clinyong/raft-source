// The live idle probe of the Pi idle recycle strategy.
//
// The idle fact and the token fact are independent. The 2026-11-09 stall
// logged "context_unavailable" and "tokens=unknown" while the session had
// already settled, so a probe that gave up on a reason would have reported
// "unknown" exactly when the delivery was needed.

import assert from "node:assert/strict";
import { test } from "vitest";
import { createPiIdleRecycle } from "./piIdleRecycle";

test("the idle probe reports the session state even when the observation carries a reason", () => {
  const strategy = createPiIdleRecycle(() => ({
    sessionId: "session-1",
    tokens: null,
    threshold: null,
    reason: "context_unavailable",
    isIdle: true,
  }));

  assert.deepEqual(strategy.probeIdle?.(), { isIdle: true });
});

test("the idle probe reports the session state when the observation is complete", () => {
  const strategy = createPiIdleRecycle(() => ({
    sessionId: "session-1",
    tokens: 1000,
    threshold: 500,
    isIdle: false,
  }));

  assert.deepEqual(strategy.probeIdle?.(), { isIdle: false });
});

test("the idle probe stays silent when the session reports no idle state", () => {
  const strategy = createPiIdleRecycle(() => ({
    sessionId: null,
    tokens: null,
    threshold: null,
    reason: "session_unavailable",
  }));

  assert.equal(strategy.probeIdle?.(), null);
});
