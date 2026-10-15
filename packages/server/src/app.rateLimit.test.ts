import assert from "node:assert/strict";
import { asMachineId } from "@botiverse/raft-shared";
import {
  loginRateLimitConfig,
  rateLimitUserMachineOrIpKey,
  rateLimitUserOrIpKey,
  shouldSkipAttachmentRateLimit,
  shouldSkipAuthRateLimit,
  shouldSkipProductFeedbackRateLimit,
} from "./app";

test("the login rate limit is opt-in: unset means upstream behaviour, unchanged", () => {
  assert.equal(loginRateLimitConfig({} as NodeJS.ProcessEnv), null);
  assert.equal(loginRateLimitConfig({ RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "" } as NodeJS.ProcessEnv), null);
  assert.equal(loginRateLimitConfig({ RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "   " } as NodeJS.ProcessEnv), null);
});

test("the login rate limit honours its settings, with a 15 minute default window", () => {
  assert.deepEqual(loginRateLimitConfig({ RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "10" } as NodeJS.ProcessEnv), {
    max: 10,
    windowMs: 15 * 60 * 1000,
  });
  assert.deepEqual(
    loginRateLimitConfig({
      RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "3",
      RAFT_AUTH_LOGIN_RATE_LIMIT_WINDOW_MS: "60000",
    } as NodeJS.ProcessEnv),
    { max: 3, windowMs: 60000 },
  );
});

test("a bad login rate limit is a configuration ERROR: it throws instead of substituting another quota", () => {
  // @Dassai 2026-10-15: an explicit error, never a silent swap to a different limit. The deploy step runs this
  // same function against the candidate configuration before stopping the running instance, so a typo cannot
  // take the service down either.
  assert.throws(() => loginRateLimitConfig({ RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "ten" } as NodeJS.ProcessEnv), /must be a positive integer/);
  assert.throws(() => loginRateLimitConfig({ RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "0" } as NodeJS.ProcessEnv), /must be a positive integer/);
  assert.throws(
    () =>
      loginRateLimitConfig({
        RAFT_AUTH_LOGIN_RATE_LIMIT_MAX: "5",
        RAFT_AUTH_LOGIN_RATE_LIMIT_WINDOW_MS: "soon",
      } as NodeJS.ProcessEnv),
    /RAFT_AUTH_LOGIN_RATE_LIMIT_WINDOW_MS must be a positive integer/,
  );
});

test("rate limit keys preserve authenticated principal identity before IP fallback", () => {
  assert.equal(rateLimitUserOrIpKey({ userId: "user-1", ip: "2001:db8::1" }), "user-1");
  assert.equal(
    rateLimitUserMachineOrIpKey({
      userId: undefined,
      machineId: asMachineId("machine-1"),
      ip: "2001:db8::1",
    }),
    "machine-1",
  );
});

test("rate limit IP fallback normalizes IPv6 subnets", () => {
  assert.equal(rateLimitUserOrIpKey({ ip: "2001:db8::1" }), "2001:db8::/56");
  assert.equal(rateLimitUserMachineOrIpKey({ ip: "2001:db8::abcd:1" }), "2001:db8::/56");
});

test("rate limit IP fallback keeps IPv4 addresses as exact buckets", () => {
  assert.equal(rateLimitUserOrIpKey({ ip: "192.0.2.128" }), "192.0.2.128");
  assert.equal(rateLimitUserMachineOrIpKey({ ip: "::ffff:192.0.2.128" }), "192.0.2.128");
});

test("attachment rate limits isolate the pglite harness without weakening production", () => {
  const harnessRequests = [
    { method: "POST", surface: "upload" },
    { method: "GET", surface: "download" },
    // The download limiter is mounted before the upload router, so the upload
    // POST also traverses this surface.
    { method: "POST", surface: "download" },
  ] as const;

  for (const request of harnessRequests) {
    assert.equal(
      shouldSkipAttachmentRateLimit({ isTestEnv: true, ...request }),
      true,
      `${request.method} ${request.surface} should not inherit a finite shared bucket in the pglite harness`,
    );
  }

  assert.equal(
    shouldSkipAttachmentRateLimit({ isTestEnv: false, method: "POST", surface: "upload" }),
    false,
  );
  assert.equal(
    shouldSkipAttachmentRateLimit({ isTestEnv: false, method: "GET", surface: "download" }),
    false,
  );
  assert.equal(
    shouldSkipAttachmentRateLimit({ isTestEnv: false, method: "GET", surface: "upload" }),
    true,
  );
});

test("auth rate-limit override is explicit and confined to the pglite test harness", () => {
  assert.equal(
    shouldSkipAuthRateLimit({ isTestEnv: true, skipInTestHarness: true, path: "/login" }),
    true,
  );
  assert.equal(
    shouldSkipAuthRateLimit({ isTestEnv: true, skipInTestHarness: false, path: "/login" }),
    false,
  );
  assert.equal(
    shouldSkipAuthRateLimit({ isTestEnv: false, skipInTestHarness: true, path: "/login" }),
    false,
  );

  for (const path of ["/invite-info", "/accept-invite"]) {
    assert.equal(
      shouldSkipAuthRateLimit({ isTestEnv: false, skipInTestHarness: false, path }),
      true,
      `${path} keeps its existing dedicated-limiter exemption`,
    );
  }
});

test("product feedback keeps production writes limited while tests and slockdev stay repeatable", () => {
  for (const method of ["GET", "HEAD", "OPTIONS"]) {
    assert.equal(
      shouldSkipProductFeedbackRateLimit({
        isSlockdev: false,
        isTestEnv: false,
        method,
      }),
      true,
      `${method} is a read-only feedback operation`,
    );
  }

  assert.equal(
    shouldSkipProductFeedbackRateLimit({
      isSlockdev: false,
      isTestEnv: false,
      method: "POST",
    }),
    false,
  );
  assert.equal(
    shouldSkipProductFeedbackRateLimit({
      isSlockdev: false,
      isTestEnv: true,
      method: "POST",
    }),
    true,
  );
  assert.equal(
    shouldSkipProductFeedbackRateLimit({
      isSlockdev: true,
      isTestEnv: false,
      method: "POST",
    }),
    true,
  );
});
