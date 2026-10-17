import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { compact, shouldCompact } from "@earendil-works/pi-coding-agent";
import {
  capPiCompactionSummaryOutput,
  createPiCompactionFitExtension,
  takePiCompactionSummaryOutputCap,
} from "./piCompactionFit";
import {
  PI_COMPACTION_POLICY_ENV_VAR,
  combinePiCompactionPolicyResolutions,
  decidePiCompactionPolicy,
  resolvePiCompactionPolicy,
} from "./piCompactionPolicy";

/**
 * The per-agent compaction policy (wiring candidate, 2026-10-17).
 *
 * What this pins: the pilot policy is configured per agent (`RAFT_PI_COMPACTION_POLICY` in the agent's own
 * env), it is opt-in (unset/disabled agents keep today's behaviour), the window comes from the model the
 * runtime actually resolved (an unresolved window applies nothing and says so), the derived trigger and
 * ceilings are the numbers the SDK will really use, and no agent id appears in the code.
 *
 * The two numbers that matter are checked against the pinned SDK rather than against themselves:
 * `shouldCompact` decides "above 60%", and the real `compact()` asks for the ceilings the policy promises.
 */

/** The first pilot's policy, exactly as an operator would set it on that agent — no agent id involved. */
const PILOT_POLICY = JSON.stringify({
  version: 1,
  enabled: true,
  triggerAboveRatio: 0.6,
  keepRecentTokens: 20_000,
  summaryOutputCapTokens: 16_384,
});

const env = (value: string | undefined): Record<string, string> =>
  value === undefined ? {} : { [PI_COMPACTION_POLICY_ENV_VAR]: value };

const model = {
  id: "policy-fixture",
  provider: "offline",
  api: "openai-completions",
  contextWindow: 1_000_000,
  maxTokens: 384_000,
};

test("the pilot policy resolves to a 60% trigger, 20k retained and the accepted 16,384 / 10,240 ceilings", () => {
  const resolution = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });
  assert.equal(resolution.status, "applied");
  assert.deepEqual(resolution.effective, {
    modelWindowTokens: 200_000,
    triggerAboveTokens: 120_000,
    reserveTokens: 80_000,
    keepRecentTokens: 20_000,
    summaryOutputCapTokens: 16_384,
    prefixOutputCapTokens: 10_240,
  });
  assert.match(resolution.readback, /window 200000/);
  assert.match(resolution.readback, /trigger above 120000 \(60%\) => reserve 80000/);
  assert.match(resolution.readback, /summary cap 16384 \(prefix 10240\)/);
});

test("the trigger is Pi's own: above 60% compacts, exactly 60% does not", () => {
  const { effective } = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });
  const settings = {
    enabled: true,
    reserveTokens: effective!.reserveTokens,
    keepRecentTokens: effective!.keepRecentTokens,
  };
  assert.equal(shouldCompact(effective!.triggerAboveTokens + 1, 200_000, settings), true);
  assert.equal(shouldCompact(effective!.triggerAboveTokens, 200_000, settings), false);
  // Without the policy the stock reserve is far smaller, so the same context would not compact yet.
  assert.equal(shouldCompact(effective!.triggerAboveTokens + 1, 200_000, { enabled: true, reserveTokens: 16_384, keepRecentTokens: 40_000 }), false);
});

test("the real SDK requests exactly the ceilings the policy promises", async () => {
  const { effective } = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });
  const preparation = {
    settings: { reserveTokens: effective!.reserveTokens, keepRecentTokens: effective!.keepRecentTokens },
    firstKeptEntryId: "retained",
    messagesToSummarize: [{ role: "user", content: "Authorized task and current progress", timestamp: 1 }],
    turnPrefixMessages: [{ role: "user", content: "Current turn checkpoint", timestamp: 2 }],
    isSplitTurn: true,
    tokensBefore: 600_001,
    previousSummary: undefined,
    fileOps: { read: new Set<string>(), written: new Set<string>(), edited: new Set<string>() },
  };
  // The wiring's two layers: the reserve moves the trigger, the adapter bounds the output.
  const cap = capPiCompactionSummaryOutput(preparation, model, effective!.summaryOutputCapTokens);
  assert.deepEqual(cap?.changed, ["history", "prefix"]);
  const caps: number[] = [];
  const stream = async (_m: unknown, _c: unknown, options: { maxTokens: number }) => {
    caps.push(options.maxTokens);
    return {
      result: async () => ({
        role: "assistant",
        content: [{ type: "text", text: "Deterministic task checkpoint." }],
        api: "openai-completions",
        provider: "offline",
        model: model.id,
        stopReason: "stop",
        timestamp: 1,
        usage: { input: 100, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 110, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      }),
    };
  };
  await compact(preparation as never, model as never, undefined, undefined, undefined, undefined, undefined, stream as never);
  assert.deepEqual(caps, [effective!.summaryOutputCapTokens, effective!.prefixOutputCapTokens]);
});

test("an agent without the policy behaves exactly as before (default/unselected control)", () => {
  const unset = resolvePiCompactionPolicy({ envVars: null, model: { contextWindow: 200_000 } });
  assert.equal(unset.status, "unset");
  assert.equal(unset.effective, null);
  assert.equal(unset.policy, null);
  assert.equal(unset.readback, "compaction pilot: not applied (not configured)");
  const other = resolvePiCompactionPolicy({ envVars: { OTHER_VAR: "1" }, model: { contextWindow: 200_000 } });
  assert.equal(other.status, "unset");
});

test("a disabled policy is inert and still reports itself", () => {
  const resolution = resolvePiCompactionPolicy({
    envVars: env(JSON.stringify({ version: 1, enabled: false })),
    model: { contextWindow: 200_000 },
  });
  assert.equal(resolution.status, "disabled");
  assert.equal(resolution.effective, null);
  assert.equal(resolution.policy?.enabled, false);
  assert.equal(resolution.readback, "compaction pilot: not applied (policy disabled)");
});

test("an unresolved model window applies nothing and says why", () => {
  for (const window of [undefined, null, 0, -1, Number.NaN]) {
    const resolution = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: window } });
    assert.equal(resolution.status, "unknown_model_window", `window=${String(window)}`);
    assert.equal(resolution.effective, null);
    assert.match(resolution.readback, /no resolved model window at session start/);
  }
  const noModel = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: undefined });
  assert.equal(noModel.status, "unknown_model_window");
});

test("a window too small to express the trigger is refused instead of rounding to a no-op", () => {
  const resolution = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 1 } });
  assert.equal(resolution.status, "window_incompatible");
  assert.match(resolution.readback, /cannot express a 60% trigger/);
});

test("a window that cannot keep 20k recent tokens below its own trigger is refused", () => {
  // 0.6 x 32768 = 19,660: the trigger would sit BELOW the retained history, so "above the trigger" would be
  // almost entirely retained context and the policy would churn every turn on a model this small.
  const resolution = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 32_768 } });
  assert.equal(resolution.status, "window_incompatible");
  assert.equal(resolution.effective, null);
  assert.match(resolution.readback, /triggers above 19660, less than twice the 20000 retained tokens/);
});

test("the same policy on a window that can host it still applies, on both sides of the line", () => {
  const smaller = JSON.stringify({
    version: 1, enabled: true, triggerAboveRatio: 0.6, keepRecentTokens: 5_000, summaryOutputCapTokens: 16_384,
  });
  const fits = resolvePiCompactionPolicy({ envVars: env(smaller), model: { contextWindow: 32_768 } });
  assert.equal(fits.status, "applied");
  assert.equal(fits.effective?.triggerAboveTokens, 19_660);
  assert.equal(fits.effective?.keepRecentTokens, 5_000);
  // 20,000 retained needs a trigger of at least 40,000, i.e. a window of at least 66,667.
  const boundary = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 66_667 } });
  assert.equal(boundary.status, "applied");
  assert.equal(boundary.effective?.triggerAboveTokens, 40_000);
  const justBelow = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 66_666 } });
  assert.equal(justBelow.status, "window_incompatible");
  assert.equal(justBelow.effective, null);
});

test("a policy that would silently change meaning is rejected, not coerced", () => {
  const cases: Array<[string, string]> = [
    ["not json", "not valid JSON"],
    ["[]", "not a JSON object"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 0.6, keepRecentTokens: 20_000 }), "summaryOutputCapTokens"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 0.6, keepRecentToken: 20_000, summaryOutputCapTokens: 16_384 }), "unknown field"],
    [JSON.stringify({ version: 2, enabled: true, triggerAboveRatio: 0.6, keepRecentTokens: 20_000, summaryOutputCapTokens: 16_384 }), "unsupported version"],
    [JSON.stringify({ version: 1, enabled: "true", triggerAboveRatio: 0.6, keepRecentTokens: 20_000, summaryOutputCapTokens: 16_384 }), "must be a boolean"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 0, keepRecentTokens: 20_000, summaryOutputCapTokens: 16_384 }), "strictly between 0 and 1"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 1, keepRecentTokens: 20_000, summaryOutputCapTokens: 16_384 }), "strictly between 0 and 1"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: "0.6", keepRecentTokens: 20_000, summaryOutputCapTokens: 16_384 }), "strictly between 0 and 1"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 0.6, keepRecentTokens: 0, summaryOutputCapTokens: 16_384 }), "keepRecentTokens"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 0.6, keepRecentTokens: 20_000.5, summaryOutputCapTokens: 16_384 }), "keepRecentTokens"],
    [JSON.stringify({ version: 1, enabled: true, triggerAboveRatio: 0.6, keepRecentTokens: 20_000, summaryOutputCapTokens: 0 }), "summaryOutputCapTokens"],
  ];
  for (const [raw, expected] of cases) {
    const resolution = resolvePiCompactionPolicy({ envVars: env(raw), model: { contextWindow: 200_000 } });
    assert.equal(resolution.status, "invalid", raw);
    assert.equal(resolution.effective, null);
    assert.ok(resolution.reason?.includes(expected), `${raw} => ${String(resolution.reason)}`);
    assert.match(resolution.readback, /^compaction pilot: not applied \(/);
  }
});

test("the window is re-resolved per call, so a model change re-derives the trigger and ceilings", () => {
  const windows = [100_000, 200_000, 1_000_000];
  const seen = windows.map((contextWindow) =>
    resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow } }).effective!,
  );
  assert.deepEqual(seen.map((e) => e.triggerAboveTokens), [60_000, 120_000, 600_000]);
  assert.deepEqual(seen.map((e) => e.reserveTokens), [40_000, 80_000, 400_000]);
  // The retained history and the output ceilings are window-independent: a bigger window must not
  // quietly raise them, and a smaller one must not quietly lower them.
  assert.deepEqual(seen.map((e) => e.keepRecentTokens), [20_000, 20_000, 20_000]);
  assert.deepEqual(seen.map((e) => e.summaryOutputCapTokens), [16_384, 16_384, 16_384]);
  assert.deepEqual(seen.map((e) => e.prefixOutputCapTokens), [10_240, 10_240, 10_240]);
});

test("no agent id is baked into the policy code — the pilot is whoever carries the env var", () => {
  const source = readFileSync(new URL("./piCompactionPolicy.ts", import.meta.url), "utf8");
  assert.equal(/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/.test(source), false, "policy code must not contain a UUID");
  assert.match(source, /PI_COMPACTION_POLICY_ENV_VAR = "RAFT_PI_COMPACTION_POLICY"/);
});

test("the two moments are reconciled: cap built before the model, settings applied after it", () => {
  const cap = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });
  const sessionSame = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });

  // Both agree: the settings follow the authoritative window, no mismatch reported.
  const agreeing = combinePiCompactionPolicyResolutions(cap, sessionSame);
  assert.equal(agreeing.apply?.status, "applied");
  assert.equal(agreeing.windowMismatch, null);
  assert.equal(agreeing.readback, sessionSame.readback);

  // The window moved in between: the settings still follow the FINAL window (the ceiling does not depend on
  // it), and the change is reported rather than hidden.
  const sessionChanged = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 400_000 } });
  const changed = combinePiCompactionPolicyResolutions(cap, sessionChanged);
  assert.equal(changed.apply?.effective?.modelWindowTokens, 400_000);
  assert.equal(changed.apply?.effective?.triggerAboveTokens, 240_000);
  assert.deepEqual(changed.windowMismatch, { capWindowTokens: 200_000, sessionWindowTokens: 400_000 });
  assert.match(changed.readback, /window changed between the cap build and the final resolution: 200000 -> 400000/);

  // No cap ceiling to install: nothing is applied at all, because an earlier trigger with an unbounded
  // summary request is exactly the combination the policy exists to prevent.
  const capInert = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: undefined });
  const noCap = combinePiCompactionPolicyResolutions(capInert, sessionSame);
  assert.equal(noCap.apply, null);
  assert.match(noCap.readback, /the summary cap could not be built for this session: no resolved model window/);

  // The session cannot host it: nothing is applied.
  const sessionInert = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 32_768 } });
  const noTrigger = combinePiCompactionPolicyResolutions(cap, sessionInert);
  assert.equal(noTrigger.apply, null);
  assert.match(noTrigger.readback, /less than twice the 20000 retained tokens/);

  // Two unset policies: nothing to report beyond the ordinary case.
  const unset = resolvePiCompactionPolicy({ envVars: null, model: { contextWindow: 200_000 } });
  const bothUnset = combinePiCompactionPolicyResolutions(unset, unset);
  assert.equal(bothUnset.apply, null);
  assert.equal(bothUnset.readback, "compaction pilot: not applied (not configured)");
});

test("an inert final result clears the ceiling, and the readback describes the state that is left", () => {
  const cap = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });
  const applied = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 200_000 } });
  const incompatible = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 32_768 } });

  // Applied: the option keeps the policy ceiling, and nothing claims a clearance.
  const kept: { summaryOutputCapTokens?: number } = { summaryOutputCapTokens: 16_384 };
  const keptDecision = decidePiCompactionPolicy({ capResolution: cap, sessionResolution: applied, capOption: kept });
  assert.equal(kept.summaryOutputCapTokens, 16_384);
  assert.equal(keptDecision.capCeilingActive, true);
  assert.equal(keptDecision.capInstalledWithoutTrigger, false);
  assert.equal(keptDecision.readback, applied.readback);

  // Inert: the ceiling goes with it, and BOTH the readback and the flags say so.
  const cleared: { summaryOutputCapTokens?: number } = { summaryOutputCapTokens: 16_384 };
  const clearedDecision = decidePiCompactionPolicy({ capResolution: cap, sessionResolution: incompatible, capOption: cleared });
  assert.equal(cleared.summaryOutputCapTokens, undefined);
  assert.equal(clearedDecision.apply, null);
  assert.equal(clearedDecision.capCeilingActive, false);
  assert.equal(clearedDecision.capInstalledWithoutTrigger, false);
  assert.match(clearedDecision.readback, /less than twice the 20000 retained tokens/);
  assert.match(clearedDecision.readback, /the ceiling was cleared with it/);
  assert.doesNotMatch(clearedDecision.readback, /stays in the extension/);
});

test("the ceiling really is late-bound: the adapter reads whatever the option holds at compaction time", () => {
  // This is the mechanism r3 relies on. The factory is built once, during session creation, but the handler
  // reads the option per request -- so clearing it after an inert final resolution disables the cap layer.
  const option: { summaryOutputCapTokens?: number } = { summaryOutputCapTokens: 16_384 };
  const handlers: Array<(event: unknown, ctx: unknown) => unknown> = [];
  (createPiCompactionFitExtension(option) as (pi: unknown) => unknown)({
    on: (name: string, handler: (event: unknown, ctx: unknown) => unknown) => {
      if (name === "session_before_compact") handlers.push(handler);
    },
  });
  assert.equal(handlers.length, 1);
  const sessionManager = {};
  const preparation = {
    settings: { reserveTokens: 80_000, keepRecentTokens: 20_000 },
    firstKeptEntryId: "retained",
    messagesToSummarize: [{ role: "user", content: "Authorized task and current progress", timestamp: 1 }],
    turnPrefixMessages: [],
    isSplitTurn: false,
    tokensBefore: 600_001,
    previousSummary: undefined,
    fileOps: { read: new Set<string>(), written: new Set<string>(), edited: new Set<string>() },
  };
  const ctx = { sessionManager, model: { ...model, maxTokens: 384_000 } };

  handlers[0]!({ preparation, reason: "threshold" }, ctx);
  assert.equal(takePiCompactionSummaryOutputCap(sessionManager)?.historyOutputCapTokens, 16_384);

  option.summaryOutputCapTokens = undefined;
  handlers[0]!({ preparation, reason: "threshold" }, ctx);
  assert.equal(takePiCompactionSummaryOutputCap(sessionManager), null, "a cleared ceiling must not bind");
});

test("a policy whose numbers do not take effect is rolled back, not reported as applied", () => {
  const cap = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 1_000_000 } });
  const session = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 1_000_000 } });
  const option: { summaryOutputCapTokens?: number } = { summaryOutputCapTokens: 16_384 };
  const calls: string[] = [];
  // Stands in for a settings surface that silently keeps other values (an unmodelled precedence).
  const lyingSurface = {
    read: () => { calls.push("read"); return { reserveTokens: 10_000, keepRecentTokens: 3_000 }; },
    apply: () => { calls.push("apply"); },
    restore: () => { calls.push("restore"); },
  };
  const decision = decidePiCompactionPolicy({
    capResolution: cap,
    sessionResolution: session,
    capOption: option,
    settings: lyingSurface,
  });
  assert.equal(decision.settingsVerified, false);
  assert.equal(decision.apply, null);
  assert.equal(decision.capCeilingActive, false);
  assert.equal(option.summaryOutputCapTokens, undefined, "the ceiling goes when the settings roll back");
  assert.match(decision.readback, /did not take effect: read back reserve 10000 \/ keep recent 3000, wanted 400000 \/ 20000/);
  assert.deepEqual(calls, ["read", "apply", "read", "restore"]);
});

test("a surface that behaves is verified and left applied", () => {
  const cap = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 1_000_000 } });
  const session = resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow: 1_000_000 } });
  const option: { summaryOutputCapTokens?: number } = { summaryOutputCapTokens: 16_384 };
  let current = { reserveTokens: 16_384, keepRecentTokens: 20_000 };
  const surface = {
    read: () => current,
    apply: (effective: { reserveTokens: number; keepRecentTokens: number }) => { current = { reserveTokens: effective.reserveTokens, keepRecentTokens: effective.keepRecentTokens }; },
    restore: (values: { reserveTokens: number; keepRecentTokens: number }) => { current = values; },
  };
  const decision = decidePiCompactionPolicy({ capResolution: cap, sessionResolution: session, capOption: option, settings: surface });
  assert.equal(decision.settingsVerified, true);
  assert.equal(decision.apply?.status, "applied");
  assert.equal(current.reserveTokens, 400_000); // 1M window: trigger above 600,000, reserve 400,000
  assert.equal(option.summaryOutputCapTokens, 16_384);
});
