import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { compact, shouldCompact } from "@earendil-works/pi-coding-agent";
import { capPiCompactionSummaryOutput } from "./piCompactionFit";
import {
  PI_COMPACTION_POLICY_ENV_VAR,
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
  assert.equal(resolution.status, "unknown_model_window");
  assert.match(resolution.readback, /cannot express a 60% trigger/);
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
  const windows = [32_768, 200_000, 1_000_000];
  const seen = windows.map((contextWindow) =>
    resolvePiCompactionPolicy({ envVars: env(PILOT_POLICY), model: { contextWindow } }).effective!,
  );
  assert.deepEqual(seen.map((e) => e.triggerAboveTokens), [19_660, 120_000, 600_000]);
  assert.deepEqual(seen.map((e) => e.reserveTokens), [13_108, 80_000, 400_000]);
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
