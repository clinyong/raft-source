import assert from "node:assert/strict";
import {
  SettingsManager,
  compact,
  shouldCompact,
} from "@earendil-works/pi-coding-agent";
import {
  PI_SUMMARY_OUTPUT_CAP_TOKENS,
  capPiCompactionSummaryOutput,
  createPiCompactionFitExtension,
  piCompactionFitBudget,
  piCompactionFitTelemetryAttrs,
  takePiCompactionFit,
  takePiCompactionSummaryOutputCap,
} from "./piCompactionFit";

/**
 * The summary budget (@Kirin's pilot, 2026-10-17).
 *
 * What this pins: the request Pi prepares for a history summary asks for up to `min(floor(0.8 x reserve),
 * model.maxTokens)` output tokens — with a 600K history and the stock reserve that is 320,000, which Raft
 * then subtracts from the input budget, trimming history to fit a summary nobody needs that long. Capping
 * the prepared request must (a) land exactly on the cap, (b) leave the threshold, the retained context and
 * the model's own output budget untouched, (c) keep a pre-existing smaller budget smaller, and (d) be
 * recorded separately from a trim, so "cap applied, nothing trimmed" stays distinguishable.
 *
 * The first three use the pinned SDK's real `compact()` with a fake stream, so the numbers are Pi's, not ours.
 */

const model = {
  id: "summary-cap-fixture",
  provider: "offline",
  api: "openai-completions",
  contextWindow: 1_000_000,
  maxTokens: 384_000,
};

function prepared(settings: { reserveTokens: number; keepRecentTokens: number }, split = false) {
  return {
    settings,
    firstKeptEntryId: "retained",
    messagesToSummarize: [{ role: "user", content: "Authorized task and current progress", timestamp: 1 }],
    turnPrefixMessages: split ? [{ role: "user", content: "Current turn checkpoint", timestamp: 2 }] : [],
    isSplitTurn: split,
    tokensBefore: 600_001,
    previousSummary: undefined,
    fileOps: { read: new Set<string>(), written: new Set<string>(), edited: new Set<string>() },
  };
}

/** Collect the `maxTokens` Pi asks for in each summarizer request. */
async function requestCaps(preparation: unknown): Promise<number[]> {
  const caps: number[] = [];
  const stream = async (_model: unknown, _context: unknown, options: { maxTokens: number }) => {
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
  await compact(
    preparation as never,
    model as never,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    stream as never,
  );
  return caps;
}

test("the prepared summary is capped without touching the threshold, retention or the model", async () => {
  const manager = SettingsManager.inMemory({
    compaction: { enabled: true, reserveTokens: 400_000, keepRecentTokens: 20_000 },
  });
  const settings = manager.getCompactionSettings(model);
  const preparation = prepared(settings);

  // The trigger reads the live settings, so it must not move when the preparation is capped.
  assert.equal(shouldCompact(600_000, 1_000_000, settings), false);
  assert.equal(shouldCompact(600_001, 1_000_000, settings), true);

  const budgetBefore = piCompactionFitBudget(preparation, model, "threshold");
  assert.equal(capPiCompactionSummaryOutput(preparation), PI_SUMMARY_OUTPUT_CAP_TOKENS);

  assert.deepEqual(await requestCaps(preparation), [PI_SUMMARY_OUTPUT_CAP_TOKENS]);
  // Retention and the live settings object are untouched; the preparation got a copy.
  assert.equal(preparation.settings.keepRecentTokens, 20_000);
  assert.equal(settings.reserveTokens, 400_000);
  assert.notEqual(preparation.settings, settings);
  assert.equal(manager.getCompactionSettings(model).reserveTokens, 400_000);
  assert.equal(model.maxTokens, 384_000);
  // And the input budget Raft trims against grows, because less output must be reserved.
  assert.equal(budgetBefore, Math.floor(1_000_000 * 0.85) - Math.min(Math.floor(0.8 * 400_000), 384_000) - 2_000);
  assert.equal(
    piCompactionFitBudget(preparation, model, "threshold"),
    Math.floor(1_000_000 * 0.85) - PI_SUMMARY_OUTPUT_CAP_TOKENS - 2_000,
  );
});

test("a split turn gets two bounded requests: the cap for history, half of it for the prefix", async () => {
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 }, true);
  assert.equal(capPiCompactionSummaryOutput(preparation), PI_SUMMARY_OUTPUT_CAP_TOKENS);
  assert.deepEqual(await requestCaps(preparation), [PI_SUMMARY_OUTPUT_CAP_TOKENS, 10_240]);
});

test("an existing smaller summary budget stays smaller, and re-applying the cap is a no-op", () => {
  const preparation = prepared({ reserveTokens: 16_384, keepRecentTokens: 20_000 });
  assert.equal(capPiCompactionSummaryOutput(preparation), null);
  assert.equal(preparation.settings.reserveTokens, 16_384);

  const larger = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  assert.equal(capPiCompactionSummaryOutput(larger), PI_SUMMARY_OUTPUT_CAP_TOKENS);
  const once = larger.settings.reserveTokens;
  assert.equal(capPiCompactionSummaryOutput(larger), null);
  assert.equal(larger.settings.reserveTokens, once);
  // The model's own ceiling still wins when it is lower than the cap.
  assert.equal(
    Math.min(Math.floor(0.8 * once), model.maxTokens) === Math.floor(0.8 * once),
    true,
    "the cap is below this model's maxTokens, so the cap is what binds",
  );
});

test("the extension caps before fitting, and records it for telemetry separately from a trim", () => {
  type Handler = (event: { preparation: unknown; reason: string }, ctx: { sessionManager: object; model: unknown }) => unknown;
  // Collected through an array: assigning to a local inside the factory would leave TS narrowing it to `never`.
  const handlers: Handler[] = [];
  createPiCompactionFitExtension()({
    on: (name: string, h: Handler) => {
      if (name === "session_before_compact") handlers.push(h);
    },
  } as never);
  const handler = handlers[0];
  assert.ok(handler, "the extension registers a session_before_compact handler");

  const sessionManager = {};
  // A preparation that fits comfortably: the cap applies, nothing needs trimming.
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });

  handler({ preparation, reason: "threshold" }, { sessionManager, model });

  assert.equal(preparation.settings.reserveTokens, Math.floor(PI_SUMMARY_OUTPUT_CAP_TOKENS / 0.8));
  assert.equal(takePiCompactionSummaryOutputCap(sessionManager), PI_SUMMARY_OUTPUT_CAP_TOKENS);
  assert.equal(takePiCompactionFit(sessionManager), undefined, "nothing was trimmed, so there is no fit");
  const attrs = piCompactionFitTelemetryAttrs(takePiCompactionFit(sessionManager), takePiCompactionSummaryOutputCap(sessionManager));
  // Both reads are take-once: the second read of each returns nothing, so the values above are asserted first.
  assert.equal(takePiCompactionSummaryOutputCap(sessionManager), null);
  assert.equal(attrs.compaction_fit_applied, undefined);
});

test("telemetry carries the cap next to a trim, and stays silent about a trim that did not happen", () => {
  const fit = {
    trigger: "threshold" as const,
    budgetTokens: 10,
    tokensBefore: 100,
    tokensAfter: 100,
    droppedMessages: 0,
    droppedTokens: 0,
    truncatedMessages: 0,
    truncatedChars: 0,
  };
  const both = piCompactionFitTelemetryAttrs(fit, PI_SUMMARY_OUTPUT_CAP_TOKENS);
  assert.equal(both.compaction_fit_applied, true);
  assert.equal(both.compaction_summary_output_cap_tokens, PI_SUMMARY_OUTPUT_CAP_TOKENS);
  const capOnly = piCompactionFitTelemetryAttrs(undefined, PI_SUMMARY_OUTPUT_CAP_TOKENS);
  assert.equal(capOnly.compaction_fit_applied, undefined);
  assert.equal(capOnly.compaction_summary_output_cap_tokens, PI_SUMMARY_OUTPUT_CAP_TOKENS);
  const neither = piCompactionFitTelemetryAttrs(undefined, null);
  assert.deepEqual(neither, {});
});

test("a model whose own output limit is smaller reports that limit, not the requested cap", async () => {
  const smallModel = { ...model, id: "summary-cap-small", maxTokens: 8_192 };
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  // The effective cap is the model's own limit; that is what telemetry must show.
  assert.equal(capPiCompactionSummaryOutput(preparation, smallModel), 8_192);
  assert.equal(preparation.settings.reserveTokens, Math.floor(8_192 / 0.8));
  const caps: number[] = [];
  const stream = async (_m: unknown, _c: unknown, options: { maxTokens: number }) => {
    caps.push(options.maxTokens);
    return {
      result: async () => ({
        role: "assistant",
        content: [{ type: "text", text: "Deterministic task checkpoint." }],
        api: "openai-completions",
        provider: "offline",
        model: smallModel.id,
        stopReason: "stop",
        timestamp: 1,
        usage: { input: 100, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 110, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      }),
    };
  };
  await compact(preparation as never, smallModel as never, undefined, undefined, undefined, undefined, undefined, stream as never);
  assert.deepEqual(caps, [8_192]);
});
