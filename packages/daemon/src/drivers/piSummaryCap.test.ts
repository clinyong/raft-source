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
  fitPiCompactionPreparation,
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
 * then subtracts from the input budget, trimming history to fit a summary nobody needs that long. The cap
 * must (a) land exactly on the cap, (b) leave the threshold, the retained context and the model's own output
 * budget untouched, (c) keep a pre-existing smaller budget smaller and stay idempotent, (d) be **opt-in** —
 * an unselected agent must behave exactly as before, and (e) be reported only when it actually lowered the
 * request, reporting the effective SDK budget rather than the requested ceiling.
 *
 * Most cases drive the pinned SDK's real `compact()` with a fake stream, so the asserted numbers are Pi's own.
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
async function requestCaps(preparation: unknown, m: typeof model = model): Promise<number[]> {
  const caps: number[] = [];
  const stream = async (_model: unknown, _context: unknown, options: { maxTokens: number }) => {
    caps.push(options.maxTokens);
    return {
      result: async () => ({
        role: "assistant",
        content: [{ type: "text", text: "Deterministic task checkpoint." }],
        api: "openai-completions",
        provider: "offline",
        model: m.id,
        stopReason: "stop",
        timestamp: 1,
        usage: { input: 100, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 110, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      }),
    };
  };
  await compact(
    preparation as never,
    m as never,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    stream as never,
  );
  return caps;
}

type Handler = (event: { preparation: unknown; reason: string }, ctx: { sessionManager: object; model: unknown }) => unknown;

function handlerOf(factory: () => unknown): Handler {
  const handlers: Handler[] = [];
  (factory() as (pi: unknown) => unknown)({
    on: (name: string, h: Handler) => {
      if (name === "session_before_compact") handlers.push(h);
    },
  });
  const handler = handlers[0];
  assert.ok(handler, "the extension registers a session_before_compact handler");
  return handler;
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
  assert.equal(capPiCompactionSummaryOutput(preparation, model), PI_SUMMARY_OUTPUT_CAP_TOKENS);

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
  assert.equal(capPiCompactionSummaryOutput(preparation, model), PI_SUMMARY_OUTPUT_CAP_TOKENS);
  assert.deepEqual(await requestCaps(preparation), [PI_SUMMARY_OUTPUT_CAP_TOKENS, 10_240]);
});

test("an existing smaller summary budget stays smaller, and re-applying the cap is a no-op", () => {
  const preparation = prepared({ reserveTokens: 16_384, keepRecentTokens: 20_000 });
  assert.equal(capPiCompactionSummaryOutput(preparation, model), null);
  assert.equal(preparation.settings.reserveTokens, 16_384);

  const larger = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  assert.equal(capPiCompactionSummaryOutput(larger, model), PI_SUMMARY_OUTPUT_CAP_TOKENS);
  const once = larger.settings.reserveTokens;
  assert.equal(capPiCompactionSummaryOutput(larger, model), null);
  assert.equal(larger.settings.reserveTokens, once);
});

test("a model whose own limit is already below the cap is left alone and reported as nothing", async () => {
  const smallModel = { ...model, id: "summary-cap-small", maxTokens: 8_192 };
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  // The cap would not lower the request (the model's limit already does), so it does not apply and must not
  // be reported as a binding constraint.
  assert.equal(capPiCompactionSummaryOutput(preparation, smallModel), null);
  assert.equal(preparation.settings.reserveTokens, 400_000);
  assert.deepEqual(await requestCaps(preparation, smallModel), [8_192]);
});

test("a model between the cap and the uncapped request reports the effective budget", async () => {
  const midModel = { ...model, id: "summary-cap-mid", maxTokens: 24_000 };
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  assert.equal(capPiCompactionSummaryOutput(preparation, midModel), PI_SUMMARY_OUTPUT_CAP_TOKENS);
  assert.deepEqual(await requestCaps(preparation, midModel), [PI_SUMMARY_OUTPUT_CAP_TOKENS]);
});

test("the cap is opt-in: the default factory does not cap, and history fitting still runs", () => {
  const sessionManager = {};
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  // No option ⇒ no cap (the pilot is per-agent; production passes nothing until the wiring is reviewed).
  handlerOf(() => createPiCompactionFitExtension())({ preparation, reason: "threshold" }, { sessionManager, model });
  assert.equal(preparation.settings.reserveTokens, 400_000);
  assert.equal(takePiCompactionSummaryOutputCap(sessionManager), null);
  // …and the existing behaviour is intact: a request that fits is fitted, not capped.
  assert.equal(takePiCompactionFit(sessionManager), undefined);
  // Content, not `tokensBefore`, is what the fit measures: one message far larger than the budget must be cut.
  const huge = {
    ...prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 }),
    messagesToSummarize: [{ role: "user", content: "x".repeat(2_000_000), timestamp: 1 }],
  };
  const fit = fitPiCompactionPreparation(huge as never, model, "overflow");
  assert.ok(fit, "history fitting still trims an oversized request");
  assert.equal(fit.truncatedMessages, 1);
});

test("opted in, the extension caps before fitting and records it separately from a trim", () => {
  const sessionManager = {};
  // A preparation that fits comfortably: the cap applies, nothing needs trimming.
  const preparation = prepared({ reserveTokens: 400_000, keepRecentTokens: 20_000 });
  handlerOf(() => createPiCompactionFitExtension({ summaryOutputCapTokens: PI_SUMMARY_OUTPUT_CAP_TOKENS }))(
    { preparation, reason: "threshold" },
    { sessionManager, model },
  );

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
