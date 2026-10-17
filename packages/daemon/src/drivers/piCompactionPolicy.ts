/**
 * Per-agent opt-in compaction policy for the built-in (Pi) runtime.
 *
 * The policy is DATA on the agent, never a name in this file: an agent opts in by
 * carrying the `RAFT_PI_COMPACTION_POLICY` env var in its own agent config, so the
 * pilot is selected by configuration and this module never learns an agent id.
 * Unset, disabled, malformed or unresolved-window policies resolve to "inert" and
 * leave the runtime exactly as it behaves today.
 *
 * The policy drives two layers that only make sense together:
 *
 *   1. WHEN Pi compacts. `shouldCompact(contextTokens, contextWindow, settings)`
 *      is `contextTokens > contextWindow - settings.reserveTokens`
 *      (pi-coding-agent `core/compaction/compaction.js`), so a trigger "above 60% of
 *      the window" is expressed as `reserveTokens = window - floor(0.60 x window)`.
 *      Triggering earlier makes the summary requests LARGER, which is why layer 2
 *      exists.
 *   2. HOW BIG the summary requests may be. Pi asks for
 *      `min(floor(0.8 x reserveTokens), model.maxTokens)` output tokens for history
 *      and `min(floor(0.5 x reserveTokens), model.maxTokens)` for a split turn's
 *      prefix. Those are the ceilings the accepted summary-output cap adapter bounds,
 *      through the adapter's own derivation — the numbers here are the contract the
 *      wiring hands that adapter, and their equality is asserted in tests rather than
 *      assumed (see `piCompactionPolicy.test.ts`).
 *
 * Readback: `resolvePiCompactionPolicy` returns a human-readable `readback` line for
 * every outcome, including the inert ones, so an operator can see what an agent
 * actually got instead of inferring it from a config file.
 */

/** Env var an agent sets (in its own agent config) to opt in. JSON, see the contract below. */
export const PI_COMPACTION_POLICY_ENV_VAR = "RAFT_PI_COMPACTION_POLICY";

/** Policy schema version. A different version is rejected rather than guessed at. */
export const PI_COMPACTION_POLICY_VERSION = 1;

/** History share of the reserve that Pi's summarizer request uses. Mirrors compaction.js:512. */
export const PI_COMPACTION_HISTORY_SHARE_OF_RESERVE = 0.8;

/** Split-turn prefix share of the reserve that Pi uses. Mirrors compaction.js:727. */
export const PI_COMPACTION_PREFIX_SHARE_OF_RESERVE = 0.5;

export interface PiCompactionPolicy {
  version: typeof PI_COMPACTION_POLICY_VERSION;
  enabled: boolean;
  /** Compact once the context passes this share of the model's window. 0 < ratio < 1. */
  triggerAboveRatio: number;
  /** History Pi keeps verbatim when it compacts. */
  keepRecentTokens: number;
  /** Output-token ceiling for the summarizer, handed to the cap adapter. */
  summaryOutputCapTokens: number;
}

/** What the policy resolves to once the model is known. */
export interface PiCompactionPolicyEffective {
  /** The window the trigger was derived from — the resolved model's own window. */
  modelWindowTokens: number;
  /** Context tokens above which Pi compacts. */
  triggerAboveTokens: number;
  /** `reserveTokens` handed to Pi: `window - triggerAboveTokens`. */
  reserveTokens: number;
  keepRecentTokens: number;
  /** Ceiling for the history summary request, handed to the cap adapter. */
  summaryOutputCapTokens: number;
  /** Ceiling the adapter derives for a split turn's prefix; the policy's other cap. */
  prefixOutputCapTokens: number;
}

export type PiCompactionPolicyStatus =
  /** Policy configured, window resolved: the wiring applies it. */
  | "applied"
  /** No policy env var on this agent: today's behaviour, nothing to report beyond that. */
  | "unset"
  /** Configured but `enabled: false`. */
  | "disabled"
  /** Configured but unusable (schema, types, ranges): never applied, reason recorded. */
  | "invalid"
  /** Configured, but the resolved model reports no usable window: nothing is applied. */
  | "unknown_model_window"
  /** Configured and the window is known, but it cannot host this policy: nothing is applied. */
  | "window_incompatible";

export interface PiCompactionPolicyResolution {
  status: PiCompactionPolicyStatus;
  /** The parsed policy, present whenever one was configured — including inert outcomes. */
  policy: PiCompactionPolicy | null;
  /** Present only when `status` is "applied". */
  effective: PiCompactionPolicyEffective | null;
  /** Why the policy is inert, for "invalid" and "unknown_model_window". */
  reason: string | null;
  /** One-line readback for logs and review; never empty. */
  readback: string;
}

const POLICY_KEYS = new Set([
  "version",
  "enabled",
  "triggerAboveRatio",
  "keepRecentTokens",
  "summaryOutputCapTokens",
]);

type PolicyParseOutcome =
  | { ok: true; policy: PiCompactionPolicy }
  | { ok: false; reason: string; policy: PiCompactionPolicy | null };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

/**
 * Parse the policy env var. Strict on purpose: a pilot that silently accepts a typo
 * ("keepRecentToken") or a coerced string would report a policy nobody configured.
 */
export function parsePiCompactionPolicy(raw: string | null | undefined): PolicyParseOutcome {
  if (raw === null || raw === undefined || raw.trim() === "") {
    return { ok: false, reason: "not configured", policy: null };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    return { ok: false, reason: `not valid JSON (${error instanceof Error ? error.message : String(error)})`, policy: null };
  }
  if (!isPlainObject(parsed)) {
    return { ok: false, reason: "not a JSON object", policy: null };
  }
  const unknownKeys = Object.keys(parsed).filter((key) => !POLICY_KEYS.has(key));
  if (unknownKeys.length > 0) {
    return { ok: false, reason: `unknown field(s): ${unknownKeys.sort().join(", ")}`, policy: null };
  }
  if (parsed.version !== PI_COMPACTION_POLICY_VERSION) {
    return {
      ok: false,
      reason: `unsupported version ${JSON.stringify(parsed.version)} (expected ${PI_COMPACTION_POLICY_VERSION})`,
      policy: null,
    };
  }
  if (typeof parsed.enabled !== "boolean") {
    return { ok: false, reason: `"enabled" must be a boolean`, policy: null };
  }
  // A disabled policy is complete as written: the remaining fields are only read when it is enabled.
  if (parsed.enabled === false) {
    return {
      ok: true,
      policy: {
        version: PI_COMPACTION_POLICY_VERSION,
        enabled: false,
        triggerAboveRatio: 0,
        keepRecentTokens: 0,
        summaryOutputCapTokens: 0,
      },
    };
  }
  const ratio = parsed.triggerAboveRatio;
  if (typeof ratio !== "number" || !Number.isFinite(ratio) || ratio <= 0 || ratio >= 1) {
    return { ok: false, reason: `"triggerAboveRatio" must be a number strictly between 0 and 1`, policy: null };
  }
  if (!isPositiveInteger(parsed.keepRecentTokens)) {
    return { ok: false, reason: `"keepRecentTokens" must be a positive integer`, policy: null };
  }
  if (!isPositiveInteger(parsed.summaryOutputCapTokens)) {
    return { ok: false, reason: `"summaryOutputCapTokens" must be a positive integer`, policy: null };
  }
  return {
    ok: true,
    policy: {
      version: PI_COMPACTION_POLICY_VERSION,
      enabled: true,
      triggerAboveRatio: ratio,
      keepRecentTokens: parsed.keepRecentTokens,
      summaryOutputCapTokens: parsed.summaryOutputCapTokens,
    },
  };
}

function inert(
  status: Exclude<PiCompactionPolicyStatus, "applied">,
  policy: PiCompactionPolicy | null,
  reason: string,
): PiCompactionPolicyResolution {
  return { status, policy, effective: null, reason, readback: `compaction pilot: not applied (${reason})` };
}

/**
 * Resolve the policy for one agent against the model the runtime actually resolved.
 * Pure: no I/O, no caching, so a model change is a new call with a new window rather
 * than a stale decision carried over.
 */
export function resolvePiCompactionPolicy(options: {
  envVars?: Record<string, string> | null | undefined;
  model?: { contextWindow?: number | null } | null | undefined;
}): PiCompactionPolicyResolution {
  const parsed = parsePiCompactionPolicy(options.envVars?.[PI_COMPACTION_POLICY_ENV_VAR]);
  if (!parsed.ok) {
    // "not configured" is the ordinary case for every agent that has not opted in.
    return inert(parsed.reason === "not configured" ? "unset" : "invalid", parsed.policy, parsed.reason);
  }
  const { policy } = parsed;
  if (!policy.enabled) return inert("disabled", policy, "policy disabled");

  const window = options.model?.contextWindow;
  if (typeof window !== "number" || !Number.isFinite(window) || window <= 0) {
    // Covers both shapes of "no window to derive from": a model that reports none, and a model that is not
    // resolvable yet when this runs (the cap extension is built while the runtime loads its extensions).
    return inert("unknown_model_window", policy, "no resolved model window at session start");
  }
  const triggerAboveTokens = Math.floor(policy.triggerAboveRatio * window);
  const reserveTokens = window - triggerAboveTokens;
  // A trigger that cannot fire (0 tokens) or a reserve that covers the whole window
  // would leave Pi compacting at every turn or never: refuse instead of guessing.
  if (triggerAboveTokens < 1 || reserveTokens < 1) {
    return inert("window_incompatible", policy, `model window ${window} cannot express a ${policy.triggerAboveRatio * 100}% trigger`);
  }
  // Pi keeps `keepRecentTokens` verbatim and summarizes ONLY what is older than that. If the retention does
  // not leave the trigger room for a summary at least as large as itself, "above the trigger" is already
  // almost all retained history and the policy degenerates into per-turn churn on a smaller model. Half the
  // trigger is the line: at compaction time at least as much history is summarized as is kept.
  const minimumTriggerTokens = policy.keepRecentTokens * 2;
  if (triggerAboveTokens < minimumTriggerTokens) {
    return inert(
      "window_incompatible",
      policy,
      `window ${window} triggers above ${triggerAboveTokens}, less than twice the ${policy.keepRecentTokens} retained tokens`,
    );
  }
  const prefixOutputCapTokens = Math.floor(PI_COMPACTION_PREFIX_SHARE_OF_RESERVE * Math.floor(
    policy.summaryOutputCapTokens / PI_COMPACTION_HISTORY_SHARE_OF_RESERVE,
  ));
  const effective: PiCompactionPolicyEffective = {
    modelWindowTokens: window,
    triggerAboveTokens,
    reserveTokens,
    keepRecentTokens: policy.keepRecentTokens,
    summaryOutputCapTokens: policy.summaryOutputCapTokens,
    prefixOutputCapTokens,
  };
  const percent = Math.round(policy.triggerAboveRatio * 100);
  return {
    status: "applied",
    policy,
    effective,
    reason: null,
    readback: [
      "compaction pilot: applied",
      `window ${window}`,
      `trigger above ${triggerAboveTokens} (${percent}%) => reserve ${reserveTokens}`,
      `keep recent ${policy.keepRecentTokens}`,
      `summary cap ${policy.summaryOutputCapTokens} (prefix ${prefixOutputCapTokens})`,
    ].join("; "),
  };
}

/**
 * One policy, two moments: the cap extension must be built during session creation (before the model is
 * resolved, so its ceiling comes from a resolution against the model known then), while the trigger and the
 * retained-history settings are applied afterwards, when the session's real model is known. This function is
 * what reconciles the two, and it is where the "no half-applied policy" rule lives:
 *
 * - both applied  -> the settings use the FINAL window (the authoritative one), and a cap built from an
 *                    earlier window does not invalidate it: the ceiling is a window-independent policy value.
 *                    A window difference is reported, not hidden.
 * - cap not built -> the policy is not applied at all. An earlier trigger with an unbounded summary request
 *                    is exactly the combination the policy exists to prevent.
 * - settings cannot host it -> same: nothing is applied. The ceiling may still be present in the extension;
 *                    the adapter only ever LOWERS an oversized request, so this direction is bounded, and it
 *                    is reported rather than passed off as an applied policy.
 */
export function combinePiCompactionPolicyResolutions(
  capResolution: PiCompactionPolicyResolution,
  sessionResolution: PiCompactionPolicyResolution,
): {
  apply: PiCompactionPolicyResolution | null;
  /** True when the extension carries the ceiling but the session settings were left alone. */
  capInstalledWithoutTrigger: boolean;
  windowMismatch: { capWindowTokens: number | null; sessionWindowTokens: number | null } | null;
  readback: string;
} {
  if (sessionResolution.status === "applied") {
    if (capResolution.status !== "applied") {
      return {
        apply: null,
        capInstalledWithoutTrigger: false,
        windowMismatch: null,
        readback: `compaction pilot: not applied (the summary cap could not be built for this session: ${capResolution.reason ?? capResolution.status})`,
      };
    }
    const capWindowTokens = capResolution.effective?.modelWindowTokens ?? null;
    const sessionWindowTokens = sessionResolution.effective?.modelWindowTokens ?? null;
    const windowMismatch = capWindowTokens === sessionWindowTokens ? null : { capWindowTokens, sessionWindowTokens };
    return {
      apply: sessionResolution,
      capInstalledWithoutTrigger: false,
      windowMismatch,
      readback: windowMismatch
        ? `${sessionResolution.readback} (window changed between the cap build and the final resolution: ${String(capWindowTokens)} -> ${String(sessionWindowTokens)}; the ceiling does not depend on it)`
        : sessionResolution.readback,
    };
  }
  return {
    apply: null,
    capInstalledWithoutTrigger: capResolution.status === "applied",
    windowMismatch: null,
    readback: capResolution.status === "applied"
      ? `${sessionResolution.readback}; the ceiling stays in the extension but the policy was not applied to settings`
      : sessionResolution.readback,
  };
}
