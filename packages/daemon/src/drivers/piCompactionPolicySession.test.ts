import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { buildBuiltInAgentDir, buildBuiltInSessionDir, createPiAgentSessionForContext } from "./pi";
import { PI_COMPACTION_POLICY_ENV_VAR } from "./piCompactionPolicy";

/**
 * Session-level acceptance for the wiring (2026-10-17).
 *
 * @Dassai's engineering review found that a session whose own `.pi/settings.json` carries a
 * `compaction.modelOverrides` entry for its model kept that entry's numbers while the readback claimed the
 * policy applied: Pi resolves a token setting as "model override -> ordinary setting -> built-in default".
 * These cases drive real session creation with that settings file present, and assert the effective settings
 * the SDK reports -- not just our own bookkeeping.
 */

const POLICY = JSON.stringify({
  version: 1,
  enabled: true,
  triggerAboveRatio: 0.6,
  keepRecentTokens: 20_000,
  summaryOutputCapTokens: 16_384,
});

async function createFixtureSession(options: {
  selected: boolean;
  existingModelOverride: boolean;
  contextWindow?: number;
  /** Extra settings-file content (e.g. compaction disabled, or an override for another model). */
  settings?: Record<string, unknown>;
}) {
  const root = mkdtempSync(path.join(os.tmpdir(), "policy-session-fixture-"));
  const workspace = path.join(root, "workspace");
  mkdirSync(workspace);
  const settings = options.existingModelOverride
    ? { ...options.settings, compaction: { ...(options.settings?.compaction as object ?? {}), modelOverrides: { "openai/model-window-probe": { reserveTokens: 10_000, keepRecentTokens: 3_000 } } } }
    : options.settings;
  if (settings) {
    mkdirSync(path.join(workspace, ".pi"));
    writeFileSync(path.join(workspace, ".pi/settings.json"), JSON.stringify(settings));
  }
  const original = ModelRegistry.prototype.find;
  const spy = vi.spyOn(ModelRegistry.prototype, "find").mockImplementation(function (this: ModelRegistry, provider: string, id: string) {
    const value = original.call(this, provider, id);
    if (provider === "openai" && id === "model-window-probe" && value) {
      return { ...value, contextWindow: options.contextWindow ?? 1_000_000, maxTokens: 24_000 };
    }
    return value;
  });
  try {
    const ctx = {
      agentId: "policy-session-fixture",
      workingDirectory: workspace,
      standingPrompt: "",
      slockCliPath: "/tmp/offline-policy-cli.js",
      daemonApiKey: "offline-daemon-key",
      config: {
        name: "Policy session fixture", serverUrl: "http://127.0.0.1:1", authToken: "offline-agent-key",
        runtime: "builtin", sessionId: null, runtimeContext: null, model: "model-window-probe",
        envVars: options.selected ? { [PI_COMPACTION_POLICY_ENV_VAR]: POLICY } : null,
        runtimeConfig: {
          version: 1, runtime: "builtin", hostUserState: "forbidden",
          provider: { kind: "gateway", providerId: "openai-compatible", baseUrl: "http://localhost:9876/v1", apiKey: "offline-test-key" },
          model: { kind: "custom", name: "model-window-probe" }, mode: { kind: "default" },
        },
      },
    };
    const session = await createPiAgentSessionForContext(ctx as never, "policy-session-fixture", {
      agentDir: buildBuiltInAgentDir(workspace),
      sessionDir: buildBuiltInSessionDir(workspace),
      isolateHostProviderEnv: true,
      exposeLaunchEnvToTools: false,
    });
    return session;
  } finally {
    spy.mockRestore();
  }
}

test("a selected session applies the policy even when its own settings file overrides that model", async () => {
  const session = await createFixtureSession({ selected: true, existingModelOverride: true });
  try {
    // 1M window, 60% trigger: reserve 400,000, and the policy's retention wins over the file's 10,000/3,000.
    expect(session.settingsManager.getCompactionSettings(session.model)).toEqual({
      enabled: true, reserveTokens: 400_000, keepRecentTokens: 20_000,
    });
  } finally {
    session.dispose();
  }
});

test("without a settings-file override the selected session is identical (control)", async () => {
  const session = await createFixtureSession({ selected: true, existingModelOverride: false });
  try {
    expect(session.settingsManager.getCompactionSettings(session.model)).toEqual({
      enabled: true, reserveTokens: 400_000, keepRecentTokens: 20_000,
    });
  } finally {
    session.dispose();
  }
});

test("an unselected session keeps its own per-model settings untouched (control)", async () => {
  const session = await createFixtureSession({ selected: false, existingModelOverride: true });
  try {
    expect(session.settingsManager.getCompactionSettings(session.model)).toEqual({
      enabled: true, reserveTokens: 10_000, keepRecentTokens: 3_000,
    });
  } finally {
    session.dispose();
  }
});

test("an unselected session without an override keeps the stock reserve (control)", async () => {
  const session = await createFixtureSession({ selected: false, existingModelOverride: false });
  try {
    expect(session.settingsManager.getCompactionSettings(session.model)).toEqual({
      enabled: true, reserveTokens: 16_384, keepRecentTokens: 20_000,
    });
  } finally {
    session.dispose();
  }
});

test("the daemon's compaction-enabled override survives for an unselected agent whose file disables it", async () => {
  // The daemon has always forced Pi's SDK compaction on so lifecycle events surface. That must not depend
  // on the policy applying: an unselected agent with `compaction.enabled: false` keeps the override.
  const session = await createFixtureSession({ selected: false, existingModelOverride: false, settings: { compaction: { enabled: false } } });
  try {
    expect(session.settingsManager.getCompactionEnabled()).toBe(true);
  } finally {
    session.dispose();
  }
});

test("another model's own override is untouched by applying or rolling back the policy", async () => {
  const session = await createFixtureSession({
    selected: true,
    existingModelOverride: false,
    settings: { compaction: { modelOverrides: { "openai/another-model": { reserveTokens: 5_000, keepRecentTokens: 1_000 } } } },
  });
  try {
    expect(session.settingsManager.getCompactionSettings(session.model)).toEqual({
      enabled: true, reserveTokens: 400_000, keepRecentTokens: 20_000,
    });
    const other = session.settingsManager.getCompactionSettings({ provider: "openai", id: "another-model" } as never);
    expect(other.reserveTokens).toBe(5_000);
    expect(other.keepRecentTokens).toBe(1_000);
  } finally {
    session.dispose();
  }
});
