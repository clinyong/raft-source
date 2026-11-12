import assert from "node:assert/strict";
import type { AgentContext } from "../../auth/env";
import { createCommandContext } from "../../core/context";
import { sessionResetCommand } from "./reset";

const agent: AgentContext = {
  agentId: "self", serverUrl: "https://example.test", serverId: "server",
  token: "test-token", clientMode: "self-hosted-runner", secretSource: "profile-credential-file", activeCapabilities: null,
};

test("session reset targets only self and reports acceptance, not completion", async () => {
  const writes: string[] = [];
  const requests: unknown[] = [];
  const ctx = createCommandContext({
    io: { stdout: { write: (c) => { writes.push(String(c)); return true; } }, stderr: { write: () => true } },
    loadAgentContext: () => agent,
    createApiClient: () => ({ request: async (method: string, path: string, body: unknown) => {
      requests.push({ method, path, body });
      return { ok: true, status: 202, error: null, data: { status: "accepted" } };
    } }) as any,
  });
  await sessionResetCommand.handler(ctx);
  assert.deepEqual(requests, [{ method: "POST", path: "/internal/agent-api/self/session-reset", body: undefined }]);
  assert.match(writes.join(""), /accepted/);
  assert.doesNotMatch(writes.join(""), /completed|successfully restarted/);
});

test("reset_in_progress is reported with do-not-retry guidance, not CHECK_FAILED", async () => {
  const ctx = createCommandContext({
    io: { stdout: { write: () => true }, stderr: { write: () => true } },
    loadAgentContext: () => agent,
    createApiClient: () => ({ request: async () => ({
      ok: false, status: 409,
      error: "Launch is no longer current",
      errorCode: "reset_in_progress",
      data: null,
    }) }) as any,
  });
  await assert.rejects(
    async () => { await sessionResetCommand.handler(ctx); },
    (err: unknown) => {
      const cliError = err as { code?: string; message?: string };
      assert.equal(cliError.code, "RESET_IN_PROGRESS");
      assert.match(cliError.message ?? "", /still finishing/);
      assert.match(cliError.message ?? "", /do not retry within this turn/);
      return true;
    },
  );
});
