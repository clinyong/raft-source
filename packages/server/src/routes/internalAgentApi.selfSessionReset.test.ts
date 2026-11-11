import { createApiTest } from "../test/integration/apiTest";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getDb } from "../db/index";
import { users, agents } from "../db/schema";
import { eq } from "drizzle-orm";
import { AgentOrchestrator } from "../services/agentOrchestrator";
import { fixturePasswordHash } from "../test/integration/credentials";
import { createServer } from "../services/serverService";
import { createAgent, assignMachine } from "../services/agentService";
import { registerMachine } from "../services/machineService";
import { mintAgentCredential } from "../services/agentCredentialService";

const test = createApiTest({ humanActivityMuteFlagDefaultEnabled: true, onboardingOpenerFlagDefaultEnabled: false });

test("self reset uses the authenticated agent and refuses a target", async ({ app }) => {
  const suffix = randomUUID();
  const [owner] = await getDb().insert(users).values({
    email: `self-reset-${suffix}@slock.test`, name: `self-reset-${suffix}`,
    displayName: "Owner", passwordHash: await fixturePasswordHash("password123"), emailVerified: true,
  }).returning();
  const server = await createServer("Self Reset", `self-reset-${suffix}`, owner!.id);
  const self = await createAgent(server.id, "Self", { runtime: "claude", model: "sonnet" });
  const other = await createAgent(server.id, "Other", { runtime: "claude", model: "sonnet" });
  const { machine } = await registerMachine(server.id, owner!.id, "self-reset-machine");
  await assignMachine(self.id, machine.id);
  const credential = await mintAgentCredential({ agentId: self.id, scopes: ["read", "channels"], name: "self", createdByUserId: null });
  const calls: unknown[] = [];
  app.app.set("agentOrchestrator", { requestSelfSessionReset: async (agentId: string, launchId: string) => {
    calls.push({ agentId, launchId }); return "accepted";
  } });
  const post = (body?: unknown, launch = "launch-1") => fetch(`${app.baseUrl}/internal/agent-api/self/session-reset`, {
    method: "POST", headers: { Authorization: `Bearer ${credential.apiKey}`, "Content-Type": "application/json", "X-Agent-Launch-Id": launch },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const targeted = await post({ agentId: other.id });
  assert.equal(targeted.status, 400, await targeted.text());
  assert.deepEqual(calls, []);
  assert.equal((await post(undefined, "")).status, 400);
  const result = await post();
  assert.equal(result.status, 202);
  assert.deepEqual(await result.json(), { status: "accepted" });
  assert.deepEqual(calls, [{ agentId: self.id, launchId: "launch-1" }]);

  const orchestrator = new AgentOrchestrator();
  (orchestrator as any).validateMachineAgentMessage = async () => ({ id: self.id, serverId: server.id, machineId: machine.id, sessionId: null });
  app.app.set("agentOrchestrator", orchestrator);
  await orchestrator.handleMachineMessage(machine.id, {
    type: "agent:self_session_reset:failed", agentId: self.id, launchId: "launch-1", sessionId: "old", reason: "restart_timeout",
  });
  const [stored] = await getDb().select({ error: agents.lastRuntimeError }).from(agents).where(eq(agents.id, self.id));
  assert.match(stored!.error!.message, /Self session reset failed/);
  assert.match(stored!.error!.message, /\d{4}-\d{2}-\d{2}T/);
  const info = await fetch(`${app.baseUrl}/internal/agent-api/users/${encodeURIComponent(self.name)}/channels`, {
    headers: { Authorization: `Bearer ${credential.apiKey}` },
  });
  assert.equal(info.status, 200, await info.clone().text());
  const visible = await info.json() as any;
  assert.match(visible.user.activityDetail, /Self session reset failed/);
  assert.match(visible.user.activityDetail, /\d{4}-\d{2}-\d{2}T/);
  orchestrator.shutdown();
});
