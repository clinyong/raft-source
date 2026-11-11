import assert from "node:assert/strict";
import type { ServerToMachineMessage } from "@botiverse/raft-shared";
import { AgentOrchestrator } from "./agentOrchestrator";

function setup() {
  const orchestrator = new AgentOrchestrator();
  const internals = orchestrator as any;
  const sent: ServerToMachineMessage[] = [];
  const resets: string[] = [];
  const agent = { id: "agent", serverId: "server", machineId: "machine", sessionId: "old" };
  internals.validateMachineAgentMessage = async () => agent;
  internals.shouldAcceptLifecycleEvent = () => true;
  internals.rememberRuntimeError = async () => true;
  internals.broadcastActivity = () => {};
  internals.sendToMachine = async (_machine: string, msg: ServerToMachineMessage) => { sent.push(msg); return true; };
  internals.resetAgent = async (agentId: string, mode: string) => { resets.push(`${agentId}:${mode}`); };
  internals.getAuthoritativeAgentForDelivery = async () => agent;
  return { orchestrator, sent, resets };
}

test("duplicate readiness for the same launch initiates one reset", async () => {
  const { orchestrator, resets } = setup();
  const ready = { type: "agent:self_session_reset:ready", agentId: "agent", launchId: "launch", sessionId: "old" } as const;
  await orchestrator.handleMachineMessage("machine", ready);
  await orchestrator.handleMachineMessage("machine", ready);
  assert.deepEqual(resets, ["agent:session"]);
});

test("a confirmed pre-clear cancellation prevents a later readiness frame", async () => {
  const { orchestrator, sent, resets } = setup();
  await orchestrator.handleMachineMessage("machine", { type: "agent:self_session_reset:cancel", agentId: "agent", launchId: "launch", sessionId: "old" });
  assert.deepEqual(sent, [{ type: "agent:self_session_reset:cancel_result", agentId: "agent", launchId: "launch", notStarted: true }]);
  await orchestrator.handleMachineMessage("machine", { type: "agent:self_session_reset:ready", agentId: "agent", launchId: "launch", sessionId: "old" });
  assert.deepEqual(resets, []);
  assert.equal(sent.at(-1)?.type, "agent:self_session_reset:rejected");
});

test("unknown outcome triggers at most one recovery request", async () => {
  const { orchestrator, resets } = setup();
  const recovery = { type: "agent:self_session_reset:recover", agentId: "agent", launchId: "launch", sessionId: "old" } as const;
  await orchestrator.handleMachineMessage("machine", recovery);
  await orchestrator.handleMachineMessage("machine", recovery);
  assert.deepEqual(resets, ["agent:session"]);
});

test("failed reset publishes a timestamped reason to persisted status and App activity", async () => {
  const { orchestrator } = setup();
  const internals = orchestrator as any;
  const saved: any[] = [];
  const broadcast: any[] = [];
  internals.rememberRuntimeError = async (_agent: string, error: unknown) => { saved.push(error); return true; };
  internals.broadcastActivity = (...args: unknown[]) => { broadcast.push(args); };
  await orchestrator.handleMachineMessage("machine", {
    type: "agent:self_session_reset:failed", agentId: "agent", launchId: "launch", sessionId: "old", reason: "restart_timeout",
  });
  assert.match(saved[0].message, /Self session reset failed/);
  assert.match(saved[0].message, /\d{4}-\d{2}-\d{2}T/);
  assert.equal(broadcast[0][2], saved[0].message);
  assert.equal(broadcast[0][1], "error");
});
