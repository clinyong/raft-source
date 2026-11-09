// Pi idle delivery fallback (2026-11-09).
//
// The daemon decides "is this agent busy?" from its own bookkeeping. That copy
// can be stale: on 2026-11-07 and 2026-11-09 a Pi session had already settled
// while the daemon still believed the runtime was working, so queued messages
// were never delivered as a new turn and the agent sat silent.
//
// These tests pin the fallback that closes that hole: after a delay, ask the
// Pi session itself whether it is idle, and if it is, deliver the queued
// messages as an idle write (which starts a new turn).

import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, onTestFinished, test, vi } from "vitest";
import type { ChildProcess } from "node:child_process";
import { asAxSurfaceText, type AgentConfig, type AgentMessage, type AxSurfaceText, type MachineToServerMessage } from "@botiverse/raft-shared";
import { AgentProcessManager } from "./agentProcessManager";
import { logger } from "./logger";
import { releaseAgentManagerForTests } from "./testing/agentManagerTeardown";
import type { RuntimeDriver, SpawnContext, SpawnResult, ParsedEvent } from "./drivers/index";
import type { IdleRecycleStrategy } from "./drivers/types";

const FALLBACK_DELAY_MS = 30_000;

class FakeChildProcess extends EventEmitter {
  stdout = new EventEmitter();
  stderr = new EventEmitter();
  readonly stdinWrites: string[] = [];
  stdin = {
    write: (chunk: string) => {
      this.stdinWrites.push(chunk);
      return true;
    },
  };
  killed = false;
  kill(): boolean {
    this.killed = true;
    this.emit("exit", 143, "SIGTERM");
    this.emit("close", 143, "SIGTERM");
    return true;
  }
}

class FakePiDriver implements RuntimeDriver {
  /** What the live Pi session reports. Null models "no answer available". */
  liveIdle: { isIdle: boolean } | null = { isIdle: false };
  readonly idleRecycle: IdleRecycleStrategy = {
    configure: (config) => ({ config }),
    readFacts: async () => ({
      transcriptPath: null,
      lastActivityAtMs: null,
      lastContextTokens: null,
      unavailableReason: "test_fake",
    }),
    checkWork: async () => ({ status: "unknown" as const, reason: "test_fake" }),
    stateSafe: () => false,
    logSkip: () => false,
    wakeGapReason: () => null,
    wakeReason: "large_context",
    buildPanel: async () => null,
    panelUnavailableReason: "test_fake",
    probeIdle: () => this.liveIdle,
  };
  readonly id = "pi";
  readonly lifecycle = { kind: "persistent", stdin: "direct", inFlightWake: "steer" } as const;
  readonly communication = { chat: "slock_cli", runtimeControl: "none" } as const;
  readonly session = { recovery: "resume_or_fresh" } as const;
  readonly model = { detectedModelsVerifiedAs: "launchable" } as const;
  readonly supportsStdinNotification = true;
  readonly busyDeliveryMode = "direct" as const;
  readonly liveSessionReadyAt = "turn_end" as const;
  readonly supportsNativeStandingPrompt = true;
  readonly spawnCalls: SpawnContext[] = [];
  readonly processes: FakeChildProcess[] = [];

  spawn(ctx: SpawnContext): SpawnResult {
    this.spawnCalls.push(ctx);
    const child = new FakeChildProcess();
    this.processes.push(child);
    return { process: child as unknown as ChildProcess };
  }

  parseLine(): ParsedEvent[] {
    return [];
  }

  encodeStdinMessage(text?: string): string | null {
    return text ? JSON.stringify({ text }) : null;
  }

  buildSystemPrompt(): AxSurfaceText {
    return asAxSurfaceText("pi standing prompt");
  }
}

function makeConfig(overrides: Partial<AgentConfig> = {}): AgentConfig {
  return {
    name: "pi-agent",
    displayName: "Pi Agent",
    description: "test agent",
    model: "pi-model",
    runtime: "pi",
    reasoningEffort: null,
    constructedWakeContext: true,
    envVars: null,
    sessionId: null,
    serverUrl: "http://localhost:3001",
    authToken: "sk_machine_test",
    agentCredentialKey: "sk_agent_test",
    agentCredentialId: null,
    ...overrides,
  };
}

function makeMessage(content: string, overrides: Partial<AgentMessage> = {}): AgentMessage {
  return {
    channel_id: "channel-1",
    channel_name: "general",
    channel_type: "channel",
    sender_id: "user-1",
    sender_name: "dassai",
    sender_type: "human",
    content,
    timestamp: "2026-11-09T04:00:00.000Z",
    ...overrides,
  } as AgentMessage;
}

async function withManager(
  fn: (ctx: { driver: FakePiDriver; manager: AgentProcessManager; dataDir: string; sent: MachineToServerMessage[] }) => Promise<void>,
): Promise<void> {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "slock-pi-idle-fallback-test-"));
  const sent: MachineToServerMessage[] = [];
  const driver = new FakePiDriver();
  const manager = new AgentProcessManager(
    (msg) => sent.push(msg),
    "sk_machine_test",
    {
      dataDir,
      serverUrl: "https://daemon.example.com",
      driverResolver: () => driver,
      runtimeSessionHomeDir: dataDir,
    },
  );
  try {
    await fn({ driver, manager, dataDir, sent });
  } finally {
    await releaseAgentManagerForTests(manager);
    await rm(dataDir, { recursive: true, force: true });
  }
}

function getProcess(manager: AgentProcessManager, agentId: string): any {
  return (manager as any).agents.get(agentId);
}

/** A running Pi agent the daemon believes is busy, with one queued message. */
async function startBusyPiWithQueuedMessage(
  ctx: { driver: FakePiDriver; manager: AgentProcessManager },
  agentId: string,
  content: string,
): Promise<any> {
  await ctx.manager.startAgent(agentId, makeConfig({ sessionId: "session-pi-1" }));
  const ap = getProcess(ctx.manager, agentId);
  ap.sessionId = "session-pi-1";
  ap.sessionReadyForDelivery = true;
  // The daemon's own idle copy says "busy": the state that went stale.
  (ctx.manager as any).commitApmIdleState(agentId, ap, false);
  const message = makeMessage(content, { message_id: `msg-${content}`, seq: 11 });
  ap.inbox.push(message);
  ap.notifications.add();
  return ap;
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("the fallback delivers a queued message as a new turn once Pi reports idle", async () => {
  vi.useFakeTimers();
  await withManager(async ({ driver, manager }) => {
    const ap = await startBusyPiWithQueuedMessage({ driver, manager }, "agent-pi-1", "queued while busy");
    driver.liveIdle = { isIdle: true };

    (manager as any).schedulePiIdleDeliveryFallback("agent-pi-1", "test");
    await vi.advanceTimersByTimeAsync(FALLBACK_DELAY_MS - 1);
    assert.equal(driver.processes[0]!.stdinWrites.length, 0, "the fallback does not fire early");

    await vi.advanceTimersByTimeAsync(2);
    assert.equal(driver.processes[0]!.stdinWrites.length, 1, "the queued message was written to the session");
    // The idle write is the content-free inbox notice: it starts a new turn
    // and the bodies stay in the daemon-local inbox.
    assert.match(driver.processes[0]!.stdinWrites[0]!, /Raft inbox notice/);
    assert.match(driver.processes[0]!.stdinWrites[0]!, /msg-queu/);
    assert.equal((manager as any).isApmIdle(ap), true, "the daemon agrees the session is idle again");
  });
});

test("the fallback stays silent while Pi still reports busy", async () => {
  vi.useFakeTimers();
  await withManager(async ({ driver, manager }) => {
    await startBusyPiWithQueuedMessage({ driver, manager }, "agent-pi-2", "still working");
    driver.liveIdle = { isIdle: false };

    (manager as any).schedulePiIdleDeliveryFallback("agent-pi-2", "test");
    await vi.advanceTimersByTimeAsync(FALLBACK_DELAY_MS + 1_000);

    assert.equal(driver.processes[0]!.stdinWrites.length, 0, "a busy Pi session is left alone");
    assert.equal((manager as any).isApmIdle(getProcess(manager, "agent-pi-2")), false, "the daemon keeps its own state");
  });
});

test("the fallback does not repeat a message the session was already told about", async () => {
  vi.useFakeTimers();
  await withManager(async ({ driver, manager }) => {
    const ap = await startBusyPiWithQueuedMessage({ driver, manager }, "agent-pi-3", "already told");
    driver.liveIdle = { isIdle: true };
    // The session already received this message: it is the agent's own
    // deferral, not work the daemon still owes the runtime.
    const message = ap.inbox[0];
    ap.notifications.recordNoticeWritten("fingerprint-pi-3", ap.sessionId, [message]);

    (manager as any).schedulePiIdleDeliveryFallback("agent-pi-3", "test");
    await vi.advanceTimersByTimeAsync(FALLBACK_DELAY_MS + 1_000);

    assert.equal(driver.processes[0]!.stdinWrites.length, 0, "a told message is not told again");
  });
});

test("Pi idle while the daemon recorded busy: the daemon state is corrected, the message is delivered, and the disagreement is logged", async () => {
  vi.useFakeTimers();
  await withManager(async ({ driver, manager }) => {
    const ap = await startBusyPiWithQueuedMessage({ driver, manager }, "agent-pi-4", "delivered by the fallback");
    driver.liveIdle = { isIdle: true };
    const warn = vi.spyOn(logger, "warn").mockImplementation(() => {});
    const info = vi.spyOn(logger, "info").mockImplementation(() => {});

    assert.equal((manager as any).isApmIdle(ap), false, "precondition: the daemon believes Pi is busy");
    (manager as any).schedulePiIdleDeliveryFallback("agent-pi-4", "test");
    await vi.advanceTimersByTimeAsync(FALLBACK_DELAY_MS + 1_000);

    assert.equal(driver.processes[0]!.stdinWrites.length, 1, "the message reached the idle session");
    assert.equal((manager as any).isApmIdle(ap), true, "the daemon idle state was corrected");
    const warnText = warn.mock.calls.map((call) => String(call[0])).join("\n");
    assert.match(warnText, /Pi reports idle but the daemon recorded busy/, "the disagreement is logged as a WARN");
    assert.match(warnText, /outstandingToolUses=/, "the WARN carries the daemon's tool-use count");
    const infoText = info.mock.calls.map((call) => String(call[0])).join("\n");
    assert.match(infoText, /Pi idle delivery fallback/, "the fallback path logs an INFO line to the runner log");
  });
});
