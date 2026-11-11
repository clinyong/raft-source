import assert from "node:assert/strict";
import { test } from "vitest";
import type { RuntimeSession } from "./drivers/index";
import { OUTPUT_REJECTION_TRACE_WINDOW_MS, RuntimeProcessBindingFence, type RuntimeBindingProcess } from "./runtimeProcessBindingFence";

function process(id: string): RuntimeBindingProcess {
  return {
    runtime: { descriptor: { transport: "stdio" } } as unknown as RuntimeSession,
    driver: { id: "codex" },
    config: { runtime: "codex", runtimeContext: { serverId: "s", machineId: "m" } },
    launchId: "launch",
    sessionId: "session",
    processInstanceId: id,
  };
}

test("a replaced process streaming output records one rejection per window, with the count it suppressed", () => {
  let now = 1_000;
  const records: Record<string, unknown>[] = [];
  const stale = process("old");
  const current = process("new");
  const fence = new RuntimeProcessBindingFence({
    getCurrentProcess: () => current,
    recordTrace: (name, attrs) => { if (name === "daemon.agent.runtime_binding.rejected") records.push(attrs ?? {}); },
    now: () => now,
  });
  const binding = (p: RuntimeBindingProcess) => ({
    agentId: "a", serverId: "s", machineId: "m", configuredRuntimeId: "codex", driverId: "codex",
    initialLaunchId: "launch", activeLaunchId: "launch", startSessionId: "session", activeSessionId: "session",
    processInstanceId: p.processInstanceId,
  });
  fence.bind(stale.runtime, binding(stale));
  fence.bind(current.runtime, binding(current));

  for (let i = 0; i < 5_000; i += 1) assert.equal(fence.acceptOutput("a", stale, stale.runtime, "stdout"), false);
  assert.equal(fence.acceptOutput("a", stale, stale.runtime, "stderr"), false);
  assert.equal(records[0]?.agent_id, "a");
  assert.equal(records[0]?.process_instance_id, "old");
  assert.deepEqual(records.map((r) => [r.source, r.reason, r.suppressed_count]), [
    ["stdout", "inactive_process_generation", 0],
    ["stderr", "inactive_process_generation", 0],
  ], "each source is recorded the first time, the rest of the burst is only counted");

  now += OUTPUT_REJECTION_TRACE_WINDOW_MS;
  fence.acceptOutput("a", stale, stale.runtime, "stdout");
  assert.deepEqual(records.at(-1)?.suppressed_count, 4_999);
  assert.equal(records.length, 3);

  // Input rejections are one per send attempt and are never collapsed.
  fence.send("a", stale, { mode: "idle", text: "x", sessionId: "session" }, "stdin_idle_delivery");
  fence.send("a", stale, { mode: "idle", text: "x", sessionId: "session" }, "stdin_idle_delivery");
  assert.equal(records.filter((r) => r.direction === "input").length, 2);
});

test("handoff guard blocks every input source before the runtime receives bytes", () => {
  let writes = 0;
  let paused = true;
  const current = process("current");
  Object.assign(current.runtime, { send: () => { writes += 1; return { ok: true, acceptedAs: "prompt" }; } });
  const fence = new RuntimeProcessBindingFence({
    getCurrentProcess: () => current,
    recordTrace: () => {},
    shouldBlockInput: () => paused,
  });
  fence.bind(current.runtime, {
    agentId: "a", serverId: "s", machineId: "m", configuredRuntimeId: "codex", driverId: "codex",
    initialLaunchId: "launch", activeLaunchId: "launch", startSessionId: "session", activeSessionId: "session",
    processInstanceId: current.processInstanceId,
  });
  for (const source of ["app_inbox_notice", "busy_stdin_notification", "stdin_idle_delivery", "stdin_idle_delivery_retry", "future_entry"]) {
    assert.equal(fence.send("a", current, { mode: "idle", text: "x", sessionId: "session" }, source).ok, false);
  }
  assert.equal(writes, 0);
  paused = false;
  assert.equal(fence.send("a", current, { mode: "idle", text: "x", sessionId: "session" }, "stdin_idle_delivery").ok, true);
  assert.equal(writes, 1);
});
