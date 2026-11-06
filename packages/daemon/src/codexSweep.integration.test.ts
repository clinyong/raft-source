/** Isolated real Codex app-server + Raft manager. Opt in explicitly: it uses
 * subscription quota, copies auth into a temporary HOME, and never contacts
 * a Raft server. The old rollout is seeded with synthetic actions. */
import assert from "node:assert/strict";
import { mkdtemp, mkdir, copyFile, chmod, writeFile, appendFile, readdir, rm, symlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { onTestFinished, test } from "vitest";
import { existsSync } from "node:fs";
import { AgentProcessManager } from "./agentProcessManager";
import { CodexDriver } from "./drivers/codex";
import { installManagedRunnerCredentialFetch } from "./testing/managedRunnerCredentialFetch";
import { releaseAgentManagerForTests } from "./testing/agentManagerTeardown";

const live = process.env.RUN_CODEX_INTEGRATION_TESTS === "1" ? test : test.skip;

live("isolated Codex sweep stops then wakes a fresh session with a briefing", { timeout: 120_000 }, async () => {
  const originalHome = os.homedir();
  const root = await mkdtemp(path.join(os.tmpdir(), "codex-sweep-live-"));
  onTestFinished(async () => { await rm(root, { recursive: true, force: true }); });
  const home = path.join(root, "home"); const bin = path.join(root, "bin"); const dataDir = path.join(root, "agents");
  await mkdir(path.join(home, ".codex"), { recursive: true }); await mkdir(bin); await mkdir(dataDir);
  await copyFile(path.join(originalHome, ".codex", "auth.json"), path.join(home, ".codex", "auth.json"));
  await chmod(path.join(home, ".codex", "auth.json"), 0o600);
  const codexBinary = (process.env.PATH ?? "").split(path.delimiter).map((dir) => path.join(dir, "codex")).find(existsSync);
  assert.ok(codexBinary, "Codex must be installed on PATH");
  await symlink(codexBinary, path.join(bin, "codex"));
  const stub = path.join(root, "raft-stub.js");
  await writeFile(stub, 'console.log("isolated stub: no Raft server");\n');
  const saved = { HOME: process.env.HOME, CODEX_HOME: process.env.CODEX_HOME, PATH: process.env.PATH, RAFT_HOME: process.env.RAFT_HOME, SLOCK_HOME: process.env.SLOCK_HOME, RAFT_WAKE_RECYCLE_SWEEP_MS: process.env.RAFT_WAKE_RECYCLE_SWEEP_MS };
  Object.assign(process.env, { HOME: home, CODEX_HOME: path.join(home, ".codex"), PATH: `${bin}:/usr/bin:/bin:/usr/sbin:/sbin`, RAFT_HOME: root, SLOCK_HOME: root, RAFT_WAKE_RECYCLE_SWEEP_MS: "250" });
  const restoreFetch = installManagedRunnerCredentialFetch();
  const sent: any[] = [];
  const manager = new AgentProcessManager((m) => sent.push(m), "fake-machine-token", {
    dataDir, slockHome: root, slockCliPath: stub, serverUrl: "http://127.0.0.1:9", driverResolver: () => new CodexDriver(), runtimeSessionHomeDir: home,
  });
  const wait = async (predicate: () => boolean, label: string, ms = 35_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) { if (predicate()) return; await new Promise((r) => setTimeout(r, 100)); }
    throw new Error(`timeout: ${label}`);
  };
  try {
    const config: any = { name: "isolated", displayName: "Isolated", description: "Isolated test agent", model: "gpt-6.1-terra", runtime: "codex", reasoningEffort: null, constructedWakeContext: true, envVars: { HOME: home, CODEX_HOME: path.join(home, ".codex"), PATH: `${bin}:/usr/bin:/bin:/usr/sbin:/sbin`, RAFT_CODEX_WAKE_RECYCLE: "1", RAFT_CODEX_WAKE_RECYCLE_MIN_CONTEXT_TOKENS: "1", RAFT_CODEX_WAKE_RECYCLE_MIN_IDLE_MS: "1000" }, sessionId: null, serverUrl: "http://127.0.0.1:9", authToken: "fake-token", agentCredentialKey: "fake-agent-token", agentCredentialId: null };
    const marker = "violet-koala-2846";
    await manager.startAgent("isolated", config, undefined, undefined, "Reply READY. Do not use tools.");
    await wait(() => { const ap = (manager as any).agents.get("isolated"); return Boolean(ap?.sessionId && ap.gatedSteering.isIdle); }, "first Codex turn");
    const first = (manager as any).agents.get("isolated");
    const oldId = first.sessionId as string;
    const findRollout = async (dir: string): Promise<string | null> => {
      for (const e of await readdir(dir, { withFileTypes: true })) { const p = path.join(dir, e.name); if (e.isDirectory()) { const found = await findRollout(p); if (found) return found; } else if (e.name.endsWith(`-${oldId}.jsonl`)) return p; }
      return null;
    };
    const rollout = await findRollout(path.join(home, ".codex", "sessions"));
    assert.ok(rollout);
    const workspace = path.join(dataDir, "isolated");
    await writeFile(path.join(workspace, "work.md"), `Task marker: ${marker}\n`);
    const timestamp = new Date().toISOString();
    const item = (payload: unknown) => JSON.stringify({ timestamp, type: "response_item", payload });
    const actions = Array.from({ length: 5 }, (_, n) => [item({ type: "custom_tool_call", name: "exec", call_id: `synthetic-${n}`, input: 'tools.exec_command({cmd:"cat work.md"})' }), item({ type: "custom_tool_call_output", call_id: `synthetic-${n}`, output: [{ type: "text", text: `Task marker: ${marker}` }] })]).flat();
    const previous = item({ type: "message", role: "user", content: [{ type: "input_text", text: `[target=#test msg=abcdef12 time=2026-11-05 00:00:00Z type=human] @test: Remember task marker ${marker}.` }] });
    const usage = JSON.stringify({ timestamp, type: "event_msg", payload: { type: "token_count", info: { last_token_usage: { input_tokens: 20000 } } } });
    await appendFile(rollout, [previous, ...actions, usage].join("\n") + "\n");
    await wait(() => !(manager as any).agents.has("isolated") && Boolean((manager as any).lifecycleRecords.getRestartSnapshot("isolated")), "idle sweep stop");
    const accepted = await manager.deliverMessage("isolated", { channel_id: "system", channel_name: "system", channel_type: "dm", sender_id: "test", sender_name: "test", sender_type: "system", message_id: "wake-test", content: "Reply with the exact task marker from the recent handoff. Do not use tools.", timestamp: new Date().toISOString() } as any, { transient: true });
    assert.equal(accepted, true);
    await wait(() => { const ap = (manager as any).agents.get("isolated"); return Boolean(ap?.sessionId && ap.sessionId !== oldId); }, "new Codex session");
    await wait(() => sent.some((m) => m.type === "agent:activity" && Array.isArray(m.entries) && m.entries.some((entry: any) => entry.kind === "text" && typeof entry.text === "string" && entry.text.includes(marker))), "real Codex reply");
  } finally {
    await releaseAgentManagerForTests(manager); restoreFetch();
    for (const [key, value] of Object.entries(saved)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    await rm(root, { recursive: true, force: true });
  }
});
