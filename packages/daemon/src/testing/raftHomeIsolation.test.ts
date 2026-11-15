/**
 * The regression test for the pollution this file's subject exists for: a
 * `DaemonCore` built without `slockHome` must record runtime CLI versions in a
 * throwaway home, never in the Raft home this host actually uses.
 *
 * It deliberately does not import `./raftHomeIsolation`: importing that module
 * is enough to run its side effects, so the test would pass even when the file
 * is missing from `vitest.config.ts` `setupFiles` — the case it is here to catch.
 * The setup publishes what it replaced as `RAFT_TEST_PREVIOUS_RAFT_HOME`, and a
 * run without it fails on the first assertion below.
 */
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { existsSync } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "vitest";
import { DaemonCore, type RuntimeDetection } from "../core";
import type { WebSocketLike } from "../connection";
import { resolveDefaultRaftHome, resolveRaftHome } from "../raftHome";
import { readRuntimeCliVersionState, runtimeCliVersionsPath } from "../runtimeCliVersionTracker";

const ISOLATED_PREFIX = path.join(os.tmpdir(), "raft-daemon-test-home-");
const PREVIOUS_HOME_ENV = "RAFT_TEST_PREVIOUS_RAFT_HOME";
/** A fake runtime id: a real one would make the model-catalog publish probe a real CLI. */
const PROBE_RUNTIME = "isolation-probe-runtime";

class FakeWebSocket extends EventEmitter implements WebSocketLike {
  readyState = 0;
  send(_data: string): void {}
  close(): void { this.readyState = 3; this.emit("close", 1000, Buffer.from("")); }
  terminate(): void { this.readyState = 3; this.emit("close", 1006, Buffer.from("")); }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 20));

/** Publish a fake detection through the daemon's ordinary detection → ready → track path. */
async function detectWithoutSlockHome(detection: RuntimeDetection): Promise<void> {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), "raft-daemon-isolation-"));
  const socket = new FakeWebSocket();
  const core = new DaemonCore({
    serverUrl: "https://daemon.example.com",
    apiKey: `sk_machine_isolation_${process.pid}`,
    dataDir,
    runtimeDetector: () => detection,
    connectionOptions: { wsFactory: () => socket, minReconnectDelayMs: 10 },
  });
  try {
    core.start();
    socket.emit("upgrade", { headers: {} });
    socket.readyState = 1;
    socket.emit("open");
    socket.emit("message", Buffer.from(JSON.stringify({ type: "machine:context", machineId: "machine-test", serverId: "server-test" })));
    await tick();
  } finally {
    await core.stop();
    await rm(dataDir, { recursive: true, force: true });
  }
}

/** Does this file exist, and if so with which byte count and modification time? */
async function fingerprint(file: string): Promise<{ file: string; exists: boolean; size?: number; mtimeMs?: number }> {
  try {
    const info = await stat(file);
    return { file, exists: true, size: info.size, mtimeMs: info.mtimeMs };
  } catch {
    return { file, exists: false };
  }
}

test("a test run resolves a throwaway Raft home, never the host's", () => {
  const isolated = resolveRaftHome();
  const previous = process.env[PREVIOUS_HOME_ENV];

  assert.ok(
    typeof previous === "string",
    "packages/daemon/src/testing/raftHomeIsolation.ts is not in vitest.config.ts setupFiles: a DaemonCore built "
      + `without slockHome then resolves the host's real Raft home (${isolated}) and writes machine state into it`,
  );
  assert.ok(
    isolated.startsWith(ISOLATED_PREFIX),
    `resolveRaftHome() returned ${isolated}, outside the throwaway home the test setup creates`,
  );
  assert.ok(
    !previous.startsWith(ISOLATED_PREFIX),
    `the recorded original home is a previous test file's throwaway home (${previous}), so the host-file check below would vouch for the wrong directory`,
  );
  assert.equal(process.env.SLOCK_HOME, isolated);
  assert.equal(process.env.RAFT_HOME, undefined, "RAFT_HOME wins over SLOCK_HOME, so a run must not leave it pointing anywhere");
  assert.notEqual(isolated, resolveDefaultRaftHome(os.homedir()), "the run must not resolve the host's default Raft home");
  if (previous) assert.notEqual(isolated, path.resolve(previous), "the run must not resolve the home the environment pointed at");
});

test("a DaemonCore built without slockHome records versions in the throwaway home and leaves the host's files alone", async () => {
  const isolated = resolveRaftHome();
  const previous = process.env[PREVIOUS_HOME_ENV];
  assert.ok(
    typeof previous === "string",
    "packages/daemon/src/testing/raftHomeIsolation.ts is not in vitest.config.ts setupFiles: the home this run would "
      + `have written to is the one the environment names (${previous ?? "unset"}), which this test cannot see and cannot vouch for`,
  );
  // Every home this run could realistically have landed on before the fix: the
  // one the environment named, and the default `~/.raft` / `~/.slock`.
  const hostHomes = [...new Set(
    [previous, resolveDefaultRaftHome(os.homedir())]
      .filter((home): home is string => Boolean(home))
      .map((home) => path.resolve(home))
      .filter((home) => home !== isolated),
  )];
  const hostFiles = hostHomes.map((home) => runtimeCliVersionsPath(home));
  const before = await Promise.all(hostFiles.map(fingerprint));

  const record = runtimeCliVersionsPath(isolated);
  await detectWithoutSlockHome({ ids: [PROBE_RUNTIME], versions: { [PROBE_RUNTIME]: "0.0.1" } });

  // The record is written behind a promise chain, so give the write its moment;
  // failing here means the track-then-write path did not run and the rest of
  // this test would prove nothing.
  for (let waited = 0; !existsSync(record) && waited < 3000; waited += 20) await tick();
  assert.ok(existsSync(record), `the detection wrote no ${record}`);
  const state = await readRuntimeCliVersionState(isolated);
  assert.equal(state.versions[PROBE_RUNTIME], "0.0.1");

  assert.notEqual(record, runtimeCliVersionsPath(resolveDefaultRaftHome(os.homedir())));
  assert.deepEqual(await Promise.all(hostFiles.map(fingerprint)), before, "the detection changed a file outside the throwaway home");
});
