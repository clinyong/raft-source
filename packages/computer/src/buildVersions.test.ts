import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

test("__build-versions includes build provenance fields", async () => {
  const home = await mkdtemp(join(tmpdir(), "computer-build-versions-"));
  try {
    const entry = fileURLToPath(new URL("./index.ts", import.meta.url));
    const result = spawnSync(process.execPath, ["--import", "@oxc-node/core/register", entry, "__build-versions"], {
      encoding: "utf8",
      timeout: 20_000,
      env: { ...process.env, RAFT_HOME: home, SLOCK_HOME: home },
    });
    assert.equal(result.status, 0, result.stderr);
    const reported = JSON.parse(result.stdout);
    assert.deepEqual(Object.keys(reported).sort(), [
      "cliVersion",
      "commitSha",
      "computerVersion",
      "daemonVersion",
      "sourceDirty",
    ]);
    assert.equal(reported.commitSha, null);
    assert.equal(reported.sourceDirty, null);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});
