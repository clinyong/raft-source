import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { assertBuildIdentity, resolveBuildIdentity } from "./source-identity.mjs";

function makeRepository() {
  const root = mkdtempSync(join(tmpdir(), "computer-source-identity-"));
  const git = (...args) => execFileSync("git", ["-C", root, ...args], { stdio: "pipe" });
  git("init", "--quiet");
  git("config", "user.name", "Computer build identity test");
  git("config", "user.email", "computer-build-identity@example.invalid");
  writeFileSync(join(root, "tracked.txt"), "committed\n");
  git("add", "tracked.txt");
  git("commit", "--quiet", "-m", "fixture");
  const commitSha = git("rev-parse", "HEAD").toString().trim();
  return { root, commitSha, git };
}

test("native builder parses and rejects malformed or non-HEAD --commit-sha before building", () => {
  const builder = fileURLToPath(new URL("./build.mjs", import.meta.url));
  const malformed = spawnSync(process.execPath, [builder, "--commit-sha", "short"], { encoding: "utf8" });
  assert.equal(malformed.status, 1);
  assert.match(malformed.stderr, /full 40-character lowercase commit SHA/);

  const mismatch = spawnSync(process.execPath, [builder, "--commit-sha", "0".repeat(40)], { encoding: "utf8" });
  assert.equal(mismatch.status, 1);
  assert.match(mismatch.stderr, /does not match build worktree HEAD/);
});

test("build identity injects the exact full HEAD commit and rejects invalid or mismatched input", () => {
  const repo = makeRepository();
  try {
    assert.deepEqual(resolveBuildIdentity({ repoRoot: repo.root, commitSha: repo.commitSha }), {
      commitSha: repo.commitSha,
      sourceDirty: false,
    });
    assert.throws(
      () => resolveBuildIdentity({ repoRoot: repo.root, commitSha: repo.commitSha.slice(0, 12) }),
      /full 40-character/,
    );
    const otherCommit = `${repo.commitSha.slice(0, 39)}${repo.commitSha.endsWith("0") ? "1" : "0"}`;
    assert.throws(
      () => resolveBuildIdentity({ repoRoot: repo.root, commitSha: otherCommit }),
      /does not match build worktree HEAD/,
    );
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test("build identity records tracked and untracked worktree changes as dirty", () => {
  const repo = makeRepository();
  try {
    writeFileSync(join(repo.root, "tracked.txt"), "modified\n");
    writeFileSync(join(repo.root, "untracked.txt"), "untracked\n");
    const identity = resolveBuildIdentity({ repoRoot: repo.root, commitSha: repo.commitSha });
    assert.deepEqual(identity, { commitSha: repo.commitSha, sourceDirty: true });
    assert.throws(
      () => assertBuildIdentity({ identity, expectedCommitSha: repo.commitSha, expectedSourceDirty: false }),
      /sourceDirty mismatch/,
    );
    assert.doesNotThrow(
      () => assertBuildIdentity({ identity, expectedCommitSha: repo.commitSha, expectedSourceDirty: true }),
    );
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test("native identity verification rejects SHA mismatch and malformed SHA metadata", () => {
  const expectedCommitSha = "b87bd2dabf2ec75f91e453249a6409b68148e2c3";
  assert.throws(
    () => assertBuildIdentity({
      identity: { commitSha: "0".repeat(40), sourceDirty: false },
      expectedCommitSha,
      expectedSourceDirty: false,
    }),
    /commitSha mismatch/,
  );
  assert.throws(
    () => assertBuildIdentity({
      identity: { commitSha: "short", sourceDirty: false },
      expectedCommitSha,
      expectedSourceDirty: false,
    }),
    /commitSha mismatch/,
  );
});
