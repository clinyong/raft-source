import { spawnSync } from "node:child_process";

const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/;

function git(repoRoot, args) {
  const result = spawnSync("git", ["-C", repoRoot, ...args], {
    encoding: "utf8",
    windowsHide: true,
  });
  if (result.error || result.status !== 0) {
    const detail = result.error?.message ?? result.stderr?.trim() ?? `exit ${result.status ?? result.signal}`;
    throw new Error(`could not read build source identity (git ${args.join(" ")}): ${detail}`);
  }
  return result.stdout;
}

export function resolveBuildIdentity({ repoRoot, commitSha }) {
  if (commitSha !== undefined && !FULL_COMMIT_SHA.test(commitSha)) {
    throw new Error("--commit-sha must be a full 40-character lowercase commit SHA");
  }

  const actualSha = git(repoRoot, ["rev-parse", "--verify", "HEAD^{commit}"]).trim();
  if (!FULL_COMMIT_SHA.test(actualSha)) {
    throw new Error(`git returned an invalid HEAD commit SHA: ${JSON.stringify(actualSha)}`);
  }
  if (commitSha !== undefined && commitSha !== actualSha) {
    throw new Error(`--commit-sha ${commitSha} does not match build worktree HEAD ${actualSha}`);
  }

  const status = git(repoRoot, ["status", "--porcelain", "--untracked-files=all"]);
  return { commitSha: actualSha, sourceDirty: status.length > 0 };
}

export function assertBuildIdentity({ identity, expectedCommitSha, expectedSourceDirty }) {
  if (identity.commitSha !== expectedCommitSha) {
    throw new Error(
      `native smoke commitSha mismatch: got ${JSON.stringify(identity.commitSha)}, expected ${expectedCommitSha}`,
    );
  }
  if (identity.sourceDirty !== expectedSourceDirty) {
    throw new Error(
      `native smoke sourceDirty mismatch: got ${JSON.stringify(identity.sourceDirty)}, expected ${expectedSourceDirty}`,
    );
  }
}

export function isFullCommitSha(value) {
  return typeof value === "string" && FULL_COMMIT_SHA.test(value);
}
