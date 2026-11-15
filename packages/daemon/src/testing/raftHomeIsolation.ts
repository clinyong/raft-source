/**
 * Vitest setup for the daemon package: every test file runs against its own
 * throwaway Raft home under the OS temporary directory.
 *
 * Why: `DaemonCore` writes machine state — `computer/runtime-cli-versions.json`
 * (see `runtimeCliVersionTracker.ts`) — into the Raft home it resolved, and when
 * it is not given a `slockHome` it resolves `RAFT_HOME` / `SLOCK_HOME` from the
 * environment (`raftHome.ts`). On a developer machine those are usually unset,
 * so a test landed on `~/.raft`. On an agent host they point at the live Raft
 * home, and the same test wrote fixture runtime versions into production: on
 * 2026-11-15 a full test run left four test-only runtime ids in the live
 * `runtime-cli-versions.json` and a downgrade notice that could no longer clear,
 * because the runtime it named was no longer installed to be detected again.
 *
 * So neither the guard in `DaemonCore` (it trusts RAFT_HOME as a deliberate
 * choice, and on an agent host it genuinely is one) nor "remember to pass
 * `slockHome`" is enough. Point the home environment at a temporary directory
 * here and every test — whoever runs it, on whatever host — is safe by
 * construction.
 *
 * Which variables: `SLOCK_HOME` is set and `RAFT_HOME` is cleared. Both resolve
 * the home (`resolveRaftHome` prefers `RAFT_HOME`), so clearing the one that
 * wins and setting the one every test already manipulates covers every reader
 * — and leaves the ~16 existing tests that point `SLOCK_HOME` at their own
 * temporary directory working as they were written. Setting `RAFT_HOME` too
 * would silently override each of those tests and hide what they assert.
 *
 * Registered in `vitest.config.ts` (`setupFiles`), next to the network guard:
 * a test that reaches a real transport fails, and so does one that resolves the
 * host's real Raft home (`src/testing/raftHomeIsolation.test.ts`).
 */
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll } from "vitest";
import { RAFT_HOME_ENV, SLOCK_HOME_ENV } from "../raftHome";

/** Prefix of the throwaway home. The regression test spells this out itself, on purpose:
 *  importing this module there would run the side effects below and hide the case it exists for. */
export const ISOLATED_RAFT_HOME_PREFIX = "raft-daemon-test-home-";

/** Set to the home the environment named before the first test file replaced it (`""` when it named none).
 *  The regression test reads the name literally, for the same reason as above. */
export const PREVIOUS_RAFT_HOME_ENV = "RAFT_TEST_PREVIOUS_RAFT_HOME";

// The first file in a worker records what the host really had; later files in the
// same worker inherit the variables this file set, so they keep that first record
// instead of replacing it with a throwaway home (`""` is a record too, hence ??).
const previous = process.env[PREVIOUS_RAFT_HOME_ENV]
  ?? (process.env[RAFT_HOME_ENV]?.trim() || process.env[SLOCK_HOME_ENV]?.trim() || "");

/** The throwaway Raft home every `DaemonCore` in this test file resolves to. */
export const isolatedRaftHome = mkdtempSync(path.join(os.tmpdir(), ISOLATED_RAFT_HOME_PREFIX));

process.env[SLOCK_HOME_ENV] = isolatedRaftHome;
delete process.env[RAFT_HOME_ENV];
process.env[PREVIOUS_RAFT_HOME_ENV] = previous;

afterAll(() => {
  rmSync(isolatedRaftHome, { recursive: true, force: true });
});
