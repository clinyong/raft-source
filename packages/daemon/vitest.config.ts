import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    // Local runs regenerate snapshots for review; CI only checks committed output.
    update: !process.env.CI,
    environment: "node",
    include: ["src/**/*.test.ts"],
    // Unit tests never reach the network (src/testing/networkGuard.ts) and never
    // resolve the host's real Raft home (src/testing/raftHomeIsolation.ts).
    setupFiles: ["./src/testing/raftHomeIsolation.ts", "./src/testing/networkGuard.ts"],
    pool: "forks",
    // CI runners (blacksmith-4vcpu, 16 GB) intermittently OOM-kill this suite
    // (exit 137, no test failure — runs 38031513722 / 38023257486): one fork per
    // CPU each carries a Node heap plus spawned CLI children from the APM tests.
    // Cap CI at two workers; peak memory halves and the suite stays well inside
    // the 20-minute step ceiling. Local runs keep full parallelism.
    // vitest 5.x takes the cap as top-level maxWorkers (poolOptions.forks does
    // not exist in this version's config types).
    maxWorkers: process.env.CI ? 2 : undefined,
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
