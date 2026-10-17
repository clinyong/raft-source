import { PiSdkRuntimeSession, __piPromptsInFlightForTest } from "./pi";

const tick = () => new Promise((resolve) => setImmediate(resolve));

/**
 * A fake SDK session whose three state getters are the ones the SDK exposes
 * (`isStreaming` = an agent run is active, `isCompacting` = compaction is
 * running, `isIdle` = neither). Prompting admits a run, the way the SDK does;
 * prompting while compacting throws, as `AgentSession.prompt()` does; steering
 * only queues, so a steer with no run running is drained by nothing.
 */
function createFakeSession() {
  let streaming = false;
  let compacting = false;
  const waiters: Array<() => void> = [];
  let listener: ((event: { type: string }) => void) | undefined;
  const releaseIfIdle = () => {
    if (streaming || compacting) return;
    for (const resolve of waiters.splice(0)) resolve();
  };
  const session = {
    sessionId: "fake-session",
    prompts: [] as string[],
    steers: [] as string[],
    closed: false,
    get isStreaming() {
      return streaming;
    },
    get isCompacting() {
      return compacting;
    },
    /** The SDK's own predicate: no agent run, no compaction. */
    get isIdle() {
      return !streaming && !compacting;
    },
    async prompt(text: string) {
      if (compacting) {
        throw new Error(
          "Cannot submit a prompt while compaction is in progress. Wait for compaction to finish and retry.",
        );
      }
      session.prompts.push(text);
      streaming = true;
      // The SDK announces the live run on its event stream; the driver releases
      // its start admission from that observation, not only from `finally`.
      listener?.({ type: "turn_start" });
    },
    async steer(text: string) {
      session.steers.push(text);
    },
    async waitForIdle() {
      if (!streaming && !compacting) return;
      await new Promise<void>((resolve) => waiters.push(resolve));
    },
    subscribe(cb: (event: { type: string }) => void) {
      listener = cb;
      return () => {
        listener = undefined;
      };
    },
    /** The agent run finished: the session is settled and no turn is live. */
    endTurn() {
      streaming = false;
      releaseIfIdle();
    },
    /** The runtime was disposed by the driver. */
    dispose() {
      session.closed = true;
    },
    /** The SDK's compaction entry point starts outside any agent run. */
    beginCompaction() {
      compacting = true;
    },
    endCompaction() {
      compacting = false;
      releaseIfIdle();
    },
  };
  return session;
}

function createRuntime(session: ReturnType<typeof createFakeSession>) {
  const ctx = {
    agentId: "routing-test",
    workingDirectory: "/tmp/routing-test",
    standingPrompt: "",
    config: { runtime: "pi", sessionId: null, runtimeContext: null },
  } as never;
  return new PiSdkRuntimeSession(ctx, () => {}, async () => session as never);
}

/**
 * The same adapter also backs the `builtin` runtime
 * (`BuiltInDriver.createSession`), so the routing behaviour is asserted for
 * that configuration too rather than assumed to follow from the Pi one.
 */
function createBuiltinRuntime(session: ReturnType<typeof createFakeSession>) {
  const ctx = {
    agentId: "routing-test-builtin",
    workingDirectory: "/tmp/routing-test",
    standingPrompt: "",
    config: { runtime: "builtin", sessionId: null, runtimeContext: null },
  } as never;
  return new PiSdkRuntimeSession(ctx, () => {}, async () => session as never);
}

async function startedRuntime() {
  const session = createFakeSession();
  const runtime = createRuntime(session);
  await runtime.start({ text: "initial task" });
  await tick();
  session.prompts.length = 0; // the initial prompt already ran; measure delivery only
  return { session, runtime };
}

test("a busy write to a settled, non-compacting session starts a prompt instead of stranding a steer", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  const result = runtime.send({ mode: "busy", text: "fresh follow-up" });
  // The caller asked for the busy lane; which lane the runtime actually took is
  // asserted below through the session calls.
  expect(result).toEqual({ ok: true, acceptedAs: "steer" });
  await tick();
  await tick();

  expect(session.prompts).toEqual(["fresh follow-up"]);
  expect(session.steers).toEqual([]);
  await runtime.dispose();
});

test("a busy write during a live turn still steers", async () => {
  const { session, runtime } = await startedRuntime();

  const result = runtime.send({ mode: "busy", text: "steer this" });
  expect(result).toEqual({ ok: true, acceptedAs: "steer" });
  await tick();
  await tick();

  expect(session.steers).toEqual(["steer this"]);
  expect(session.prompts).toEqual([]);
  await runtime.dispose();
});

test("a busy write during compaction waits for the boundary and is consumed exactly once", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();
  session.beginCompaction();

  runtime.send({ mode: "busy", text: "arrived during compaction" });
  await tick();
  await tick();
  // Nothing written yet: the SDK refuses prompt() while compacting and a steer
  // would have no run to drain it.
  expect(session.prompts).toEqual([]);
  expect(session.steers).toEqual([]);

  session.endCompaction();
  await tick();
  await tick();

  expect(session.prompts).toEqual(["arrived during compaction"]);
  expect(session.steers).toEqual([]);
  await runtime.dispose();
});

test("the initial prompt keeps its ordering ahead of a readiness-debt busy write", async () => {
  const session = createFakeSession();
  const runtime = createRuntime(session);

  // The readiness-debt settlement enqueues a busy write immediately after the
  // initial prompt is registered; FIFO ordering must keep the prompt first.
  await runtime.start({ text: "initial task" });
  runtime.send({ mode: "busy", text: "readiness-debt notice" });
  await tick();
  await tick();

  expect(session.prompts).toEqual(["initial task"]);
  expect(session.steers).toEqual(["readiness-debt notice"]);
  await runtime.dispose();
});

test("only the first delivery of a burst starts a turn; the rest steer", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  runtime.send({ mode: "busy", text: "first" });
  runtime.send({ mode: "busy", text: "second" });
  runtime.send({ mode: "busy", text: "third" });
  await tick();
  await tick();

  // "first" saw a settled session and admitted the run; by the time the others
  // were called a run was live, so they steer rather than starting turns.
  expect(session.prompts).toEqual(["first"]);
  expect(session.steers).toEqual(["second", "third"]);
  await runtime.dispose();
});

test("explicit idle delivery is unchanged", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  const result = runtime.send({ mode: "idle", text: "idle delivery" });
  expect(result).toEqual({ ok: true, acceptedAs: "prompt" });
  await tick();
  await tick();

  expect(session.prompts).toEqual(["idle delivery"]);
  expect(session.steers).toEqual([]);
  await runtime.dispose();
});

test("deliveries released by one compaction boundary start a single turn", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();
  session.beginCompaction();

  runtime.send({ mode: "busy", text: "first" });
  runtime.send({ mode: "busy", text: "second" });
  runtime.send({ mode: "busy", text: "third" });
  await tick();
  session.endCompaction();
  await tick();
  await tick();

  // All three waited on the same boundary; only the first may start a run, and
  // the other two must not lose their input to a steer with no run to drain it.
  expect(session.prompts).toEqual(["first"]);
  expect(session.steers).toEqual(["second", "third"]);
  await runtime.dispose();
});

test("a burst during asynchronous prompt admission still starts one turn", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  // The SDK admits a prompt asynchronously, so `isStreaming` stays false for a
  // window after `prompt()` is called.
  let admit!: () => void;
  const admission = new Promise<void>((resolve) => {
    admit = resolve;
  });
  const originalPrompt = session.prompt;
  session.prompt = async (text: string) => {
    await admission;
    await originalPrompt(text);
  };

  runtime.send({ mode: "busy", text: "first" });
  runtime.send({ mode: "busy", text: "second" });
  runtime.send({ mode: "busy", text: "third" });
  await tick();
  await tick();
  admit();
  await tick();
  await tick();

  expect(session.prompts).toEqual(["first"]);
  expect(session.steers).toEqual(["second", "third"]);
  await runtime.dispose();
});

test("a revoked launch cannot resume a delivery that waited for compaction", async () => {
  let current = true;
  const session = createFakeSession();
  const ctx = {
    agentId: "routing-test",
    workingDirectory: "/tmp/routing-test",
    standingPrompt: "",
    config: { runtime: "pi", sessionId: null, runtimeContext: null },
    assertLaunchCurrent() {
      if (!current) throw new Error("launch revoked");
    },
  } as never;
  const runtime = new PiSdkRuntimeSession(ctx, () => {}, async () => session as never);
  await runtime.start({ text: "initial task" });
  await tick();
  session.prompts.length = 0;
  session.endTurn();
  session.beginCompaction();

  runtime.send({ mode: "busy", text: "revoked input" });
  await tick();
  current = false;
  session.endCompaction();
  await tick();
  await tick();

  // The launch was fenced at dispatch: nothing is written, in either lane.
  expect(session.prompts).toEqual([]);
  expect(session.steers).toEqual([]);
  await runtime.dispose();
});

test("a disposed runtime starts nothing that was waiting for compaction", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();
  session.beginCompaction();

  runtime.send({ mode: "busy", text: "cancelled" });
  await tick();
  await runtime.dispose();
  session.endCompaction();
  await tick();
  await tick();

  expect(session.prompts).toEqual([]);
  expect(session.steers).toEqual([]);
});

test("the initial prompt shares the admission with a readiness delivery", async () => {
  const session = createFakeSession();
  let admit!: () => void;
  const gate = new Promise<void>((resolve) => {
    admit = resolve;
  });
  const originalPrompt = session.prompt;
  session.prompt = async (text: string) => {
    if (text === "initial task") await gate;
    await originalPrompt(text);
  };
  // Records how many prompts had already been admitted when the steer landed:
  // a steering message written before any run exists is never drained.
  const promptsAtSteer: number[] = [];
  const originalSteer = session.steer;
  session.steer = async (text: string) => {
    promptsAtSteer.push(session.prompts.length);
    await originalSteer(text);
  };
  const runtime = createRuntime(session);

  await runtime.start({ text: "initial task" });
  runtime.send({ mode: "busy", text: "readiness notice" });
  await tick();
  await tick();
  admit();
  await tick();
  await tick();

  // The readiness delivery must not start a turn ahead of the task it was
  // supposed to accompany, and its steer must join that task's run.
  expect(session.prompts).toEqual(["initial task"]);
  expect(session.steers).toEqual(["readiness notice"]);
  expect(promptsAtSteer).toEqual([1]);
  await runtime.dispose();
});

test("a revoked launch cannot steer into a live run", async () => {
  let current = true;
  const session = createFakeSession();
  const ctx = {
    agentId: "routing-test",
    workingDirectory: "/tmp/routing-test",
    standingPrompt: "",
    config: { runtime: "pi", sessionId: null, runtimeContext: null },
    assertLaunchCurrent() {
      if (!current) throw new Error("launch revoked");
    },
  } as never;
  const runtime = new PiSdkRuntimeSession(ctx, () => {}, async () => session as never);
  await runtime.start({ text: "initial task" });
  await tick();

  current = false;
  runtime.send({ mode: "busy", text: "revoked steering" });
  await tick();
  await tick();

  expect(session.steers).toEqual([]);
  await runtime.dispose();
});

test("an older prompt completing cannot release the next start admission", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  let finishFirst!: () => void;
  const firstDone = new Promise<void>((resolve) => {
    finishFirst = resolve;
  });
  let admitSecond!: () => void;
  const secondAdmission = new Promise<void>((resolve) => {
    admitSecond = resolve;
  });
  const originalPrompt = session.prompt;
  session.prompt = async (text: string) => {
    if (text === "second") await secondAdmission;
    await originalPrompt(text);
    if (text === "first") await firstDone;
  };

  runtime.send({ mode: "busy", text: "first" });
  await tick();
  session.endTurn();
  runtime.send({ mode: "busy", text: "second" });
  await tick();
  runtime.send({ mode: "busy", text: "third" });
  await tick();
  finishFirst();
  await tick();
  await tick();
  admitSecond();
  await tick();
  await tick();

  // "first" completing must not free "second"'s admission: "third" waits for
  // that admission and joins the run instead of starting a third turn.
  expect(session.prompts).toEqual(["first", "second"]);
  expect(session.steers).toEqual(["third"]);
  await runtime.dispose();
});

test("an idle delivery cannot overwrite a pending busy start", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  let admit!: () => void;
  const gate = new Promise<void>((resolve) => {
    admit = resolve;
  });
  const originalPrompt = session.prompt;
  session.prompt = async (text: string) => {
    await gate;
    await originalPrompt(text);
  };

  runtime.send({ mode: "busy", text: "first" });
  await tick();
  runtime.send({ mode: "busy", text: "second" });
  await tick();
  runtime.send({ mode: "idle", text: "third" });
  await tick();
  admit();
  await tick();
  await tick();

  // The idle delivery waits on the busy start's admission instead of replacing
  // it, and then joins the run that start produced.
  expect(session.prompts).toEqual(["first"]);
  expect(session.steers).toEqual(["second", "third"]);
  await runtime.dispose();
});

test("consecutive idle deliveries share one asynchronous admission", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();

  let admit!: () => void;
  const gate = new Promise<void>((resolve) => {
    admit = resolve;
  });
  const originalPrompt = session.prompt;
  session.prompt = async (text: string) => {
    await gate;
    await originalPrompt(text);
  };

  runtime.send({ mode: "idle", text: "first" });
  runtime.send({ mode: "idle", text: "second" });
  await tick();
  await tick();
  admit();
  await tick();
  await tick();

  expect(session.prompts).toEqual(["first"]);
  expect(session.steers).toEqual(["second"]);
  await runtime.dispose();
});

test("a delivery waiting on a compaction boundary stops when the runtime closes", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();
  session.beginCompaction();
  await tick();
  const baseline = __piPromptsInFlightForTest();

  runtime.send({ mode: "busy", text: "waiting on compaction" });
  await tick();
  await tick();
  expect(__piPromptsInFlightForTest() - baseline).toBe(1);

  await runtime.dispose();
  await tick();
  await tick();

  // The compaction never settles in this test; the wait must still end with the
  // runtime rather than outlive it.
  expect(__piPromptsInFlightForTest() - baseline).toBe(0);
  expect(session.prompts).toEqual([]);
  expect(session.steers).toEqual([]);
});

test("a rejected compaction wait fails the delivery once instead of retrying", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();
  session.beginCompaction();
  await tick();
  let waits = 0;
  session.waitForIdle = async () => {
    waits += 1;
    throw new Error("idle wait rejected");
  };
  const events: Array<{ kind?: string }> = [];
  runtime.on("runtime_event", (event: unknown) => events.push(event as { kind?: string }));

  runtime.send({ mode: "busy", text: "cannot wait" });
  await tick();
  await tick();
  await tick();

  // One visible failure, no retry loop, nothing written in either lane.
  expect(waits).toBe(1);
  expect(events.filter((event) => event.kind === "delivery_error")).toHaveLength(1);
  expect(session.prompts).toEqual([]);
  expect(session.steers).toEqual([]);
  await runtime.dispose();
});

test("the builtin runtime configuration routes the same way", async () => {
  const session = createFakeSession();
  const runtime = createBuiltinRuntime(session);
  await runtime.start({ text: "initial task" });
  await tick();
  session.prompts.length = 0;

  // Settled: a busy write starts the run.
  session.endTurn();
  runtime.send({ mode: "busy", text: "settled busy write" });
  await tick();
  await tick();
  expect(session.prompts).toEqual(["settled busy write"]);

  // Live run: a busy write steers into it.
  runtime.send({ mode: "busy", text: "live busy write" });
  await tick();
  await tick();
  expect(session.steers).toEqual(["live busy write"]);
  await runtime.dispose();
});

test("disposal releases admission waiters whose owner is still pending", async () => {
  const { session, runtime } = await startedRuntime();
  session.endTurn();
  await tick();
  const baseline = __piPromptsInFlightForTest();

  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  session.prompt = async () => {
    await gate;
  };

  runtime.send({ mode: "busy", text: "owner" });
  runtime.send({ mode: "busy", text: "sibling" });
  await tick();
  await tick();
  await runtime.dispose();
  await tick();
  await tick();

  // Only the already-dispatched owner may still be in flight; the waiter must
  // close on disposal rather than waiting out its owner.
  expect(__piPromptsInFlightForTest() - baseline).toBe(1);
  finish();
  await tick();
  await tick();
});
