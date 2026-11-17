import assert from "node:assert/strict";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
  createPiContextReminderExtension,
  createPiContextReminderExtensions,
} from "./piContextReminderExtension";

type Handler = (event: unknown, context: ExtensionContext) => unknown;

function createFixture(options: {
  agentId?: string;
  sessionId?: string;
  usage?: unknown;
  reserveTokens?: unknown;
} = {}) {
  let sessionId = options.sessionId ?? "session-1";
  let usage: unknown = Object.hasOwn(options, "usage") ? options.usage : { tokens: 58_000, contextWindow: 100_000 };
  let reserveTokens: unknown = Object.hasOwn(options, "reserveTokens") ? options.reserveTokens : 10_000;
  let idle = true;
  const handlers = new Map<string, Handler>();
  const sessionManager = { getSessionId: () => sessionId };
  createPiContextReminderExtension(
    options.agentId ?? "agent-1",
    () => reserveTokens as number,
  )({
    on: (name: string, handler: Handler) => handlers.set(name, handler),
  } as unknown as ExtensionAPI);

  const context = () => ({
    sessionManager,
    model: { id: "example", provider: "example" },
    isIdle: () => idle,
    getContextUsage: () => usage,
  }) as unknown as ExtensionContext;
  return {
    setSessionId(value: string) { sessionId = value; },
    setUsage(value: unknown) { usage = value; },
    setReserveTokens(value: unknown) { reserveTokens = value; },
    setIdle(value: boolean) { idle = value; },
    turnEnd() { handlers.get("turn_end")?.({ type: "turn_end" }, context()); },
    input(text: string, streamingBehavior?: "steer" | "followUp") {
      return handlers.get("input")?.({
        type: "input",
        text,
        source: "rpc",
        ...(streamingBehavior ? { streamingBehavior } : {}),
      }, context()) as { action: string; text?: string } | undefined;
    },
  };
}

function reminderText(percentage: number): string {
  return `[Raft 系统提醒] 上下文已用到约 ${percentage}%，请先把交接写入 MEMORY.md，再运行 raft session reset。`;
}

test("Pi context reminder factory is registered only for managed Pi runtime", () => {
  const getReserveTokens = () => 10_000;
  assert.equal(createPiContextReminderExtensions("builtin", "agent-1", getReserveTokens).length, 0);
  assert.equal(createPiContextReminderExtensions("claude", "agent-1", getReserveTokens).length, 0);
  assert.equal(createPiContextReminderExtensions("external", "agent-1", getReserveTokens).length, 0);
  assert.equal(createPiContextReminderExtensions("pi", "agent-1", getReserveTokens).length, 1);
});

test("Pi context reminder uses the dynamic threshold, skips steer, and resets for a new session", () => {
  const fixture = createFixture({ usage: { tokens: 57_999, contextWindow: 100_000 } });
  fixture.turnEnd();
  assert.deepEqual(fixture.input("below threshold"), { action: "continue" });
  fixture.setUsage({ tokens: 58_000, contextWindow: 100_000 });
  fixture.turnEnd();

  fixture.setIdle(false);
  assert.deepEqual(fixture.input("original steer text", "steer"), { action: "continue" });
  fixture.setIdle(true);
  const first = fixture.input("next message");
  assert.deepEqual(first, { action: "transform", text: `${reminderText(58)}\n\nnext message` });
  assert.deepEqual(fixture.input("later message"), { action: "continue" });

  fixture.setSessionId("session-2");
  fixture.setUsage({ tokens: 140_000, contextWindow: 200_000 });
  fixture.turnEnd();
  const second = fixture.input("new session message");
  assert.deepEqual(second, { action: "transform", text: `${reminderText(70)}\n\nnew session message` });
});

test("Pi context reminder fails closed for missing or invalid usage and reserve values", () => {
  const cases: Array<{ name: string; usage: unknown; reserveTokens: unknown }> = [
    { name: "missing usage", usage: undefined, reserveTokens: 10_000 },
    { name: "null tokens", usage: { tokens: null, contextWindow: 100_000 }, reserveTokens: 10_000 },
    { name: "invalid tokens", usage: { tokens: Number.NaN, contextWindow: 100_000 }, reserveTokens: 10_000 },
    { name: "negative tokens", usage: { tokens: -1, contextWindow: 100_000 }, reserveTokens: 10_000 },
    { name: "missing window", usage: { tokens: 80_000 }, reserveTokens: 10_000 },
    { name: "invalid window", usage: { tokens: 80_000, contextWindow: Number.POSITIVE_INFINITY }, reserveTokens: 10_000 },
    { name: "missing reserve", usage: { tokens: 80_000, contextWindow: 100_000 }, reserveTokens: undefined },
    { name: "invalid reserve", usage: { tokens: 80_000, contextWindow: 100_000 }, reserveTokens: -1 },
    { name: "threshold unavailable", usage: { tokens: 1, contextWindow: 40_000 }, reserveTokens: 16_384 },
  ];

  for (const item of cases) {
    const fixture = createFixture({ usage: item.usage, reserveTokens: item.reserveTokens });
    fixture.turnEnd();
    assert.deepEqual(fixture.input("next message"), { action: "continue" }, item.name);
  }
});

test("Pi context reminder leaves a timed-out steer unchanged and remains pending", () => {
  const fixture = createFixture();
  fixture.turnEnd();

  fixture.setIdle(false);
  assert.deepEqual(fixture.input("original fallback text", "steer"), { action: "continue" });
  fixture.setIdle(true);
  assert.deepEqual(fixture.input("next idle input"), {
    action: "transform",
    text: `${reminderText(58)}\n\nnext idle input`,
  });
});
