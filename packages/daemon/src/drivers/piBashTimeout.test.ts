import assert from "node:assert/strict";
import { createPiBashTimeoutExtension, PI_DEFAULT_BASH_TIMEOUT_SECONDS, piBashTimeoutSeconds } from "./piBashTimeout";

type Handler = (event: { toolName: string; input: Record<string, unknown> }) => unknown;

/** Runs the extension against a fake Pi API and returns the registered tool_call handler. */
function toolCallHandler(): Handler {
  let handler: Handler | undefined;
  const pi = {
    on(event: string, fn: Handler) {
      if (event === "tool_call") handler = fn;
      return pi;
    },
  };
  createPiBashTimeoutExtension()(pi as never);
  assert.ok(handler, "the extension must register a tool_call handler");
  return handler;
}

test("a bash call without a timeout gets the default one", () => {
  const event: { toolName: string; input: Record<string, unknown> } = {
    toolName: "bash",
    input: { command: "sleep 100000" },
  };
  toolCallHandler()(event);
  assert.equal(event.input.timeout, PI_DEFAULT_BASH_TIMEOUT_SECONDS);
});

test("a timeout the model chose is kept", () => {
  const event: { toolName: string; input: Record<string, unknown> } = {
    toolName: "bash",
    input: { command: "pnpm build", timeout: 600 },
  };
  toolCallHandler()(event);
  assert.equal(event.input.timeout, 600);
});

test("other tools are untouched", () => {
  const event: { toolName: string; input: Record<string, unknown> } = {
    toolName: "read",
    input: { path: "/tmp/x" },
  };
  toolCallHandler()(event);
  assert.equal(event.input.timeout, undefined);
});

test("an unusable timeout is replaced by the default", () => {
  assert.equal(piBashTimeoutSeconds({ timeout: 0 }), undefined);
  assert.equal(piBashTimeoutSeconds({ timeout: -5 }), undefined);
  assert.equal(piBashTimeoutSeconds({ timeout: Number.NaN }), undefined);
  assert.equal(piBashTimeoutSeconds({ timeout: "600" }), undefined);
  assert.equal(piBashTimeoutSeconds({ timeout: 600 }), 600);
  assert.equal(piBashTimeoutSeconds({}), undefined);
});
