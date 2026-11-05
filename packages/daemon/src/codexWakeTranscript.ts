/**
 * Convert Codex rollout records into the transcript lines the wake panel reads.
 *
 * The panel was written against Claude Code transcripts: one JSON object per
 * line, `assistant` lines carrying `tool_use` blocks and `user` lines carrying
 * their `tool_result`. A Codex rollout stores the same facts differently, and
 * its tool calls are JavaScript snippets rather than plain commands.
 *
 * This module never executes a rollout record. It reads the records as data and
 * keeps only what it can pair exactly: one shell call written as a JSON string
 * literal, plus the recorded output for that call. Anything it cannot establish
 * — a batched call, a dynamically built command, a missing result, or an output
 * that suggests a shell session is still running — is reported as
 * unrecoverable instead of being guessed at.
 */

/** Marker line a caller can look for to abandon the whole panel. */
export const UNRECOVERABLE_WAKE_MARKER = '{"type":"codex_unrecoverable_tool"}';

/** Only calls this recent veto recovery. Older unknowns have already been
 * overtaken by later work, so they do not block a new session. */
const RECENT_TURN_WINDOW = 5;

const RECORD_TYPE_EVENT_MESSAGE = "event_msg";
const RECORD_TYPE_RESPONSE_ITEM = "response_item";
const RECORD_TYPE_COMPACTED = "compacted";
const PAYLOAD_TYPE_TASK_STARTED = "task_started";
const PAYLOAD_TYPE_COMPACTION = "compaction";
const PAYLOAD_TYPE_MESSAGE = "message";
const ROLE_USER = "user";
const FUNCTION_CALL = "function_call";
const EXEC_COMMAND_FUNCTION_NAME = "exec_command";

const TOOL_CALL_TYPES = new Set(["custom_tool_call", FUNCTION_CALL]);
const TOOL_OUTPUT_TYPES = new Set(["custom_tool_call_output", "function_call_output"]);
const TEXT_PART_TYPES = new Set(["text", "input_text"]);

/** Any shell call, used to recognise a snippet this converter must not interpret. */
const SHELL_CALL_PATTERN = /tools\.(?:exec_command|write_stdin)\s*\(/;

/** Counting form of the exec call, used to reject a snippet with two calls. */
const EXEC_COMMAND_CALL_PATTERN = /tools\.exec_command\s*\(/g;

/** One `tools.exec_command({ cmd: "…" })` invocation with a quoted literal. */
const SHELL_CALL_WITH_LITERAL = /tools\.exec_command\(\s*\{\s*cmd\s*:\s*("(?:[^"\\]|\\.)*")/;

/** A command that talks to Raft; losing one of these loses real work. */
const RAFT_COMMAND_PATTERN = /\braft\s+(?:message|task)\s+/;

/** A result that mentions a session id may belong to a still-running shell. */
const RUNNING_SESSION_PATTERN = /\bsession[_ ]id\b/i;

/** Stamped marker line, used to read back the turn a marker came from. */
const STAMPED_MARKER = /^\{"type":"codex_unrecoverable_tool","turn":(\d+)\}$/;

type JsonObject = Record<string, unknown>;

interface PendingCall {
  command: string;
  timestamp: string | null;
  turn: number;
}

/** What one converter run has seen so far. */
interface ConvertState {
  lines: string[];
  calls: Map<string, PendingCall>;
  unknownRaftCalls: Map<string, number>;
  unknownOtherCalls: Map<string, string | null>;
  turn: number;
}

function asObject(value: unknown): JsonObject | null {
  if (value === null || typeof value !== "object") return null;
  if (Array.isArray(value)) return null;
  return value as JsonObject;
}

function readString(source: JsonObject | null, key: string): string | null {
  const value = source?.[key];
  return typeof value === "string" ? value : null;
}

function parseLine(line: string): JsonObject | null {
  try {
    return asObject(JSON.parse(line));
  } catch {
    return null;
  }
}

function payloadOf(record: JsonObject): JsonObject | null {
  return asObject(record.payload);
}

function readTimestamp(record: JsonObject): string | null {
  return readString(record, "timestamp");
}

function isTaskStarted(record: JsonObject, payload: JsonObject | null): boolean {
  if (record.type !== RECORD_TYPE_EVENT_MESSAGE) return false;
  return readString(payload, "type") === PAYLOAD_TYPE_TASK_STARTED;
}

function isCompaction(record: JsonObject, payload: JsonObject | null): boolean {
  if (record.type === RECORD_TYPE_COMPACTED) return true;
  if (record.type !== RECORD_TYPE_RESPONSE_ITEM) return false;
  return readString(payload, "type") === PAYLOAD_TYPE_COMPACTION;
}

/** Text used only to classify an unparseable call, never to run anything. */
function describeCallSource(payload: JsonObject): string {
  const input = readString(payload, "input");
  if (input !== null) return input;
  const callArguments = readString(payload, "arguments");
  if (callArguments !== null) return callArguments;
  return "";
}

function countExecCalls(text: string): number {
  const matches = text.match(EXEC_COMMAND_CALL_PATTERN);
  return matches === null ? 0 : matches.length;
}

/** Accept only one quoted literal passed to exactly one exec call. */
function readCommandLiteral(input: string): string | null {
  const literal = SHELL_CALL_WITH_LITERAL.exec(input);
  if (literal === null) return null;
  if (countExecCalls(input) !== 1) return null;
  try {
    const command: unknown = JSON.parse(literal[1]!);
    return typeof command === "string" ? command : null;
  } catch {
    return null;
  }
}

/** Read the command of a call, or null when it is not statically known. */
function readCallCommand(payload: JsonObject): string | null {
  if (readString(payload, "type") === FUNCTION_CALL) {
    if (readString(payload, "name") !== EXEC_COMMAND_FUNCTION_NAME) return null;
    const callArguments = readString(payload, "arguments");
    if (callArguments === null) return null;
    try {
      const parsed = asObject(JSON.parse(callArguments));
      return readString(parsed, "cmd");
    } catch {
      return null;
    }
  }
  const input = readString(payload, "input");
  if (input === null) return null;
  return readCommandLiteral(input);
}

/** Flatten a recorded tool output into text, or null when it is not text. */
function readOutputText(output: unknown): string | null {
  const direct = typeof output === "string" ? output : null;
  if (direct !== null) return direct;
  if (!Array.isArray(output)) return null;
  const parts: string[] = [];
  for (const part of output) {
    const parsed = asObject(part);
    const type = readString(parsed, "type");
    const text = readString(parsed, "text");
    if (type === null || text === null) continue;
    if (!TEXT_PART_TYPES.has(type)) continue;
    parts.push(text);
  }
  return parts.join("\n");
}

/** A tool call as the panel expects it from a Claude-style transcript. */
function toolUseLine(call: PendingCall, callId: string): string {
  const content = { type: "tool_use", id: callId, name: "Bash", input: { command: call.command } };
  return JSON.stringify({ type: "assistant", timestamp: call.timestamp, message: { content: [content] } });
}

/** The recorded result of a tool call, in the same shape. */
function toolResultLine(timestamp: string | null, callId: string, text: string): string {
  const content = { type: "tool_result", tool_use_id: callId, content: text };
  return JSON.stringify({ type: "user", timestamp, message: { content: [content] } });
}

/** A call kept only by name, because its command could not be read. */
function unrecognizedCallLines(timestamp: string | null, callId: string, resultTimestamp: string | null): string[] {
  const use = { type: "tool_use", id: callId, name: "Bash", input: { command: "[unrecognized tool invocation]" } };
  const result = { type: "tool_result", tool_use_id: callId, content: "[result omitted]" };
  return [
    JSON.stringify({ type: "assistant", timestamp, message: { content: [use] } }),
    JSON.stringify({ type: "user", timestamp: resultTimestamp, message: { content: [result] } }),
  ];
}

function userMessageLine(record: JsonObject, payload: JsonObject): string | null {
  const content = payload.content;
  if (!Array.isArray(content)) return null;
  const parts: JsonObject[] = [];
  for (const part of content) {
    const parsed = asObject(part);
    const type = readString(parsed, "type");
    const text = readString(parsed, "text");
    if (type !== "input_text" || text === null) continue;
    parts.push({ type: "text", text });
  }
  return JSON.stringify({ type: "user", timestamp: readTimestamp(record), message: { content: parts } });
}

function markUnrecoverable(state: ConvertState, turn: number): void {
  state.lines.push(JSON.stringify({ type: "codex_unrecoverable_tool", turn }));
}

function resetForCompaction(state: ConvertState): void {
  state.calls.clear();
  state.unknownRaftCalls.clear();
  state.unknownOtherCalls.clear();
  state.lines = [];
}

function rememberCall(state: ConvertState, payload: JsonObject, callId: string, record: JsonObject): void {
  const command = readCallCommand(payload);
  const timestamp = readTimestamp(record);
  if (command !== null) {
    state.calls.set(callId, { command, timestamp, turn: state.turn });
    return;
  }
  const source = describeCallSource(payload);
  const talksToRaft = RAFT_COMMAND_PATTERN.test(source);
  const input = readString(payload, "input");
  const hidesShellCall = input !== null && SHELL_CALL_PATTERN.test(input);
  if (talksToRaft || hidesShellCall) {
    state.unknownRaftCalls.set(callId, state.turn);
    return;
  }
  state.unknownOtherCalls.set(callId, timestamp);
}

function pairToolResult(state: ConvertState, record: JsonObject, payload: JsonObject, callId: string): void {
  const call = state.calls.get(callId);
  if (call === undefined) {
    pairUnknownResult(state, record, callId);
    return;
  }
  state.calls.delete(callId);
  const timestamp = readTimestamp(record);
  const output = readOutputText(payload.output);
  if (output === null) {
    if (RAFT_COMMAND_PATTERN.test(call.command)) markUnrecoverable(state, call.turn);
    return;
  }
  // The transcript alone cannot show that a yielded shell has finished.
  if (RUNNING_SESSION_PATTERN.test(output)) markUnrecoverable(state, call.turn);
  state.lines.push(toolUseLine(call, callId));
  state.lines.push(toolResultLine(timestamp, callId, output));
}

function pairUnknownResult(state: ConvertState, record: JsonObject, callId: string): void {
  const raftTurn = state.unknownRaftCalls.get(callId);
  if (raftTurn !== undefined) {
    markUnrecoverable(state, raftTurn);
    state.unknownRaftCalls.delete(callId);
    return;
  }
  if (!state.unknownOtherCalls.has(callId)) return;
  const callTimestamp = state.unknownOtherCalls.get(callId) ?? null;
  state.lines.push(...unrecognizedCallLines(callTimestamp, callId, readTimestamp(record)));
  state.unknownOtherCalls.delete(callId);
}

function handleResponseItem(state: ConvertState, record: JsonObject, payload: JsonObject): void {
  const type = readString(payload, "type");
  const callId = readString(payload, "call_id");
  if (callId === null) return;
  if (TOOL_CALL_TYPES.has(type ?? "")) {
    rememberCall(state, payload, callId, record);
    return;
  }
  if (TOOL_OUTPUT_TYPES.has(type ?? "")) {
    pairToolResult(state, record, payload, callId);
    return;
  }
}

function handleRecord(state: ConvertState, record: JsonObject): void {
  const payload = payloadOf(record);
  if (isTaskStarted(record, payload)) {
    state.turn += 1;
    return;
  }
  if (isCompaction(record, payload)) {
    resetForCompaction(state);
    return;
  }
  if (record.type !== RECORD_TYPE_RESPONSE_ITEM) return;
  if (payload === null) return;
  if (readString(payload, "type") === PAYLOAD_TYPE_MESSAGE && readString(payload, "role") === ROLE_USER) {
    const line = userMessageLine(record, payload);
    if (line !== null) state.lines.push(line);
    return;
  }
  handleResponseItem(state, record, payload);
}

/** Close out calls whose result never appeared. */
function finishPendingCalls(state: ConvertState): void {
  for (const call of state.calls.values()) {
    if (!RAFT_COMMAND_PATTERN.test(call.command)) continue;
    markUnrecoverable(state, call.turn);
  }
  for (const turn of state.unknownRaftCalls.values()) {
    markUnrecoverable(state, turn);
  }
}

function keepLine(state: ConvertState, line: string): boolean {
  const stamped = STAMPED_MARKER.exec(line);
  if (stamped === null) return true;
  const markerTurn = Number(stamped[1]);
  return state.turn - markerTurn < RECENT_TURN_WINDOW;
}

/**
 * Convert one transcript tail. Returns Claude-shaped lines, or a single
 * {@link UNRECOVERABLE_WAKE_MARKER} when a recent call cannot be reconstructed.
 */
export function codexWakeLines(lines: string[]): string[] {
  const state: ConvertState = {
    lines: [],
    calls: new Map(),
    unknownRaftCalls: new Map(),
    unknownOtherCalls: new Map(),
    turn: 0,
  };
  for (const line of lines) {
    const record = parseLine(line);
    if (record === null) continue;
    handleRecord(state, record);
  }
  finishPendingCalls(state);
  const kept = state.lines.filter((line) => keepLine(state, line));
  const collapsed = kept.map((line) => (STAMPED_MARKER.test(line) ? UNRECOVERABLE_WAKE_MARKER : line));
  return collapsed;
}
