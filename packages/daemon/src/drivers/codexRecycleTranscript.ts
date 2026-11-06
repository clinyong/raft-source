import { open, readFile, stat } from "node:fs/promises";
import os from "node:os";
import { resolveRuntimeSessionRef } from "./runtimeArtifacts";
import type { SessionResumeFacts } from "../wakeSessionRecycle";
import { isCompactionRecord, parseCodexRecord, readContextTokens, readStringField, type JsonObject } from "../codexWakeTranscript";
const TRANSCRIPT_TAIL_BYTES = 512 * 1024;
const SESSION_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Read the tail of a file as text, dropping the first partial line when the
 * read started mid-file. Null means the tail could not be read. */
async function readTranscriptTailText(filePath: string): Promise<string | null> {
  const info = await stat(filePath);
  const start = Math.max(0, info.size - TRANSCRIPT_TAIL_BYTES);
  const buffer = Buffer.alloc(info.size - start);
  const handle = await open(filePath, "r");
  try {
    let read = 0;
    while (read < buffer.length) {
      const result = await handle.read(buffer, read, buffer.length - read, start + read);
      if (!result.bytesRead) return null;
      read += result.bytesRead;
    }
  } finally {
    await handle.close();
  }
  const text = buffer.toString("utf8");
  if (start === 0) return text;
  const firstBreak = text.indexOf("\n");
  return firstBreak === -1 ? "" : text.slice(firstBreak + 1);
}

/** Latest activity timestamp a record carries, or the running maximum. */
function latestTimestamp(current: number | null, record: JsonObject): number | null {
  const raw = readStringField(record, "timestamp");
  if (raw === null) return current;
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) return current;
  return Math.max(current ?? 0, parsed);
}

/** Read Codex's last request, whose input includes cached tokens. A damaged
 * tail is unknown, never permission to discard the old session. */
export async function readCodexSessionResumeFacts(sessionId: string, homeDir = os.homedir()): Promise<SessionResumeFacts> {
  const empty: SessionResumeFacts = { transcriptPath: null, lastActivityAtMs: null, lastContextTokens: null };
  if (!SESSION_ID_PATTERN.test(sessionId)) return empty;
  const ref = resolveRuntimeSessionRef("codex", sessionId, homeDir);
  if (!ref.reachable || !ref.path) return empty;
  if (!ref.path.endsWith(`-${sessionId}.jsonl`)) return empty;
  const unknown: SessionResumeFacts = { ...empty, transcriptPath: ref.path };
  try {
    const text = await readTranscriptTailText(ref.path);
    if (text === null) return unknown;
    let lastActivityAtMs: number | null = null;
    let lastContextTokens: number | null = null;
    for (const line of text.split("\n")) {
      if (!line) continue;
      const record = parseCodexRecord(line);
      if (record === null) return unknown;
      lastActivityAtMs = latestTimestamp(lastActivityAtMs, record);
      if (isCompactionRecord(record)) lastContextTokens = null;
      const tokens = readContextTokens(record);
      if (tokens !== null) lastContextTokens = tokens;
    }
    return { transcriptPath: ref.path, lastActivityAtMs, lastContextTokens };
  } catch {
    return unknown;
  }
}

/** Check the whole bounded Codex rollout for unfinished tool calls and live
 * code-mode cells. Shell commands are checked separately against the live
 * app-server background terminal list; printed tool output can omit status. */
export async function codexOutstandingToolCalls(transcriptPath: string): Promise<number | null> {
  const MAX_BYTES = 32 * 1024 * 1024;
  try {
    const before = await stat(transcriptPath);
    if (!before.isFile() || before.size > MAX_BYTES) return null;
    const text = await readFile(transcriptPath, "utf8");
    const after = await stat(transcriptPath);
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || (text.length > 0 && !text.endsWith("\n"))) return null;
    const pending = new Map<string, { name: string; input: string }>();
    const cells = new Set<string>();
    let sawCompletedTurn = false;
    let turnOpen = false;
    for (const line of text.split("\n")) {
      if (!line) continue;
      const record = parseCodexRecord(line);
      if (!record) return null;
      const payload = record.payload;
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        if (record.type === "response_item" || record.type === "event_msg") return null;
        continue;
      }
      const p = payload as Record<string, unknown>;
      if (record.type === "event_msg") {
        if (p.type === "task_started") turnOpen = true;
        if (p.type === "task_complete") { turnOpen = false; sawCompletedTurn = true; }
        continue;
      }
      if (record.type !== "response_item") continue;
      const isCall = p.type === "function_call" || p.type === "custom_tool_call";
      const isOutput = p.type === "function_call_output" || p.type === "custom_tool_call_output";
      if (!isCall && !isOutput) continue;
      const id = typeof p.call_id === "string" && p.call_id ? p.call_id : null;
      if (!id) return null;
      if (isCall) {
        const name = typeof p.name === "string" ? p.name : "";
        const input = typeof p.input === "string" ? p.input : typeof p.arguments === "string" ? p.arguments : "";
        // Spawned agents and asynchronous user callbacks can survive the tool
        // result and are not covered by the terminal-list API.
        if (/spawn_agent|request_user_input_async/i.test(`${name} ${input}`)) return null;
        if (pending.has(id)) return null;
        pending.set(id, { name, input });
      } else {
        const call = pending.get(id);
        if (!call || p.output === undefined || p.output === null) return null;
        pending.delete(id);
        if (typeof p.output !== "string" && !Array.isArray(p.output)) return null;
        const output = typeof p.output === "string" ? p.output : p.output
          .map((part: unknown) => part && typeof part === "object" && typeof (part as Record<string, unknown>).text === "string" ? (part as Record<string, string>).text : "").join("\n");
        if (call.name === "exec") {
          const yielded = /^Script running with cell ID\s+(\S+)/.exec(output);
          if (yielded) {
            if (cells.has(yielded[1]!)) return null;
            cells.add(yielded[1]!);
          }
        } else if (call.name === "wait") {
          let cellId: unknown;
          try { cellId = (JSON.parse(call.input) as Record<string, unknown>).cell_id; } catch { return null; }
          if (typeof cellId !== "string" || !cells.has(cellId)) return null;
          if (/^Script (?:completed|terminated)\b/.test(output)) cells.delete(cellId);
          else if (!/^Script running with cell ID\b/.test(output)) return null;
        } else if (/^Script running with cell ID\b/.test(output)) return null;
      }
    }
    if (!sawCompletedTurn || turnOpen) return null;
    return pending.size + cells.size;
  } catch {
    return null;
  }
}
