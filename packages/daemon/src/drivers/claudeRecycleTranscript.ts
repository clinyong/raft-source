import { open, stat } from "node:fs/promises";
import os from "node:os";
import { resolveRuntimeSessionRef } from "./runtimeArtifacts";
import type { SessionResumeFacts } from "../wakeSessionRecycle";
const TRANSCRIPT_TAIL_BYTES = 512 * 1024;

function contextTokensFromUsage(usage: unknown): number | null {
  if (!usage || typeof usage !== "object" || Array.isArray(usage)) return null;
  const record = usage as Record<string, unknown>;
  const finite = (value: unknown): number => (typeof value === "number" && Number.isFinite(value) ? value : 0);
  const total = finite(record.input_tokens)
    + finite(record.cache_read_input_tokens)
    + finite(record.cache_creation_input_tokens);
  return total > 0 ? total : null;
}

/**
 * Read the two facts the recycle decision needs from the tail of the Claude
 * session transcript: when the session last progressed, and how large its
 * context was on the last completed request. Any unreadable/missing fact
 * yields nulls, and the caller falls back to a plain resume.
 *
 * Reading the transcript (rather than tracking live state) makes the decision
 * correct across daemon restarts and costs one bounded file read per wake.
 */
export async function readClaudeSessionResumeFacts(
  sessionId: string,
  homeDir: string = os.homedir(),
): Promise<SessionResumeFacts> {
  const ref = resolveRuntimeSessionRef("claude", sessionId, homeDir);
  if (!ref.reachable || !ref.path) {
    return { transcriptPath: null, lastActivityAtMs: null, lastContextTokens: null };
  }
  try {
    const info = await stat(ref.path);
    const readStart = Math.max(0, info.size - TRANSCRIPT_TAIL_BYTES);
    const toRead = info.size - readStart;
    const buffer = Buffer.alloc(toRead);
    const fd = await open(ref.path, "r");
    try {
      let bytesRead = 0;
      while (bytesRead < toRead) {
        const result = await fd.read(buffer, bytesRead, toRead - bytesRead, readStart + bytesRead);
        if (result.bytesRead === 0) break;
        bytesRead += result.bytesRead;
      }
    } finally {
      await fd.close();
    }
    const lines = buffer.toString("utf8").split("\n");
    // A mid-file window starts inside a record; drop the leading fragment.
    if (readStart > 0) lines.shift();

    let lastActivityAtMs: number | null = null;
    let lastContextTokens: number | null = null;
    for (const line of lines) {
      if (!line) continue;
      let record: Record<string, unknown>;
      try {
        record = JSON.parse(line) as Record<string, unknown>;
      } catch {
        continue;
      }
      if (!record || typeof record !== "object" || Array.isArray(record)) continue;
      if (typeof record.timestamp === "string") {
        const parsed = Date.parse(record.timestamp);
        if (Number.isFinite(parsed)) lastActivityAtMs = Math.max(lastActivityAtMs ?? 0, parsed);
      }
      if (record.type === "assistant") {
        const message = record.message;
        if (message && typeof message === "object" && !Array.isArray(message)) {
          const tokens = contextTokensFromUsage((message as Record<string, unknown>).usage);
          if (tokens !== null) lastContextTokens = tokens;
        }
      }
    }
    return { transcriptPath: ref.path, lastActivityAtMs, lastContextTokens };
  } catch {
    return { transcriptPath: ref.path, lastActivityAtMs: null, lastContextTokens: null };
  }
}

/**
 * Static transcript markers that a session has EVER created background work
 * (background bash, Monitor, async subagents, task notifications). The live
 * `background_tasks_changed` roster is not persisted to the transcript, so the
 * sweep's "definitely no background business" gate is the conservative
 * complement: any marker ever seen disqualifies the session from sweeping.
 * Markers only ever accumulate in an append-only transcript, so the scan is
 * incremental: previously scanned bytes are never re-read, and a hit is final.
 */
const BACKGROUND_TASK_CREATE_PATTERNS = [
  /Monitor started \(task ([a-z0-9]+)/g,
  /background with ID: ([a-z0-9]+)/g,
  /moved to the background \(ID: ([a-z0-9]+)\)/g,
] as const;
const BACKGROUND_TASK_TERMINAL_PATTERNS = [
  /<task-id>([a-z0-9]+)<\/task-id>[\s\S]{0,400}?<status>(?:completed|failed|stopped|killed)<\/status>/g,
  /Successfully stopped task: ([a-z0-9]+)/g,
] as const;

interface BackgroundTaskScanState {
  scannedBytes: number;
  live: Set<string>;
}

const backgroundTaskScans = new Map<string, BackgroundTaskScanState>();
const MARKER_SCAN_CHUNK = 4 * 1024 * 1024;
const MARKER_OVERLAP = 512;

/**
 * Count background tasks (monitors, background bash, async subagents) that
 * were created in this transcript and have no terminal notification yet.
 * Fleet measurement (RFC 070): 95.7% of real cold wakes carry zero
 * outstanding tasks, so this paired accounting — versus "ever created any",
 * which only 12.5% pass — is what makes the sweep reach the fleet. The scan
 * is incremental over the append-only file with the live-set carried between
 * scans; an unmatched creation stays outstanding forever (fail-armed), and
 * read errors report null (unknown → the sweep must skip).
 */
export async function transcriptOutstandingBackgroundTasks(transcriptPath: string): Promise<number | null> {
  const state = backgroundTaskScans.get(transcriptPath) ?? { scannedBytes: 0, live: new Set<string>() };
  try {
    const info = await stat(transcriptPath);
    if (info.size <= state.scannedBytes) return state.live.size;
    const fd = await open(transcriptPath, "r");
    try {
      let position = Math.max(0, state.scannedBytes - MARKER_OVERLAP);
      const buffer = Buffer.alloc(MARKER_SCAN_CHUNK);
      while (position < info.size) {
        const { bytesRead } = await fd.read(buffer, 0, MARKER_SCAN_CHUNK, position);
        if (bytesRead === 0) break;
        const text = buffer.subarray(0, bytesRead).toString("utf8");
        for (const pattern of BACKGROUND_TASK_CREATE_PATTERNS) {
          for (const match of text.matchAll(pattern)) state.live.add(match[1]!);
        }
        for (const pattern of BACKGROUND_TASK_TERMINAL_PATTERNS) {
          for (const match of text.matchAll(pattern)) state.live.delete(match[1]!);
        }
        position += bytesRead;
      }
      state.scannedBytes = Math.max(state.scannedBytes, Math.min(position, info.size));
    } finally {
      await fd.close();
    }
    backgroundTaskScans.set(transcriptPath, state);
    return state.live.size;
  } catch {
    return null;
  }
}

