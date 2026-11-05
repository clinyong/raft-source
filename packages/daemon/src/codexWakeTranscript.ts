/** Convert the subset of Codex tool records that can be paired without
 * executing transcript code. Unknown or batched calls are deliberately lost. */
export function codexWakeLines(lines: string[]): string[] {
  const result: string[] = [];
  const calls = new Map<string, { command: string; timestamp: string | null; turn: number }>();
  const unknownRaftCalls = new Map<string, number>();
  const unknownOtherCalls = new Map<string, string | null>();
  let turn = 0;
  const markUnknown = (at: number) => result.push(JSON.stringify({ type: "codex_unrecoverable_tool", turn: at }));
  for (const line of lines) {
    let record: any;
    try { record = JSON.parse(line); } catch { continue; }
    if (record?.type === "event_msg" && record.payload?.type === "task_started") { turn++; continue; }
    if (record?.type === "compacted" || (record?.type === "response_item" && record.payload?.type === "compaction")) {
      calls.clear(); unknownRaftCalls.clear(); unknownOtherCalls.clear(); result.length = 0; continue;
    }
    if (record?.type !== "response_item" || !record.payload) continue;
    const p = record.payload;
    if ((p.type === "custom_tool_call" || p.type === "function_call") && typeof p.call_id === "string") {
      let command: string | null = null;
      if (p.type === "function_call" && p.name === "exec_command" && typeof p.arguments === "string") {
        try { const args = JSON.parse(p.arguments); if (typeof args.cmd === "string") command = args.cmd; } catch { /* unknown */ }
      } else if (p.type === "custom_tool_call" && typeof p.input === "string") {
        // Accept only an unambiguous JSON-quoted cmd literal in one exec call.
        const matches = [...p.input.matchAll(/tools\.exec_command\(\s*\{\s*cmd\s*:\s*("(?:[^"\\]|\\.)*")/g)];
        if (matches.length === 1 && (p.input.match(/tools\.exec_command\s*\(/g) ?? []).length === 1) {
          try { command = JSON.parse(matches[0]![1]!); } catch { /* unknown */ }
        }
      }
      if (command !== null) calls.set(p.call_id, { command, timestamp: typeof record.timestamp === "string" ? record.timestamp : null, turn });
      else if (/\braft\s+(?:message|task)\s+/.test(String(p.input ?? p.arguments ?? "")) || /tools\.(?:exec_command|write_stdin)\s*\(/.test(String(p.input ?? ""))) unknownRaftCalls.set(p.call_id, turn);
      else unknownOtherCalls.set(p.call_id, typeof record.timestamp === "string" ? record.timestamp : null);
    } else if ((p.type === "custom_tool_call_output" || p.type === "function_call_output") && typeof p.call_id === "string") {
      const call = calls.get(p.call_id);
      if (!call) {
        const at = unknownRaftCalls.get(p.call_id);
        if (at !== undefined) { markUnknown(at); unknownRaftCalls.delete(p.call_id); }
        else if (unknownOtherCalls.has(p.call_id)) {
          result.push(JSON.stringify({ type: "assistant", timestamp: unknownOtherCalls.get(p.call_id), message: { content: [{ type: "tool_use", id: p.call_id, name: "Bash", input: { command: "[unrecognized tool invocation]" } }] } }));
          result.push(JSON.stringify({ type: "user", timestamp: record.timestamp, message: { content: [{ type: "tool_result", tool_use_id: p.call_id, content: "[result omitted]" }] } }));
          unknownOtherCalls.delete(p.call_id);
        }
        continue;
      }
      calls.delete(p.call_id);
      const output = typeof p.output === "string" ? p.output : Array.isArray(p.output)
        ? p.output.filter((part: any) => (part?.type === "text" || part?.type === "input_text") && typeof part.text === "string").map((part: any) => part.text).join("\n") : null;
      if (output === null) { if (/\braft\s+(?:message|task)\s+/.test(call.command)) markUnknown(call.turn); continue; }
      // A yielded shell session may still be running. The transcript alone
      // cannot establish that it is safe to discard its parent session.
      if (/\bsession[_ ]id\b/i.test(output)) markUnknown(call.turn);
      result.push(JSON.stringify({ type: "assistant", timestamp: call.timestamp, message: { content: [{ type: "tool_use", id: p.call_id, name: "Bash", input: { command: call.command } }] } }));
      result.push(JSON.stringify({ type: "user", timestamp: record.timestamp, message: { content: [{ type: "tool_result", tool_use_id: p.call_id, content: output }] } }));
    } else if (p.type === "message" && p.role === "user" && Array.isArray(p.content)) {
      const content = p.content.filter((part: any) => part?.type === "input_text" && typeof part.text === "string").map((part: any) => ({ type: "text", text: part.text }));
      result.push(JSON.stringify({ type: "user", timestamp: record.timestamp, message: { content } }));
    }
  }
  for (const call of calls.values()) if (/\braft\s+(?:message|task)\s+/.test(call.command)) markUnknown(call.turn);
  for (const at of unknownRaftCalls.values()) markUnknown(at);
  return result.flatMap((line) => {
    const marker = /^\{"type":"codex_unrecoverable_tool","turn":(\d+)\}$/.exec(line);
    return marker ? (turn - Number(marker[1]) < 5 ? ['{"type":"codex_unrecoverable_tool"}'] : []) : [line];
  });
}
