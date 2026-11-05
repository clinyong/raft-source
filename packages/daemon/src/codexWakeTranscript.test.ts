import { describe, expect, test } from "vitest";
import { codexWakeLines } from "./codexWakeTranscript";

const line = (payload: unknown) => JSON.stringify({ timestamp: "2026-11-05T00:00:00Z", type: "response_item", payload });
const call = (id: string, input: string) => line({ type: "custom_tool_call", call_id: id, input });
const output = (id: string) => line({ type: "custom_tool_call_output", call_id: id, output: [{ type: "input_text", text: "Message sent" }] });

describe("Codex wake conversion", () => {
  test("pairs one literal shell call and result without executing the input", () => {
    const records = codexWakeLines([call("a", 'const r = await tools.exec_command({cmd:"raft message read --target test"}); text(r.output);'), output("a")]).map((record) => JSON.parse(record));
    expect(records).toHaveLength(2);
    expect(records[0].message.content[0].input.command).toBe("raft message read --target test");
    expect(records[1].message.content[0].tool_use_id).toBe("a");
  });
  test("drops ambiguous batch and unmatched results", () => {
    expect(codexWakeLines([call("a", 'tools.exec_command({cmd:"raft message read"}); tools.exec_command({cmd:"two"})'), output("a")]).every((record) => record.includes("codex_unrecoverable_tool"))).toBe(true);
  });
  test("compaction drops earlier actions and unpaired calls", () => {
    const records = codexWakeLines([call("a", 'tools.exec_command({cmd:"one"})'), output("a"), JSON.stringify({type:"compacted"}), call("b", 'tools.exec_command({cmd:"two"})'), output("b")]);
    expect(records).toHaveLength(2);
    expect(records.join("\n")).toContain("two");
    expect(records.join("\n")).not.toContain("one");
  });
  test("only recent unrecognized Raft calls veto recovery", () => {
    const start = () => JSON.stringify({ type: "event_msg", payload: { type: "task_started" } });
    const batch = call("a", 'tools.exec_command({cmd:"raft message read"}); tools.exec_command({cmd:"other"})');
    expect(codexWakeLines([start(), batch, output("a")])).toContain('{"type":"codex_unrecoverable_tool"}');
    expect(codexWakeLines([start(), batch, output("a"), start(), start(), start(), start(), start()])).not.toContain('{"type":"codex_unrecoverable_tool"}');
  });

});
