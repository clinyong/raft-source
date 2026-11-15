import { test } from "vitest";
import assert from "node:assert/strict";
import { raftSessionName, RAFT_SESSION_NAME_SUFFIX_LENGTH } from "./sessionName";

test("raftSessionName labels the agent and appends the session id suffix", () => {
  assert.equal(raftSessionName("Calpis", "00000000-0000-0000-0000-000000001a2b3c4d"), "Raft · Calpis · 1a2b3c4d");
});

test("raftSessionName trims the agent name", () => {
  assert.equal(raftSessionName("  Cindy  ", "abcdef12-0000-0000-0000-000000000000"), "Raft · Cindy · 00000000");
});

test("raftSessionName returns null when the agent name is missing", () => {
  assert.equal(raftSessionName("", "1a2b3c4d"), null);
  assert.equal(raftSessionName("   ", "1a2b3c4d"), null);
  assert.equal(raftSessionName(null, "1a2b3c4d"), null);
  assert.equal(raftSessionName(undefined, "1a2b3c4d"), null);
});

test("raftSessionName falls back to the agent-only name when there is no session id", () => {
  assert.equal(raftSessionName("Calpis", null), "Raft · Calpis");
  assert.equal(raftSessionName("Calpis", undefined), "Raft · Calpis");
  assert.equal(raftSessionName("Calpis", "   "), "Raft · Calpis");
});

test("the suffix is the session id tail, not a timestamp", () => {
  const sessionId = "9f8e7d6c-5b4a-3210-9876-543210fedcba";
  const name = raftSessionName("Calpis", sessionId);
  assert.ok(name);
  assert.ok(name.endsWith(sessionId.slice(-RAFT_SESSION_NAME_SUFFIX_LENGTH)));
  // A timestamp would look like a date; the id suffix must not.
  assert.ok(!/\d{4}-\d{2}-\d{2}/.test(name), `expected an id suffix, got ${name}`);
});

test("the suffix comes from the random tail of a UUIDv7 id, not its timestamp head", () => {
  // Codex thread ids are UUIDv7: the leading characters are a quantised creation
  // timestamp that advances only once every ~65 seconds, so two sessions of the
  // same agent created in that window would share the head — and the name.
  const codexThreadId = "01a1aedb-1b10-7352-95e7-966fe9bf7dec";
  const name = raftSessionName("Calpis", codexThreadId);
  assert.equal(name, "Raft · Calpis · e9bf7dec");
  assert.ok(!name.endsWith("01a1aedb"), "the suffix must not be the timestamp head");

  // Two ids created in the same 65-second window share the head but not the tail.
  const sameWindow = "01a1aedb-25dc-7850-b01d-c0fb3c7b5752";
  const other = raftSessionName("Calpis", sameWindow);
  assert.equal(other, "Raft · Calpis · 3c7b5752");
  assert.notEqual(other, name, "sessions in the same window must still get different names");
});

test("the suffix length is what the name carries", () => {
  assert.equal(RAFT_SESSION_NAME_SUFFIX_LENGTH, 8);
  const sessionId = "abcdef0123456789";
  assert.equal(raftSessionName("Calpis", sessionId), "Raft · Calpis · 23456789");
});
