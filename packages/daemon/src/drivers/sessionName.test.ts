import { test } from "vitest";
import assert from "node:assert/strict";
import { raftSessionName, RAFT_SESSION_NAME_SUFFIX_LENGTH } from "./sessionName";

test("raftSessionName labels the agent and appends the session id prefix", () => {
  assert.equal(raftSessionName("Calpis", "1a2b3c4d-0000-0000-0000-000000000000"), "Raft · Calpis · 1a2b3c4d");
});

test("raftSessionName trims the agent name", () => {
  assert.equal(raftSessionName("  Cindy  ", "abcdef12-0000-0000-0000-000000000000"), "Raft · Cindy · abcdef12");
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

test("the suffix is the session id prefix, not a timestamp", () => {
  const sessionId = "9f8e7d6c-5b4a-3210-9876-543210fedcba";
  const name = raftSessionName("Calpis", sessionId);
  assert.ok(name);
  assert.ok(name.endsWith(sessionId.slice(0, RAFT_SESSION_NAME_SUFFIX_LENGTH)));
  // A timestamp would look like a date; the id prefix must not.
  assert.ok(!/\d{4}-\d{2}-\d{2}/.test(name), `expected an id prefix, got ${name}`);
});

test("the suffix length is what the name carries", () => {
  assert.equal(RAFT_SESSION_NAME_SUFFIX_LENGTH, 8);
  const sessionId = "abcdef0123456789";
  assert.equal(raftSessionName("Calpis", sessionId), "Raft · Calpis · abcdef01");
});
