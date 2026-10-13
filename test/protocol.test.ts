import { test } from "node:test";
import assert from "node:assert/strict";
import { ProtocolError, decodeEnvelope, formatSize, parseSize, parseSubject, sanitizeToken, subject } from "../src/protocol.ts";

const b = (s: string) => new TextEncoder().encode(s);
const code = (fn: () => unknown) => {
  try {
    fn();
  } catch (e) {
    return (e as ProtocolError).code;
  }
  return 0;
};

test("verb-first subjects round-trip", () => {
  const s = subject("prompt", "cc", "srini", "proj");
  assert.equal(s, "agents.prompt.cc.srini.proj");
  assert.deepEqual(parseSubject(s), { verb: "prompt", agent: "cc", owner: "srini", name: "proj" });
  assert.equal(parseSubject("agents.cc.srini.proj"), null); // v0.2 shape is not ours
});

test("plain text is shorthand for {prompt}", () => {
  assert.deepEqual(decodeEnvelope(b("hello"), { attachmentsOk: false }), { prompt: "hello" });
});

test("leading whitespace then { means JSON; unknown fields survive", () => {
  const env = decodeEnvelope(b('  \n{"prompt":"hi","trace":"x"}'), { attachmentsOk: false });
  assert.equal(env.prompt, "hi");
  assert.equal(env.trace, "x");
});

test("400s: empty, bad JSON, missing prompt, attachments refused, bad base64, unsafe filename", () => {
  const ok = { attachmentsOk: true };
  assert.equal(code(() => decodeEnvelope(b(""), ok)), 400);
  assert.equal(code(() => decodeEnvelope(b("{nope"), ok)), 400);
  assert.equal(code(() => decodeEnvelope(b('{"x":1}'), ok)), 400);
  assert.equal(code(() => decodeEnvelope(b('{"prompt":"   "}'), ok)), 400);
  const att = (f: string, c: string) => b(JSON.stringify({ prompt: "p", attachments: [{ filename: f, content: c }] }));
  assert.equal(code(() => decodeEnvelope(att("a.txt", "aGk="), { attachmentsOk: false })), 400);
  assert.equal(code(() => decodeEnvelope(att("a.txt", "a-b_"), ok)), 400); // URL-safe alphabet
  assert.equal(code(() => decodeEnvelope(att("a.txt", "aGk"), ok)), 400); // unpadded
  assert.equal(code(() => decodeEnvelope(att("../a.txt", "aGk="), ok)), 400);
  assert.equal(decodeEnvelope(att("a.txt", "aGk="), ok).attachments?.[0].filename, "a.txt");
});

test("sizes and tokens", () => {
  assert.equal(parseSize("1MB"), 1048576);
  assert.equal(parseSize("512KB"), 524288);
  assert.equal(formatSize(8 * 1024 * 1024), "8MB");
  assert.equal(sanitizeToken("My Project.v2!"), "my-project-v2");
});
