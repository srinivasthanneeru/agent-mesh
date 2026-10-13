#!/usr/bin/env node
// Stand-in for the `claude` CLI used in tests: emits the same stream-json
// event shapes (init → partial text deltas → tool_use → result), or a JSON
// result for --output-format json (used by the meta-agent router).
const args = process.argv.slice(2);
const prompt = args[args.indexOf("-p") + 1] ?? "";
const fmt = args[args.indexOf("--output-format") + 1];
const resume = args.includes("--resume") ? args[args.indexOf("--resume") + 1] : null;
const sid = resume ?? "11111111-2222-3333-4444-555555555555";
const out = (o) => process.stdout.write(JSON.stringify(o) + "\n");

if (fmt === "json") {
  // Router: pick every agent whose label mentions a word from the request, else the first.
  const labels = [...prompt.matchAll(/^- ([^:]+):/gm)].map((m) => m[1]);
  const req = /User request: (.*)/.exec(prompt)?.[1] ?? prompt;
  const hit = labels.filter((l) => req.toLowerCase().includes(l.split("/")[0]));
  const targets = (hit.length ? hit : labels.slice(0, 1)).map((agent) => ({ agent, prompt: req }));
  out({ type: "result", is_error: false, result: labels.length ? JSON.stringify({ targets }) : `synth: ${prompt.length} chars` });
  process.exit(0);
}

out({ type: "system", subtype: "init", session_id: sid, cwd: process.cwd() });
out({ type: "stream_event", event: { type: "message_start" } });
const words = `${resume ? "(resumed) " : ""}fake-claude in ${process.cwd().split("/").pop()} got: ${prompt}`.split(/(\s+)/);
for (const w of words) out({ type: "stream_event", event: { type: "content_block_delta", delta: { type: "text_delta", text: w } } });
out({ type: "assistant", message: { content: [{ type: "text", text: words.join("") }, { type: "tool_use", name: "Read", input: { file_path: "README.md" } }] } });
out({ type: "result", subtype: "success", is_error: false, session_id: sid, total_cost_usd: 0.0012, duration_ms: 42, num_turns: 1, result: words.join("") });
