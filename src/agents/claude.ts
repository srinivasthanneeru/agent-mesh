// Claude Code agent: puts a long-lived, headless Claude Code conversation on
// the fabric. Each instance remembers its conversation across prompts.
//
//   npm run claude -- --cwd ~/code/project [--name project] [--model sonnet]
//                     [--permission-mode default|acceptEdits|plan|bypassPermissions]
//                     [--tools Read,Glob,Grep]

import { basename, resolve } from "node:path";
import { AgentHost } from "../host.ts";
import { ProtocolError, sanitizeToken } from "../protocol.ts";
import { ClaudeSession } from "../runners/claude-runner.ts";
import { connectMesh, defaultOwner, onShutdown, parseArgs } from "../conn.ts";

const [flags] = parseArgs(process.argv.slice(2));
const cwd = resolve(String(flags.cwd ?? process.cwd()));
const name = String(flags.name ?? (sanitizeToken(basename(cwd)) || "main"));

const session = new ClaudeSession({
  cwd,
  model: flags.model ? String(flags.model) : undefined,
  permissionMode: String(flags["permission-mode"] ?? "default"),
  allowedTools: flags.tools ? String(flags.tools).split(",") : ["Read", "Glob", "Grep"],
});

const nc = await connectMesh("claude-agent");
const host = new AgentHost({
  nc,
  agent: "cc",
  agentId: "claude-code",
  owner: String(flags.owner ?? defaultOwner()),
  name,
  session: name,
  description: `Claude Code in ${cwd}`,
  metadata: { cwd, harness: "claude-code" },
  heartbeatIntervalS: 5,
  keepAliveS: 15,
  onPrompt: async (env, res, ctx) => {
    if (session.isBusy) throw new ProtocolError(409, "this session is already working on a prompt");
    const r = await session.run(env.prompt, res, ctx.signal);
    res.raw("usage", { cost_usd: r.costUsd, total_cost_usd: session.totalCostUsd, turns: r.turns, duration_ms: r.durationMs });
  },
});

await host.start();
onShutdown(async () => {
  await host.stop();
  await nc.drain();
});
