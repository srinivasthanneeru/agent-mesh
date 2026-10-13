// Generic CLI agent: wraps ANY command-line tool that takes a prompt and
// writes an answer to stdout. The prompt is appended as the last argument
// (or piped to stdin with --stdin). This is how heterogeneous tools join the
// same fabric without per-vendor SDKs.
//
//   npm run cli -- --agent llama --name local --cmd "ollama run llama3.2"
//   npm run cli -- --agent gemini --cmd "gemini -p"
//   npm run cli -- --agent codex --cmd "codex exec"

import { spawn } from "node:child_process";
import { AgentHost } from "../host.ts";
import { ProtocolError, sanitizeToken } from "../protocol.ts";
import { connectMesh, defaultOwner, onShutdown, parseArgs } from "../conn.ts";

const [flags] = parseArgs(process.argv.slice(2));
if (typeof flags.cmd !== "string") {
  console.error('usage: cli.ts --cmd "<command>" [--agent token] [--name name] [--stdin] [--cwd dir]');
  process.exit(2);
}
const [bin, ...baseArgs] = flags.cmd.split(/\s+/);
const agent = sanitizeToken(String(flags.agent ?? bin));

const nc = await connectMesh(`${agent}-agent`);
const host = new AgentHost({
  nc,
  agent,
  owner: String(flags.owner ?? defaultOwner()),
  name: String(flags.name ?? "main"),
  description: `CLI wrapper: ${flags.cmd}`,
  metadata: { cmd: flags.cmd },
  heartbeatIntervalS: 5,
  keepAliveS: 15,
  onPrompt: (env, res, ctx) =>
    new Promise<void>((resolve, reject) => {
      const useStdin = flags.stdin === true;
      const child = spawn(bin, useStdin ? baseArgs : [...baseArgs, env.prompt], {
        cwd: typeof flags.cwd === "string" ? flags.cwd : undefined,
        stdio: [useStdin ? "pipe" : "ignore", "pipe", "pipe"],
      });
      if (useStdin) child.stdin!.end(env.prompt);
      const kill = () => child.kill("SIGTERM");
      ctx.signal.addEventListener("abort", kill, { once: true });
      let err = "";
      child.stdout!.on("data", (d) => res.send(d.toString()));
      child.stderr!.on("data", (d) => (err = (err + d).slice(-2000)));
      child.on("error", (e) => reject(new ProtocolError(500, `cannot start ${bin}: ${e.message}`)));
      child.on("close", (code) => {
        ctx.signal.removeEventListener("abort", kill);
        code ? reject(new ProtocolError(500, `${bin} exited ${code}: ${err.trim().split("\n").pop() ?? ""}`)) : resolve();
      });
    }),
});

await host.start();
onShutdown(async () => {
  await host.stop();
  await nc.drain();
});
