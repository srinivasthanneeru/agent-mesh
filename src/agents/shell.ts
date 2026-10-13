// Shell agent: runs the prompt as a shell command in a fixed working
// directory, streaming stdout/stderr. Before running, it pauses the stream
// with a §7 mid-stream query and waits for the caller to approve.
//
//   npm run shell -- [--cwd /path] [--name box1] [--yes]   (--yes skips approval)

import { spawn } from "node:child_process";
import { hostname } from "node:os";
import { AgentHost } from "../host.ts";
import { ProtocolError, sanitizeToken } from "../protocol.ts";
import { connectMesh, defaultOwner, onShutdown, parseArgs } from "../conn.ts";

const [flags] = parseArgs(process.argv.slice(2));
const cwd = String(flags.cwd ?? process.cwd());
const nc = await connectMesh("shell-agent");

const host = new AgentHost({
  nc,
  agent: "shell",
  owner: String(flags.owner ?? defaultOwner()),
  name: String(flags.name ?? (sanitizeToken(hostname().split(".")[0]) || "local")),
  description: `Runs shell commands in ${cwd} (asks before running)`,
  metadata: { cwd, host: hostname() },
  heartbeatIntervalS: 5,
  keepAliveS: 15,
  onPrompt: async (env, res, ctx) => {
    const cmd = env.prompt.trim();
    if (flags.yes !== true) {
      const answer = await res.ask(`Run \`${cmd}\` in ${cwd} on ${hostname()}? (yes/no)`, 120_000);
      if (answer === null) throw new ProtocolError(409, "no approval received (timed out)");
      if (!/^y(es)?$/i.test(answer.trim())) {
        res.send("not run: caller declined.");
        return;
      }
    }
    await new Promise<void>((resolve) => {
      const child = spawn("/bin/sh", ["-c", cmd], { cwd, stdio: ["ignore", "pipe", "pipe"] });
      const kill = () => child.kill("SIGTERM");
      ctx.signal.addEventListener("abort", kill, { once: true });
      child.stdout.on("data", (d) => res.send(d.toString()));
      child.stderr.on("data", (d) => res.send(d.toString()));
      child.on("close", (code) => {
        ctx.signal.removeEventListener("abort", kill);
        if (code) res.send(`\n[exit ${code}]`);
        resolve();
      });
    });
  },
});

await host.start();
onShutdown(async () => {
  await host.stop();
  await nc.drain();
});
