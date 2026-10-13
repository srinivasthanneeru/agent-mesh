// Echo agent: the smallest compliant agent. Streams the prompt back word by
// word and reports any attachments it received.
//
//   npm run echo -- [--name main] [--owner you]

import { AgentHost } from "../host.ts";
import { connectMesh, defaultOwner, onShutdown, parseArgs } from "../conn.ts";

const [flags] = parseArgs(process.argv.slice(2));
const nc = await connectMesh("echo-agent");

const host = new AgentHost({
  nc,
  agent: "echo",
  owner: String(flags.owner ?? defaultOwner()),
  name: String(flags.name ?? "main"),
  description: "Echoes prompts back, word by word",
  attachmentsOk: true,
  heartbeatIntervalS: 5,
  onPrompt: async (env, res) => {
    for (const word of `echo: ${env.prompt}`.split(/(\s+)/)) {
      res.send(word);
      await new Promise((r) => setTimeout(r, 25));
    }
    for (const a of env.attachments ?? []) {
      res.send(`\n[attachment ${a.filename}: ${Buffer.from(a.content, "base64").length} bytes]`);
    }
  },
});

await host.start();
onShutdown(async () => {
  await host.stop();
  await nc.drain();
});
