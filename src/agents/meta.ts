// Meta-agent: an agent that orchestrates the other agents on the fabric.
// Because it is itself a protocol agent, anything that can prompt an agent
// (the CLI, `nats req`, another meta-agent) can use it.
//
// Routing, in order:
//   "@cc @shell do X"   explicit: fan out to the matching agents (see matchTargets)
//   "@all do X"         every worker agent
//   "do X"              ask Claude to choose agents + write per-agent instructions
//                       (disable with --no-llm; then it lists agents instead)
//
// Sub-agent questions (§7 queries) are relayed up to whoever prompted the
// meta-agent, and the answer is passed back down. With --synthesize, Claude
// merges all agents' answers into one final response.
//
//   npm run meta -- [--name main] [--synthesize] [--no-llm] [--model haiku]

import { AgentHost, type Responder } from "../host.ts";
import { Mesh, label, matchTargets, type AgentRecord } from "../caller.ts";
import { ProtocolError } from "../protocol.ts";
import { claudeText } from "../runners/claude-runner.ts";
import { connectMesh, defaultOwner, onShutdown, parseArgs } from "../conn.ts";

const [flags] = parseArgs(process.argv.slice(2));
const useLlm = flags["no-llm"] !== true;
const model = typeof flags.model === "string" ? flags.model : undefined;

const nc = await connectMesh("meta-agent");
const mesh = new Mesh(nc);
await mesh.watch();

async function workers(): Promise<AgentRecord[]> {
  const all = await mesh.discover({ waitMs: 500 });
  return matchTargets(all, "all"); // workers only: no controllers, no orchestrators (incl. ourselves)
}

function describe(agents: AgentRecord[]): string {
  return agents.map((a) => `- ${label(a)}: ${a.description}`).join("\n");
}

interface Plan {
  targets: { agent: string; prompt: string }[];
}

async function plan(prompt: string, agents: AgentRecord[]): Promise<Plan> {
  const mentions = [...prompt.matchAll(/(^|\s)@([\w/*-]+)/g)].map((m) => m[2]);
  const body = prompt.replace(/(^|\s)@[\w/*-]+/g, " ").trim();
  if (mentions.length) {
    const picked = matchTargets(agents, mentions); // `agents` is already workers-only
    return { targets: picked.map((a) => ({ agent: label(a), prompt: body })) };
  }
  if (!useLlm) return { targets: [] };

  const answer = await claudeText(
    [
      "You route a user's request to one or more agents. Available agents:",
      describe(agents),
      "",
      `User request: ${prompt}`,
      "",
      'Reply with ONLY JSON: {"targets":[{"agent":"<exact label>","prompt":"<instruction for that agent>"}]}.',
      "Pick the fewest agents that can do the job. Use an empty list if none fit.",
    ].join("\n"),
    { model },
  );
  const json = answer.slice(answer.indexOf("{"), answer.lastIndexOf("}") + 1);
  const p = JSON.parse(json) as Plan;
  const known = new Set(agents.map(label));
  return { targets: (p.targets ?? []).filter((t) => known.has(t.agent)) };
}

async function run(prompt: string, res: Responder): Promise<void> {
  const agents = await workers();
  if (agents.length === 0) {
    res.send("No worker agents are online.");
    return;
  }
  const p = await plan(prompt, agents);
  if (p.targets.length === 0) {
    res.send(`No agent selected. Mention one with @name. Online:\n${describe(agents)}`);
    return;
  }
  res.raw("plan", p);
  res.send(`Routing to ${p.targets.map((t) => t.agent).join(", ")}\n`);

  const byLabel = new Map(agents.map((a) => [label(a), a]));
  const answers = new Map<string, string>();

  await Promise.all(
    p.targets.map(async (t) => {
      const a = byLabel.get(t.agent)!;
      let buf = "";
      let full = "";
      // Line-buffered so concurrent agents' output interleaves by line, not by token.
      const flush = (final = false) => {
        const lines = buf.split("\n");
        buf = lines.pop()!;
        for (const l of lines) res.send(`[${t.agent}] ${l}\n`);
        if (final && buf) res.send(`[${t.agent}] ${buf}\n`);
      };
      for await (const ev of mesh.prompt(a, t.prompt, { inactivityMs: 120_000 })) {
        if (ev.type === "text") {
          buf += ev.text;
          full += ev.text;
          flush();
        } else if (ev.type === "query") {
          // Relay the sub-agent's question to our caller, then pass the answer down.
          const reply = await res.ask(`[${t.agent}] ${ev.query.prompt}`, 120_000);
          ev.answer(reply ?? "no");
        } else if (ev.type === "unknown") {
          const c = ev.chunk as { type: string; data: unknown };
          if (c.type === "tool") res.raw("tool", { agent: t.agent, ...(c.data as object) });
        } else if (ev.type === "error") {
          res.send(`[${t.agent}] error ${ev.code}: ${ev.message}\n`);
        } else if (ev.type === "timeout") {
          res.send(`[${t.agent}] timed out\n`);
        }
      }
      flush(true);
      answers.set(t.agent, full);
    }),
  );

  if (flags.synthesize === true && answers.size > 1) {
    res.send("\n── synthesis ──\n");
    const merged = [...answers].map(([k, v]) => `### ${k}\n${v}`).join("\n\n");
    res.send(await claudeText(`Request: ${prompt}\n\nAgent answers:\n${merged}\n\nWrite one concise, combined answer.`, { model }));
  }
}

const self = new AgentHost({
  nc,
  agent: "meta",
  owner: String(flags.owner ?? defaultOwner()),
  name: String(flags.name ?? "main"),
  description: "Routes prompts to other agents (@mention or LLM routing), relays their questions",
  metadata: { role: "orchestrator" },
  heartbeatIntervalS: 5,
  keepAliveS: 15,
  onPrompt: async (env, res) => {
    try {
      await run(env.prompt, res);
    } catch (e) {
      throw new ProtocolError(500, `meta-agent failed: ${(e as Error).message}`);
    }
  },
});
await self.start();

onShutdown(async () => {
  await self.stop();
  await mesh.close();
  await nc.drain();
});
