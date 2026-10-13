// mesh: operator console for every agent on the fabric.
//
//   mesh ls                               discover agents + liveness
//   mesh watch                            live heartbeats, joins and departures
//   mesh ask <target> <prompt...>         prompt one agent, or fan out if several match
//        [--attach file] [--answer yes]   (--answer auto-replies to mid-stream queries)
//   mesh spawn <cwd> [--name n] [--model m] [--lifetime 900] [--tools Read,Edit] [--mode acceptEdits]
//   mesh sessions                         list sessions on the cc-pool controller
//   mesh stop <session>
//   mesh chat                             REPL: "@target msg", "@a,@b msg", "@all msg", /ls, /use <target>
//
// <target> matches agent labels "token/owner/name": exact, substring, "all",
// or a comma list. e.g. `echo`, `cc/`, `shell,cc`, `all`.

import { readFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { createInterface, type Interface } from "node:readline/promises";
import { Mesh, label, matchTargets, uniqueBySubject, type AgentRecord, type StreamEvent } from "./caller.ts";
import { connectMesh, parseArgs } from "./conn.ts";

const [flags, args] = parseArgs(process.argv.slice(2));
const cmd = args.shift() ?? "help";

const tty = process.stdout.isTTY;
const c = (code: number) => (s: string) => (tty ? `\x1b[${code}m${s}\x1b[0m` : s);
const dim = c(2), bold = c(1), red = c(31), green = c(32), yellow = c(33);
const PALETTE = [36, 35, 34, 33, 32, 96, 95, 94];

const nc = await connectMesh("mesh-cli");
const mesh = new Mesh(nc);
await mesh.watch(); // before discovery (§8.5)

// One line iterator shared by the chat loop and query prompts, so no input is
// lost between reads (works for a TTY and for piped scripts alike).
let rl: Interface | null = null;
let lines: AsyncIterator<string> | null = null;
async function readLine(prompt: string): Promise<string | null> {
  rl ??= createInterface({ input: process.stdin, output: process.stdout, terminal: process.stdin.isTTY });
  lines ??= rl[Symbol.asyncIterator]();
  process.stdout.write(prompt);
  const r = await lines.next();
  if (!process.stdin.isTTY && !r.done) process.stdout.write(`${r.value}\n`); // echo scripted input
  return r.done ? null : r.value;
}

try {
  switch (cmd) {
    case "ls":
    case "agents":
      await list();
      break;
    case "watch":
      await watch();
      break;
    case "ask":
      await ask(args.shift() ?? "", args.join(" "));
      break;
    case "spawn":
      await spawnSession(args[0]);
      break;
    case "sessions":
      await sessions();
      break;
    case "stop":
      await stopSession(args[0]);
      break;
    case "chat":
      await chat();
      break;
    case "help":
    case "--help":
      console.log(readFileSync(new URL(import.meta.url)).toString().split("\n").slice(2, 15).map((l) => l.slice(3)).join("\n"));
    default:
      throw new Error(`unknown command "${cmd}" (try: mesh help)`);
  }
} catch (e) {
  console.error(red(`error: ${(e as Error).message}`));
  process.exitCode = 1;
} finally {
  (rl as Interface | null)?.close();
  await mesh.close();
  await nc.drain();
}

// ---------------------------------------------------------------------------

async function list(): Promise<void> {
  const agents = await mesh.discover();
  if (!agents.length) return console.log(dim("no agents on the fabric"));
  const unique = uniqueBySubject(agents);
  const instances = (a: AgentRecord) => agents.filter((x) => x.endpoints.prompt.subject === a.endpoints.prompt.subject).length;
  const statuses = await Promise.all(unique.map((a) => mesh.status(a, 1500).catch(() => null)));
  const rows = unique.map((a, i) => {
    const hb = statuses[i];
    const live = hb ? green("●") : red("○");
    const n = instances(a);
    const busy = (n > 1 ? yellow(` ×${n}`) : "") + (hb && Number(hb.busy) > 0 ? yellow(` busy:${hb.busy}`) : "");
    const extra = Object.keys(a.endpoints).filter((e) => !["prompt", "status"].includes(e));
    return [`${live} ${bold(label(a))}${busy}`, a.description + (extra.length ? dim(`  [+${extra.join(",")}]`) : "")];
  });
  const w = Math.max(...rows.map((r) => r[0].length));
  for (const [l, d] of rows) console.log(`${l.padEnd(w)}  ${d}`);
  console.log(dim(`\n${unique.length} agent(s), ${agents.length} instance(s)`));
}

async function watch(): Promise<void> {
  const seen = new Map<string, { label: string; at: number; interval: number }>();
  console.log(dim("watching agents.hb.*.*.* (Ctrl-C to exit)"));
  mesh.onHeartbeat((hb, subj) => {
    const l = subj.split(".").slice(2).join("/");
    if (!seen.has(hb.instance_id)) console.log(`${green("+ join")}  ${bold(l)}  ${dim(hb.instance_id)}`);
    seen.set(hb.instance_id, { label: l, at: Date.now(), interval: hb.interval_s });
  });
  setInterval(() => {
    for (const [id, s] of seen) {
      if (Date.now() - s.at > 3 * s.interval * 1000) {
        console.log(`${red("- gone")}  ${bold(s.label)}  ${dim(id)}`);
        seen.delete(id);
      }
    }
  }, 1000);
  await new Promise(() => {});
}

async function ask(target: string, prompt: string): Promise<void> {
  if (!target || !prompt) throw new Error("usage: mesh ask <target> <prompt>");
  const agents = matchTargets(await mesh.discover(), target);
  if (!agents.length) throw new Error(`no agent matches "${target}" (try: mesh ls)`);
  const attachments = typeof flags.attach === "string"
    ? [{ filename: basename(flags.attach), content: readFileSync(flags.attach).toString("base64") }]
    : undefined;
  await render(agents, prompt, { attachments });
}

/** Streams one or many agents to the terminal. Single agent: raw stream. Many: line-prefixed. */
async function render(agents: AgentRecord[], prompt: string, opts: { attachments?: { filename: string; content: string }[] } = {}) {
  const single = agents.length === 1;
  const colorOf = new Map(agents.map((a, i) => [a, c(PALETTE[i % PALETTE.length])]));
  const bufs = new Map<AgentRecord, string>();
  const tag = (a: AgentRecord) => colorOf.get(a)!(`[${label(a)}]`);
  let atLineStart = true;
  const out = (a: AgentRecord, s: string) => {
    if (single) {
      process.stdout.write(s);
      if (s) atLineStart = s.endsWith("\n");
      return;
    }
    const lines = ((bufs.get(a) ?? "") + s).split("\n");
    bufs.set(a, lines.pop()!);
    for (const l of lines) console.log(`${tag(a)} ${l}`);
  };
  // Notes (tools, cost, errors) flush the agent's partial line first so order is preserved.
  const note = (a: AgentRecord, s: string) => {
    if (single) {
      process.stdout.write(`${atLineStart ? "" : "\n"}${dim(s)}\n`);
      atLineStart = true;
      return;
    }
    const rest = bufs.get(a);
    if (rest) console.log(`${tag(a)} ${rest}`);
    bufs.set(a, "");
    console.log(`${tag(a)} ${dim(s)}`);
  };

  if (!single) console.log(dim(`fan-out to ${agents.length} agents: ${agents.map(label).join(", ")}`));
  const t0 = Date.now();
  const firstByte = new Map<AgentRecord, number>();

  const handle = async (a: AgentRecord, ev: StreamEvent) => {
    switch (ev.type) {
      case "ack":
        if (!firstByte.has(a)) firstByte.set(a, Date.now() - t0);
        break;
      case "text":
        out(a, ev.text);
        break;
      case "query": {
        const q = `${single ? "" : `[${label(a)}] `}${ev.query.prompt} `;
        const reply = typeof flags.answer === "string" ? flags.answer : ((await readLine(yellow(`? ${q}`))) ?? "no");
        ev.answer(reply);
        break;
      }
      case "error":
        note(a, red(`error ${ev.code}: ${ev.message}`));
        break;
      case "timeout":
        note(a, red("no chunk within inactivity timeout; giving up"));
        break;
      case "unknown": {
        const ch = ev.chunk as { type: string; data: any };
        if (ch.type === "tool") note(a, `⚙ ${ch.data.agent ? `[${ch.data.agent}] ` : ""}${ch.data.name} ${JSON.stringify(ch.data.input ?? {}).slice(0, 120)}`);
        else if (ch.type === "usage" && ch.data?.cost_usd !== undefined) note(a, `$${Number(ch.data.cost_usd).toFixed(4)} this turn, $${Number(ch.data.total_cost_usd).toFixed(4)} session`);
        else if (ch.type === "status_note") note(a, String(ch.data?.message ?? ""));
        else if (ch.type === "plan") note(a, `plan: ${ch.data.targets.map((t: any) => t.agent).join(", ")}`);
        break;
      }
      case "done": {
        note(a, `done in ${((Date.now() - t0) / 1000).toFixed(1)}s (ack ${firstByte.get(a) ?? "?"}ms)`);
        break;
      }
    }
  };

  // Queries need the terminal one at a time; everything else streams concurrently.
  let chain = Promise.resolve();
  for await (const ev of mesh.fanout(agents, prompt, { attachments: opts.attachments, inactivityMs: 120_000 })) {
    const { from, ...e } = ev;
    chain = chain.then(() => handle(from, e as StreamEvent));
    if (e.type === "query") await chain;
  }
  await chain;
}

async function controller(): Promise<AgentRecord> {
  const ctl = (await mesh.discover()).find((a) => a.metadata.role === "controller" && a.endpoints.spawn);
  if (!ctl) throw new Error("no controller on the fabric (start one: npm run controller)");
  return ctl;
}

async function spawnSession(cwd?: string): Promise<void> {
  if (!cwd) throw new Error("usage: mesh spawn <cwd> [--name n] [--model m] [--lifetime s]");
  const body: Record<string, unknown> = { cwd: resolve(cwd) };
  if (typeof flags.name === "string") body.name = flags.name;
  if (typeof flags.model === "string") body.model = flags.model;
  if (typeof flags.mode === "string") body.permission_mode = flags.mode;
  if (typeof flags.tools === "string") body.tools = flags.tools.split(",");
  if (typeof flags.lifetime === "string") body.max_lifetime_s = Number(flags.lifetime);
  const r = await mesh.call<{ session_id: string; subject: string }>(await controller(), "spawn", body);
  console.log(`${green("spawned")} ${bold(r.session_id)}  ${dim(r.subject)}`);
}

async function sessions(): Promise<void> {
  const r = await mesh.call<{ sessions: any[] }>(await controller(), "list");
  if (!r.sessions.length) return console.log(dim("no sessions"));
  for (const s of r.sessions) {
    const life = s.remaining_lifetime_s === null ? "" : dim(` expires in ${s.remaining_lifetime_s}s`);
    console.log(`${s.busy ? yellow("●") : green("●")} ${bold(s.session_id)}  ${s.cwd}  ${dim(`turns ${s.turns}, $${s.total_cost_usd.toFixed(4)}`)}${life}`);
  }
}

async function stopSession(id?: string): Promise<void> {
  if (!id) throw new Error("usage: mesh stop <session>");
  await mesh.call(await controller(), "stop", { session_id: id });
  console.log(`${red("stopped")} ${id}`);
}

async function chat(): Promise<void> {
  let current: string | null = null;
  console.log(dim('mesh chat — "@target message" routes, "@all message" fans out, /ls lists, /use <target> sets default, /quit exits'));
  while (true) {
    const raw = await readLine(bold(`${current ?? "mesh"}> `));
    if (raw === null) return;
    const line = raw.trim();
    if (!line) continue;
    if (line === "/quit" || line === "/exit") return;
    if (line === "/ls") {
      await list();
      continue;
    }
    if (line.startsWith("/use ")) {
      current = line.slice(5).trim();
      continue;
    }
    const m = /^((?:@[\w/*,-]+[\s,]*)+)(.*)$/s.exec(line);
    const target = m ? m[1].replace(/\s+/g, ",").replace(/,+/g, ",") : current;
    const prompt = m ? m[2].trim() : line;
    if (!target) {
      console.log(dim("no target: use @name or /use <target>"));
      continue;
    }
    const agents = matchTargets(await mesh.discover(), target);
    if (!agents.length) {
      console.log(red(`no agent matches ${target}`));
      continue;
    }
    await render(agents, prompt);
    console.log();
  }
}
