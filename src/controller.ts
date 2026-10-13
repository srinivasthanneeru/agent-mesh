// Headless session controller.
//
// A control-plane agent at agents.{verb}.cc-pool.{owner}.control that adds
// three extension verbs:
//
//   agents.spawn.cc-pool.{owner}.control  {cwd, name?, model?, permission_mode?, tools?, max_lifetime_s?}
//   agents.stop.cc-pool.{owner}.control   {session_id}
//   agents.list.cc-pool.{owner}.control   {}
//
// Every spawned session is its own first-class agent at
// agents.prompt.cc-pool.{owner}.{session_id}: discoverable, promptable,
// heartbeating, and removed from the fabric when stopped or expired.
//
//   npm run controller -- [--owner you] [--root ~/code]   (--root limits allowed cwds)

import { existsSync, statSync } from "node:fs";
import { resolve, sep } from "node:path";
import { AgentHost } from "./host.ts";
import { ProtocolError, isValidToken, sanitizeToken } from "./protocol.ts";
import { ClaudeSession } from "./runners/claude-runner.ts";
import { connectMesh, defaultOwner, onShutdown, parseArgs } from "./conn.ts";

const TOKEN = "cc-pool";

interface Managed {
  id: string;
  host: AgentHost;
  claude: ClaudeSession;
  cwd: string;
  createdAt: number;
  expiresAt: number | null;
  timer: ReturnType<typeof setTimeout> | null;
}

const [flags] = parseArgs(process.argv.slice(2));
const owner = String(flags.owner ?? defaultOwner());
const root = typeof flags.root === "string" ? resolve(flags.root) : null;
const sessions = new Map<string, Managed>();

const nc = await connectMesh("cc-pool-controller");

const controller = new AgentHost({
  nc,
  agent: TOKEN,
  owner,
  name: "control",
  description: "Spawns and manages headless Claude Code sessions",
  metadata: { role: "controller", ...(root ? { root } : {}) },
  heartbeatIntervalS: 5,
  onPrompt: (_env, res) => {
    res.send(
      [
        `cc-pool controller. Sessions: ${sessions.size}`,
        `spawn: agents.spawn.${TOKEN}.${owner}.control {"cwd": "/abs/path", "name"?, "model"?, "permission_mode"?, "tools"?: ["Read"], "max_lifetime_s"?}`,
        `stop:  agents.stop.${TOKEN}.${owner}.control {"session_id": "..."}`,
        `list:  agents.list.${TOKEN}.${owner}.control`,
        ...[...sessions.values()].map((s) => `  - ${s.id}  ${s.cwd}`),
      ].join("\n"),
    );
  },
});
await controller.start();

controller.addEndpoint("spawn", async (msg) => {
  const req = parseBody(msg.data);
  const cwd = resolve(String(req.cwd ?? ""));
  if (!req.cwd || !existsSync(cwd) || !statSync(cwd).isDirectory()) throw new ProtocolError(400, `cwd is not a directory: ${cwd}`);
  if (root && cwd !== root && !cwd.startsWith(root + sep)) throw new ProtocolError(403, `cwd must be under ${root}`);

  const id = req.name ? sanitizeToken(String(req.name)) : `s-${crypto.randomUUID().slice(0, 8)}`;
  if (!isValidToken(id)) throw new ProtocolError(400, "invalid session name");
  if (sessions.has(id)) throw new ProtocolError(409, `session ${id} already exists`);

  const claude = new ClaudeSession({
    cwd,
    model: req.model ? String(req.model) : undefined,
    permissionMode: String(req.permission_mode ?? "default"),
    allowedTools: Array.isArray(req.tools) ? req.tools.map(String) : ["Read", "Glob", "Grep"],
    maxTurns: typeof req.max_turns === "number" ? req.max_turns : undefined,
  });

  const host = new AgentHost({
    nc,
    agent: TOKEN,
    agentId: "claude-code",
    owner,
    name: id,
    session: id,
    description: `Claude Code session in ${cwd}`,
    metadata: { cwd, role: "session", controller: controller.subjects.prompt },
    heartbeatIntervalS: 5,
    keepAliveS: 15,
    onPrompt: async (env, res, ctx) => {
      if (claude.isBusy) throw new ProtocolError(409, "session is busy");
      const r = await claude.run(env.prompt, res, ctx.signal);
      res.raw("usage", { cost_usd: r.costUsd, total_cost_usd: claude.totalCostUsd, turns: r.turns });
    },
  });
  await host.start();

  const lifetime = typeof req.max_lifetime_s === "number" ? req.max_lifetime_s : null;
  const m: Managed = {
    id,
    host,
    claude,
    cwd,
    createdAt: Date.now(),
    expiresAt: lifetime ? Date.now() + lifetime * 1000 : null,
    timer: lifetime ? setTimeout(() => void stopSession(id), lifetime * 1000) : null,
  };
  sessions.set(id, m);
  return { session_id: id, subject: host.subjects.prompt, status_subject: host.subjects.status, heartbeat_subject: host.subjects.hb, cwd };
});

controller.addEndpoint("stop", async (msg) => {
  const { session_id } = parseBody(msg.data);
  if (!sessions.has(String(session_id))) throw new ProtocolError(404, `no session ${session_id}`);
  await stopSession(String(session_id));
  return { ok: true, session_id };
});

controller.addEndpoint("list", () => ({
  sessions: [...sessions.values()].map((s) => ({
    session_id: s.id,
    cwd: s.cwd,
    subject: s.host.subjects.prompt,
    busy: s.claude.isBusy,
    turns: s.claude.turns,
    total_cost_usd: s.claude.totalCostUsd,
    age_s: Math.round((Date.now() - s.createdAt) / 1000),
    remaining_lifetime_s: s.expiresAt ? Math.max(0, Math.round((s.expiresAt - Date.now()) / 1000)) : null,
  })),
}));

async function stopSession(id: string): Promise<void> {
  const s = sessions.get(id);
  if (!s) return;
  sessions.delete(id);
  if (s.timer) clearTimeout(s.timer);
  await s.host.stop(); // aborts any running turn and deregisters the micro service
  process.stderr.write(`[cc-pool] stopped ${id}\n`);
}

function parseBody(data: Uint8Array): Record<string, any> {
  if (data.length === 0) return {};
  try {
    const v = JSON.parse(new TextDecoder().decode(data));
    if (v && typeof v === "object") return v;
  } catch {
    /* fallthrough */
  }
  throw new ProtocolError(400, "body must be a JSON object");
}

onShutdown(async () => {
  await Promise.all([...sessions.keys()].map(stopSession));
  await controller.stop();
  await nc.drain();
});
