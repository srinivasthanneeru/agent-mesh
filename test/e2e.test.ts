// End-to-end: real nats-server, real micro services, real wire traffic.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { connect } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/nats-core";
import { AgentHost } from "../src/host.ts";
import { Mesh, matchTargets, type StreamEvent } from "../src/caller.ts";
import { ProtocolError } from "../src/protocol.ts";

let server: ChildProcess;
let nc: NatsConnection;
let mesh: Mesh;
const hosts: AgentHost[] = [];

async function freePort(): Promise<number> {
  return new Promise((r) => {
    const s = createServer().listen(0, () => {
      const p = (s.address() as { port: number }).port;
      s.close(() => r(p));
    });
  });
}

async function collect(it: AsyncIterable<StreamEvent>, onQuery?: (q: string) => string) {
  const evs: StreamEvent[] = [];
  for await (const ev of it) {
    evs.push(ev);
    if (ev.type === "query") ev.answer(onQuery?.(ev.query.prompt) ?? "yes");
  }
  return evs;
}

const text = (evs: StreamEvent[]) => evs.filter((e) => e.type === "text").map((e) => (e as { text: string }).text).join("");

before(async () => {
  const port = await freePort();
  server = spawn("nats-server", ["-p", String(port)], { stdio: "ignore" });
  for (let i = 0; i < 50; i++) {
    try {
      nc = await connect({ servers: `127.0.0.1:${port}` });
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  mesh = new Mesh(nc);
  await mesh.watch();

  const mk = async (o: Partial<ConstructorParameters<typeof AgentHost>[0]> & { onPrompt: any; agent: string; name: string }) => {
    const h = new AgentHost({ nc, owner: "t", heartbeatIntervalS: 1, log: () => {}, ...o });
    await h.start();
    hosts.push(h);
    return h;
  };

  await mk({ agent: "echo", name: "a", attachmentsOk: true, onPrompt: (env: any, res: any) => res.send(`echo:${env.prompt}`) });
  // Two instances of the same identity → queue group load-balances (§3.4).
  for (let i = 0; i < 2; i++) await mk({ agent: "lb", name: "x", onPrompt: (_e: any, res: any) => res.send(`inst${i}`) });
  await mk({
    agent: "asker",
    name: "q",
    onPrompt: async (_e: any, res: any) => {
      const a = await res.ask("proceed?", 2000);
      res.send(`got:${a}`);
    },
  });
  await mk({
    agent: "fail",
    name: "f",
    onPrompt: (_e: any, res: any) => {
      res.send("partial");
      throw new ProtocolError(429, "slow down", { error: "rate_limited", retry_after_s: 3 });
    },
  });
  await mk({
    agent: "slow",
    name: "s",
    keepAliveS: 0.2,
    onPrompt: async (_e: any, res: any) => {
      await new Promise((r) => setTimeout(r, 900));
      res.send("finally");
    },
  });
  await mk({ agent: "ctl", name: "control", metadata: { role: "controller" }, onPrompt: () => {} });
  await mk({ agent: "meta", name: "m", metadata: { role: "orchestrator" }, onPrompt: () => {} });
});

after(async () => {
  for (const h of hosts) await h.stop();
  await mesh.close();
  await nc.close();
  server.kill();
});

test("discovery via $SRV.INFO.agents returns metadata and prompt endpoint", async () => {
  const agents = await mesh.discover({ waitMs: 300 });
  const echo = agents.find((a) => a.token === "echo")!;
  assert.equal(echo.protocolVersion, "0.3");
  assert.equal(echo.owner, "t");
  assert.equal(echo.endpoints.prompt.subject, "agents.prompt.echo.t.a");
  assert.equal(echo.endpoints.prompt.queue_group, "agents");
  assert.equal(echo.endpoints.prompt.metadata?.attachments_ok, "true");
  assert.ok(echo.endpoints.prompt.metadata?.max_payload);
  assert.equal(echo.endpoints.status.subject, "agents.status.echo.t.a");
});

test("stream: ack first, content, then terminator", async () => {
  const [echo] = matchTargets(await mesh.discover({ waitMs: 300 }), "echo");
  const evs = await collect(mesh.prompt(echo, "hi"));
  assert.deepEqual(evs.map((e) => e.type), ["ack", "text", "done"]);
  assert.equal(text(evs), "echo:hi");
});

test("mid-stream query round-trip (§7)", async () => {
  const [a] = matchTargets(await mesh.discover({ waitMs: 300 }), "asker");
  const evs = await collect(mesh.prompt(a, "go"), () => "yes please");
  assert.equal(text(evs), "got:yes please");
});

test("error mid-stream: partial content, error headers + JSON body, then terminator", async () => {
  const [a] = matchTargets(await mesh.discover({ waitMs: 300 }), "fail");
  const evs = await collect(mesh.prompt(a, "go"));
  assert.deepEqual(evs.map((e) => e.type), ["ack", "text", "error", "done"]);
  const err = evs[2] as Extract<StreamEvent, { type: "error" }>;
  assert.equal(err.code, 429);
  assert.equal((err.body as any).retry_after_s, 3);
});

test("keep-alive acks hold the stream open past a short inactivity timeout", async () => {
  const [a] = matchTargets(await mesh.discover({ waitMs: 300 }), "slow");
  const evs = await collect(mesh.prompt(a, "go", { inactivityMs: 500 }));
  assert.equal(text(evs), "finally");
  assert.ok(evs.filter((e) => e.type === "ack").length > 1);
});

test("inactivity timeout fires when nothing arrives", async () => {
  const [a] = matchTargets(await mesh.discover({ waitMs: 300 }), "echo");
  const fake = { ...a, endpoints: { ...a.endpoints, prompt: { ...a.endpoints.prompt, subject: "agents.prompt.nobody.t.x" } } };
  const evs = await collect(mesh.prompt(fake, "hi", { inactivityMs: 200 }));
  assert.deepEqual(evs.map((e) => e.type), ["timeout"]);
});

test("client-side validation: attachments refused locally when attachments_ok=false", async () => {
  const [a] = matchTargets(await mesh.discover({ waitMs: 300 }), "asker");
  const evs = await collect(mesh.prompt(a, "x", { attachments: [{ filename: "a", content: "aGk=" }] }));
  assert.equal(evs[0].type, "error");
  assert.equal(evs.length, 1); // never published
});

test("same-identity instances: one logical agent, queue group spreads load", async () => {
  const all = await mesh.discover({ waitMs: 300 });
  assert.equal(all.filter((a) => a.token === "lb").length, 2);
  const targets = matchTargets(all, "lb");
  assert.equal(targets.length, 1);
  const seen = new Set<string>();
  for (let i = 0; i < 20; i++) seen.add(text(await collect(mesh.prompt(targets[0], "who"))));
  assert.deepEqual([...seen].sort(), ["inst0", "inst1"]);
});

test("fan-out merges streams from several agents", async () => {
  const targets = matchTargets(await mesh.discover({ waitMs: 300 }), "echo,lb");
  const by = new Map<string, string>();
  for await (const ev of mesh.fanout(targets, "hey")) if (ev.type === "text") by.set(ev.from.token, ev.text);
  assert.equal(by.get("echo"), "echo:hey");
  assert.match(by.get("lb")!, /^inst[01]$/);
});

test("matchTargets: token/name exact, no substring bleed, controllers/orchestrators only when named", async () => {
  const all = await mesh.discover({ waitMs: 300 });
  assert.deepEqual(matchTargets(all, "ech").map((a) => a.token), []);
  assert.deepEqual(matchTargets(all, "all").some((a) => a.token === "ctl" || a.token === "meta"), false);
  assert.deepEqual(matchTargets(all, "meta").map((a) => a.token), ["meta"]);
  assert.deepEqual(matchTargets(all, "ctl").map((a) => a.token), ["ctl"]);
  assert.deepEqual(matchTargets(all, "@echo/t").map((a) => a.token), ["echo"]);
});

test("status endpoint and heartbeats agree on instance_id", async () => {
  const [a] = matchTargets(await mesh.discover({ waitMs: 300 }), "echo");
  const hb = await mesh.status(a);
  assert.equal(hb.instance_id, a.instanceId);
  await new Promise((r) => setTimeout(r, 1200));
  assert.equal(mesh.liveness(a.instanceId), "online");
});

test("stopping an agent removes it from discovery", async () => {
  const h = new AgentHost({ nc, agent: "temp", owner: "t", name: "z", log: () => {}, onPrompt: () => {} });
  await h.start();
  assert.equal(matchTargets(await mesh.discover({ waitMs: 300 }), "temp").length, 1);
  await h.stop();
  assert.equal(matchTargets(await mesh.discover({ waitMs: 300 }), "temp").length, 0);
});
