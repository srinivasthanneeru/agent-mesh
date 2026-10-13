// Caller side: discover agents, track liveness, prompt, stream, fan out.
//
//   const mesh = new Mesh(nc)
//   await mesh.watch()                        // subscribe to heartbeats first (§8.5)
//   const agents = await mesh.discover()      // $SRV.INFO.agents
//   for await (const ev of mesh.prompt(agents[0], "hi")) ...
//   for await (const ev of mesh.fanout(agents, "hi")) ...   // one prompt, many agents

import { createInbox, type NatsConnection, type Msg, type Subscription } from "@nats-io/nats-core";
import {
  ERROR_CODE_HEADER,
  ERROR_DESC_HEADER,
  HB_WILDCARD,
  PROTOCOL_VERSION,
  SERVICE_NAME,
  chunkText,
  parseSize,
  parseSubject,
  type Attachment,
  type Chunk,
  type Heartbeat,
  type QueryData,
} from "./protocol.ts";

export interface EndpointInfo {
  name: string;
  subject: string;
  queue_group?: string;
  metadata?: Record<string, string>;
}

export interface AgentRecord {
  instanceId: string;
  agent: string; // metadata.agent (canonical harness id)
  token: string; // 3rd subject token (may be an abbreviation)
  owner: string;
  name: string; // 5th subject token
  session?: string;
  protocolVersion: string;
  version: string;
  description: string;
  metadata: Record<string, string>;
  endpoints: Record<string, EndpointInfo>;
}

export type StreamEvent =
  | { type: "ack" }
  | { type: "text"; text: string; attachments?: Attachment[] }
  | { type: "status"; status: string }
  | { type: "query"; query: QueryData; answer: (reply: string) => void }
  | { type: "error"; code: number; message: string; body?: unknown }
  | { type: "done" }
  | { type: "timeout" }
  | { type: "unknown"; chunk: unknown };

export interface PromptOptions {
  attachments?: Attachment[];
  /** Extra envelope fields, passed through untouched. */
  extra?: Record<string, unknown>;
  /** Gap allowed between chunks before giving up (§6.6). Default 60s. */
  inactivityMs?: number;
}

export type LabeledEvent = StreamEvent & { from: AgentRecord };

const dec = new TextDecoder();

export function label(a: AgentRecord): string {
  return `${a.token}/${a.owner}/${a.name}`;
}

/** Major.minor compatibility check (§11.2). 0.x must match exactly. */
export function isCompatible(v: string): boolean {
  const [maj, min] = v.split(".");
  const [ourMaj, ourMin] = PROTOCOL_VERSION.split(".");
  return maj === ourMaj && (maj !== "0" || min === ourMin);
}

export class Mesh {
  private readonly lastSeen = new Map<string, { hb: Heartbeat; at: number; name: string }>();
  private hbSub: Subscription | null = null;
  private readonly hbListeners = new Set<(hb: Heartbeat, subject: string) => void>();

  readonly nc: NatsConnection;

  constructor(nc: NatsConnection) {
    this.nc = nc;
  }

  /** Subscribe to heartbeats. Call before discover() so no beat is missed (§8.5). */
  async watch(filter = HB_WILDCARD): Promise<void> {
    if (this.hbSub) return;
    this.hbSub = this.nc.subscribe(filter, {
      callback: (err, m) => {
        if (err) return;
        try {
          const hb = JSON.parse(m.string()) as Heartbeat;
          this.lastSeen.set(hb.instance_id, { hb, at: Date.now(), name: parseSubject(m.subject)?.name ?? "" });
          for (const l of this.hbListeners) l(hb, m.subject);
        } catch {
          /* ignore malformed beats */
        }
      },
    });
    await this.nc.flush();
  }

  onHeartbeat(fn: (hb: Heartbeat, subject: string) => void): () => void {
    this.hbListeners.add(fn);
    return () => this.hbListeners.delete(fn);
  }

  /** Online = seen within 3 × interval_s (§8.2). Unknown if never seen. */
  liveness(instanceId: string): "online" | "offline" | "unknown" {
    const s = this.lastSeen.get(instanceId);
    if (!s) return "unknown";
    return Date.now() - s.at <= 3 * s.hb.interval_s * 1000 ? "online" : "offline";
  }

  heartbeatOf(instanceId: string): Heartbeat | undefined {
    return this.lastSeen.get(instanceId)?.hb;
  }

  /** Scatter-gather on $SRV.INFO.agents; every instance answers once. */
  async discover(opts: { waitMs?: number; filter?: (a: AgentRecord) => boolean } = {}): Promise<AgentRecord[]> {
    const raw = await this.gather(`$SRV.INFO.${SERVICE_NAME}`, opts.waitMs ?? 750);
    const out: AgentRecord[] = [];
    for (const m of raw) {
      try {
        const info = JSON.parse(m.string());
        const endpoints: Record<string, EndpointInfo> = {};
        for (const e of info.endpoints ?? []) endpoints[e.name] = e;
        const prompt = endpoints.prompt;
        if (!prompt) continue;
        const parsed = parseSubject(prompt.subject);
        const md: Record<string, string> = info.metadata ?? {};
        const rec: AgentRecord = {
          instanceId: info.id,
          agent: md.agent ?? "?",
          token: parsed?.agent ?? md.agent ?? "?",
          owner: md.owner ?? parsed?.owner ?? "?",
          name: parsed?.name ?? md.session ?? info.id,
          session: md.session,
          protocolVersion: md.protocol_version ?? "?",
          version: info.version,
          description: info.description ?? "",
          metadata: md,
          endpoints,
        };
        if (!opts.filter || opts.filter(rec)) out.push(rec);
      } catch {
        /* skip malformed records */
      }
    }
    return out.sort((a, b) => label(a).localeCompare(label(b)));
  }

  /** One-shot status (§8.7); also feeds the liveness tracker. */
  async status(a: AgentRecord, timeoutMs = 2000): Promise<Heartbeat> {
    const subj = a.endpoints.status?.subject;
    if (!subj) throw new Error(`${label(a)} has no status endpoint`);
    const m = await this.nc.request(subj, "", { timeout: timeoutMs });
    const hb = JSON.parse(m.string()) as Heartbeat;
    this.lastSeen.set(hb.instance_id, { hb, at: Date.now(), name: a.name });
    return hb;
  }

  /** Request/reply against an extension verb (spawn/stop/list/...). */
  async call<T = unknown>(a: AgentRecord, verb: string, body: unknown = {}, timeoutMs = 15_000): Promise<T> {
    const subj = a.endpoints[verb]?.subject;
    if (!subj) throw new Error(`${label(a)} has no "${verb}" endpoint`);
    const m = await this.nc.request(subj, JSON.stringify(body), { timeout: timeoutMs });
    const code = m.headers?.get(ERROR_CODE_HEADER);
    if (code) throw new Error(`${verb} failed (${code}): ${m.headers?.get(ERROR_DESC_HEADER)}`);
    return JSON.parse(m.string()) as T;
  }

  /**
   * Prompt one agent and stream its reply. Always publishes to the subject
   * learned from discovery; never builds one from identity (§4.3).
   */
  async *prompt(a: AgentRecord, text: string, opts: PromptOptions = {}): AsyncGenerator<StreamEvent> {
    const ep = a.endpoints.prompt;
    if (!isCompatible(a.protocolVersion)) {
      yield { type: "error", code: 400, message: `incompatible protocol_version ${a.protocolVersion}` };
      return;
    }

    // Client-side validation against endpoint metadata (§5.4).
    const hasAtt = (opts.attachments?.length ?? 0) > 0;
    if (hasAtt && ep.metadata?.attachments_ok !== "true") {
      yield { type: "error", code: 400, message: "agent does not accept attachments" };
      return;
    }
    const body =
      hasAtt || opts.extra ? JSON.stringify({ ...opts.extra, prompt: text, attachments: opts.attachments }) : text;
    const bytes = new TextEncoder().encode(body);
    if (ep.metadata?.max_payload && bytes.length > parseSize(ep.metadata.max_payload)) {
      yield { type: "error", code: 400, message: `payload ${bytes.length}B exceeds max_payload ${ep.metadata.max_payload}` };
      return;
    }

    yield* this.stream(ep.subject, bytes, opts.inactivityMs ?? 60_000);
  }

  /**
   * Fan-out: the same prompt to many agents concurrently, merged into one
   * stream. Each event is labeled with the agent it came from.
   */
  async *fanout(agents: AgentRecord[], text: string, opts: PromptOptions = {}): AsyncGenerator<LabeledEvent> {
    agents = uniqueBySubject(agents);
    const q = new AsyncQueue<LabeledEvent>();
    let open = agents.length;
    if (open === 0) return;
    for (const a of agents) {
      (async () => {
        try {
          for await (const ev of this.prompt(a, text, opts)) q.push({ ...ev, from: a });
        } catch (e) {
          q.push({ type: "error", code: 500, message: String(e), from: a });
        } finally {
          if (--open === 0) q.close();
        }
      })();
    }
    yield* q;
  }

  async close(): Promise<void> {
    this.hbSub?.unsubscribe();
    this.hbSub = null;
  }

  // -------------------------------------------------------------------------

  private async *stream(subj: string, payload: Uint8Array, inactivityMs: number): AsyncGenerator<StreamEvent> {
    const inbox = createInbox();
    const q = new AsyncQueue<Msg>();
    const sub = this.nc.subscribe(inbox, { callback: (err, m) => (err ? q.close() : q.push(m)) });
    this.nc.publish(subj, payload, { reply: inbox });

    try {
      while (true) {
        const m = await q.next(inactivityMs);
        if (m === TIMEOUT) {
          yield { type: "timeout" };
          return;
        }
        if (m === undefined) return;

        // Headers are checked before the body on every message (§12).
        const code = m.headers?.get(ERROR_CODE_HEADER);
        if (code) {
          let body: unknown;
          try {
            body = m.data.length ? JSON.parse(dec.decode(m.data)) : undefined;
          } catch {
            /* description header is the fallback */
          }
          const message = (body as { message?: string })?.message ?? m.headers?.get(ERROR_DESC_HEADER) ?? "error";
          yield { type: "error", code: Number(code), message, body };
          continue; // the terminator follows
        }

        if (m.data.length === 0 && !m.headers) {
          yield { type: "done" };
          return;
        }

        let chunk: Chunk;
        try {
          chunk = JSON.parse(dec.decode(m.data));
        } catch {
          continue;
        }
        switch (chunk.type) {
          case "status":
            yield chunk.data === "ack" ? { type: "ack" } : { type: "status", status: String(chunk.data) };
            break;
          case "response": {
            const d = chunk.data as { attachments?: Attachment[] } | string;
            yield { type: "text", text: chunkText(chunk) ?? "", attachments: typeof d === "object" ? d.attachments : undefined };
            break;
          }
          case "query": {
            const query = chunk.data as QueryData;
            let answered = false;
            yield {
              type: "query",
              query,
              answer: (reply) => {
                if (answered) return;
                answered = true;
                this.nc.publish(query.reply_subject, reply);
              },
            };
            break;
          }
          default:
            yield { type: "unknown", chunk }; // forward-compat: callers may ignore (§6.6)
        }
      }
    } finally {
      // Cancellation is just dropping interest in the inbox (§6.7).
      sub.unsubscribe();
    }
  }

  private async gather(subj: string, waitMs: number): Promise<Msg[]> {
    const inbox = createInbox();
    const out: Msg[] = [];
    const sub = this.nc.subscribe(inbox, { callback: (_e, m) => void out.push(m) });
    this.nc.publish(subj, "", { reply: inbox });
    await new Promise((r) => setTimeout(r, waitMs));
    sub.unsubscribe();
    return out;
  }
}

/**
 * Resolves a target expression to agents. Each comma/space-separated part
 * matches an agent's token ("cc"), name ("box1"), canonical id
 * ("claude-code"), or a label prefix ("cc/srini", "cc/srini/box1").
 * "all" / "*" selects every worker: agents whose metadata.role is
 * "controller" or "orchestrator" are only matched when named, which keeps
 * meta-agents from fanning out to each other in a loop. A leading "@" is ignored.
 */
export function matchTargets(agents: AgentRecord[], target: string | string[]): AgentRecord[] {
  const parts = (Array.isArray(target) ? target : target.split(/[,\s]+/))
    .map((t) => t.replace(/^@/, "").trim())
    .filter(Boolean);
  const workers = agents.filter((a) => a.metadata.role !== "controller" && a.metadata.role !== "orchestrator");
  if (parts.includes("all") || parts.includes("*")) return uniqueBySubject(workers);
  const hit = (a: AgentRecord, p: string) =>
    a.token === p || a.name === p || a.agent === p || `${label(a)}/`.startsWith(p.endsWith("/") ? p : `${p}/`);
  return uniqueBySubject(agents.filter((a) => parts.some((p) => hit(a, p))));
}

/**
 * Instances sharing a prompt subject are one logical agent: the "agents"
 * queue group load-balances between them (§3.4), so address each subject once.
 */
export function uniqueBySubject(agents: AgentRecord[]): AgentRecord[] {
  const seen = new Set<string>();
  return agents.filter((a) => {
    const s = a.endpoints.prompt.subject;
    if (seen.has(s)) return false;
    seen.add(s);
    return true;
  });
}

// ---------------------------------------------------------------------------

const TIMEOUT = Symbol("timeout");

/** Minimal push/pull queue bridging NATS callbacks to async iteration. */
class AsyncQueue<T> implements AsyncIterable<T> {
  private items: T[] = [];
  private waiters: ((v: T | undefined) => void)[] = [];
  private closed = false;

  push(v: T): void {
    const w = this.waiters.shift();
    if (w) w(v);
    else this.items.push(v);
  }

  close(): void {
    this.closed = true;
    for (const w of this.waiters.splice(0)) w(undefined);
  }

  next(timeoutMs?: number): Promise<T | undefined | typeof TIMEOUT> {
    if (this.items.length) return Promise.resolve(this.items.shift()!);
    if (this.closed) return Promise.resolve(undefined);
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const w = (v: T | undefined) => {
        if (timer) clearTimeout(timer);
        resolve(v);
      };
      this.waiters.push(w);
      if (timeoutMs) {
        timer = setTimeout(() => {
          this.waiters = this.waiters.filter((x) => x !== w);
          resolve(TIMEOUT);
        }, timeoutMs);
      }
    });
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    while (true) {
      const v = await this.next();
      if (v === undefined || v === TIMEOUT) return;
      yield v;
    }
  }
}
