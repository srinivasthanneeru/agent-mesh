// AgentHost: puts any piece of code on the NATS fabric as a protocol agent.
//
//   const host = new AgentHost({ nc, agent: "echo", owner: "me", name: "main", onPrompt })
//   await host.start()
//
// It registers the "agents" micro service, serves `prompt` (streaming) and
// `status` (request/reply), publishes heartbeats, and lets extension verbs
// (spawn/stop/list, ...) be added with addEndpoint().

import { headers, createInbox, type NatsConnection, type MsgHdrs } from "@nats-io/nats-core";
import { Svcm, type Service, type ServiceMsg } from "@nats-io/services";
import {
  ERROR_CODE_HEADER,
  ERROR_DESC_HEADER,
  PROTOCOL_VERSION,
  ProtocolError,
  QUEUE_GROUP,
  SERVICE_NAME,
  decodeEnvelope,
  encodeChunk,
  formatSize,
  isValidToken,
  subject,
  type Attachment,
  type Envelope,
  type Heartbeat,
} from "./protocol.ts";

export interface PromptContext {
  /** Headers on the incoming request (e.g. tracing, sender claims). */
  headers?: MsgHdrs;
  /** Subject the request arrived on. */
  subject: string;
  /** Aborted when the host stops; long-running handlers should honour it. */
  signal: AbortSignal;
}

export type PromptHandler = (env: Envelope, res: Responder, ctx: PromptContext) => Promise<void> | void;

export interface AgentHostOptions {
  nc: NatsConnection;
  agent: string; // 3rd subject token, e.g. "cc", "echo"
  /** Canonical harness id for metadata.agent (e.g. "claude-code"); defaults to `agent`. */
  agentId?: string;
  owner: string; // 4th subject token
  name: string; // 5th subject token (instance / session name)
  session?: string; // metadata.session for session-aware harnesses
  description?: string;
  version?: string;
  attachmentsOk?: boolean;
  /** Defaults to the server's negotiated max_payload. */
  maxPayload?: number;
  heartbeatIntervalS?: number;
  /** While a prompt runs, re-send a status=ack this often so callers' inactivity timers don't fire. */
  keepAliveS?: number;
  metadata?: Record<string, string>;
  onPrompt: PromptHandler;
  log?: (line: string) => void;
}

/** Writes one prompt's response stream. Created per request by AgentHost. */
export class Responder {
  private ended = false;
  private readonly enc = new TextEncoder();

  private readonly nc: NatsConnection;
  private readonly reply: string;

  constructor(nc: NatsConnection, reply: string) {
    this.nc = nc;
    this.reply = reply;
  }

  get done(): boolean {
    return this.ended;
  }

  /** Emits a `response` chunk. Callers concatenate these in order. */
  send(text: string, attachments?: Attachment[]): void {
    if (this.ended || text.length === 0 && !attachments?.length) return;
    this.nc.publish(
      this.reply,
      encodeChunk({ type: "response", data: attachments?.length ? { text, attachments } : text }),
    );
  }

  /** Emits a `status` chunk. Only "ack" is standard; callers ignore others. */
  status(token: string): void {
    if (this.ended) return;
    this.nc.publish(this.reply, encodeChunk({ type: "status", data: token }));
  }

  /** Emits a chunk of any type; callers that don't know the type skip it. */
  raw(type: string, data: unknown): void {
    if (this.ended) return;
    this.nc.publish(this.reply, encodeChunk({ type, data }));
  }

  /**
   * Mid-stream query (§7): pauses to ask the caller something and waits for
   * exactly one reply on a fresh inbox. Resolves null on timeout, which also
   * doubles as the only way an agent learns its caller went away (§6.7).
   */
  async ask(prompt: string, timeoutMs = 60_000): Promise<string | null> {
    if (this.ended) return null;
    const replySubject = createInbox();
    const sub = this.nc.subscribe(replySubject, { max: 1, timeout: timeoutMs });
    this.nc.publish(
      this.reply,
      encodeChunk({ type: "query", data: { id: crypto.randomUUID(), reply_subject: replySubject, prompt } }),
    );
    try {
      for await (const m of sub) {
        const body = m.string();
        // The reply follows the §5.1 envelope rules: plain text or {"prompt": ...}.
        if (body.trimStart().startsWith("{")) {
          try {
            return String(JSON.parse(body).prompt ?? "");
          } catch {
            return body;
          }
        }
        return body;
      }
    } catch {
      // subscription timeout
    }
    return null;
  }

  /** Error message followed by the terminator (§9.3). */
  error(code: number, message: string, body?: Record<string, unknown>): void {
    if (this.ended) return;
    const h = headers();
    h.set(ERROR_CODE_HEADER, String(code));
    h.set(ERROR_DESC_HEADER, message);
    this.nc.publish(this.reply, body ? this.enc.encode(JSON.stringify(body)) : new Uint8Array(0), { headers: h });
    this.end();
  }

  /** Zero-byte, header-less terminator (§6.5). Idempotent. */
  end(): void {
    if (this.ended) return;
    this.ended = true;
    this.nc.publish(this.reply, new Uint8Array(0));
  }
}

export class AgentHost {
  private service: Service | null = null;
  private hbTimer: ReturnType<typeof setInterval> | null = null;
  private readonly abort = new AbortController();
  private readonly log: (line: string) => void;
  private readonly inflight = new Set<Promise<void>>();
  readonly startedAt = Date.now();
  private served = 0;

  private readonly opts: AgentHostOptions;

  constructor(opts: AgentHostOptions) {
    this.opts = opts;
    for (const [k, v] of Object.entries({ agent: opts.agent, owner: opts.owner, name: opts.name })) {
      if (!isValidToken(v)) throw new Error(`${k} "${v}" is not a valid subject token ([a-z0-9_-], 1-63 chars)`);
    }
    this.log = opts.log ?? ((l) => process.stderr.write(`[${opts.agent}/${opts.name}] ${l}\n`));
  }

  get subjects() {
    const { agent, owner, name } = this.opts;
    return {
      prompt: subject("prompt", agent, owner, name),
      status: subject("status", agent, owner, name),
      hb: subject("hb", agent, owner, name),
    };
  }

  get instanceId(): string {
    if (!this.service) throw new Error("not started");
    return this.service.info().id;
  }

  get signal(): AbortSignal {
    return this.abort.signal;
  }

  async start(): Promise<void> {
    const { nc, agent, owner, session } = this.opts;
    const svcm = new Svcm(nc);
    this.service = await svcm.add({
      name: SERVICE_NAME,
      version: this.opts.version ?? "0.1.0",
      description: this.opts.description ?? `${agent} agent`,
      metadata: {
        ...this.opts.metadata,
        agent: this.opts.agentId ?? agent,
        owner,
        ...(session ? { session } : {}),
        protocol_version: PROTOCOL_VERSION,
      },
    });

    const maxPayload = this.opts.maxPayload ?? nc.info?.max_payload ?? 1024 * 1024;
    this.service.addEndpoint("prompt", {
      subject: this.subjects.prompt,
      queue: QUEUE_GROUP, // §3.3: explicit, never the framework default
      metadata: { max_payload: formatSize(maxPayload), attachments_ok: String(!!this.opts.attachmentsOk) },
      handler: (err, msg) => {
        if (err) return;
        const p = this.handlePrompt(msg, maxPayload).finally(() => this.inflight.delete(p));
        this.inflight.add(p);
      },
    });

    this.service.addEndpoint("status", {
      subject: this.subjects.status,
      queue: QUEUE_GROUP,
      handler: (err, msg) => {
        if (err) return;
        try {
          msg.respond(JSON.stringify(this.heartbeat()));
        } catch (e) {
          msg.respondError(500, String(e));
        }
      },
    });

    // Heartbeats only after registration so discovery finds our metadata (§8.2).
    const every = this.opts.heartbeatIntervalS ?? 30;
    const beat = () => nc.publish(this.subjects.hb, JSON.stringify(this.heartbeat()));
    beat();
    this.hbTimer = setInterval(beat, every * 1000);
    this.log(`listening on ${this.subjects.prompt} (instance ${this.instanceId})`);
  }

  /**
   * Adds an extension verb, e.g. addEndpoint("spawn", handler) serves
   * agents.spawn.{agent}.{owner}.{name}. Plain request/reply.
   */
  addEndpoint(verb: string, handler: (msg: ServiceMsg) => Promise<unknown> | unknown): string {
    if (!this.service) throw new Error("start() first");
    const subj = subject(verb, this.opts.agent, this.opts.owner, this.opts.name);
    this.service.addEndpoint(verb, {
      subject: subj,
      handler: async (err, msg) => {
        if (err) return;
        try {
          const out = await handler(msg);
          msg.respond(JSON.stringify(out ?? { ok: true }));
        } catch (e) {
          const pe = e instanceof ProtocolError ? e : new ProtocolError(500, String((e as Error)?.message ?? e));
          msg.respondError(pe.code, pe.message, pe.body ? JSON.stringify(pe.body) : undefined);
        }
      },
    });
    return subj;
  }

  heartbeat(): Heartbeat {
    return {
      agent: this.opts.agentId ?? this.opts.agent,
      owner: this.opts.owner,
      ...(this.opts.session ? { session: this.opts.session } : {}),
      instance_id: this.service?.info().id ?? "",
      ts: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
      interval_s: this.opts.heartbeatIntervalS ?? 30,
      // Extra fields are allowed (§8.3) and ignored by callers that don't know them.
      busy: this.inflight.size,
      served: this.served,
      uptime_s: Math.round((Date.now() - this.startedAt) / 1000),
    };
  }

  private async handlePrompt(msg: ServiceMsg, maxPayload: number): Promise<void> {
    if (!msg.reply) return; // nobody to stream to
    const res = new Responder(this.opts.nc, msg.reply);
    // Mandatory first chunk, before any latency-inducing work (§6.4).
    res.status("ack");
    this.served++;

    let keepAlive: ReturnType<typeof setInterval> | null = null;
    try {
      if (msg.data.length > maxPayload) throw new ProtocolError(400, "request exceeds max_payload");
      const env = decodeEnvelope(msg.data, { attachmentsOk: !!this.opts.attachmentsOk });
      if (this.opts.keepAliveS) keepAlive = setInterval(() => res.status("ack"), this.opts.keepAliveS * 1000);
      await this.opts.onPrompt(env, res, { headers: msg.headers, subject: msg.subject, signal: this.abort.signal });
      res.end();
    } catch (e) {
      const pe = e instanceof ProtocolError ? e : new ProtocolError(500, String((e as Error)?.message ?? e));
      if (pe.code === 500) this.log(`prompt failed: ${pe.message}`);
      res.error(pe.code, pe.message, pe.body);
    } finally {
      if (keepAlive) clearInterval(keepAlive);
    }
  }

  async stop(): Promise<void> {
    this.abort.abort();
    if (this.hbTimer) clearInterval(this.hbTimer);
    this.hbTimer = null;
    await Promise.allSettled([...this.inflight]);
    await this.service?.stop().catch(() => {});
    this.service = null;
  }
}
