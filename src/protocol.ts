// Wire contract shared by agent hosts and callers.
//
// Mirrors the Synadia Agent Protocol for NATS v0.3 (verb-first subjects):
//
//   agents.{verb}.{agent}.{owner}.{name}      verb ∈ prompt | hb | status | <extension>
//
//   - Every agent registers as a NATS micro service named "agents".
//   - `prompt` streams typed JSON chunks to the caller's reply inbox:
//       {"type":"status","data":"ack"}      mandatory first chunk
//       {"type":"response","data":"..."}    content (string or {text, attachments})
//       {"type":"query","data":{...}}       mid-stream question for the caller
//     and ends with a zero-byte, header-less terminator.
//   - Errors are a message with Nats-Service-Error-Code / Nats-Service-Error
//     headers, followed by the terminator.
//   - Heartbeats are pub/sub on agents.hb.{agent}.{owner}.{name}.

export const SERVICE_NAME = "agents";
export const QUEUE_GROUP = "agents";
export const PROTOCOL_VERSION = "0.3";
export const HB_WILDCARD = "agents.hb.*.*.*";

export const ERROR_CODE_HEADER = "Nats-Service-Error-Code";
export const ERROR_DESC_HEADER = "Nats-Service-Error";

export type Verb = "prompt" | "hb" | "status" | (string & {});

export function subject(verb: Verb, agent: string, owner: string, name: string): string {
  return `agents.${verb}.${agent}.${owner}.${name}`;
}

/** Splits a verb-first subject back into its identity tokens. */
export function parseSubject(s: string): { verb: string; agent: string; owner: string; name: string } | null {
  const t = s.split(".");
  if (t.length !== 5 || t[0] !== "agents") return null;
  return { verb: t[1], agent: t[2], owner: t[3], name: t[4] };
}

const TOKEN_RE = /^[a-z0-9_-]{1,63}$/;

export function isValidToken(tok: string): boolean {
  return TOKEN_RE.test(tok);
}

/** Lowercases and replaces anything outside [a-z0-9_-] so user input is subject-safe. */
export function sanitizeToken(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63);
}

// ---------------------------------------------------------------------------
// Request envelope (§5)

export interface Attachment {
  filename: string;
  content: string; // standard, padded base64
  [k: string]: unknown;
}

export interface Envelope {
  prompt: string;
  attachments?: Attachment[];
  [k: string]: unknown; // unknown fields are tolerated and preserved (§5.6)
}

export class ProtocolError extends Error {
  readonly code: 400 | 401 | 403 | 404 | 409 | 429 | 500;
  readonly body?: Record<string, unknown>;

  constructor(code: ProtocolError["code"], message: string, body?: Record<string, unknown>) {
    super(message);
    this.code = code;
    this.body = body;
  }
}

const B64_RE = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/**
 * Discrimination rule (§5.3): skip leading whitespace; `{` means JSON envelope,
 * anything else is plain-text shorthand for {"prompt": payload}.
 */
export function decodeEnvelope(payload: Uint8Array, opts: { attachmentsOk: boolean }): Envelope {
  if (payload.length === 0) throw new ProtocolError(400, "empty payload");
  const text = new TextDecoder().decode(payload);
  const trimmed = text.replace(/^[\t\n\r ]+/, "");

  let env: Envelope;
  if (trimmed.startsWith("{")) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new ProtocolError(400, "invalid JSON envelope");
    }
    if (typeof parsed !== "object" || parsed === null || typeof (parsed as Envelope).prompt !== "string") {
      throw new ProtocolError(400, "prompt missing or empty");
    }
    env = parsed as Envelope;
  } else {
    env = { prompt: text };
  }

  if (env.prompt.trim().length === 0) throw new ProtocolError(400, "prompt missing or empty");

  const atts = env.attachments;
  if (atts !== undefined) {
    if (!Array.isArray(atts)) throw new ProtocolError(400, "attachments must be an array");
    if (atts.length > 0 && !opts.attachmentsOk) throw new ProtocolError(400, "attachments not accepted by this agent");
    atts.forEach((a, i) => {
      if (typeof a?.filename !== "string" || typeof a?.content !== "string") {
        throw new ProtocolError(400, `attachment[${i}] needs filename and content`);
      }
      if (a.filename.includes("/") || a.filename.includes("\\") || a.filename === ".." || a.filename === "") {
        throw new ProtocolError(400, `attachment[${i}] has unsafe filename`);
      }
      if (!B64_RE.test(a.content)) throw new ProtocolError(400, `attachment[${i}] has invalid base64 content`);
    });
  }
  return env;
}

// ---------------------------------------------------------------------------
// Response chunks (§6)

export type Chunk =
  | { type: "status"; data: string }
  | { type: "response"; data: string | { text: string; attachments?: Attachment[]; [k: string]: unknown } }
  | { type: "query"; data: QueryData }
  | { type: string; data: unknown }; // unknown types are ignored by callers (§6.6)

export interface QueryData {
  id: string;
  reply_subject: string;
  prompt: string;
  attachments?: Attachment[];
}

const enc = new TextEncoder();

export function encodeChunk(chunk: Chunk): Uint8Array {
  return enc.encode(JSON.stringify(chunk));
}

export function chunkText(chunk: Chunk): string | null {
  if (chunk.type !== "response") return null;
  const d = chunk.data as Extract<Chunk, { type: "response" }>["data"];
  return typeof d === "string" ? d : (d?.text ?? "");
}

// ---------------------------------------------------------------------------
// Heartbeat / status payload (§8.3)

export interface Heartbeat {
  agent: string;
  owner: string;
  session?: string;
  instance_id: string;
  ts: string;
  interval_s: number;
  [k: string]: unknown;
}

// ---------------------------------------------------------------------------
// Sizes

/** Parses "512KB" / "1MB" style strings from endpoint metadata (§2.1). */
export function parseSize(s: string): number {
  const m = /^(\d+)(B|KB|MB|GB)$/.exec(s.trim());
  if (!m) throw new Error(`bad size: ${s}`);
  const mult = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3 }[m[2] as "B" | "KB" | "MB" | "GB"];
  return Number(m[1]) * mult;
}

export function formatSize(n: number): string {
  for (const [unit, mult] of [["GB", 1024 ** 3], ["MB", 1024 ** 2], ["KB", 1024]] as const) {
    if (n % mult === 0) return `${n / mult}${unit}`;
  }
  return `${n}B`;
}
