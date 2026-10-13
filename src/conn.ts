// Connection + identity defaults shared by every process.
//   NATS_URL   (default nats://127.0.0.1:4222)
//   NATS_CREDS optional .creds file (operator mode / Synadia Cloud)
//   MESH_OWNER owner token (default: sanitized $USER)

import { connect } from "@nats-io/transport-node";
import { credsAuthenticator, type NatsConnection } from "@nats-io/nats-core";
import { readFileSync } from "node:fs";
import { sanitizeToken } from "./protocol.ts";

export async function connectMesh(name: string): Promise<NatsConnection> {
  const servers = process.env.NATS_URL ?? "nats://127.0.0.1:4222";
  const creds = process.env.NATS_CREDS;
  return connect({
    servers,
    name,
    ...(creds ? { authenticator: credsAuthenticator(readFileSync(creds)) } : {}),
    maxReconnectAttempts: -1,
  });
}

export function defaultOwner(): string {
  return sanitizeToken(process.env.MESH_OWNER ?? process.env.USER ?? "anon") || "anon";
}

/** Tiny --flag value parser: returns [flags, positionals]. */
export function parseArgs(argv: string[]): [Record<string, string | true>, string[]] {
  const flags: Record<string, string | true> = {};
  const pos: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--") {
      pos.push(...argv.slice(i + 1));
      break;
    }
    if (a.startsWith("--")) {
      const [k, v] = a.slice(2).split("=", 2);
      if (v !== undefined) flags[k] = v;
      else if (argv[i + 1] !== undefined && !argv[i + 1].startsWith("--")) flags[k] = argv[++i];
      else flags[k] = true;
    } else pos.push(a);
  }
  return [flags, pos];
}

/** Stop cleanly on Ctrl-C / SIGTERM. */
export function onShutdown(fn: () => Promise<void>): void {
  let stopping = false;
  const h = async () => {
    if (stopping) process.exit(1);
    stopping = true;
    await fn().catch(() => {});
    process.exit(0);
  };
  process.on("SIGINT", h);
  process.on("SIGTERM", h);
}
