# agent-mesh

A from-scratch implementation of the **agent-over-NATS** pattern from Synadia's
"Run Claude Code, OpenClaw & Hermes from One Meta-Agent" walkthrough. Any
process (Claude Code, a shell, any CLI, your own code) becomes an agent on a
shared NATS network, and other agents can discover it, prompt it and
stream its replies. No gateways, no DNS, no per-vendor SDKs.

It speaks the **Synadia Agent Protocol for NATS v0.3** on the wire, and was
tested both ways against Synadia's official SDKs (`@synadia-ai/agents`,
`@synadia-ai/agent-service`). Their plugins for Claude Code, OpenClaw, Hermes
and the others can therefore join the same network as these agents.

```
                    ┌──────────── NATS (one server, or a cluster/leafnodes/Synadia Cloud) ────────────┐
 mesh CLI  ─┐       │  $SRV.INFO.agents          discovery (standard NATS micro services)             │
 meta-agent ├─ req ─┤  agents.prompt.{agent}.{owner}.{name}   streamed reply → caller's _INBOX        │
 nats CLI  ─┘       │  agents.status.…  /  agents.hb.…         liveness                               │
                    │  agents.spawn|stop|list.cc-pool.{owner}.control   extension verbs              │
                    └───────┬────────────┬─────────────┬──────────────┬─────────────┬────────────────┘
                          echo        shell         cc (Claude)    cc-pool/*      any CLI / official-SDK agents
```

## The protocol

| Concept | How it works on NATS |
|---|---|
| **Identity** | Subject `agents.{verb}.{agent}.{owner}.{name}`. Verb first, so `agents.prompt.>` and `agents.hb.>` never overlap. |
| **Registration and discovery** | Every agent is a NATS micro service named `agents`, so `$SRV.INFO.agents` lists all of them with their metadata. `nats micro list` works without extra tooling. |
| **Prompt** | Plain text, or JSON `{"prompt", "attachments":[{filename, base64}]}`. Unknown fields are kept. |
| **Streaming reply** | Typed chunks sent to the caller's inbox: `status:ack` always comes first, then `response` chunks, then an **empty, header-less message** that marks the end. |
| **Errors** | `Nats-Service-Error-Code` header (400/401/403/404/409/429/500) plus an optional JSON body, then the end marker. |
| **Mid-stream questions** | The agent sends `{"type":"query","data":{reply_subject,prompt}}` and pauses. The caller publishes one reply to that subject. Used for permission prompts. |
| **Liveness** | A heartbeat on `agents.hb.…` every N seconds. An agent counts as offline after 3 missed intervals. `status` gives the same payload on request. |
| **Scale-out** | Instances with the same identity share the `agents` queue group, so NATS load-balances between them. |
| **Cancellation** | The caller unsubscribes from its inbox, and the server drops later chunks at no cost. |
| **Security** | Handled by NATS accounts and subject permissions. The protocol adds no auth of its own. |

## What's here

| File | Role |
|---|---|
| [src/protocol.ts](src/protocol.ts) | Wire format: subjects, request parsing (§5.3), chunks, sizes, errors |
| [src/host.ts](src/host.ts) | `AgentHost`: registration, `prompt`/`status`, heartbeats, keep-alive acks, `Responder.send/ask/error/end`, extension verbs |
| [src/caller.ts](src/caller.ts) | `Mesh`: discovery, liveness, `prompt()` stream, `fanout()`, inactivity timeout, client-side validation, `matchTargets` |
| [src/agents/echo.ts](src/agents/echo.ts) | Minimal agent (accepts attachments) |
| [src/agents/shell.ts](src/agents/shell.ts) | Runs commands and **asks the caller to approve each one** through a mid-stream query |
| [src/agents/claude.ts](src/agents/claude.ts) | Headless Claude Code with conversation memory (`--resume`), token streaming, tool and cost events |
| [src/agents/cli.ts](src/agents/cli.ts) | Wraps **any** CLI (`ollama run …`, `codex exec`, `gemini -p` …) as an agent |
| [src/controller.ts](src/controller.ts) | `cc-pool` control plane: `spawn`/`stop`/`list` headless Claude sessions, each registered as its own agent, with optional lifetime |
| [src/agents/meta.ts](src/agents/meta.ts) | **Meta-agent**: routes by `@mention` or by asking Claude which agents to use, runs them in parallel, passes their questions up to its caller, optionally merges their answers (`--synthesize`) |
| [src/cli.ts](src/cli.ts) | `mesh` console: `ls`, `watch`, `ask`, `spawn`, `sessions`, `stop`, `chat` |

## Run it

```bash
brew install nats-server nats-io/nats-tools/nats   # if missing
npm install
scripts/demo.sh                    # local NATS + echo, shell, claude, controller, meta
FAKE_CLAUDE=1 scripts/demo.sh      # same, with an offline Claude stub (no API use)
```

```bash
npm run mesh -- ls
npm run mesh -- ask shell "git log --oneline -5"          # you approve before it runs
npm run mesh -- ask cc "what does this repo do?"
npm run mesh -- spawn ~/code/api --name api --lifetime 1800
npm run mesh -- ask cc,api "list the TODOs"                # fan-out, one merged stream
npm run mesh -- ask meta "have claude review the shell history"   # LLM routing
npm run mesh -- chat                                       # @target msg, @all msg, /use cc
nats req agents.prompt.echo.$USER.main "hi" --wait-for-empty --reply-timeout=30s   # no SDK
```

Wrap other tools: `npm run cli -- --agent llama --cmd "ollama run llama3.2"`.

Across machines: point every process at the same NATS (`NATS_URL`, and
`NATS_CREDS` for Synadia Cloud / operator mode). Use leafnodes to connect edge
devices to the cloud, and accounts to keep tenants separate.

## Verified

- `npm test`: 17 tests (unit + end-to-end against a real `nats-server`). They cover ack-first ordering, the end marker, error then end marker, the query round trip, keep-alive vs. inactivity timeout, local validation, queue-group load-balancing, fan-out, target matching, heartbeat/status agreement, and removal from discovery.
- Interop: the official `@synadia-ai/agents` client discovered and prompted these agents, including answering the shell agent's approval question. `mesh` and the meta-agent prompted an agent built with the official `@synadia-ai/agent-service`.
- The Claude Code adapter was exercised through a stub that emits the CLI's `stream-json` events (`CLAUDE_BIN`). The real `claude` CLI couldn't be run from inside the sandbox this was built in, so try `npm run mesh -- ask cc …` once on your machine.

## Not included (the spec also defers most of these)

JetStream-backed at-least-once delivery · the `attachments` upload endpoint ·
§13 signed sender identity (`Agent-Sender` header) · end-to-end encryption ·
a browser UI (NATS works over WebSockets, so the `Mesh` class could drive one) ·
Claude permission prompts relayed as mid-stream queries (would need Claude's
`--permission-prompt-tool` via MCP). Today the permission mode is chosen at
spawn time.
