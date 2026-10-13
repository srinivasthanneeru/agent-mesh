# agent-mesh: notes for AI coding agents

Read this first. It's the shared context for Codex (which reads `AGENTS.md`)
and Claude Code (whose `CLAUDE.md` imports this file). History, decisions
and open items are in [docs/SESSION_NOTES.md](docs/SESSION_NOTES.md).

## What this is

A mesh of AI agents on NATS. Every agent (Claude Code, Ollama models, a
shell runner, any CLI, a meta-agent) registers as a NATS micro service named
`agents` and speaks the **Synadia Agent Protocol for NATS v0.3**, so callers
can discover it, prompt it, and stream its answer token by token. Spec:
https://github.com/synadia-ai/synadia-agent-sdk-docs (`core-protocol.md`).

The owner (Srini) is **learning Go and AI**: explain Go idioms when you
touch Go code, and prefer clear, idiomatic code over clever code.

## Layout

```
agent-mesh/
├── go/            ← primary implementation (Go 1.27, module "agentmesh")
│   ├── protocol/  wire rules: subjects, envelope, chunks, errors (no networking)
│   ├── agent/     Host + Responder: turn a Go func into an agent (answering side)
│   ├── mesh/      Mesh: Discover, Prompt, Fanout (iter.Seq), MatchTargets (asking side)
│   ├── claude/    drives the `claude` CLI (stream-json) → chunks; session memory via --resume
│   ├── kvstore/   Store interface: JetStream KV (durable) or in-memory; retains sessions
│   ├── meshmcp/   MCP server: mesh_agents / mesh_ask / mesh_sessions / mesh_session
│   ├── ollama/    Ollama /api/chat adapter: token streaming + conversation memory
│   │   └── ollamatest/  fake Ollama server for tests and offline demos
│   ├── cmd/       programs: echo shell claude ollama cliagent controller meta mesh fakeollama
│   ├── examples/race/   race-detector exercise (deliberately broken)
│   └── scripts/demo.sh  build + start a fleet (FAKE_CLAUDE=1, FAKE_OLLAMA=1)
├── src/ test/     TypeScript implementation (Node 23, --experimental-strip-types); same protocol
├── scripts/demo.sh  TypeScript fleet launcher
└── go-agent/      the original ~100-line minimal Go agent (teaching example)
```

Go and TypeScript interoperate on the wire. Both fleets use the same agent
names, so **run only one fleet at a time** (identical names share a queue
group and NATS would split prompts between them).

## Commands

```bash
# Go (from go/)
go vet ./... && go test -race ./...        # 44+ tests; embeds nats-server and a fake Ollama
go build -o bin/ ./cmd/...
FAKE_CLAUDE=1 FAKE_OLLAMA=1 scripts/demo.sh   # offline fleet; `scripts/demo.sh stop`
bin/mesh ls | bin/mesh chat | bin/mesh ask <target> "<prompt>" [-answer yes]
bin/ollama -model llama3.2                  # real Ollama must be running (ollama serve)

# TypeScript (from repo root)
npm test && npx tsc -p .
FAKE_CLAUDE=1 scripts/demo.sh ; npm run mesh -- chat

# Raw protocol, no SDK
nats micro list
nats req agents.prompt.echo.$USER.main "hi" --wait-for-empty --reply-timeout=30s
```

Env: `NATS_URL` (default nats://127.0.0.1:4222), `NATS_CREDS`, `MESH_OWNER`,
`CLAUDE_BIN` (point at `go/claude/testdata/fake-claude.sh` or
`test/fixtures/fake-claude.mjs` for offline runs), `CLAUDE_FIRST_OUTPUT_S`,
`OLLAMA_HOST`.

## Retained sessions

Agents save sessions in the JetStream KV bucket `agent_mesh`, so they survive
restarts. **nats-server must run with JetStream** (`-js`); both demo scripts
start it with `-js -sd ~/.agent-mesh/nats` (permanent storage). Without it,
agents log a warning and fall back to memory.

| Key | Value | Written by |
|---|---|---|
| `claude.<owner>.<name>` | Claude Code session id | `cmd/claude` after each successful turn; deleted by `/new` |
| `ollama.<owner>.<name>.<conversation>` | JSON message history | `cmd/ollama` after each successful turn; deleted by `/reset` |

Inspect with `nats kv ls agent_mesh` / `nats kv get agent_mesh <key>`. The
Claude agent's replies end with `cd <dir> && claude --resume <id>`, which
continues the same conversation in Claude Code (sessions are per directory).
Agent commands: Claude `/session`, `/new`; Ollama `/history`, `/reset`.
Not persisted yet: `cc-pool` controller sessions, per-session cost totals.

## MCP server (seeing sessions from other apps)

`go/cmd/meshmcp` exposes the mesh over MCP (stdio), so Claude Desktop, Claude
Code, Codex or any MCP client can list, read and continue the same sessions:

- `mesh_agents`: who is online. `mesh_ask {target, prompt, conversation?}`:
  prompt agents, continuing their saved conversation.
- `mesh_sessions {agent?}` / `mesh_session {session}`: saved Ollama
  conversations (from KV) and Claude Code sessions (id from KV, transcript
  read-only from `~/.claude/projects/*/<id>.jsonl`, with the resume command).
- Safety: agent questions (§7 queries) are always answered "no" via MCP, since
  the caller is another AI; approvals happen in `bin/mesh chat`.

Registered in Claude Desktop (`~/Library/Application Support/Claude/
claude_desktop_config.json`, `mcpServers.agent-mesh`, a backup was made).
Not registered in Claude Code or Codex yet; to add them:
`claude mcp add --scope user agent-mesh -- ~/code/agent-mesh/go/bin/meshmcp`
and `[mcp_servers.agent-mesh] command = "…/go/bin/meshmcp"` in
`~/.codex/config.toml`. Rebuild with `go build -o bin/ ./cmd/...` after changes.

## Protocol rules that matter when changing code

- Subjects are verb-first: `agents.{prompt|hb|status|<ext>}.{agent}.{owner}.{name}`.
- `prompt` and `status` endpoints use queue group `"agents"` explicitly.
- A reply is: `{"type":"status","data":"ack"}` **first**, then `response` /
  `query` / custom chunks, then a **zero-byte, header-less terminator**.
  Errors = `Nats-Service-Error-Code` header message, then the terminator.
- Callers must ignore unknown chunk types; we add `tool`, `usage`, `plan`,
  `status_note`, `thinking`. Envelope unknown fields are preserved (the
  Ollama agent uses `"conversation"` this way).
- Callers address the subject from `$SRV.INFO.agents`, never a constructed one,
  and send once per prompt subject (`UniqueBySubject`).
- `metadata.role` = `controller` or `orchestrator` is excluded from `all`
  (prevents meta-agents looping).

## Conventions and hard-won lessons

- Only record state after the thing it describes succeeded (Claude session
  ids, Ollama memory). A stale `--resume` id once made every reply empty.
- Never fail silently: surface the CLI's error text, subtype or stderr.
- Child CLIs get a first-output watchdog and (Go, Unix) a process-group kill.
- Strip `CLAUDECODE` / `CLAUDE_CODE_*` env vars before spawning `claude`.
- Go: `gofmt`, `go vet`, `go test -race` must pass. Guard shared state with a
  mutex or atomics; pass channels into goroutines instead of reading fields.
- Keep comments explanatory for a Go learner, matching the existing density.
