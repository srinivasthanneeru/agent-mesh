# Session notes

Working log for picking this project up later in Claude Code or Codex.
Newest status first.

## Status (2026-10-13, later)

- **MCP server (done):** `go/meshmcp` +
  `go/cmd/meshmcp` let Claude Desktop (and later Codex) see and continue mesh
  sessions. Verified live by launching `bin/meshmcp` over stdio against the
  running mesh: listed and showed the saved llama conversation, continued it
  ("Your name is Srini."), and auto-declined the shell agent's approval
  question. Registered in Claude Desktop only (user's choice); Claude Code and
  Codex registration commands are in AGENTS.md.
- Why MCP rather than writing into each app's storage: the Ollama app's chat
  API needs an internal token and its SQLite schema is internal (v19) and
  changes often; Claude/Codex session files are undocumented too. MCP is the
  supported, shared interface, and NATS stays the single source of truth.
- Ollama app note: its settings include an airplane mode (keeps it offline).

## Status (2026-10-13)

- **Retained sessions (done):** new `go/kvstore` package (JetStream KV with an
  in-memory fallback). The Claude agent saves its Claude session id and
  resumes it after a restart, and every reply shows
  `cd <dir> && claude --resume <id>`; `/session` and `/new` commands. The
  Ollama agent saves each conversation and reloads it after a restart. Verified
  live: llama3.2 recalled the user's name after an agent restart; Claude
  resumed (fake CLI). Demo scripts now start nats-server with
  `-js -sd ~/.agent-mesh/nats`.
- The Mac had restarted, so the long-running nats-server (port 4222) was gone.
  Start it again with `scripts/demo.sh` (it now enables JetStream).
- The Ollama desktop app is now installed and is the local Ollama server
  (models stay local; the app connects to ollama.com only for optional account
  and cloud features).

## Status (2026-10-12)

- Go implementation (`go/`): builds; `go test -race ./...` passes (claude,
  mesh, ollama, protocol packages).
- TypeScript implementation (root): 17 tests passing as of 2026-09-23.
- Live-verified with a real local model: `bin/ollama -model llama3.2` streams
  token by token (~47 tok/s on the Mac mini) and remembers the conversation.
  `qwen3` (8B, a thinking model) also works but is slow (~19 tok/s, long
  reasoning before answering).
- Claude Code agent: logic verified with the fake CLI. The real `claude` CLI on
  this Mac was **logged out** (OAuth expired) at last check: run `claude`, then
  `/login`. Verify with `claude -p hi`.

## Open items (in priority order)

1. **Finish "thinking" display (half done).** `go/ollama/ollama.go` now
   forwards reasoning from thinking models as `thinking` chunks, and
   `bin/ollama` has `-think true|false`. Still to do:
   - `go/cmd/mesh/main.go` `describeChunk`/`render`: handle `thinking`: stream
     it dimmed for a single agent; in fan-out show one "thinking…" note per
     agent instead of one line per token (today each token prints a line
     reading "thinking", which is noisy).
   - Same in the TS console `src/cli.ts` (it already ignores unknown types, but
     would print nothing useful).
   - Add tests: the fake (`ollamatest`) already emits thinking tokens for
     models whose name contains "qwen3" unless `think:false` is sent. Assert
     they arrive as Raw("thinking"), are not stored in memory, and that
     `Think: "false"` sends `"think": false`.
2. TypeScript has no Ollama adapter (Go only). Port `go/ollama` to
   `src/runners/ollama-runner.ts` + `src/agents/ollama.ts` if the TS side is kept.
3. Relay Claude's permission prompts as §7 mid-stream queries (needs
   `--permission-prompt-tool` via MCP); today permission mode is fixed at spawn.
4. Ollama `/model <name>` command per conversation (exercise 5 in go/README.md).
4b. Persist `cc-pool` controller sessions too (today only `cmd/claude` saves its
   session id); port retained sessions to the TypeScript side if it is kept.
5. Not implemented (spec defers most): JetStream at-least-once delivery, the
   `attachments` upload endpoint, §13 signed sender identity, E2E encryption,
   a browser UI.

## How we got here

1. Explained TypeSafe's "System One" / Jev models (typed, calibrated, fast
   decision models) and where they fit.
2. Studied Synadia's video "Run Claude Code, OpenClaw & Hermes from One
   Meta-Agent" (TpybQ73E6jQ), its repo (synadia-ai/synadia-agents) and the
   protocol spec, then built a from-scratch TypeScript implementation.
3. Verified wire compatibility both ways with Synadia's official SDKs
   (`@synadia-ai/agents`, `@synadia-ai/agent-service`).
4. Debugged the Claude agent: expired CLI login → silent retries (added a
   first-output watchdog); stale `--resume` id → empty replies (only keep the
   session id after success, auto-start fresh on resume failure, never
   swallow errors); duplicate fleets splitting prompts (demo.sh now replaces
   the fleet).
5. Ported everything to idiomatic Go for learning (iterators, goroutines,
   context, embedded nats-server tests, build tags for process-group kill).
6. Built the Ollama adapter (memory + token streaming + fake server), tested
   live with llama3.2 and qwen3; discovered thinking models stream reasoning
   in a separate field → open item 1.

## Decisions

- Git repo at the project root (`~/code/agent-mesh`); `~/code` itself is not a
  repo and holds unrelated projects. Remote: `origin` =
  git@github.com:srinivasthanneeru/agent-mesh.git (public), branch `main`. Git has no global
  user.name/email on this Mac, so set one (`git config --global ...`) or pass
  `-c user.name=... -c user.email=...` when committing.

- Go is the primary implementation going forward; TS kept for reference.
- Sessions are retained in NATS JetStream KV (bucket `agent_mesh`), not files:
  NATS is already the shared fabric, so any agent on any machine can reach
  them, and no new software is needed. Only state from successful turns is saved.
- One conversation answers one prompt at a time (409 if busy); different
  conversations run in parallel.
- Agent names: Claude `cc/<owner>/<dir>`, Ollama `ollama/<owner>/<model>`,
  session pool `cc-pool/<owner>/<session>`; meta-agent `meta/<owner>/main`.
