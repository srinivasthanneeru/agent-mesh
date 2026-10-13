# agent-mesh (Go)

A Go port of agent-mesh: AI agents as discoverable, promptable, streaming
services on NATS, speaking the Synadia Agent Protocol for NATS v0.3. It uses
the same protocol as the TypeScript version in `../src`, and the two
interoperate: Go agents appear in the TypeScript console and vice versa.

```bash
go test -race ./...                 # 44 tests, with an embedded NATS server and a fake Ollama
FAKE_CLAUDE=1 scripts/demo.sh       # build + start a demo fleet (offline Claude stub)
bin/mesh chat
scripts/demo.sh stop
```

Run **one fleet at a time** (Go or TypeScript). Both use the same agent names
(`echo/<you>/main`, `meta/<you>/main`, ...), and agents with the same name
share a queue group, so NATS would split your prompts between the two fleets.
Stop the other one first: `scripts/demo.sh stop` in `../` or here.

## Layout

```
go/
├── protocol/        wire rules: subjects, envelope, chunks, errors, sizes  (no networking)
├── agent/           Host + Responder: turn any Go func into an agent        (answering side)
├── mesh/            Mesh: discover, liveness, Prompt, Fanout, MatchTargets  (asking side)
├── claude/          drive the Claude Code CLI, translate stream-json → chunks
├── kvstore/         Store interface: JetStream KV or memory; keeps sessions across restarts
├── ollama/          Ollama adapter: token streaming + conversation memory over its HTTP API
│   └── ollamatest/  a fake Ollama server (like net/http/httptest) for tests and demos
├── internal/env/    connect to NATS, owner name, Ctrl-C handling
├── examples/race/   a deliberately broken program for the race-detector exercise
└── cmd/             the programs (each directory = one `package main`)
    ├── echo/        smallest agent
    ├── shell/       runs commands, asks before each one (mid-stream query)
    ├── claude/      Claude Code agent with conversation memory
    ├── ollama/      a local Ollama model with memory: -model llama3.2
    ├── fakeollama/  stand-in Ollama server for offline demos
    ├── cliagent/    wrap any CLI: -cmd "gemini -p"
    ├── controller/  spawn/stop/list headless Claude sessions
    ├── meta/        meta-agent: routes to other agents, relays their questions
    └── mesh/        the console: ls, watch, ask, spawn, sessions, stop, chat
```

Go dependencies only point one way (`cmd → mesh/agent/claude → protocol`),
and the compiler refuses import cycles.

## Suggested reading order

1. **`protocol/protocol.go`**: structs, struct tags (`json:"..."`), methods,
   multiple return values, `(value, ok)`, custom error types, `errors.As`.
2. **`protocol/protocol_test.go`**: table-driven tests and `t.Run` subtests.
3. **`cmd/echo/main.go`**: a whole program: `flag`, `defer`, closures, pointers from `flag.String`.
4. **`agent/agent.go`**: goroutines, `sync.Mutex`, `sync.WaitGroup`,
   `sync/atomic`, `context` cancellation, `select` over channels, tickers.
5. **`mesh/mesh.go`**: iterators (`iter.Seq`, `yield`), and the fan-in
   pattern (many goroutines → one channel) in `Fanout`.
6. **`claude/runner.go`**: `os/exec`, pipes, `bufio.Scanner`, interfaces
   satisfied implicitly (`Sink`), `time.AfterFunc`. See also
   `proc_unix.go` / `proc_other.go` for build tags.
7. **`mesh/mesh_test.go`**: embedding `nats-server` in a test, `t.Cleanup`, `t.Helper`.
8. **`cmd/mesh/main.go`**: a CLI with subcommands, and a stdin reader
   goroutine that shares input through a channel.

## TypeScript → Go, side by side

| Idea | TypeScript (`../src`) | Go (here) |
|---|---|---|
| Stream of events | `async *prompt()` + `for await` | `iter.Seq[Event]` + `for ev := range` |
| Run things at once | `Promise.all`, async callbacks | goroutines (`go f()`) + `sync.WaitGroup` |
| Merge streams | custom `AsyncQueue` | a channel fed by several goroutines |
| Cancel work | `AbortController` / `AbortSignal` | `context.Context` / `cancel()` |
| Errors | `throw` / `try…catch` | returned `error` values, `errors.As` / `errors.Is` |
| Error with a code | `class ProtocolError extends Error` | `type Error struct` with an `Error()` method |
| "Implements" | `implements` or structural types | implicit: having the methods is enough |
| Shared state | single-threaded, no locks needed | `sync.Mutex` / `atomic`, checked by `-race` |
| JSON field names | object keys | struct tags `json:"reply_subject"` |
| Unknown JSON | `any` | `json.RawMessage`, `map[string]any` |
| Timers | `setTimeout` / `setInterval` | `time.AfterFunc` / `time.Ticker` |
| Child process | `child_process.spawn` | `exec.CommandContext` + `StdoutPipe` |
| NATS micro service | `@nats-io/services` `Svcm` | `github.com/nats-io/nats.go/micro` |
| Tests | `node:test` + external server | `testing` + embedded `nats-server` |

## The Ollama agent

`cmd/ollama` + `ollama/` put a local model on the mesh. Ollama's `/api/chat`
is stateless, so memory is the agent keeping the message list and sending
it with every request. Tokens stream to the caller as they're generated.

```bash
brew install ollama && ollama serve     # in its own terminal (or open the Ollama app)
ollama pull llama3.2                    # about 2 GB
go run ./cmd/ollama -model llama3.2     # registers as agents.prompt.ollama.<you>.llama3-2
```

| You send | It does |
|---|---|
| `@ollama what is NATS?` | answers, streaming token by token, remembering earlier turns |
| `@ollama /history` | shows what it remembers |
| `@ollama /reset` | forgets the conversation |
| JSON `{"prompt": "...", "conversation": "alice"}` | keeps a separate memory per conversation id |

Flags: `-system "..."` (system prompt), `-history 40` (messages kept),
`-keep-alive 10m` (how long Ollama keeps the model in memory), `-url`
(default `$OLLAMA_HOST` or `http://127.0.0.1:11434`). Run several with
different `-model` values to put several local models on the mesh.

Details worth reading in `ollama/ollama.go`: a failed or cancelled reply is
never added to memory; the same conversation answers one prompt at a time
(409 otherwise) while different conversations run in parallel; a missing
model returns 404 with the `ollama pull` command; and a first-token watchdog
(5 minutes, since first loads are slow) stops a hung request.

No Ollama yet? `FAKE_OLLAMA=1 scripts/demo.sh` uses `cmd/fakeollama`, which
speaks the same API and replies deterministically.

## Retained sessions

Agents save their sessions in NATS's own key-value store (JetStream KV,
bucket `agent_mesh`), so a restart doesn't lose them. Start nats-server with
`-js` (the demo script does, storing data in `~/.agent-mesh/nats`).

- **Claude agent:** keeps its Claude session id. After a restart it continues
  the same conversation. Every reply ends with
  `continue in Claude Code: cd <dir> && claude --resume <id>`. Send `/session`
  to see it, `/new` to start over (the old conversation stays in Claude Code).
- **Ollama agent:** keeps each conversation's messages. `/reset` deletes them.

```bash
nats kv ls agent_mesh                 # what's saved
nats kv get agent_mesh claude.<you>.<dir>
```

Go ideas in `kvstore/`: an interface with two implementations (`jsStore`,
`memStore`), chosen in `main()` (dependency injection), and one test table run
against both (`TestStores`).

## What's different from the TypeScript version

- **Flags use Go's style.** Write `-name x` (one dash). The console accepts
  flags before or after the other arguments (`bin/mesh ask shell "ls" -answer yes`).
- **Claude is killed as a process group** on Unix (`claude/proc_unix.go`),
  so a hung CLI can't hold the output pipe open through a child process.
- **Everything else matches**, including the fixes made while testing the
  TypeScript version: the stall watchdog, keeping the conversation id only
  after a successful turn, the automatic fresh start when resuming fails,
  errors never swallowed, and orchestrators excluded from `all`.

## Exercises

1. Add a `reverse` agent in `cmd/reverse/` (copy `cmd/echo`). Hint: convert to `[]rune`.
2. Make `echo` ask "shout? (yes/no)" with `res.Ask` and upper-case its answer when the caller says yes.
3. Write a test in `mesh/mesh_test.go` for your new agent.
4. Run `go run -race ./examples/race` and read the DATA RACE report, then fix it
   with a `sync.Mutex` or `sync/atomic` until the report disappears. The same
   detector runs over this whole project with `go test -race ./...`.
5. Give the Ollama agent a `/model <name>` command that switches models for a
   conversation. Add a test using `ollamatest` with two models installed.
