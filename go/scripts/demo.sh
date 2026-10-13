#!/usr/bin/env bash
# Builds the Go programs and starts a demo fleet on your local NATS server.
#   scripts/demo.sh                 real Claude Code (your `claude` login)
#   FAKE_CLAUDE=1 scripts/demo.sh   offline stub, no API calls
#   FAKE_OLLAMA=1 scripts/demo.sh   add an Ollama agent backed by a fake Ollama server
#   OLLAMA_MODEL=qwen3 scripts/demo.sh   pick the model when real Ollama is running (default llama3.2)
#   scripts/demo.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .run bin
export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
[[ "${FAKE_CLAUDE:-}" == 1 ]] && export CLAUDE_BIN="$PWD/claude/testdata/fake-claude.sh"

stop_fleet() { pkill -f "$PWD/bin/(echo|shell|claude|controller|meta|ollama|fakeollama)( |$)" 2>/dev/null || true; }
if [[ "${1:-}" == stop ]]; then stop_fleet; echo stopped; exit 0; fi

go build -o bin/ ./cmd/...
if ! nc -z 127.0.0.1 4222 2>/dev/null; then
  nats-server -p 4222 > .run/nats.log 2>&1 &
  sleep 0.5
fi
stop_fleet   # re-running replaces the fleet instead of adding duplicates
sleep 0.3

WORK="${WORK_DIR:-$(cd .. && pwd)}"
"$PWD/bin/echo"                      > .run/echo.log 2>&1 &
"$PWD/bin/shell" -dir "$WORK"        > .run/shell.log 2>&1 &
"$PWD/bin/claude" -dir "$WORK"       > .run/claude.log 2>&1 &
"$PWD/bin/controller"                > .run/controller.log 2>&1 &
"$PWD/bin/meta"                      > .run/meta.log 2>&1 &

# Ollama: a fake server on request, otherwise real Ollama if it's running.
if [[ "${FAKE_OLLAMA:-}" == 1 ]]; then
  "$PWD/bin/fakeollama" > .run/fakeollama.url 2>&1 &
  sleep 0.5
  "$PWD/bin/ollama" -url "$(head -1 .run/fakeollama.url)" -name llama > .run/ollama.log 2>&1 &
elif curl -sf http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
  "$PWD/bin/ollama" -model "${OLLAMA_MODEL:-llama3.2}" > .run/ollama.log 2>&1 &
fi
sleep 1.5
bin/mesh ls
cat <<TXT

Try:  bin/mesh ask echo "hello"
      bin/mesh ask shell "git status"          (asks you to approve)
      bin/mesh ask cc "what does this repo do?"
      bin/mesh ask ollama "hi, my name is Srini"; bin/mesh ask ollama "what's my name?"
      bin/mesh chat
Logs in .run/   Stop with: scripts/demo.sh stop
TXT
