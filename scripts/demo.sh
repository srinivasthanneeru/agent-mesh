#!/usr/bin/env bash
# Starts a local NATS server plus a demo fleet of agents, then prints what to try.
#   scripts/demo.sh            real Claude Code (uses your `claude` CLI login)
#   FAKE_CLAUDE=1 scripts/demo.sh   offline stub, no API calls
#   scripts/demo.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .run
export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
[[ "${FAKE_CLAUDE:-}" == 1 ]] && export CLAUDE_BIN="$PWD/test/fixtures/fake-claude.mjs"
n() { node --experimental-strip-types --no-warnings "$@"; }

if [[ "${1:-}" == stop ]]; then
  pkill -f "src/agents/|src/controller.ts" || true
  [[ -f .run/nats.pid ]] && kill "$(cat .run/nats.pid)" 2>/dev/null || true
  rm -f .run/nats.pid; echo "stopped"; exit 0
fi

if ! nats-server --help >/dev/null 2>&1; then echo "install nats-server: brew install nats-server nats-io/nats-tools/nats"; exit 1; fi
if ! nc -z 127.0.0.1 4222 2>/dev/null; then
  mkdir -p "$HOME/.agent-mesh/nats"
  # -js turns on JetStream (sessions are saved in its KV store); -sd keeps
  # that data in a permanent folder so it survives reboots.
  nats-server -p 4222 -js -sd "$HOME/.agent-mesh/nats" -m 8222 > .run/nats.log 2>&1 & echo $! > .run/nats.pid
  sleep 0.5
fi

# Re-running the demo replaces the fleet instead of adding duplicates
# (same-identity duplicates would share the queue group and split prompts).
pkill -f "src/agents/|src/controller.ts" 2>/dev/null && sleep 0.5 || true

WORK="${WORK_DIR:-$PWD}"
n src/agents/echo.ts                          > .run/echo.log 2>&1 &
n src/agents/shell.ts --cwd "$WORK"           > .run/shell.log 2>&1 &
n src/agents/claude.ts --cwd "$WORK"          > .run/claude.log 2>&1 &
n src/controller.ts                           > .run/controller.log 2>&1 &
n src/agents/meta.ts                          > .run/meta.log 2>&1 &
sleep 1.5
n src/cli.ts ls
cat <<TXT

Try:
  npm run mesh -- ask echo "hello"
  npm run mesh -- ask shell "git status"            # asks you to approve first
  npm run mesh -- ask cc "what does this repo do?"
  npm run mesh -- spawn ~/code/other-project --name other
  npm run mesh -- ask cc,other "summarize the README"   # fan-out
  npm run mesh -- ask meta "@all say hi"
  npm run mesh -- chat
  nats req agents.prompt.echo.$USER.main "raw wire" --wait-for-empty --reply-timeout=30s
Logs in .run/   Stop with: scripts/demo.sh stop
TXT
