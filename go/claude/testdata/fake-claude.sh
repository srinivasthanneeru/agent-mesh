#!/bin/sh
# Stand-in for the `claude` CLI: emits the same stream-json event shapes.
#   FAKE_STALL=1       print init, then hang (tests the watchdog)
#   --resume stale     fail like "No conversation found" (tests auto-retry)
prompt=""; resume=""
while [ $# -gt 0 ]; do
  case "$1" in
    -p) prompt="$2"; shift ;;
    --resume) resume="$2"; shift ;;
  esac
  shift
done
if [ -n "$FAKE_STALL" ]; then
  echo '{"type":"system","subtype":"init","session_id":"s1"}'
  exec sleep 30   # exec: sleep replaces the shell, like a real hung CLI
fi
if [ "$resume" = "stale" ]; then
  echo "No conversation found with session ID: stale" >&2
  echo '{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":0,"session_id":"stale","total_cost_usd":0}'
  exit 1
fi
sid=${resume:-sess-1}
pre=""; [ -n "$resume" ] && pre="(resumed) "
echo "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$sid\"}"
echo "{\"type\":\"stream_event\",\"event\":{\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"${pre}got: $prompt\"}}}"
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"README.md"}}]}}'
echo "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"$sid\",\"total_cost_usd\":0.001,\"num_turns\":1}"
