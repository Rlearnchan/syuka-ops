#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_DIR/data/logs"

echo "== launchctl list =="
launchctl list | rg 'syuka' || true
echo

echo "== syuka =="
launchctl print gui/$(id -u)/com.syuka.ops.incremental 2>/dev/null | sed -n '1,40p' || echo "not loaded"
echo

echo "== recent logs =="
for file in \
  "$LOG_DIR/workspace_slack_bot.launchd.out.log" \
  "$LOG_DIR/workspace_slack_bot.launchd.err.log" \
  "$LOG_DIR/launchd.out.log" \
  "$LOG_DIR/launchd.err.log"
do
  if [[ -f "$file" ]]; then
    echo "-- $file"
    tail -n 5 "$file"
    echo
  fi
done
