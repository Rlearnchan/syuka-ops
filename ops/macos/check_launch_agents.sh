#!/bin/zsh
set -euo pipefail

echo "== launchctl list =="
launchctl list | rg 'daeyeon|syuka' || true
echo

echo "== daeyeon =="
launchctl print gui/$(id -u)/com.daeyeon.sync.balance 2>/dev/null | sed -n '1,40p' || echo "not loaded"
echo

echo "== syuka =="
launchctl print gui/$(id -u)/com.syuka.ops.incremental 2>/dev/null | sed -n '1,40p' || echo "not loaded"
echo

echo "== recent logs =="
for file in \
  /Users/bae/Documents/code/daeyeon/sync.log \
  /Users/bae/Documents/code/daeyeon/sync.err.log \
  /Users/bae/Documents/code/syuka-gpt/syuka-ops/data/logs/launchd.out.log \
  /Users/bae/Documents/code/syuka-gpt/syuka-ops/data/logs/launchd.err.log
do
  if [[ -f "$file" ]]; then
    echo "-- $file"
    tail -n 5 "$file"
    echo
  fi
done
