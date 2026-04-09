#!/bin/zsh
set -euo pipefail

BASE_DIR="/Users/bae/Documents/code/syuka-gpt/syuka-ops/data"
PROJECT_DIR="/Users/bae/Documents/code/syuka-gpt/syuka-ops"
LOG_DIR="$BASE_DIR/logs"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"

mkdir -p "$LOG_DIR"

export PYTHONPATH="$PROJECT_DIR/src"

"$PYTHON_BIN" -m syuka_ops.cli \
  --mode incremental \
  --base-dir "$BASE_DIR" \
  --cookies-from-browser chrome

"$PYTHON_BIN" -m syuka_ops.report --base-dir "$BASE_DIR"
