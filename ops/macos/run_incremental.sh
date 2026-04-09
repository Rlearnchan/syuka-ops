#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BASE_DIR="$PROJECT_DIR/data"
LOG_DIR="$BASE_DIR/logs"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"

mkdir -p "$LOG_DIR"

export PYTHONPATH="$PROJECT_DIR/src"

"$PYTHON_BIN" -m syuka_ops.cli \
  --mode incremental \
  --base-dir "$BASE_DIR" \
  --cookies-from-browser chrome

"$PYTHON_BIN" -m syuka_ops.report --base-dir "$BASE_DIR"
