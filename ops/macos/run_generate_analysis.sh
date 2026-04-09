#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BASE_DIR="$PROJECT_DIR/data"
LOG_DIR="$BASE_DIR/logs"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
OLLAMA_URL="${SYUKA_ANALYSIS_BASE_URL:-http://127.0.0.1:11434}"
MODEL="${SYUKA_ANALYSIS_MODEL:-gemma3:4b}"
LIMIT="${1:-}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/generate_analysis_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

export PYTHONPATH="$PROJECT_DIR/src"

if ! curl -fsS "$OLLAMA_URL/api/tags" >/dev/null 2>&1; then
  echo "[info] Ollama server not ready. Opening Ollama.app..." | tee -a "$LOG_FILE"
  open -a Ollama
fi

echo "[info] Waiting for Ollama at $OLLAMA_URL ..." | tee -a "$LOG_FILE"
for _ in {1..60}; do
  if curl -fsS "$OLLAMA_URL/api/tags" >/dev/null 2>&1; then
    echo "[info] Ollama is ready." | tee -a "$LOG_FILE"
    break
  fi
  sleep 2
done

if ! curl -fsS "$OLLAMA_URL/api/tags" >/dev/null 2>&1; then
  echo "[error] Ollama did not become ready in time." | tee -a "$LOG_FILE"
  exit 1
fi

CMD=(
  "$PYTHON_BIN" -m syuka_ops.cli
  --mode generate-analysis
  --base-dir "$BASE_DIR"
  --analysis-model "$MODEL"
  --analysis-base-url "$OLLAMA_URL"
)

if [[ -n "$LIMIT" ]]; then
  CMD+=(--analysis-limit "$LIMIT")
fi

echo "[info] Starting analysis with model=$MODEL limit=${LIMIT:-all}" | tee -a "$LOG_FILE"
"${CMD[@]}" 2>&1 | tee -a "$LOG_FILE"
echo "[info] Finished. Log saved to $LOG_FILE" | tee -a "$LOG_FILE"
