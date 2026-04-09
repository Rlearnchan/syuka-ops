#!/bin/zsh
set -euo pipefail

PROJECT_DIR="/Users/bae/Documents/code/syuka-gpt/syuka-ops"
BASE_DIR="$PROJECT_DIR/data"
LOG_DIR="$BASE_DIR/logs"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
OLLAMA_URL="${SYUKA_ANALYSIS_BASE_URL:-http://127.0.0.1:11434}"
MODEL="${SYUKA_ANALYSIS_MODEL:-gemma3:4b}"
COOKIES_FROM_BROWSER="${YT_DLP_COOKIES_FROM_BROWSER:-chrome}"
METRIC_RECENT_DAYS="${SYUKA_METRIC_RECENT_DAYS:-30}"
ANALYSIS_DATE_FROM="${SYUKA_ANALYSIS_DATE_FROM:-$(date -v-30d +%F)}"
INCREMENTAL_DATE_FROM="${SYUKA_INCREMENTAL_DATE_FROM:-$ANALYSIS_DATE_FROM}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/daily_update_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

export PYTHONPATH="$PROJECT_DIR/src"

run_step() {
  local label="$1"
  shift
  echo "" | tee -a "$LOG_FILE"
  echo "[step] $label" | tee -a "$LOG_FILE"
  "$@" 2>&1 | tee -a "$LOG_FILE"
}

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

echo "[info] Daily update started." | tee -a "$LOG_FILE"
echo "[info] metric_recent_days=$METRIC_RECENT_DAYS incremental_date_from=$INCREMENTAL_DATE_FROM analysis_date_from=$ANALYSIS_DATE_FROM model=$MODEL cookies_from_browser=$COOKIES_FROM_BROWSER" | tee -a "$LOG_FILE"

run_step \
  "Refresh recent metrics" \
  "$PYTHON_BIN" -m syuka_ops.cli \
  --mode refresh-metrics \
  --base-dir "$BASE_DIR" \
  --recent-days "$METRIC_RECENT_DAYS" \
  --cookies-from-browser "$COOKIES_FROM_BROWSER"

run_step \
  "Incremental subtitles and metadata" \
  "$PYTHON_BIN" -m syuka_ops.cli \
  --mode incremental \
  --base-dir "$BASE_DIR" \
  --date-from "$INCREMENTAL_DATE_FROM" \
  --cookies-from-browser "$COOKIES_FROM_BROWSER"

run_step \
  "Generate recent analysis" \
  "$PYTHON_BIN" -m syuka_ops.cli \
  --mode generate-analysis \
  --base-dir "$BASE_DIR" \
  --date-from "$ANALYSIS_DATE_FROM" \
  --analysis-model "$MODEL" \
  --analysis-base-url "$OLLAMA_URL"

echo "" | tee -a "$LOG_FILE"
echo "[info] Daily update finished. Log saved to $LOG_FILE" | tee -a "$LOG_FILE"
