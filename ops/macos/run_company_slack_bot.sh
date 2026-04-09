#!/bin/zsh
set -euo pipefail

PROJECT_DIR="/Users/bae/Documents/code/syuka-gpt/syuka-ops"
ENV_FILE="$PROJECT_DIR/.env.company"

cd "$PROJECT_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[error] .env.company 파일이 없습니다."
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

if [[ -z "${SLACK_BOT_TOKEN:-}" || -z "${SLACK_APP_TOKEN:-}" ]]; then
  echo "[error] .env.company 에 SLACK_BOT_TOKEN / SLACK_APP_TOKEN 이 필요합니다."
  exit 1
fi

export PYTHONUNBUFFERED=1
export PYTHONPATH="./src"
export SYUKA_DATA_DIR="${SYUKA_DATA_DIR:-./data}"

exec ./.venv/bin/python -m syuka_ops.slack_bot
