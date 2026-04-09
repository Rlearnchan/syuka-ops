#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_DIR"

if [[ -n "${ENV_FILE:-}" ]]; then
  ENV_FILE="$ENV_FILE"
elif [[ -f "$PROJECT_DIR/.env.workspace" ]]; then
  ENV_FILE="$PROJECT_DIR/.env.workspace"
else
  ENV_FILE="$PROJECT_DIR/.env.company"
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[error] environment file not found: $ENV_FILE"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

if [[ -z "${SLACK_BOT_TOKEN:-}" || -z "${SLACK_APP_TOKEN:-}" ]]; then
  echo "[error] SLACK_BOT_TOKEN / SLACK_APP_TOKEN are required in $ENV_FILE"
  exit 1
fi

export PYTHONUNBUFFERED=1
export PYTHONPATH="./src"
export SYUKA_DATA_DIR="${SYUKA_DATA_DIR:-./data}"

exec ./.venv/bin/python -m syuka_ops.slack_bot
