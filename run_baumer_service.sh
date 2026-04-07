#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

PYTHON_BIN="${PYTHON_BIN:-$REPO_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

API_HOST="${API_HOST:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"

echo "[service] $(date '+%F %T') starting FastAPI on ${API_HOST}:${API_PORT}"
exec "$PYTHON_BIN" -m uvicorn baumer_api_service:app --app-dir "$REPO_DIR/tools" --host "$API_HOST" --port "$API_PORT"
