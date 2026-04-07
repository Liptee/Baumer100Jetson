#!/usr/bin/env bash
set -euo pipefail

# Optional:
# export LOGIN="<github_login>"
# export TOKEN="<github_token_or_pat>"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-baumer_recorder}"

cd "$REPO_DIR"

if [[ -n "${LOGIN:-}" && -n "${TOKEN:-}" ]]; then
  git pull "https://${LOGIN}:${TOKEN}@github.com/<OWNER>/<REPO>.git"
else
  git pull
fi

sudo systemctl daemon-reload
sudo systemctl restart "${SERVICE_NAME}.service"
sudo systemctl status --no-pager "${SERVICE_NAME}.service" | sed -n '1,25p'

