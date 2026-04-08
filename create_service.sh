#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-baumer_recorder}"
SERVICE_USER="${SERVICE_USER:-$USER}"
SERVICE_GROUP="${SERVICE_GROUP:-$(id -gn "$SERVICE_USER")}"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
ENV_PATH="/etc/default/${SERVICE_NAME}"
RUNNER_PATH="${REPO_DIR}/run_baumer_service.sh"

if [[ ! -f "$RUNNER_PATH" ]]; then
  echo "Runner not found: $RUNNER_PATH"
  exit 1
fi

chmod +x "$RUNNER_PATH"

if [[ ! -f "$ENV_PATH" ]]; then
  sudo tee "$ENV_PATH" >/dev/null <<EOF
# ${SERVICE_NAME} runtime settings
PYTHON_BIN=${REPO_DIR}/.venv/bin/python
API_HOST=0.0.0.0
API_PORT=8000
PIXEL_FORMAT=gray8
WIDTH=1024
HEIGHT=768
TARGET_FPS=120
MIN_FPS=100
MAX_FPS=120
DURATION=10
EXPOSURE_US=9500
GAIN=1
ROI_CENTER=auto
ROI_X=
ROI_Y=
CROP_TOP=0
CROP_BOTTOM=0
CROP_LEFT=0
CROP_RIGHT=0
CAMERA_ID=
DEVICE=
SNAPSHOT_DIR=${REPO_DIR}/capture
TELEMETRY_ENABLE=0
TELEMETRY_DEVICE=
TELEMETRY_BAUD=115200
TELEMETRY_WAIT_HEARTBEAT=5
TELEMETRY_MSG_TYPES=
TELEMETRY_MAX_RATE_HZ=0
TELEMETRY_REQUEST_STREAMS=on
TELEMETRY_REQUEST_TYPES=ATTITUDE,LOCAL_POSITION_NED,GLOBAL_POSITION_INT
TELEMETRY_REQUEST_RATE_HZ=50
EOF
fi

sudo tee "$UNIT_PATH" >/dev/null <<EOF
[Unit]
Description=Baumer FastAPI Recorder Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${REPO_DIR}
EnvironmentFile=-${ENV_PATH}
ExecStart=/bin/bash ${RUNNER_PATH}
Restart=always
RestartSec=1
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}.service"
sudo systemctl restart "${SERVICE_NAME}.service"

echo "Service installed: ${SERVICE_NAME}.service"
echo "Edit config: ${ENV_PATH}"
echo "Logs: sudo journalctl -u ${SERVICE_NAME}.service -f"
