#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

PYTHON_BIN="${PYTHON_BIN:-$REPO_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

BACKEND="${BACKEND:-gst-raw}"
PIXEL_FORMAT="${PIXEL_FORMAT:-gray8}"
WIDTH="${WIDTH:-1024}"
HEIGHT="${HEIGHT:-768}"
TARGET_FPS="${TARGET_FPS:-120}"
MIN_FPS="${MIN_FPS:-100}"
MAX_FPS="${MAX_FPS:-120}"
DURATION="${DURATION:-10}"
EXPOSURE_US="${EXPOSURE_US:-9500}"
GAIN="${GAIN:-1}"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$REPO_DIR/capture}"

ROI_CENTER="${ROI_CENTER:-auto}"
ROI_X="${ROI_X:-}"
ROI_Y="${ROI_Y:-}"

CROP_TOP="${CROP_TOP:-0}"
CROP_BOTTOM="${CROP_BOTTOM:-0}"
CROP_LEFT="${CROP_LEFT:-0}"
CROP_RIGHT="${CROP_RIGHT:-0}"

CAMERA_ID="${CAMERA_ID:-}"
DEVICE="${DEVICE:-}"

LOOP_RECORDING="${LOOP_RECORDING:-1}"
LOOP_DELAY_SEC="${LOOP_DELAY_SEC:-1}"

mkdir -p "$SNAPSHOT_DIR"

while true; do
  cmd=(
    "$PYTHON_BIN" "$REPO_DIR/tools/baumer_record_headless.py"
    --backend "$BACKEND"
    --pixel-format "$PIXEL_FORMAT"
    --width "$WIDTH"
    --height "$HEIGHT"
    --target-fps "$TARGET_FPS"
    --min-fps "$MIN_FPS"
    --max-fps "$MAX_FPS"
    --duration "$DURATION"
    --exposure-us "$EXPOSURE_US"
    --gain "$GAIN"
    --roi-center "$ROI_CENTER"
    --crop-top "$CROP_TOP"
    --crop-bottom "$CROP_BOTTOM"
    --crop-left "$CROP_LEFT"
    --crop-right "$CROP_RIGHT"
    --snapshot-dir "$SNAPSHOT_DIR"
  )

  if [[ -n "$ROI_X" ]]; then
    cmd+=(--roi-x "$ROI_X")
  fi
  if [[ -n "$ROI_Y" ]]; then
    cmd+=(--roi-y "$ROI_Y")
  fi
  if [[ -n "$CAMERA_ID" ]]; then
    cmd+=(--camera-id "$CAMERA_ID")
  fi
  if [[ -n "$DEVICE" ]]; then
    cmd+=(--device "$DEVICE")
  fi

  echo "[service] $(date '+%F %T') start: ${cmd[*]}"
  set +e
  "${cmd[@]}"
  rc=$?
  set -e
  echo "[service] $(date '+%F %T') exit code: $rc"

  if [[ "$LOOP_RECORDING" != "1" ]]; then
    exit "$rc"
  fi
  sleep "$LOOP_DELAY_SEC"
done

