#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-${REPO_DIR}/.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi
FFMPEG_BIN="${FFMPEG_BIN:-ffmpeg}"

BITNESS="gray8"
BITNESS_SET=0
ROI_CENTER="auto"
ROI_X=""
ROI_Y=""
CROP_TOP=0
CROP_BOTTOM=0
CROP_LEFT=0
CROP_RIGHT=0

WIDTH=1024
HEIGHT=768
TARGET_FPS=120
MIN_FPS=100
MAX_FPS=120
DURATION=10
EXPOSURE_US=9500
GAIN=1
CAMERA_ID=""
DEVICE=""
SNAPSHOT_DIR="${REPO_DIR}/capture"

usage() {
  cat <<'EOF'
Usage:
  tools/record_and_decode.sh [options]

Core options:
  --bitness <gray8|y16|8|16>    Capture bitness (required)
  --roi-center <auto|on|off>    Hardware ROI center mode (default: auto)
  --roi-x <int>                 Hardware ROI X offset
  --roi-y <int>                 Hardware ROI Y offset
  --crop-top <int>              Software crop top
  --crop-bottom <int>           Software crop bottom
  --crop-left <int>             Software crop left
  --crop-right <int>            Software crop right

Capture options:
  --width <int>                 Capture width (default: 1024)
  --height <int>                Capture height (default: 768)
  --target-fps <int|float>      Target fps (default: 120)
  --min-fps <int>               Minimal fps for mode search (default: 100)
  --max-fps <int>               Maximal fps for mode search (default: 120)
  --duration <sec>              Record duration in seconds (default: 10)
  --exposure-us <float>         Exposure in us (default: 9500)
  --gain <float>                Gain (default: 1)
  --camera-id <str>             Camera serial hint
  --device </dev/videoX>        Force camera device
  --snapshot-dir <path>         Output dir (default: ./capture)

Other:
  -h, --help                    Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bitness) BITNESS="${2:-}"; BITNESS_SET=1; shift 2 ;;
    --roi-center) ROI_CENTER="${2:-}"; shift 2 ;;
    --roi-x) ROI_X="${2:-}"; shift 2 ;;
    --roi-y) ROI_Y="${2:-}"; shift 2 ;;
    --crop-top) CROP_TOP="${2:-}"; shift 2 ;;
    --crop-bottom) CROP_BOTTOM="${2:-}"; shift 2 ;;
    --crop-left) CROP_LEFT="${2:-}"; shift 2 ;;
    --crop-right) CROP_RIGHT="${2:-}"; shift 2 ;;
    --width) WIDTH="${2:-}"; shift 2 ;;
    --height) HEIGHT="${2:-}"; shift 2 ;;
    --target-fps) TARGET_FPS="${2:-}"; shift 2 ;;
    --min-fps) MIN_FPS="${2:-}"; shift 2 ;;
    --max-fps) MAX_FPS="${2:-}"; shift 2 ;;
    --duration) DURATION="${2:-}"; shift 2 ;;
    --exposure-us) EXPOSURE_US="${2:-}"; shift 2 ;;
    --gain) GAIN="${2:-}"; shift 2 ;;
    --camera-id) CAMERA_ID="${2:-}"; shift 2 ;;
    --device) DEVICE="${2:-}"; shift 2 ;;
    --snapshot-dir) SNAPSHOT_DIR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "${BITNESS_SET}" -ne 1 ]]; then
  echo "--bitness is required" >&2
  usage
  exit 2
fi

case "${BITNESS,,}" in
  8|gray8|grey|mono8)
    PIXEL_FORMAT="gray8"
    FFMPEG_PIXEL="gray"
    ;;
  16|y16|gray16|grey16|mono16)
    PIXEL_FORMAT="y16"
    FFMPEG_PIXEL="gray16le"
    ;;
  *)
    echo "Unsupported --bitness: ${BITNESS}. Use gray8|y16|8|16" >&2
    exit 2
    ;;
esac

if [[ "${ROI_CENTER}" != "auto" && "${ROI_CENTER}" != "on" && "${ROI_CENTER}" != "off" ]]; then
  echo "Unsupported --roi-center: ${ROI_CENTER}. Use auto|on|off" >&2
  exit 2
fi

mkdir -p "${SNAPSHOT_DIR}"
if ! command -v "${FFMPEG_BIN}" >/dev/null 2>&1; then
  echo "ffmpeg not found: ${FFMPEG_BIN}" >&2
  exit 2
fi

MARKER="$(mktemp)"
trap 'rm -f "${MARKER}"' EXIT
touch "${MARKER}"

CMD=(
  "${PYTHON_BIN}" "${REPO_DIR}/tools/baumer_record_headless.py"
  --backend gst-raw
  --pixel-format "${PIXEL_FORMAT}"
  --width "${WIDTH}"
  --height "${HEIGHT}"
  --target-fps "${TARGET_FPS}"
  --min-fps "${MIN_FPS}"
  --max-fps "${MAX_FPS}"
  --duration "${DURATION}"
  --exposure-us "${EXPOSURE_US}"
  --gain "${GAIN}"
  --roi-center "${ROI_CENTER}"
  --crop-top "${CROP_TOP}"
  --crop-bottom "${CROP_BOTTOM}"
  --crop-left "${CROP_LEFT}"
  --crop-right "${CROP_RIGHT}"
  --snapshot-dir "${SNAPSHOT_DIR}"
)

if [[ -n "${ROI_X}" ]]; then
  CMD+=(--roi-x "${ROI_X}")
fi
if [[ -n "${ROI_Y}" ]]; then
  CMD+=(--roi-y "${ROI_Y}")
fi
if [[ -n "${CAMERA_ID}" ]]; then
  CMD+=(--camera-id "${CAMERA_ID}")
fi
if [[ -n "${DEVICE}" ]]; then
  CMD+=(--device "${DEVICE}")
fi

echo "[run] ${CMD[*]}"
"${CMD[@]}"

RAW_FILE="$(find "${SNAPSHOT_DIR}" -maxdepth 1 -type f -name '*.raw' -newer "${MARKER}" | sort | tail -n 1 || true)"
if [[ -z "${RAW_FILE}" ]]; then
  RAW_FILE="$(find "${SNAPSHOT_DIR}" -maxdepth 1 -type f -name '*.raw' | sort | tail -n 1 || true)"
fi
if [[ -z "${RAW_FILE}" ]]; then
  echo "No raw file found in ${SNAPSHOT_DIR}" >&2
  exit 3
fi

META_FILE="${RAW_FILE}.json"
if [[ -f "${META_FILE}" ]]; then
  read -r META_W META_H META_FPS < <(
    python3 - "${META_FILE}" <<'PY'
import json, sys
p = sys.argv[1]
with open(p, "r", encoding="utf-8") as f:
    m = json.load(f)
w = int(m.get("width", 0) or 0)
h = int(m.get("height", 0) or 0)
fps = float(m.get("fps_from_size", 0.0) or 0.0)
if fps <= 0.0:
    fps = float(m.get("requested_fps", 0.0) or 0.0)
print(f"{w} {h} {fps:.6f}")
PY
  )
else
  META_W="${WIDTH}"
  META_H="${HEIGHT}"
  META_FPS="${TARGET_FPS}"
fi

if [[ "${META_W}" -le 0 || "${META_H}" -le 0 ]]; then
  echo "Invalid dimensions in metadata: ${META_W}x${META_H}" >&2
  exit 3
fi

if [[ "${META_FPS}" == "0.000000" || "${META_FPS}" == "0" || -z "${META_FPS}" ]]; then
  META_FPS="${TARGET_FPS}"
fi

if [[ "${PIXEL_FORMAT}" == "y16" ]]; then
  OUT_VIDEO="${RAW_FILE%.raw}.mkv"
  echo "[ffmpeg] decode ${RAW_FILE} -> ${OUT_VIDEO} (${META_W}x${META_H} @ ${META_FPS} fps, gray16le)"
  "${FFMPEG_BIN}" -y \
    -f rawvideo -pixel_format gray16le -video_size "${META_W}x${META_H}" -framerate "${META_FPS}" \
    -i "${RAW_FILE}" \
    -c:v ffv1 -level 3 -pix_fmt gray16le \
    "${OUT_VIDEO}"
else
  OUT_VIDEO="${RAW_FILE%.raw}.mp4"
  echo "[ffmpeg] decode ${RAW_FILE} -> ${OUT_VIDEO} (${META_W}x${META_H} @ ${META_FPS} fps, ${FFMPEG_PIXEL})"
  "${FFMPEG_BIN}" -y \
    -f rawvideo -pixel_format "${FFMPEG_PIXEL}" -video_size "${META_W}x${META_H}" -framerate "${META_FPS}" \
    -i "${RAW_FILE}" \
    -c:v libx264 -pix_fmt yuv420p \
    "${OUT_VIDEO}"
fi

echo "[done] raw=${RAW_FILE}"
echo "[done] video=${OUT_VIDEO}"
