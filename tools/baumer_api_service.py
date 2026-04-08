#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import os
import re
import signal
import subprocess
import threading
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel

REPO_DIR = Path(__file__).resolve().parents[1]
RECORDER_SCRIPT = REPO_DIR / "tools" / "baumer_record_headless.py"


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return int(default)
    try:
        return int(v)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    if not v:
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _env_opt_int(name: str) -> Optional[int]:
    v = os.getenv(name, "").strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "").strip().lower()
    if not v:
        return bool(default)
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return bool(default)


def _python_bin() -> str:
    env_py = os.getenv("PYTHON_BIN", "").strip()
    if env_py:
        return env_py
    venv_py = REPO_DIR / ".venv" / "bin" / "python"
    if venv_py.exists():
        return str(venv_py)
    return "python3"


DEFAULTS: Dict[str, Any] = {
    "backend": _env_str("BACKEND", "gst-raw"),
    "pixel_format": _env_str("PIXEL_FORMAT", "gray8"),
    "width": _env_int("WIDTH", 1024),
    "height": _env_int("HEIGHT", 768),
    "target_fps": _env_float("TARGET_FPS", 120.0),
    "min_fps": _env_int("MIN_FPS", 100),
    "max_fps": _env_int("MAX_FPS", 120),
    "duration": _env_float("DURATION", 10.0),
    "exposure_us": _env_float("EXPOSURE_US", 9500.0),
    "gain": _env_float("GAIN", 1.0),
    "roi_center": _env_str("ROI_CENTER", "auto"),
    "roi_x": _env_opt_int("ROI_X"),
    "roi_y": _env_opt_int("ROI_Y"),
    "crop_top": _env_int("CROP_TOP", 0),
    "crop_bottom": _env_int("CROP_BOTTOM", 0),
    "crop_left": _env_int("CROP_LEFT", 0),
    "crop_right": _env_int("CROP_RIGHT", 0),
    "camera_id": _env_str("CAMERA_ID", ""),
    "device": _env_str("DEVICE", ""),
    "snapshot_dir": _env_str("SNAPSHOT_DIR", str(REPO_DIR / "capture")),
    "telemetry_enable": _env_bool("TELEMETRY_ENABLE", False),
    "telemetry_device": _env_str("TELEMETRY_DEVICE", ""),
    "telemetry_baud": _env_int("TELEMETRY_BAUD", 115200),
    "telemetry_wait_heartbeat": _env_float("TELEMETRY_WAIT_HEARTBEAT", 5.0),
    "telemetry_msg_types": _env_str("TELEMETRY_MSG_TYPES", ""),
    "telemetry_max_rate_hz": _env_float("TELEMETRY_MAX_RATE_HZ", 0.0),
    "telemetry_request_streams": _env_str("TELEMETRY_REQUEST_STREAMS", "on"),
    "telemetry_request_types": _env_str(
        "TELEMETRY_REQUEST_TYPES",
        "ATTITUDE,LOCAL_POSITION_NED,GLOBAL_POSITION_INT",
    ),
    "telemetry_request_rate_hz": _env_float("TELEMETRY_REQUEST_RATE_HZ", 50.0),
}


class RecordRequest(BaseModel):
    backend: Optional[str] = None
    pixel_format: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    target_fps: Optional[float] = None
    min_fps: Optional[int] = None
    max_fps: Optional[int] = None
    duration: Optional[float] = None
    exposure_us: Optional[float] = None
    gain: Optional[float] = None
    roi_center: Optional[str] = None
    roi_x: Optional[int] = None
    roi_y: Optional[int] = None
    crop_top: Optional[int] = None
    crop_bottom: Optional[int] = None
    crop_left: Optional[int] = None
    crop_right: Optional[int] = None
    camera_id: Optional[str] = None
    device: Optional[str] = None
    snapshot_dir: Optional[str] = None
    telemetry_enable: Optional[bool] = None
    telemetry_device: Optional[str] = None
    telemetry_baud: Optional[int] = None
    telemetry_wait_heartbeat: Optional[float] = None
    telemetry_msg_types: Optional[str] = None
    telemetry_max_rate_hz: Optional[float] = None
    telemetry_request_streams: Optional[str] = None
    telemetry_request_types: Optional[str] = None
    telemetry_request_rate_hz: Optional[float] = None


class RecorderState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.process: Optional[subprocess.Popen[str]] = None
        self.current_job: Optional[Dict[str, Any]] = None
        self.last_job: Optional[Dict[str, Any]] = None
        self.logs: Deque[str] = deque(maxlen=4000)


STATE = RecorderState()
DONE_FILE_RE = re.compile(r"done:\s+file=([^\s]+)")

app = FastAPI(title="Baumer Recorder API", version="1.0.0")


def _ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _append_log(msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    with STATE.lock:
        STATE.logs.append(line)


def _resolve_request(req: RecordRequest) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, default_val in DEFAULTS.items():
        v = getattr(req, key, None)
        if v is None:
            out[key] = default_val
        else:
            out[key] = v
    return out


def _build_cmd(data: Dict[str, Any]) -> List[str]:
    cmd: List[str] = [
        _python_bin(),
        str(RECORDER_SCRIPT),
        "--backend",
        str(data["backend"]),
        "--pixel-format",
        str(data["pixel_format"]),
        "--width",
        str(int(data["width"])),
        "--height",
        str(int(data["height"])),
        "--target-fps",
        str(float(data["target_fps"])),
        "--min-fps",
        str(int(data["min_fps"])),
        "--max-fps",
        str(int(data["max_fps"])),
        "--duration",
        str(float(data["duration"])),
        "--exposure-us",
        str(float(data["exposure_us"])),
        "--gain",
        str(float(data["gain"])),
        "--roi-center",
        str(data["roi_center"]),
        "--crop-top",
        str(int(data["crop_top"])),
        "--crop-bottom",
        str(int(data["crop_bottom"])),
        "--crop-left",
        str(int(data["crop_left"])),
        "--crop-right",
        str(int(data["crop_right"])),
        "--snapshot-dir",
        str(data["snapshot_dir"]),
    ]
    if data.get("roi_x") is not None:
        cmd += ["--roi-x", str(int(data["roi_x"]))]
    if data.get("roi_y") is not None:
        cmd += ["--roi-y", str(int(data["roi_y"]))]
    if str(data.get("camera_id") or "").strip():
        cmd += ["--camera-id", str(data["camera_id"]).strip()]
    if str(data.get("device") or "").strip():
        cmd += ["--device", str(data["device"]).strip()]
    if bool(data.get("telemetry_enable", False)):
        cmd += ["--telemetry-enable"]
    if str(data.get("telemetry_device") or "").strip():
        cmd += ["--telemetry-device", str(data["telemetry_device"]).strip()]
    if data.get("telemetry_baud") is not None:
        cmd += ["--telemetry-baud", str(int(data["telemetry_baud"]))]
    if data.get("telemetry_wait_heartbeat") is not None:
        cmd += ["--telemetry-wait-heartbeat", str(float(data["telemetry_wait_heartbeat"]))]
    if str(data.get("telemetry_msg_types") or "").strip():
        cmd += ["--telemetry-msg-types", str(data["telemetry_msg_types"]).strip()]
    if data.get("telemetry_max_rate_hz") is not None:
        cmd += ["--telemetry-max-rate-hz", str(float(data["telemetry_max_rate_hz"]))]
    if str(data.get("telemetry_request_streams") or "").strip():
        cmd += ["--telemetry-request-streams", str(data["telemetry_request_streams"]).strip()]
    if str(data.get("telemetry_request_types") or "").strip():
        cmd += ["--telemetry-request-types", str(data["telemetry_request_types"]).strip()]
    if data.get("telemetry_request_rate_hz") is not None:
        cmd += ["--telemetry-request-rate-hz", str(float(data["telemetry_request_rate_hz"]))]
    return cmd


def _monitor_process(job_id: str, proc: subprocess.Popen[str]) -> None:
    output_file: Optional[str] = None
    try:
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            _append_log(f"[job={job_id}] {line}")
            m = DONE_FILE_RE.search(line)
            if m:
                output_file = m.group(1)
    except Exception as exc:
        _append_log(f"[job={job_id}] log reader error: {exc}")
    finally:
        rc = proc.wait()
        finished_at = _ts()
        with STATE.lock:
            cur = STATE.current_job if STATE.current_job and STATE.current_job.get("job_id") == job_id else None
            if cur is None:
                cur = {"job_id": job_id}
            cur["finished_at"] = finished_at
            cur["return_code"] = int(rc)
            if output_file:
                cur["output_file"] = output_file
            if int(rc) == 0:
                cur["status"] = "success"
            elif int(rc) < 0:
                cur["status"] = "stopped"
            else:
                cur["status"] = "failed"
            STATE.last_job = dict(cur)
            if STATE.current_job and STATE.current_job.get("job_id") == job_id:
                STATE.current_job = None
            if STATE.process is proc:
                STATE.process = None
        _append_log(f"[job={job_id}] finished rc={rc}")


@app.get("/api/health")
def api_health() -> Dict[str, Any]:
    with STATE.lock:
        running = bool(STATE.process and STATE.process.poll() is None)
    return {"ok": True, "running": running, "time": _ts()}


@app.get("/api/record/status")
def api_record_status() -> Dict[str, Any]:
    with STATE.lock:
        running = bool(STATE.process and STATE.process.poll() is None)
        current = dict(STATE.current_job) if STATE.current_job else None
        last = dict(STATE.last_job) if STATE.last_job else None
    return {"running": running, "current_job": current, "last_job": last}


@app.get("/api/record/logs")
def api_record_logs(tail: int = 200) -> Dict[str, Any]:
    tail_n = max(1, min(int(tail), 2000))
    with STATE.lock:
        lines = list(STATE.logs)[-tail_n:]
    return {"tail": tail_n, "lines": lines}


@app.post("/api/record/start")
def api_record_start(payload: Optional[RecordRequest] = Body(default=None)) -> Dict[str, Any]:
    req = payload or RecordRequest()
    if not RECORDER_SCRIPT.exists():
        raise HTTPException(status_code=500, detail=f"Recorder script not found: {RECORDER_SCRIPT}")

    data = _resolve_request(req)
    cmd = _build_cmd(data)

    with STATE.lock:
        if STATE.process and STATE.process.poll() is None:
            raise HTTPException(status_code=409, detail="Record job is already running")

        os.makedirs(str(data["snapshot_dir"]), exist_ok=True)
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(REPO_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to start recorder: {exc}")

        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "status": "running",
            "started_at": _ts(),
            "command": cmd,
            "params": data,
        }
        STATE.process = proc
        STATE.current_job = job

    _append_log(f"[job={job_id}] started")
    th = threading.Thread(target=_monitor_process, args=(job_id, proc), daemon=True, name=f"rec-{job_id[:8]}")
    th.start()

    return {"accepted": True, "job": job}


@app.post("/api/record/stop")
def api_record_stop() -> Dict[str, Any]:
    with STATE.lock:
        proc = STATE.process
        cur = dict(STATE.current_job) if STATE.current_job else None
    if proc is None or proc.poll() is not None:
        raise HTTPException(status_code=409, detail="No active record job")

    try:
        # Graceful stop first: recorder handles SIGINT and finalizes sidecar json.
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=8.0)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2.0)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to stop record job: {exc}")

    _append_log(f"[job={cur.get('job_id') if cur else '-'}] stop requested")
    return {"stopped": True, "job_id": cur.get("job_id") if cur else None}
