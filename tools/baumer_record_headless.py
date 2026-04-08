#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import signal
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Optional

STOP_REQUESTED = threading.Event()


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tn = threading.current_thread().name
    print(f"[{ts}] [{tn}] {msg}", flush=True)


def _handle_stop_signal(signum, _frame) -> None:
    STOP_REQUESTED.set()
    try:
        log(f"signal received: {signum}, stopping gracefully...")
    except Exception:
        pass


for _sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(_sig, _handle_stop_signal)
    except Exception:
        pass


def extract_serial_hint(text: str) -> str:
    m = re.search(r"(\d{6,})", str(text or ""))
    return m.group(1) if m else ""


def find_usb_root_for_video(video_name: str) -> Path | None:
    base = Path("/sys/class/video4linux") / video_name / "device"
    try:
        node = base.resolve()
    except Exception:
        return None
    while True:
        if (node / "idVendor").exists() and (node / "idProduct").exists():
            return node
        if node.parent == node:
            return None
        node = node.parent


def find_uvc_candidates(serial_hint: str = "") -> list[str]:
    out: list[str] = []
    for video in sorted(Path("/sys/class/video4linux").glob("video*")):
        name = video.name
        root = find_usb_root_for_video(name)
        if root is None:
            continue
        try:
            vid = (root / "idVendor").read_text(encoding="utf-8", errors="ignore").strip().lower()
            pid = (root / "idProduct").read_text(encoding="utf-8", errors="ignore").strip().lower()
        except Exception:
            continue
        serial = ""
        try:
            serial = (root / "serial").read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            pass
        product = ""
        try:
            product = (root / "product").read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            pass
        is_tis = (vid == "199e" and pid == "9405")
        serial_ok = bool(serial_hint and serial_hint in serial)
        if not (is_tis or serial_ok or ("imaging source" in product.lower())):
            continue
        dev = f"/dev/{name}"
        if os.path.exists(dev):
            out.append(dev)
    if out:
        return out
    # Last resort
    for video in sorted(Path("/sys/class/video4linux").glob("video*")):
        dev = f"/dev/{video.name}"
        if os.path.exists(dev):
            out.append(dev)
    return out


def find_telemetry_serial_candidates() -> list[str]:
    out: list[str] = []
    by_id = Path("/dev/serial/by-id")
    if by_id.exists():
        for p in sorted(by_id.glob("*")):
            try:
                real = str(p.resolve())
            except Exception:
                real = str(p)
            if real and real not in out:
                out.append(real)
    for patt in ("/dev/ttyACM*", "/dev/ttyUSB*"):
        for p in sorted(Path("/dev").glob(Path(patt).name)):
            s = str(p)
            if s not in out:
                out.append(s)
    return out


def set_controls_v4l2(dev: str, exposure_us: float, gain: float) -> None:
    ctl = shutil.which("v4l2-ctl")
    if not ctl:
        log("v4l2-ctl not found, skipping hard control set")
        return
    # UVC exposure_absolute unit is typically 100us.
    exposure_abs = max(1, int(round(float(exposure_us) / 100.0)))
    cmd = [
        ctl,
        "--device",
        dev,
        "--set-ctrl",
        f"exposure_auto=1,exposure_absolute={exposure_abs},gain={int(round(gain))}",
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=2.0)
        log(f"v4l2-ctl set ok: {out.strip() or 'no output'}")
    except Exception as exc:
        log(f"v4l2-ctl set failed: {exc}")


def set_roi_v4l2(dev: str, roi_x: int | None, roi_y: int | None, roi_center: str) -> None:
    ctl = shutil.which("v4l2-ctl")
    if not ctl:
        log("v4l2-ctl not found, skipping ROI setup")
        return
    have_offsets = (roi_x is not None) or (roi_y is not None)
    if not have_offsets and str(roi_center) == "auto":
        return

    ctrl_parts: list[str] = []
    center_mode = str(roi_center).strip().lower()
    if center_mode in ("on", "off"):
        ctrl_parts.append(f"roi_auto_center={1 if center_mode == 'on' else 0}")
    if have_offsets:
        # Manual offsets require center disabled.
        if center_mode == "auto":
            ctrl_parts.append("roi_auto_center=0")
        ctrl_parts.append("auto_functions_roi_control=0")
        if roi_x is not None:
            ctrl_parts.append(f"roi_offset_x={int(roi_x)}")
        if roi_y is not None:
            ctrl_parts.append(f"roi_offset_y={int(roi_y)}")
    if not ctrl_parts:
        return

    cmd = [ctl, "--device", dev, "--set-ctrl", ",".join(ctrl_parts)]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=2.0)
        log(f"v4l2 ROI set ok: {out.strip() or 'no output'}")
    except Exception as exc:
        log(f"v4l2 ROI set failed: {exc}")
        # Best-effort fallback: set controls one by one so unsupported controls
        # do not block the entire ROI setup.
        for part in ctrl_parts:
            one = [ctl, "--device", dev, "--set-ctrl", part]
            try:
                subprocess.check_output(one, stderr=subprocess.STDOUT, text=True, timeout=2.0)
                log(f"v4l2 ROI partial ok: {part}")
            except Exception as inner:
                log(f"v4l2 ROI partial failed ({part}): {inner}")


def _v4l2_supported_fps(device: str, width: int, height: int, pixel_code: str = "GREY") -> list[int]:
    ctl = shutil.which("v4l2-ctl")
    if not ctl:
        return []
    try:
        out = subprocess.check_output(
            [ctl, "-d", device, "--list-formats-ext"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3.0,
        )
    except Exception:
        return []

    wanted = pixel_code.strip().upper()
    in_fmt = False
    in_size = False
    vals: list[int] = []
    for raw_line in out.splitlines():
        line = raw_line.strip()
        m_fmt = re.search(r"\[\d+\]:\s*'([^']+)'", line)
        if m_fmt:
            in_fmt = (m_fmt.group(1).strip().upper() == wanted)
            in_size = False
            continue
        if not in_fmt:
            continue
        m_size = re.search(r"Size:\s*Discrete\s*(\d+)x(\d+)", line)
        if m_size:
            in_size = (int(m_size.group(1)) == int(width) and int(m_size.group(2)) == int(height))
            continue
        if not in_size:
            continue
        m_fps = re.search(r"\(([\d.]+)\s*fps\)", line, flags=re.IGNORECASE)
        if not m_fps:
            continue
        fps_i = int(round(float(m_fps.group(1))))
        if fps_i > 0 and fps_i not in vals:
            vals.append(fps_i)
    vals.sort(reverse=True)
    return vals


def _v4l2_supported_sizes(device: str, pixel_code: str = "GREY") -> list[tuple[int, int]]:
    ctl = shutil.which("v4l2-ctl")
    if not ctl:
        return []
    try:
        out = subprocess.check_output(
            [ctl, "-d", device, "--list-formats-ext"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3.0,
        )
    except Exception:
        return []

    wanted = pixel_code.strip().upper()
    in_fmt = False
    sizes: list[tuple[int, int]] = []
    for raw_line in out.splitlines():
        line = raw_line.strip()
        m_fmt = re.search(r"\[\d+\]:\s*'([^']+)'", line)
        if m_fmt:
            in_fmt = (m_fmt.group(1).strip().upper() == wanted)
            continue
        if not in_fmt:
            continue
        m_size = re.search(r"Size:\s*Discrete\s*(\d+)x(\d+)", line)
        if not m_size:
            continue
        size = (int(m_size.group(1)), int(m_size.group(2)))
        if size not in sizes:
            sizes.append(size)
    return sizes


def _pixel_mode(pixel_arg: str) -> tuple[str, str, str, int]:
    p = str(pixel_arg or "").strip().lower()
    if p in ("grey", "gray8", "8", "8bit"):
        return ("GREY", "GRAY8", "gray8", 1)
    if p in ("y16", "gray16", "gray16le", "16", "16bit"):
        return ("Y16", "GRAY16_LE", "y16", 2)
    raise ValueError(f"unsupported pixel format: {pixel_arg}")


def _pick_best_fps(supported: list[int], target: float, min_fps: int, max_fps: int) -> int:
    if not supported:
        return max(1, int(round(float(target))))
    in_band = [v for v in supported if int(min_fps) <= int(v) <= int(max_fps)]
    if in_band:
        return max(in_band)
    under_max = [v for v in supported if int(v) <= int(max_fps)]
    if under_max:
        return max(under_max)
    return max(supported)


def _select_device_for_mode(candidates: list[str], width: int, height: int, pixel_code: str) -> str | None:
    for dev in candidates:
        fps = _v4l2_supported_fps(dev, int(width), int(height), pixel_code)
        if fps:
            return dev
    return candidates[0] if candidates else None


def _write_raw_sidecar(
    raw_path: Path,
    *,
    width: int,
    height: int,
    pixel_code: str,
    gst_format: str,
    bytes_per_pixel: int,
    fps: int,
    duration_s: float,
    device: str,
) -> None:
    size = int(raw_path.stat().st_size if raw_path.exists() else 0)
    frame_bytes = int(width) * int(height) * int(bytes_per_pixel)
    frames = int(size // frame_bytes) if frame_bytes > 0 else 0
    meta = {
        "path": str(raw_path),
        "device": str(device),
        "width": int(width),
        "height": int(height),
        "pixel_code_v4l2": str(pixel_code),
        "pixel_format_gst": str(gst_format),
        "bytes_per_pixel": int(bytes_per_pixel),
        "requested_fps": int(fps),
        "duration_s": float(duration_s),
        "file_size_bytes": int(size),
        "frame_bytes": int(frame_bytes),
        "frames_from_size": int(frames),
        "fps_from_size": float(frames / max(1e-6, float(duration_s))),
    }
    sidecar = raw_path.with_suffix(raw_path.suffix + ".json")
    sidecar.write_text(json.dumps(meta, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    log(f"sidecar saved: {sidecar}")


def _iso_utc_from_unix_ns(unix_ns: int) -> str:
    return (
        dt.datetime.fromtimestamp(float(unix_ns) / 1_000_000_000.0, tz=dt.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _write_timestamps_sidecar(
    raw_path: Path,
    *,
    start_unix_ns: int,
    end_unix_ns: int,
    start_mono_ns: int,
    end_mono_ns: int,
    frame_count: int,
    frame_bytes: int,
    capture_fps_used: float,
    progress_samples: list[tuple[int, int]] | list[list[int]] | None,
) -> None:
    frame_count = max(0, int(frame_count))
    frame_bytes = max(1, int(frame_bytes))
    start_unix_ns = int(start_unix_ns)
    end_unix_ns = int(end_unix_ns)
    start_mono_ns = int(start_mono_ns)
    end_mono_ns = int(end_mono_ns)

    # Normalize samples: (mono_ns, size_bytes)
    samples_raw = progress_samples or []
    seq: list[tuple[int, float]] = []
    for it in samples_raw:
        try:
            mono_ns = int(it[0])
            size_b = int(it[1])
        except Exception:
            continue
        if mono_ns <= 0:
            continue
        frames_f = float(size_b) / float(frame_bytes)
        if frames_f < 0.0:
            frames_f = 0.0
        seq.append((mono_ns, frames_f))
    if not seq:
        seq = [(start_mono_ns, 0.0), (end_mono_ns, float(frame_count))]
    else:
        seq.sort(key=lambda x: x[0])
        if seq[0][0] > start_mono_ns:
            seq.insert(0, (start_mono_ns, 0.0))
        if seq[-1][0] < end_mono_ns:
            seq.append((end_mono_ns, float(frame_count)))

    # Enforce non-decreasing frame progression.
    fixed: list[tuple[int, float]] = []
    prev_f = 0.0
    prev_t = -1
    for mono_ns, frames_f in seq:
        if mono_ns <= prev_t:
            continue
        if frames_f < prev_f:
            frames_f = prev_f
        fixed.append((mono_ns, frames_f))
        prev_t = mono_ns
        prev_f = frames_f
    if len(fixed) < 2:
        fixed = [(start_mono_ns, 0.0), (end_mono_ns, float(frame_count))]

    csv_path = raw_path.with_suffix(raw_path.suffix + ".timestamps.csv")
    meta_path = raw_path.with_suffix(raw_path.suffix + ".timestamps.json")

    # Write per-frame timestamps via piecewise linear interpolation on (mono, written_frames).
    with csv_path.open("w", encoding="utf-8") as fh:
        fh.write("frame_idx,mono_ns,unix_ns,utc_iso\n")
        j = 0
        n = len(fixed)
        for idx in range(frame_count):
            target = float(idx + 1)
            while (j + 1) < n and fixed[j + 1][1] < target:
                j += 1
            if (j + 1) >= n:
                mono_ns = fixed[-1][0]
            else:
                t0, f0 = fixed[j]
                t1, f1 = fixed[j + 1]
                if f1 <= f0:
                    mono_ns = t1
                else:
                    a = (target - f0) / (f1 - f0)
                    if a < 0.0:
                        a = 0.0
                    elif a > 1.0:
                        a = 1.0
                    mono_ns = int(round(float(t0) + a * float(t1 - t0)))
            unix_ns = start_unix_ns + int(mono_ns - start_mono_ns)
            fh.write(f"{idx},{mono_ns},{unix_ns},{_iso_utc_from_unix_ns(unix_ns)}\n")

    meta = {
        "path_raw": str(raw_path),
        "path_timestamps_csv": str(csv_path),
        "frame_count": int(frame_count),
        "frame_bytes": int(frame_bytes),
        "capture_fps_used": float(capture_fps_used),
        "time_base": {
            "start_unix_ns": int(start_unix_ns),
            "end_unix_ns": int(end_unix_ns),
            "start_mono_ns": int(start_mono_ns),
            "end_mono_ns": int(end_mono_ns),
            "start_utc": _iso_utc_from_unix_ns(start_unix_ns),
            "end_utc": _iso_utc_from_unix_ns(end_unix_ns),
        },
        "model": "piecewise_linear_interpolation_from_progress_samples",
        "progress_sample_count": int(len(fixed)),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    log(f"timestamps saved: {csv_path}")
    log(f"timestamps meta saved: {meta_path}")


class TelemetryCollector:
    def __init__(
        self,
        *,
        enabled: bool,
        base_path: Path,
        device: str,
        baud: int,
        wait_heartbeat_s: float,
        message_types: str,
        max_rate_hz: float,
        request_streams: bool,
        request_types: str,
        request_rate_hz: float,
    ) -> None:
        self.enabled = bool(enabled)
        self.base_path = base_path
        self.device = str(device or "").strip()
        self.baud = int(baud)
        self.wait_heartbeat_s = float(wait_heartbeat_s)
        self.max_rate_hz = float(max_rate_hz)
        self.msg_filter = {
            x.strip().upper() for x in str(message_types or "").split(",") if x.strip()
        }
        self.request_streams = bool(request_streams)
        self.request_rate_hz = float(request_rate_hz)
        self.request_types = [
            x.strip().upper() for x in str(request_types or "").split(",") if x.strip()
        ]
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.error: str = ""
        self.connected = False
        self.count = 0
        self.count_by_type: dict[str, int] = {}
        self.start_unix_ns = 0
        self.end_unix_ns = 0
        self.start_mono_ns = 0
        self.end_mono_ns = 0
        self.jsonl_path = self.base_path.with_suffix(self.base_path.suffix + ".telemetry.jsonl")
        self.csv_path = self.base_path.with_suffix(self.base_path.suffix + ".telemetry.csv")
        self.meta_path = self.base_path.with_suffix(self.base_path.suffix + ".telemetry.meta.json")

    def start(self) -> None:
        if not self.enabled:
            return
        if not self.device:
            raise RuntimeError("telemetry enabled but --telemetry-device is empty")
        self.start_unix_ns = time.time_ns()
        self.start_mono_ns = time.monotonic_ns()
        self.thread = threading.Thread(target=self._run, name="telemetry", daemon=True)
        self.thread.start()
        log(
            f"telemetry started: device={self.device}, baud={self.baud}, "
            f"filter={sorted(self.msg_filter) if self.msg_filter else 'ALL'}"
        )

    def stop(self, timeout_s: float = 3.0) -> None:
        if not self.enabled:
            return
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=max(0.1, float(timeout_s)))
        self.end_unix_ns = self.end_unix_ns or time.time_ns()
        self.end_mono_ns = self.end_mono_ns or time.monotonic_ns()
        self._write_meta()
        if self.error:
            log(f"telemetry finished with error: {self.error}")
        else:
            log(
                f"telemetry finished: messages={self.count}, "
                f"types={len(self.count_by_type)}, file={self.jsonl_path}"
            )

    def _write_meta(self) -> None:
        try:
            meta = {
                "path_jsonl": str(self.jsonl_path),
                "path_csv": str(self.csv_path),
                "device": self.device,
                "baud": int(self.baud),
                "connected": bool(self.connected),
                "message_count": int(self.count),
                "message_count_by_type": self.count_by_type,
                "filter_types": sorted(self.msg_filter),
                "time_base": {
                    "start_unix_ns": int(self.start_unix_ns),
                    "end_unix_ns": int(self.end_unix_ns),
                    "start_mono_ns": int(self.start_mono_ns),
                    "end_mono_ns": int(self.end_mono_ns),
                    "start_utc": _iso_utc_from_unix_ns(int(self.start_unix_ns)) if self.start_unix_ns else "",
                    "end_utc": _iso_utc_from_unix_ns(int(self.end_unix_ns)) if self.end_unix_ns else "",
                },
                "error": self.error,
            }
            self.meta_path.write_text(json.dumps(meta, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
            log(f"telemetry meta saved: {self.meta_path}")
        except Exception as exc:
            log(f"telemetry meta write failed: {exc}")

    def _run(self) -> None:
        try:
            from pymavlink import mavutil  # type: ignore
        except Exception as exc:
            self.error = f"pymavlink import failed: {exc}"
            return

        def _msg_name_to_id(name: str) -> Optional[int]:
            n = str(name or "").strip().upper()
            if not n:
                return None
            if n.startswith("MAVLINK_MSG_ID_"):
                n = n[len("MAVLINK_MSG_ID_") :]
            key = f"MAVLINK_MSG_ID_{n}"
            try:
                return int(getattr(mavutil.mavlink, key))
            except Exception:
                return None

        def _request_message_interval(conn, msg_name: str, hz: float) -> bool:
            msg_id = _msg_name_to_id(msg_name)
            if msg_id is None:
                log(f"telemetry request skip: unknown MAVLink message '{msg_name}'")
                return False
            if hz <= 0.0:
                interval_us = -1  # stop stream
            else:
                interval_us = max(1, int(round(1_000_000.0 / hz)))
            tgt_sys = int(getattr(conn, "target_system", 0) or 0) or 1
            tgt_comp = int(getattr(conn, "target_component", 0) or 0) or 1
            try:
                conn.mav.command_long_send(
                    tgt_sys,
                    tgt_comp,
                    mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
                    0,
                    float(msg_id),
                    float(interval_us),
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                )
                log(
                    f"telemetry request sent: {msg_name} id={msg_id} "
                    f"interval_us={interval_us} target={tgt_sys}:{tgt_comp}"
                )
                return True
            except Exception as exc:
                log(f"telemetry request failed for {msg_name}: {exc}")
                return False

        conn = None
        json_fh = None
        csv_fh = None
        csv_wr = None
        last_emit_ns: dict[str, int] = {}
        min_interval_ns = int(round(1_000_000_000.0 / self.max_rate_hz)) if self.max_rate_hz > 0.0 else 0
        try:
            json_fh = self.jsonl_path.open("w", encoding="utf-8")
            csv_fh = self.csv_path.open("w", encoding="utf-8", newline="")
            csv_wr = csv.writer(csv_fh)
            csv_wr.writerow(
                [
                    "idx",
                    "mono_ns",
                    "unix_ns",
                    "utc_iso",
                    "msg_type",
                    "src_system",
                    "src_component",
                    "time_boot_ms",
                    "time_usec",
                    "lat",
                    "lon",
                    "alt",
                    "relative_alt",
                    "vx",
                    "vy",
                    "vz",
                    "x",
                    "y",
                    "z",
                    "roll",
                    "pitch",
                    "yaw",
                    "heading",
                    "fix_type",
                    "satellites_visible",
                ]
            )
            conn = mavutil.mavlink_connection(self.device, baud=self.baud, autoreconnect=True)
            if self.wait_heartbeat_s > 0.0:
                try:
                    conn.wait_heartbeat(timeout=self.wait_heartbeat_s)
                    self.connected = True
                    log("telemetry heartbeat received")
                except Exception:
                    log("telemetry heartbeat timeout; continue without heartbeat")
            else:
                self.connected = True

            if self.request_streams and self.request_types:
                req_hz = self.request_rate_hz if self.request_rate_hz > 0.0 else 20.0
                ok_cnt = 0
                for name in self.request_types:
                    if _request_message_interval(conn, name, req_hz):
                        ok_cnt += 1
                log(
                    f"telemetry stream request: requested={len(self.request_types)} "
                    f"accepted_send={ok_cnt} rate_hz={req_hz:.1f}"
                )

            idx = 0
            while not self.stop_event.is_set() and not STOP_REQUESTED.is_set():
                msg = conn.recv_match(blocking=True, timeout=0.20)
                if msg is None:
                    continue
                mtype = str(msg.get_type() or "").upper()
                if not mtype or mtype == "BAD_DATA":
                    continue
                self.connected = True
                if self.msg_filter and mtype not in self.msg_filter:
                    continue
                mono_ns = time.monotonic_ns()
                if min_interval_ns > 0:
                    last_ns = int(last_emit_ns.get(mtype, 0))
                    if last_ns > 0 and (mono_ns - last_ns) < min_interval_ns:
                        continue
                    last_emit_ns[mtype] = mono_ns
                unix_ns = time.time_ns()
                src_sys = 0
                src_comp = 0
                try:
                    src_sys = int(msg.get_srcSystem())
                    src_comp = int(msg.get_srcComponent())
                except Exception:
                    pass
                data = msg.to_dict() if hasattr(msg, "to_dict") else {}
                idx += 1
                rec = {
                    "idx": idx,
                    "mono_ns": int(mono_ns),
                    "unix_ns": int(unix_ns),
                    "utc_iso": _iso_utc_from_unix_ns(int(unix_ns)),
                    "msg_type": mtype,
                    "src_system": src_sys,
                    "src_component": src_comp,
                    "data": data,
                }
                json_fh.write(json.dumps(rec, ensure_ascii=True) + "\n")
                csv_wr.writerow(
                    [
                        idx,
                        mono_ns,
                        unix_ns,
                        rec["utc_iso"],
                        mtype,
                        src_sys,
                        src_comp,
                        data.get("time_boot_ms", ""),
                        data.get("time_usec", ""),
                        data.get("lat", ""),
                        data.get("lon", ""),
                        data.get("alt", ""),
                        data.get("relative_alt", ""),
                        data.get("vx", ""),
                        data.get("vy", ""),
                        data.get("vz", ""),
                        data.get("x", ""),
                        data.get("y", ""),
                        data.get("z", ""),
                        data.get("roll", ""),
                        data.get("pitch", ""),
                        data.get("yaw", ""),
                        data.get("heading", data.get("hdg", "")),
                        data.get("fix_type", ""),
                        data.get("satellites_visible", ""),
                    ]
                )
                self.count = idx
                self.count_by_type[mtype] = int(self.count_by_type.get(mtype, 0)) + 1

            try:
                json_fh.flush()
                csv_fh.flush()
            except Exception:
                pass
        except Exception as exc:
            self.error = str(exc)
        finally:
            self.end_unix_ns = time.time_ns()
            self.end_mono_ns = time.monotonic_ns()
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            try:
                if json_fh is not None:
                    json_fh.close()
            except Exception:
                pass
            try:
                if csv_fh is not None:
                    csv_fh.close()
            except Exception:
                pass


def _stream_subprocess_output(prefix: str, pipe) -> None:
    try:
        for line in iter(pipe.readline, ""):
            text = str(line).strip()
            if text:
                log(f"{prefix}{text}")
    except Exception:
        pass


def record_gst_raw(
    *,
    device: str,
    out_path: Path,
    duration_s: float,
    width: int,
    height: int,
    pixel_code: str,
    gst_format: str,
    target_fps: float,
    min_fps: int,
    max_fps: int,
    crop_top: int = 0,
    crop_bottom: int = 0,
    crop_left: int = 0,
    crop_right: int = 0,
) -> tuple[Path, dict[str, float]]:
    gst = shutil.which("gst-launch-1.0")
    if not gst:
        raise RuntimeError("gst-launch-1.0 not found")

    supported = _v4l2_supported_fps(device, int(width), int(height), pixel_code)
    if supported:
        log(f"v4l2 supported fps for {pixel_code} {width}x{height}: {supported}")
    fps_best = _pick_best_fps(supported, float(target_fps), int(min_fps), int(max_fps))

    fps_candidates: list[int] = []

    def add_fps(v: int) -> None:
        iv = int(v)
        if iv > 0 and iv not in fps_candidates:
            fps_candidates.append(iv)

    add_fps(fps_best)
    for val in supported:
        if int(min_fps) <= int(val) <= int(max_fps):
            add_fps(int(val))
    for val in supported:
        if int(val) <= int(max_fps):
            add_fps(int(val))
    for val in supported:
        add_fps(int(val))
    add_fps(int(round(float(target_fps))))
    for val in (120, 119, 100, 90, 60, 30):
        add_fps(val)

    out_width = int(width) - int(crop_left) - int(crop_right)
    out_height = int(height) - int(crop_top) - int(crop_bottom)
    if out_width <= 0 or out_height <= 0:
        raise RuntimeError(
            f"invalid crop: input={int(width)}x{int(height)}, "
            f"crop l/r/t/b={int(crop_left)}/{int(crop_right)}/{int(crop_top)}/{int(crop_bottom)}"
        )
    bytes_per_pixel = 2 if str(pixel_code).upper() == "Y16" else 1
    frame_bytes = max(1, int(out_width) * int(out_height) * int(bytes_per_pixel))
    last_err = "unknown"
    for fps_i in fps_candidates:
        attempt_start_unix_ns = time.time_ns()
        attempt_start_mono_ns = time.monotonic_ns()
        progress_samples: list[tuple[int, int]] = [(attempt_start_mono_ns, 0)]
        cmd = [
            gst,
            "-e",
            "v4l2src",
            f"device={device}",
            "io-mode=2",
            "do-timestamp=true",
            "!",
            f"video/x-raw,format={gst_format},width={int(width)},height={int(height)},framerate={int(fps_i)}/1",
        ]
        if int(crop_top) or int(crop_bottom) or int(crop_left) or int(crop_right):
            cmd += [
                "!",
                "videocrop",
                f"top={int(crop_top)}",
                f"bottom={int(crop_bottom)}",
                f"left={int(crop_left)}",
                f"right={int(crop_right)}",
                "!",
                f"video/x-raw,format={gst_format},width={int(out_width)},height={int(out_height)},framerate={int(fps_i)}/1",
            ]
        cmd += [
            "!",
            "queue",
            "max-size-buffers=2048",
            "leaky=downstream",
            "!",
            "filesink",
            f"location={str(out_path)}",
            "sync=false",
        ]
        log(f"gst raw start (fps={fps_i}): {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        reader = None
        if proc.stdout is not None:
            reader = threading.Thread(
                target=_stream_subprocess_output,
                args=("[gst] ", proc.stdout),
                name="gst-log",
                daemon=True,
            )
            reader.start()

        t0 = time.monotonic()
        ok = True
        interrupted = False
        last_log = 0.0
        while True:
            elapsed = time.monotonic() - t0
            mono_now_ns = time.monotonic_ns()
            try:
                size_now_b = int(out_path.stat().st_size)
            except Exception:
                size_now_b = 0
            if not progress_samples or size_now_b != progress_samples[-1][1] or (
                mono_now_ns - progress_samples[-1][0]
            ) >= 200_000_000:
                progress_samples.append((mono_now_ns, size_now_b))
            if STOP_REQUESTED.is_set():
                interrupted = True
                break
            if elapsed >= float(duration_s):
                break
            rc = proc.poll()
            if rc is not None:
                size_b_now = float(out_path.stat().st_size if out_path.exists() else 0.0)
                frames_est_now = float(size_b_now / float(frame_bytes))
                fps_est_now = float(frames_est_now / max(1e-6, elapsed))
                early_good = (
                    elapsed >= float(duration_s) * 0.80
                    and fps_est_now >= max(1.0, float(min_fps) * 0.50)
                    and size_b_now >= float(frame_bytes) * max(1.0, float(min_fps) * float(duration_s) * 0.25)
                )
                if early_good:
                    log(
                        "gst raw finished before timer but accepted: "
                        f"rc={rc}, elapsed={elapsed:.3f}s, fps_est={fps_est_now:.1f}"
                    )
                    ok = True
                    break
                ok = False
                last_err = (
                    f"gst exited early with code {rc} at fps={fps_i}; "
                    f"elapsed={elapsed:.3f}s fps_est={fps_est_now:.1f}"
                )
                break
            if elapsed - last_log >= 1.0:
                last_log = elapsed
                size_mb = 0.0
                try:
                    size_mb = out_path.stat().st_size / (1024.0 * 1024.0)
                except Exception:
                    pass
                log(f"gst raw telemetry: elapsed={elapsed:.1f}s size={size_mb:.1f}MB")
            time.sleep(0.05)

        if ok:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                pass
            try:
                rc = proc.wait(timeout=8.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                rc = proc.wait(timeout=3.0)
            if reader is not None:
                reader.join(timeout=1.0)
            elapsed_total = max(1e-6, time.monotonic() - t0)
            attempt_end_unix_ns = time.time_ns()
            attempt_end_mono_ns = time.monotonic_ns()
            size_b = float(out_path.stat().st_size if out_path.exists() else 0)
            if not progress_samples or int(size_b) != progress_samples[-1][1]:
                progress_samples.append((attempt_end_mono_ns, int(size_b)))
            frames_est = float(size_b / float(frame_bytes))
            fps_est = float(frames_est / elapsed_total)
            # On some Jetson builds gst-launch may return non-zero on interrupt
            # even after valid EOS/write. Accept by data/elapsed evidence.
            if interrupted:
                good_by_data = size_b >= float(frame_bytes)
            else:
                good_by_data = (
                    elapsed_total >= float(duration_s) * 0.85
                    and fps_est >= max(1.0, float(min_fps) * 0.50)
                    and size_b >= float(frame_bytes) * max(1.0, float(min_fps) * float(duration_s) * 0.30)
                )
            if rc in (0, 130) or good_by_data:
                if rc not in (0, 130):
                    log(
                        "gst raw accepted by data despite non-zero rc: "
                        f"rc={rc}, elapsed={elapsed_total:.3f}s, fps_est={fps_est:.1f}"
                    )
                if interrupted:
                    log("gst raw accepted after stop request")
                stats = {
                    "elapsed_s": elapsed_total,
                    "read": 0.0,
                    "enq": 0.0,
                    "written": 0.0,
                    "q_drop": 0.0,
                    "read_fail": 0.0,
                    "write_fail": 0.0,
                    "read_fps_avg": float(fps_i),
                    "write_fps_avg": float(fps_i),
                    "capture_fps_used": float(fps_i),
                    "file_size_bytes": size_b,
                    "frames_from_size": frames_est,
                    "output_width": float(out_width),
                    "output_height": float(out_height),
                    "start_unix_ns": float(attempt_start_unix_ns),
                    "end_unix_ns": float(attempt_end_unix_ns),
                    "start_mono_ns": float(attempt_start_mono_ns),
                    "end_mono_ns": float(attempt_end_mono_ns),
                    "frame_bytes": float(frame_bytes),
                    "progress_samples": [[int(t), int(b)] for t, b in progress_samples],
                    "interrupted": 1.0 if interrupted else 0.0,
                    "backend_gst_raw": 1.0,
                }
                return out_path, stats
            last_err = (
                f"gst failed with code {rc} at fps={fps_i}; "
                f"elapsed={elapsed_total:.3f}s fps_est={fps_est:.1f}"
            )
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass
        else:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=2.0)
            except Exception:
                pass
            if reader is not None:
                reader.join(timeout=1.0)
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass
            time.sleep(0.15)

    raise RuntimeError(last_err)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Headless UVC recorder for Jetson/TIS camera")
    p.add_argument("--duration", type=float, default=5.0, help="Recording duration in seconds")
    p.add_argument("--target-fps", type=float, default=100.0, help="Target FPS")
    p.add_argument("--min-fps", type=int, default=100, help="Minimal acceptable FPS for auto-pick")
    p.add_argument("--max-fps", type=int, default=120, help="Maximal acceptable FPS for auto-pick")
    p.add_argument("--exposure-us", type=float, default=9500.0, help="Exposure in microseconds")
    p.add_argument("--gain", type=float, default=1.0, help="Gain value")
    p.add_argument("--camera-id", default="", help="Camera id/serial hint (optional)")
    p.add_argument("--device", default="", help="Force /dev/videoX (optional)")
    p.add_argument("--width", type=int, default=1024, help="Requested capture width (default: 1024)")
    p.add_argument("--height", type=int, default=768, help="Requested capture height (default: 768)")
    p.add_argument("--crop-top", type=int, default=0, help="Software crop pixels from top (after capture)")
    p.add_argument("--crop-bottom", type=int, default=0, help="Software crop pixels from bottom (after capture)")
    p.add_argument("--crop-left", type=int, default=0, help="Software crop pixels from left (after capture)")
    p.add_argument("--crop-right", type=int, default=0, help="Software crop pixels from right (after capture)")
    p.add_argument("--roi-x", type=int, default=None, help="ROI offset X (sensor coordinates)")
    p.add_argument("--roi-y", type=int, default=None, help="ROI offset Y (sensor coordinates)")
    p.add_argument(
        "--roi-center",
        choices=["auto", "on", "off"],
        default="auto",
        help="ROI auto-center control (auto/on/off). If --roi-x/--roi-y are set, center is forced off.",
    )
    p.add_argument(
        "--pixel-format",
        choices=["gray8", "y16"],
        default="gray8",
        help="Capture pixel format: gray8 or y16",
    )
    p.add_argument("--snapshot-dir", default="capture", help="Output directory")
    p.add_argument(
        "--backend",
        choices=["gst-raw"],
        default="gst-raw",
        help="Recording backend (fixed): gst-raw",
    )
    p.add_argument("--telemetry-enable", action="store_true", help="Enable MAVLink telemetry capture in parallel")
    p.add_argument("--telemetry-device", default="", help="Telemetry serial device (e.g. /dev/ttyACM0)")
    p.add_argument("--telemetry-baud", type=int, default=115200, help="Telemetry serial baudrate")
    p.add_argument(
        "--telemetry-wait-heartbeat",
        type=float,
        default=5.0,
        help="Wait heartbeat timeout in seconds (0 disables wait)",
    )
    p.add_argument(
        "--telemetry-msg-types",
        default="",
        help="Optional comma-separated MAVLink types filter (e.g. ATTITUDE,GPS_RAW_INT)",
    )
    p.add_argument(
        "--telemetry-max-rate-hz",
        type=float,
        default=0.0,
        help="Optional per-message-type max output rate (0 disables throttling)",
    )
    p.add_argument(
        "--telemetry-request-streams",
        choices=["on", "off"],
        default="on",
        help="Actively request MAVLink streams from FC (MAV_CMD_SET_MESSAGE_INTERVAL)",
    )
    p.add_argument(
        "--telemetry-request-types",
        default="ATTITUDE,LOCAL_POSITION_NED,GLOBAL_POSITION_INT",
        help="Comma-separated MAVLink message names to request from FC",
    )
    p.add_argument(
        "--telemetry-request-rate-hz",
        type=float,
        default=50.0,
        help="Requested rate for telemetry-request-types",
    )
    return p.parse_args()


def main() -> int:
    STOP_REQUESTED.clear()
    args = parse_args()
    out_dir = Path(args.snapshot_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    serial_hint = extract_serial_hint(args.camera_id)
    telemetry_device = str(args.telemetry_device or "").strip()
    if bool(args.telemetry_enable) and not telemetry_device:
        telem_candidates = find_telemetry_serial_candidates()
        if telem_candidates:
            telemetry_device = telem_candidates[0]
            log(f"telemetry auto device: {telemetry_device}")
        else:
            log("failed: telemetry enabled but no serial device found (/dev/ttyACM* or /dev/ttyUSB*)")
            return 2
    log(
        f"start: duration={args.duration:.2f}s target_fps={args.target_fps:.1f} "
        f"exposure_us={args.exposure_us:.1f} gain={args.gain:.2f} "
        f"crop(l/r/t/b)=({int(args.crop_left)}/{int(args.crop_right)}/{int(args.crop_top)}/{int(args.crop_bottom)}) "
        f"roi=({args.roi_x if args.roi_x is not None else '-'},"
        f"{args.roi_y if args.roi_y is not None else '-'}) "
        f"roi_center={args.roi_center} serial_hint={serial_hint or '-'} "
        f"telemetry={'on' if bool(args.telemetry_enable) else 'off'} "
        f"telemetry_device={telemetry_device or '-'} telemetry_baud={int(args.telemetry_baud)}"
    )

    log("record backend: gst-raw")
    try:
        pixel_code, gst_format, mode_tag, bpp = _pixel_mode(args.pixel_format)
    except Exception as exc:
        log(f"failed: {exc}")
        return 2
    candidates = [str(args.device)] if str(args.device) else find_uvc_candidates(serial_hint)
    if not candidates:
        log("failed: no /dev/video* camera candidates found")
        return 2
    dev = _select_device_for_mode(candidates, int(args.width), int(args.height), pixel_code)
    if not dev:
        log(
            f"failed: no camera supports {pixel_code} at "
            f"{int(args.width)}x{int(args.height)}"
        )
        return 2
    cap_w = int(args.width)
    cap_h = int(args.height)
    out_w = cap_w - int(args.crop_left) - int(args.crop_right)
    out_h = cap_h - int(args.crop_top) - int(args.crop_bottom)
    if out_w <= 0 or out_h <= 0:
        log(
            f"failed: invalid crop for {cap_w}x{cap_h}: "
            f"l/r/t/b={int(args.crop_left)}/{int(args.crop_right)}/{int(args.crop_top)}/{int(args.crop_bottom)}"
        )
        return 2
    log(f"camera selected: dev={dev}, mode={cap_w}x{cap_h}, pixel={pixel_code}, out={out_w}x{out_h}")
    supported_sizes = _v4l2_supported_sizes(dev, pixel_code)
    supported_fps = _v4l2_supported_fps(dev, cap_w, cap_h, pixel_code)
    if not supported_fps:
        log(
            f"failed: unsupported mode for {pixel_code}: {cap_w}x{cap_h}. "
            f"Supported sizes: {supported_sizes}"
        )
        log(
            "tip: keep hardware mode 1024x768 and crop in processing, or choose one of supported sizes above"
        )
        return 2
    set_roi_v4l2(dev, args.roi_x, args.roi_y, str(args.roi_center))
    set_controls_v4l2(dev, args.exposure_us, args.gain)
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = out_dir / f"headless_{ts}_{mode_tag}_{out_w}x{out_h}.raw"
    telemetry = TelemetryCollector(
        enabled=bool(args.telemetry_enable),
        base_path=out_path,
        device=telemetry_device,
        baud=int(args.telemetry_baud),
        wait_heartbeat_s=float(args.telemetry_wait_heartbeat),
        message_types=str(args.telemetry_msg_types),
        max_rate_hz=float(args.telemetry_max_rate_hz),
        request_streams=(str(args.telemetry_request_streams).lower() != "off"),
        request_types=str(args.telemetry_request_types),
        request_rate_hz=float(args.telemetry_request_rate_hz),
    )
    try:
        telemetry.start()
        try:
            saved_path, stats = record_gst_raw(
                device=dev,
                out_path=out_path,
                duration_s=float(args.duration),
                width=cap_w,
                height=cap_h,
                pixel_code=pixel_code,
                gst_format=gst_format,
                target_fps=float(args.target_fps),
                min_fps=int(args.min_fps),
                max_fps=int(args.max_fps),
                crop_top=int(args.crop_top),
                crop_bottom=int(args.crop_bottom),
                crop_left=int(args.crop_left),
                crop_right=int(args.crop_right),
            )
        finally:
            telemetry.stop()
    except Exception as exc:
        log(f"failed: gst-raw backend error: {exc}")
        return 3
    _write_raw_sidecar(
        saved_path,
        width=int(stats.get("output_width", out_w)),
        height=int(stats.get("output_height", out_h)),
        pixel_code=pixel_code,
        gst_format=gst_format,
        bytes_per_pixel=int(bpp),
        fps=int(round(float(stats.get("capture_fps_used", 0.0) or 0.0))),
        duration_s=float(stats.get("elapsed_s", args.duration)),
        device=dev,
    )
    _write_timestamps_sidecar(
        saved_path,
        start_unix_ns=int(stats.get("start_unix_ns", time.time_ns())),
        end_unix_ns=int(stats.get("end_unix_ns", time.time_ns())),
        start_mono_ns=int(stats.get("start_mono_ns", time.monotonic_ns())),
        end_mono_ns=int(stats.get("end_mono_ns", time.monotonic_ns())),
        frame_count=int(stats.get("frames_from_size", 0)),
        frame_bytes=int(stats.get("frame_bytes", int(out_w) * int(out_h) * int(bpp))),
        capture_fps_used=float(stats.get("capture_fps_used", 0.0)),
        progress_samples=stats.get("progress_samples"),
    )
    log(
        "done: "
        f"file={saved_path} pixel={pixel_code} "
        f"capture_fps_used={stats.get('capture_fps_used', 0.0):.1f} "
        f"size_mb={(float(stats.get('file_size_bytes', 0.0)) / (1024.0 * 1024.0)):.1f}"
    )
    if telemetry.enabled:
        log(
            "telemetry done: "
            f"messages={telemetry.count} "
            f"jsonl={telemetry.jsonl_path} "
            f"csv={telemetry.csv_path} "
            f"meta={telemetry.meta_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
