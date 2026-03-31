#!/usr/bin/env python3
"""
Hydra calibration/capture GUI for Baumer GigE cameras (macOS).

Workflow:
1) Connect page (Interface/IP/Auto Find-Fix)
2) Choice page (calibrate or load existing calibration)
3) Calibration wizard:
   - Crop calibration (16 white points, global offsets, manual point drag)
   - Black level (dark map burst)
   - Flat field (flat map burst)
   - Geometry (5 valid chessboard captures, homography solve)
4) Main capture page (grid/single-lens preview and snapshot export).
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import math
import os
import queue
import re
import shutil
import socket
import struct
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, ttk

SIOCGIFADDR = 0xC0206921
DEFAULT_PACKET_SIZE = 1440
DEFAULT_PACKET_DELAY = 1000
DEFAULT_UI_POLL_MS = 10
DEFAULT_PREVIEW_FPS = 10.0

# Avoid OpenCV OpenCL/Metal runtime crashes on macOS during chessboard detection.
os.environ.setdefault("OPENCV_OPENCL_RUNTIME", "disabled")
os.environ.setdefault("OPENCV_OPENCL_CACHE_ENABLE", "0")

try:
    import numpy as np
except Exception:  # pragma: no cover - runtime dependency
    np = None  # type: ignore[assignment]

try:
    import cv2
except Exception:  # pragma: no cover - runtime dependency
    cv2 = None  # type: ignore[assignment]
else:
    try:
        if hasattr(cv2, "ocl") and hasattr(cv2.ocl, "setUseOpenCL"):
            cv2.ocl.setUseOpenCL(False)
    except Exception:
        pass
    try:
        if hasattr(cv2, "setNumThreads"):
            cv2.setNumThreads(1)
    except Exception:
        pass

try:
    from PIL import Image, ImageTk
except Exception:  # pragma: no cover - runtime dependency
    Image = None  # type: ignore[assignment]
    ImageTk = None  # type: ignore[assignment]

try:
    from scipy.ndimage import convolve, convolve1d, gaussian_filter
except Exception:  # pragma: no cover - runtime dependency
    convolve = None  # type: ignore[assignment]
    convolve1d = None  # type: ignore[assignment]
    gaussian_filter = None  # type: ignore[assignment]

from baumer_capture_one import configure_aravis_gige_interface, open_camera_with_fallback
from camera_control import read_buffer_metadata, read_camera_runtime_metadata
from raw_decode import decode_buffer_to_ndarray, pixel_format_to_name

try:
    from baumer_force_ip import send_force_ip as gvcp_force_ip
    from baumer_gvcp_explorer import discover as gvcp_discover

    AUTO_FIX_AVAILABLE = True
except Exception:
    gvcp_force_ip = None  # type: ignore[assignment]
    gvcp_discover = None  # type: ignore[assignment]
    AUTO_FIX_AVAILABLE = False


def get_interface_ipv4(interface: str) -> str | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        packed = struct.pack("256s", interface[:15].encode("ascii", errors="ignore"))
        data = fcntl.ioctl(sock.fileno(), SIOCGIFADDR, packed)
        return socket.inet_ntoa(data[20:24])
    except OSError:
        return None
    finally:
        sock.close()


def _decode_ifconfig_netmask(token: str) -> str:
    if token.startswith("0x"):
        try:
            return socket.inet_ntoa(struct.pack(">I", int(token, 16)))
        except Exception:
            return "255.255.255.0"
    return token


def get_interface_ipv4_entries(interface: str) -> list[tuple[str, str]]:
    try:
        txt = subprocess.check_output(["ifconfig", interface], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return []
    entries: list[tuple[str, str]] = []
    for line in txt.splitlines():
        parts = line.strip().split()
        if len(parts) < 4 or parts[0] != "inet" or parts[2] != "netmask":
            continue
        ip = parts[1]
        if ip.startswith("127."):
            continue
        entries.append((ip, _decode_ifconfig_netmask(parts[3])))
    return entries


def is_ipv4_literal(value: str) -> bool:
    try:
        socket.inet_aton(value)
        return value.count(".") == 3
    except OSError:
        return False


def get_interface_ipv4_for_peer(interface: str, peer_ip: str) -> str | None:
    entries = get_interface_ipv4_entries(interface)
    if not entries:
        return get_interface_ipv4(interface)
    if is_ipv4_literal(peer_ip):
        for ip, mask in entries:
            if same_subnet(ip, peer_ip, mask):
                return ip
    return entries[0][0]


def get_interface_netmask(interface: str) -> str | None:
    try:
        txt = subprocess.check_output(["ifconfig", interface], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return None
    for line in txt.splitlines():
        parts = line.strip().split()
        if len(parts) >= 4 and parts[0] == "inet" and parts[2] == "netmask":
            nm = parts[3]
            if nm.startswith("0x"):
                try:
                    val = int(nm, 16)
                    return socket.inet_ntoa(struct.pack(">I", val))
                except Exception:
                    return None
            return nm
    return None


def ipv4_to_u32(value: str) -> int:
    return int.from_bytes(socket.inet_aton(value), "big", signed=False)


def ip_to_int(ip: str) -> int:
    return int.from_bytes(socket.inet_aton(ip), "big", signed=False)


def same_subnet(ip_a: str, ip_b: str, mask: str) -> bool:
    try:
        ma = ip_to_int(mask)
        return (ip_to_int(ip_a) & ma) == (ip_to_int(ip_b) & ma)
    except Exception:
        return False


def suggest_camera_ip(host_ip: str) -> str:
    parts = host_ip.split(".")
    if len(parts) != 4:
        return "192.168.88.1"
    try:
        a, b, c, _d = [int(p) for p in parts]
    except ValueError:
        return "192.168.88.1"
    return f"{a}.{b}.{c}.1"


def masks_cfa_bayer(shape: tuple[int, int], pattern: str = "RGGB") -> tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
    if np is None:
        raise RuntimeError("NumPy is unavailable")
    h, w = shape
    y, x = np.indices((h, w))
    y_even = (y % 2) == 0
    x_even = (x % 2) == 0
    p = pattern.upper()
    if p == "RGGB":
        r = y_even & x_even
        b = (~y_even) & (~x_even)
    elif p == "BGGR":
        r = (~y_even) & (~x_even)
        b = y_even & x_even
    elif p == "GRBG":
        r = y_even & (~x_even)
        b = (~y_even) & x_even
    elif p == "GBRG":
        r = (~y_even) & x_even
        b = y_even & (~x_even)
    else:
        raise ValueError(f"Unsupported Bayer pattern: {pattern}")
    g = ~(r | b)
    return r.astype(np.float32), g.astype(np.float32), b.astype(np.float32)


def _cnv_h(x: "np.ndarray", kernel: "np.ndarray") -> "np.ndarray":
    if convolve1d is None:
        raise RuntimeError("SciPy convolve1d is unavailable")
    return convolve1d(x, kernel, mode="mirror")


def _cnv_v(x: "np.ndarray", kernel: "np.ndarray") -> "np.ndarray":
    if convolve1d is None:
        raise RuntimeError("SciPy convolve1d is unavailable")
    return convolve1d(x, kernel, mode="mirror", axis=0)


def demosaic_bayer_menon2007(cfa: "np.ndarray", pattern: str = "RGGB") -> "np.ndarray":
    if np is None or convolve is None or convolve1d is None:
        raise RuntimeError("NumPy/SciPy demosaic dependencies are unavailable")

    cfa = np.asarray(cfa, dtype=np.float32)
    r_m, g_m, b_m = masks_cfa_bayer(cfa.shape, pattern)

    h_0 = np.asarray([0.0, 0.5, 0.0, 0.5, 0.0], dtype=np.float32)
    h_1 = np.asarray([-0.25, 0.0, 0.5, 0.0, -0.25], dtype=np.float32)

    r = cfa * r_m
    g = cfa * g_m
    b = cfa * b_m

    g_h = np.where(g_m == 0, _cnv_h(cfa, h_0) + _cnv_h(cfa, h_1), g)
    g_v = np.where(g_m == 0, _cnv_v(cfa, h_0) + _cnv_v(cfa, h_1), g)

    c_h = np.where(r_m == 1, r - g_h, 0)
    c_h = np.where(b_m == 1, b - g_h, c_h)

    c_v = np.where(r_m == 1, r - g_v, 0)
    c_v = np.where(b_m == 1, b - g_v, c_v)

    d_h = np.abs(c_h - np.pad(c_h, ((0, 0), (0, 2)), mode="reflect")[:, 2:])
    d_v = np.abs(c_v - np.pad(c_v, ((0, 2), (0, 0)), mode="reflect")[2:, :])

    k = np.asarray(
        [
            [0.0, 0.0, 1.0, 0.0, 1.0],
            [0.0, 0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 3.0, 0.0, 3.0],
            [0.0, 0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0, 0.0, 1.0],
        ],
        dtype=np.float32,
    )
    d_h = convolve(d_h, k, mode="constant")
    d_v = convolve(d_v, np.transpose(k), mode="constant")
    m = d_v >= d_h
    g = np.where(m, g_h, g_v)
    mf = np.where(m, 1.0, 0.0)

    r_rows = np.transpose(np.any(r_m == 1, axis=1)[None]) * np.ones(r.shape, dtype=np.float32)
    b_rows = np.transpose(np.any(b_m == 1, axis=1)[None]) * np.ones(b.shape, dtype=np.float32)
    k_b = np.asarray([0.5, 0.0, 0.5], dtype=np.float32)

    r = np.where(np.logical_and(g_m == 1, r_rows == 1), g + _cnv_h(r, k_b) - _cnv_h(g, k_b), r)
    r = np.where(np.logical_and(g_m == 1, b_rows == 1), g + _cnv_v(r, k_b) - _cnv_v(g, k_b), r)
    b = np.where(np.logical_and(g_m == 1, b_rows == 1), g + _cnv_h(b, k_b) - _cnv_h(g, k_b), b)
    b = np.where(np.logical_and(g_m == 1, r_rows == 1), g + _cnv_v(b, k_b) - _cnv_v(g, k_b), b)

    r = np.where(
        np.logical_and(b_rows == 1, b_m == 1),
        np.where(mf == 1, b + _cnv_h(r, k_b) - _cnv_h(b, k_b), b + _cnv_v(r, k_b) - _cnv_v(b, k_b)),
        r,
    )
    b = np.where(
        np.logical_and(r_rows == 1, r_m == 1),
        np.where(mf == 1, r + _cnv_h(b, k_b) - _cnv_h(r, k_b), r + _cnv_v(b, k_b) - _cnv_v(r, k_b)),
        b,
    )
    return np.stack([r, g, b], axis=-1)


def demosaic_bayer_fast_preview(cfa: "np.ndarray", pattern: str = "RGGB") -> "np.ndarray":
    if np is None:
        raise RuntimeError("NumPy is unavailable")
    cfa = np.asarray(cfa, dtype=np.float32)
    r_m, g_m, b_m = masks_cfa_bayer(cfa.shape, pattern)
    r_mask = r_m > 0.5
    g_mask = g_m > 0.5
    b_mask = b_m > 0.5

    p = np.pad(cfa, ((1, 1), (1, 1)), mode="edge")
    n = p[:-2, 1:-1]
    s = p[2:, 1:-1]
    w = p[1:-1, :-2]
    e = p[1:-1, 2:]
    nw = p[:-2, :-2]
    ne = p[:-2, 2:]
    sw = p[2:, :-2]
    se = p[2:, 2:]

    g = cfa.copy()
    g_est = (n + s + w + e) * 0.25
    g[~g_mask] = g_est[~g_mask]

    r = np.zeros_like(cfa, dtype=np.float32)
    b = np.zeros_like(cfa, dtype=np.float32)
    r[r_mask] = cfa[r_mask]
    b[b_mask] = cfa[b_mask]

    r_rows = np.any(r_mask, axis=1)[:, None]
    b_rows = np.any(b_mask, axis=1)[:, None]
    g_on_r_rows = np.logical_and(g_mask, r_rows)
    g_on_b_rows = np.logical_and(g_mask, b_rows)
    diag = (nw + ne + sw + se) * 0.25

    r[g_on_r_rows] = ((w + e) * 0.5)[g_on_r_rows]
    r[g_on_b_rows] = ((n + s) * 0.5)[g_on_b_rows]
    r[b_mask] = diag[b_mask]

    b[g_on_b_rows] = ((w + e) * 0.5)[g_on_b_rows]
    b[g_on_r_rows] = ((n + s) * 0.5)[g_on_r_rows]
    b[r_mask] = diag[r_mask]

    return np.stack([r, g, b], axis=-1)


def raw_to_u8(raw_array: "np.ndarray", fmt_name: str, autostretch: bool = False) -> "np.ndarray":
    if np is None:
        raise RuntimeError("NumPy is unavailable")
    if autostretch:
        arr = raw_array.astype(np.float32, copy=False)
        lo = float(np.percentile(arr, 1.0))
        hi = float(np.percentile(arr, 99.5))
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
            hi = lo + 1.0
        scaled = np.clip((arr - lo) * (255.0 / (hi - lo)), 0.0, 255.0)
        return scaled.astype(np.uint8)
    if raw_array.dtype == np.uint8:
        return raw_array
    name = fmt_name.upper()
    if "12" in name:
        return np.clip(raw_array.astype(np.uint16) >> 4, 0, 255).astype(np.uint8)
    if "10" in name:
        return np.clip(raw_array.astype(np.uint16) >> 2, 0, 255).astype(np.uint8)
    max_v = int(np.max(raw_array)) if raw_array.size else 0
    if max_v <= 1023:
        return np.clip(raw_array.astype(np.uint16) >> 2, 0, 255).astype(np.uint8)
    if max_v <= 4095:
        return np.clip(raw_array.astype(np.uint16) >> 4, 0, 255).astype(np.uint8)
    return np.clip(raw_array.astype(np.uint16) >> 8, 0, 255).astype(np.uint8)


def bayer_pattern_from_fmt(fmt_name: str) -> str | None:
    name = str(fmt_name).upper()
    if "BAYER" not in name:
        return None
    if "RG" in name:
        return "RGGB"
    if "GB" in name:
        return "GBRG"
    if "GR" in name:
        return "GRBG"
    if "BG" in name:
        return "BGGR"
    return "RGGB"


def shift_bayer_pattern(pattern: str, x_off: int, y_off: int) -> str:
    p = pattern.upper()
    if len(p) != 4:
        return p
    grid = [[p[0], p[1]], [p[2], p[3]]]
    xo = int(x_off) & 1
    yo = int(y_off) & 1
    out = [
        [grid[(yo + 0) & 1][(xo + 0) & 1], grid[(yo + 0) & 1][(xo + 1) & 1]],
        [grid[(yo + 1) & 1][(xo + 0) & 1], grid[(yo + 1) & 1][(xo + 1) & 1]],
    ]
    return f"{out[0][0]}{out[0][1]}{out[1][0]}{out[1][1]}"


def bayer_pattern_for_crop(fmt_name: str, x_off: int, y_off: int) -> str | None:
    base = bayer_pattern_from_fmt(fmt_name)
    if base is None:
        return None
    return shift_bayer_pattern(base, x_off, y_off)


def debayer_menon_rgb(
    raw_array: "np.ndarray",
    fmt_name: str,
    pattern_override: str | None = None,
    autostretch: bool = False,
) -> "np.ndarray":
    if np is None:
        raise RuntimeError("NumPy is unavailable")
    u8 = raw_to_u8(raw_array, fmt_name, autostretch=autostretch)
    pattern = (pattern_override or bayer_pattern_from_fmt(fmt_name) or "").upper()
    if not pattern:
        return np.repeat(u8[:, :, None], 3, axis=2)
    try:
        rgbf = demosaic_bayer_menon2007(u8, pattern)
        return np.clip(np.rint(rgbf), 0, 255).astype(np.uint8)
    except Exception:
        try:
            rgbf = demosaic_bayer_fast_preview(u8, pattern)
            return np.clip(np.rint(rgbf), 0, 255).astype(np.uint8)
        except Exception:
            pass
        return np.repeat(u8[:, :, None], 3, axis=2)


def debayer_fast_preview_rgb(
    raw_array: "np.ndarray",
    fmt_name: str,
    pattern_override: str | None = None,
    autostretch: bool = False,
) -> "np.ndarray":
    if np is None:
        raise RuntimeError("NumPy is unavailable")
    u8 = raw_to_u8(raw_array, fmt_name, autostretch=autostretch)
    pattern = (pattern_override or bayer_pattern_from_fmt(fmt_name) or "").upper()
    if not pattern:
        return np.repeat(u8[:, :, None], 3, axis=2)
    try:
        rgbf = demosaic_bayer_fast_preview(u8, pattern)
        return np.clip(np.rint(rgbf), 0, 255).astype(np.uint8)
    except Exception:
        return np.repeat(u8[:, :, None], 3, axis=2)


def compose_grid16_rgb(lenses_rgb: "np.ndarray", gap: int = 2) -> "np.ndarray":
    if np is None:
        raise RuntimeError("NumPy is unavailable")
    h, w = int(lenses_rgb.shape[1]), int(lenses_rgb.shape[2])
    out = np.zeros((4 * h + 3 * gap, 4 * w + 3 * gap, 3), dtype=np.uint8)
    for i in range(16):
        r = i // 4
        c = i % 4
        y0 = r * (h + gap)
        x0 = c * (w + gap)
        out[y0 : y0 + h, x0 : x0 + w] = lenses_rgb[i]
    return out


@dataclass
class FramePacket:
    width: int
    height: int
    pixel_format: int
    raw: bytes
    timestamp: float
    meta: dict[str, object] | None = None


class CameraWorker(threading.Thread):
    def __init__(
        self,
        interface: str,
        camera_ip: str,
        event_q: queue.Queue,
        cmd_q: queue.Queue,
        packet_size: int,
        packet_delay: int,
        buffers: int = 12,
        debug: bool = False,
    ) -> None:
        super().__init__(daemon=True)
        self.interface = interface
        self.camera_ip = camera_ip
        self.event_q = event_q
        self.cmd_q = cmd_q
        self.packet_size = packet_size
        self.packet_delay = packet_delay
        self.buffers = buffers
        self.debug = bool(debug)
        self.stop_event = threading.Event()
        self.stream_stall_timeout_s = 2.5
        self.stream_restart_cooldown_s = 1.2
        self._last_good_frame_ts = 0.0
        self._last_restart_ts = 0.0
        self._last_bad_status_log_ts = 0.0
        self._debug_frame_count = 0
        self._if_ip_for_stream: str | None = None
        self._stream_port: int | None = None
        self._pending_gain: float | None = None
        self._pending_exposure: float | None = None
        self._pending_refresh_controls = False
        self._last_control_apply_ts = 0.0
        self.control_apply_interval_s = 0.08
        self._exposure_us_est = 50000.0

    def stop(self) -> None:
        self.stop_event.set()

    def _emit(self, kind: str, payload: object | None = None) -> None:
        if kind == "frame" and self.event_q.qsize() > 3:
            return
        try:
            self.event_q.put_nowait((kind, payload))
        except queue.Full:
            pass
        if self.debug:
            if kind in ("status", "error", "connected", "disconnected"):
                print(f"[hydra-debug][worker] {kind}: {payload}", flush=True)

    def _apply_stream_destination(self, camera) -> None:
        if not self._if_ip_for_stream:
            return
        try:
            camera.set_integer("GevSCDA", ipv4_to_u32(self._if_ip_for_stream))
        except Exception as exc:
            self._emit("status", f"GevSCDA set failed: {exc}")
        if self._stream_port is not None:
            try:
                camera.set_integer("GevSCPHostPort", int(self._stream_port))
            except Exception as exc:
                self._emit("status", f"GevSCPHostPort set failed: {exc}")

    def _maybe_restart_acquisition(self, camera, reason: str, now_mono: float) -> None:
        if (now_mono - self._last_good_frame_ts) < self.stream_stall_timeout_s:
            return
        if (now_mono - self._last_restart_ts) < self.stream_restart_cooldown_s:
            return
        self._last_restart_ts = now_mono
        self._emit("status", f"Stream stalled ({reason}), restarting acquisition...")
        try:
            camera.stop_acquisition()
        except Exception:
            pass
        time.sleep(0.03)
        try:
            self._apply_stream_destination(camera)
        except Exception:
            pass
        try:
            camera.start_acquisition()
            self._last_good_frame_ts = time.monotonic()
            self._emit("status", "Acquisition restarted")
        except Exception as exc:
            self._emit("status", f"Acquisition restart failed: {exc}")

    def _read_controls(self, camera) -> dict[str, float]:
        out: dict[str, float] = {}
        try:
            g_min, g_max = camera.get_gain_bounds()
            out["gain_min"] = float(g_min)
            out["gain_max"] = float(g_max)
            out["gain"] = float(camera.get_gain())
        except Exception:
            pass
        try:
            e_min, e_max = camera.get_exposure_time_bounds()
            out["exposure_min"] = float(e_min)
            out["exposure_max"] = float(e_max)
            out["exposure"] = float(camera.get_exposure_time())
            self._exposure_us_est = max(20.0, float(out["exposure"]))
        except Exception:
            pass
        return out

    def _apply_command(self, camera, cmd: str, value: object | None) -> None:
        if cmd == "set_gain" and value is not None:
            camera.set_gain(float(value))
            self._emit("status", f"Gain: {float(value):.3f}")
        elif cmd == "set_exposure" and value is not None:
            camera.set_exposure_time(float(value))
            self._exposure_us_est = max(20.0, float(value))
            self._emit("status", f"Exposure: {float(value):.1f} us")
        elif cmd == "refresh_controls":
            pass
        else:
            return
        self._emit("controls", self._read_controls(camera))

    def _drain_command_queue(self) -> None:
        while True:
            try:
                cmd, value = self.cmd_q.get_nowait()
            except queue.Empty:
                break
            cmd_s = str(cmd)
            if cmd_s == "set_gain" and value is not None:
                try:
                    self._pending_gain = float(value)
                except Exception:
                    pass
            elif cmd_s == "set_exposure" and value is not None:
                try:
                    self._pending_exposure = float(value)
                except Exception:
                    pass
            elif cmd_s == "refresh_controls":
                self._pending_refresh_controls = True

    def _apply_pending_controls(self, camera, now_mono: float) -> None:
        if (now_mono - self._last_control_apply_ts) < self.control_apply_interval_s:
            return
        applied = False
        if self._pending_exposure is not None:
            value = float(self._pending_exposure)
            self._pending_exposure = None
            try:
                self._apply_command(camera, "set_exposure", value)
            except Exception as exc:
                self._emit("status", f"Command error (set_exposure): {exc}")
            applied = True
        if self._pending_gain is not None:
            value = float(self._pending_gain)
            self._pending_gain = None
            try:
                self._apply_command(camera, "set_gain", value)
            except Exception as exc:
                self._emit("status", f"Command error (set_gain): {exc}")
            applied = True
        if self._pending_refresh_controls:
            self._pending_refresh_controls = False
            self._emit("controls", self._read_controls(camera))
            applied = True
        if applied:
            self._last_control_apply_ts = now_mono

    def run(self) -> None:
        camera = None
        try:
            import gi  # noqa: PLC0415

            gi.require_version("Aravis", "0.8")
            from gi.repository import Aravis  # type: ignore  # noqa: PLC0415

            configure_aravis_gige_interface(Aravis, self.interface)
            try:
                Aravis.update_device_list()
            except Exception:
                pass

            camera, open_note = open_camera_with_fallback(Aravis, self.camera_ip, self.interface)
            if camera is None:
                raise RuntimeError(f"Cannot open camera {self.camera_ip}: {open_note}")
            if open_note:
                self._emit("status", f"Connect note: {open_note}")

            camera.gv_set_stream_options(Aravis.GvStreamOption.PACKET_SOCKET_DISABLED)
            camera.gv_set_packet_size_adjustment(Aravis.GvPacketSizeAdjustment.NEVER)
            if self.packet_size > 0:
                try:
                    camera.gv_set_packet_size(int(self.packet_size))
                except Exception:
                    pass

            stream = camera.create_stream(None, None)
            if stream is None:
                raise RuntimeError("create_stream returned None")

            if hasattr(stream, "get_port"):
                try:
                    self._stream_port = int(stream.get_port())
                except Exception:
                    self._stream_port = None

            if_ip = get_interface_ipv4_for_peer(self.interface, self.camera_ip)
            self._if_ip_for_stream = if_ip
            if self.debug:
                print(
                    f"[hydra-debug][worker] stream bind candidate: if_ip={if_ip} "
                    f"camera_ip={self.camera_ip} port={self._stream_port}",
                    flush=True,
                )
            if if_ip:
                self._apply_stream_destination(camera)

            try:
                camera.set_string("TriggerMode", "Off")
            except Exception:
                pass
            try:
                camera.set_string("ExposureAuto", "Off")
            except Exception:
                pass
            try:
                camera.set_string("GainAuto", "Off")
            except Exception:
                pass
            if self.packet_size > 0:
                try:
                    camera.set_integer("GevSCPSPacketSize", int(self.packet_size))
                except Exception:
                    pass
            if self.packet_delay > 0:
                try:
                    camera.set_integer("GevSCPD", int(self.packet_delay))
                except Exception:
                    pass

            payload = int(camera.get_payload())
            if payload <= 0:
                raise RuntimeError("Invalid payload size")
            for _ in range(max(2, self.buffers)):
                stream.push_buffer(Aravis.Buffer.new_allocate(payload))

            runtime = read_camera_runtime_metadata(camera)
            controls = self._read_controls(camera)
            self._emit(
                "connected",
                {
                    "vendor": str(camera.get_vendor_name()),
                    "model": str(camera.get_model_name()),
                    "serial": str(camera.get_device_serial_number()),
                    "pixel_format": str(camera.get_pixel_format_as_string()),
                    "payload": payload,
                    "controls": controls,
                    "runtime": runtime,
                },
            )

            try:
                camera.set_acquisition_mode(Aravis.AcquisitionMode.CONTINUOUS)
            except Exception:
                pass
            camera.start_acquisition()
            now_mono = time.monotonic()
            self._last_good_frame_ts = now_mono
            self._last_restart_ts = now_mono

            success_status = int(Aravis.BufferStatus.SUCCESS)
            while not self.stop_event.is_set():
                now_mono = time.monotonic()
                self._drain_command_queue()
                self._apply_pending_controls(camera, now_mono)

                pop_timeout_ms = int(max(200.0, min(1800.0, (self._exposure_us_est / 1000.0) * 2.5 + 120.0)))
                buffer = stream.timeout_pop_buffer(pop_timeout_ms)
                if buffer is None:
                    self._maybe_restart_acquisition(camera, "no buffers", time.monotonic())
                    continue
                try:
                    status = int(buffer.get_status())
                    if status == success_status:
                        raw = bytes(buffer.get_image_data())
                        w = int(buffer.get_image_width())
                        h = int(buffer.get_image_height())
                        pf = int(buffer.get_image_pixel_format())
                        meta = read_buffer_metadata(buffer)
                        meta["width"] = w
                        meta["height"] = h
                        meta["pixel_format_int"] = pf
                        if "pixel_format_name" not in meta:
                            try:
                                meta["pixel_format_name"] = str(camera.get_pixel_format_as_string())
                            except Exception:
                                pass
                        self._last_good_frame_ts = time.monotonic()
                        self._debug_frame_count += 1
                        if self.debug and (self._debug_frame_count % 40) == 1:
                            print(
                                f"[hydra-debug][worker] frame_ok #{self._debug_frame_count}: "
                                f"{w}x{h} pf=0x{pf:08x} bytes={len(raw)} frame_id={meta.get('frame_id')}",
                                flush=True,
                            )
                        self._emit("frame", FramePacket(w, h, pf, raw, time.time(), meta))
                    else:
                        now_bad = time.monotonic()
                        if (now_bad - self._last_bad_status_log_ts) > 1.2:
                            self._last_bad_status_log_ts = now_bad
                            self._emit("status", f"Bad frame status: {status}")
                            if self.debug:
                                print(
                                    f"[hydra-debug][worker] bad_status={status} "
                                    f"frame_id={getattr(buffer, 'get_frame_id', lambda: None)()}",
                                    flush=True,
                                )
                        # Single bad buffer is tolerated; avoid aggressive control-path resets.
                except Exception as exc:
                    self._emit("status", f"Frame decode error: {exc}")
                finally:
                    stream.push_buffer(buffer)
        except Exception as exc:
            self._emit("error", str(exc))
        finally:
            if camera is not None:
                try:
                    camera.stop_acquisition()
                except Exception:
                    pass
            self._emit("disconnected", None)


@dataclass
class CropCalibrationData:
    image_width: int
    image_height: int
    centers_xy: list[list[float]]
    offsets: dict[str, int]
    boxes: list[dict[str, int]]
    order: str = "top_to_bottom_left_to_right"


class HydraWizardApp(tk.Tk):
    def __init__(
        self,
        interface: str,
        camera_ip: str,
        output_dir: Path,
        packet_size: int = DEFAULT_PACKET_SIZE,
        packet_delay: int = DEFAULT_PACKET_DELAY,
        preview_fps: float = DEFAULT_PREVIEW_FPS,
        ui_poll_ms: int = DEFAULT_UI_POLL_MS,
        debug: bool = False,
        force_u8_mode: bool = True,
    ) -> None:
        super().__init__()
        self.title("Hydra Baumer Capture")
        self.geometry("560x250")
        self.minsize(540, 220)

        if np is None or Image is None or ImageTk is None:
            raise RuntimeError("Required dependencies are missing: numpy and pillow")

        self.packet_size = int(packet_size)
        self.packet_delay = int(packet_delay)
        self.ui_poll_ms = max(5, int(ui_poll_ms))
        self.render_interval_s = 1.0 / max(1.0, float(preview_fps))
        self.debug = bool(debug)
        self.force_u8_mode = bool(force_u8_mode)

        self.output_dir = output_dir.expanduser()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session_root: Path | None = None

        self.interface_var = tk.StringVar(value=(interface or "en10"))
        self.camera_var = tk.StringVar(value=(camera_ip or ""))
        self.camera_pick_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="Idle")
        self.connection_info_var = tk.StringVar(value="-")
        self.wavelength_map_path_var = tk.StringVar(value="")
        self.camera_scan_running = False
        self.camera_scan_display_to_id: dict[str, str] = {}

        self.gain_var = tk.DoubleVar(value=0.0)
        self.exposure_var = tk.DoubleVar(value=12000.0)
        self.gain_entry_var = tk.StringVar(value="0.0")
        self.exposure_entry_var = tk.StringVar(value="12000.0")
        self.gain_bounds = (0.0, 24.0)
        self.exposure_bounds = (100.0, 500000.0)

        self.crop_left_var = tk.IntVar(value=120)
        self.crop_right_var = tk.IntVar(value=120)
        self.crop_top_var = tk.IntVar(value=120)
        self.crop_bottom_var = tk.IntVar(value=120)
        self.manual_points_var = tk.BooleanVar(value=False)
        self.crop_limits: dict[str, int] = {"left": 120, "right": 120, "top": 120, "bottom": 120}
        self.crop_centers: "np.ndarray | None" = None
        self.crop_boxes: list[dict[str, int]] = []
        self._drag_center_idx: int | None = None

        self.dark_burst_var = tk.IntVar(value=8)
        self.flat_burst_var = tk.IntVar(value=8)
        self.flat_sigma_var = tk.DoubleVar(value=1.5)
        self.geometry_target_var = tk.IntVar(value=5)
        self.geometry_cols_var = tk.IntVar(value=3)
        self.geometry_rows_var = tk.IntVar(value=3)
        self.geometry_capture_idx = 0
        self.geometry_progress_var = tk.DoubleVar(value=0.0)
        self.geometry_progress_text_var = tk.StringVar(value="")
        self.main_view_mode = tk.StringVar(value="grid")
        self.main_lens_var = tk.IntVar(value=1)
        self.max_fps_var = tk.StringVar(value="Max FPS: -")

        self.worker: CameraWorker | None = None
        self.event_q: queue.Queue = queue.Queue(maxsize=96)
        self.cmd_q: queue.Queue = queue.Queue(maxsize=64)
        self.last_frame: FramePacket | None = None
        self.last_render_ts = 0.0
        self.connected = False
        self.auto_fix_running = False
        self.camera_info: dict[str, object] = {}

        self.current_page = "connect"
        self.calib_stage = ""
        self._calib_photo: tk.PhotoImage | None = None
        self._main_photo: tk.PhotoImage | None = None
        self._calib_display_map: tuple[float, float, float] | None = None
        self._main_display_map: tuple[float, float, float] | None = None
        self._last_rendered_frame_id: int | None = None
        self._debug_frame_count = 0
        self._debug_last_render_log_ts = 0.0
        self._dark_preview_hint_ts = 0.0
        self._render_submit_seq = 0
        self._render_applied_seq = 0
        self._render_in_q: queue.Queue = queue.Queue(maxsize=1)
        self._render_out_q: queue.Queue = queue.Queue(maxsize=2)
        self._render_stop_event = threading.Event()
        self._render_thread = threading.Thread(target=self._render_worker_loop, name="hydra-render", daemon=True)
        self._render_thread.start()

        self.frame_dedupe_id: int | None = None
        self.dark_capture_active = False
        self.flat_capture_active = False
        self.dark_frames: list["np.ndarray"] = []
        self.flat_frames: list["np.ndarray"] = []
        self.dark_map: "np.ndarray | None" = None
        self.noise_map: "np.ndarray | None" = None
        self.flat_raw_mean: "np.ndarray | None" = None
        self.flat_norm: "np.ndarray | None" = None
        self.locked_gain: float | None = None
        self.locked_exposure: float | None = None
        self.geometry_corners: list[dict[int, "np.ndarray"]] = []
        self.geometry_focus_scores: list["np.ndarray"] = []
        self.geometry_h: "np.ndarray | None" = None
        self.reference_lens = 0
        self.wavelength_mapping_entries: list[dict[str, object]] = []
        self.wavelength_mapping_source: str | None = None

        self._build_ui()
        self._update_max_fps_label()
        self._dbg(
            f"init: interface={self.interface_var.get()} camera={self.camera_var.get()} "
            f"preview_fps={1.0/self.render_interval_s:.2f} force_u8={self.force_u8_mode}"
        )
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(self.ui_poll_ms, self._poll_events)

    # ----------------------------- UI -----------------------------
    def _build_ui(self) -> None:
        self.root = ttk.Frame(self, padding=8)
        self.root.pack(fill=tk.BOTH, expand=True)
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        self.page_connect = ttk.Frame(self.root)
        self.page_choice = ttk.Frame(self.root)
        self.page_calib = ttk.Frame(self.root)
        self.page_main = ttk.Frame(self.root)
        for p in (self.page_connect, self.page_choice, self.page_calib, self.page_main):
            p.grid(row=0, column=0, sticky="nsew")

        self._build_connect_page()
        self._build_choice_page()
        self._build_calibration_page()
        self._build_main_page()
        self._show_page("connect")

    def _dbg(self, message: str) -> None:
        if not self.debug:
            return
        print(f"[hydra-debug] {message}", flush=True)

    def _build_connect_page(self) -> None:
        f = self.page_connect
        for c in range(4):
            f.columnconfigure(c, weight=1 if c == 1 else 0)
        ttk.Label(f, text="Interface").grid(row=0, column=0, sticky="w", pady=(4, 4))
        ttk.Entry(f, textvariable=self.interface_var, width=12).grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Label(f, text="Camera ID / IP").grid(row=1, column=0, sticky="w", pady=(4, 4))
        ttk.Entry(f, textvariable=self.camera_var, width=42).grid(row=1, column=1, sticky="ew", padx=(8, 8))
        ttk.Label(f, text="Detected Cameras").grid(row=2, column=0, sticky="w", pady=(4, 4))
        self.camera_pick_combo = ttk.Combobox(
            f,
            textvariable=self.camera_pick_var,
            values=[],
            state="readonly",
            width=42,
        )
        self.camera_pick_combo.grid(row=2, column=1, sticky="ew", padx=(8, 8))
        self.camera_pick_combo.bind("<<ComboboxSelected>>", self._on_camera_pick_selected)

        btn_row = ttk.Frame(f)
        btn_row.grid(row=0, column=2, rowspan=3, sticky="ns", padx=4)
        ttk.Button(btn_row, text="Auto Find/Fix", command=self._auto_find_fix).grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(btn_row, text="Scan Cameras", command=self._scan_cameras).grid(row=1, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(btn_row, text="Connect", command=self._connect).grid(row=2, column=0, sticky="ew")

        ttk.Label(f, text="Status").grid(row=3, column=0, sticky="nw", pady=(8, 0))
        ttk.Label(f, textvariable=self.status_var, wraplength=460, justify="left").grid(
            row=3, column=1, columnspan=2, sticky="w", pady=(8, 0)
        )

    def _build_choice_page(self) -> None:
        f = self.page_choice
        f.columnconfigure(0, weight=1)
        ttk.Label(f, text="Camera connected", font=("Helvetica", 14, "bold")).grid(row=0, column=0, sticky="w", pady=(4, 8))
        ttk.Label(f, textvariable=self.connection_info_var, justify="left").grid(row=1, column=0, sticky="w", pady=(0, 10))
        wmap = ttk.LabelFrame(f, text="Wavelength Mapping (Optional)")
        wmap.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        wmap.columnconfigure(0, weight=1)
        ttk.Entry(wmap, textvariable=self.wavelength_map_path_var).grid(row=0, column=0, sticky="ew", padx=(8, 6), pady=8)
        ttk.Button(wmap, text="Browse...", command=self._browse_wavelength_mapping).grid(row=0, column=1, padx=(0, 8), pady=8)
        ttk.Label(
            wmap,
            text="Format: crop_1_R.png: 730 (48 lines for L1..L16 RGB).",
            justify="left",
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8))
        ttk.Button(f, text="Calibrate Camera", command=self._start_calibration_session).grid(row=3, column=0, sticky="w", pady=4)
        ttk.Button(
            f,
            text="Use Calibration From Existing Session",
            command=self._load_existing_calibration,
        ).grid(row=4, column=0, sticky="w", pady=4)
        ttk.Button(f, text="Disconnect", command=self._disconnect).grid(row=5, column=0, sticky="w", pady=8)
        ttk.Label(f, textvariable=self.status_var, wraplength=900, justify="left").grid(row=6, column=0, sticky="w", pady=(8, 0))

    def _build_calibration_page(self) -> None:
        f = self.page_calib
        f.columnconfigure(0, weight=0, minsize=330)
        f.columnconfigure(1, weight=1)
        f.rowconfigure(1, weight=1)

        self.calib_title_var = tk.StringVar(value="Calibration")
        ttk.Label(f, textvariable=self.calib_title_var, font=("Helvetica", 13, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 6)
        )

        self.calib_controls = ttk.Frame(f)
        self.calib_controls.grid(row=1, column=0, sticky="ns", padx=(0, 8))
        self.calib_controls.columnconfigure(0, weight=1)
        self.calib_controls.grid_propagate(True)

        preview_wrap = ttk.LabelFrame(f, text="Live Preview")
        preview_wrap.grid(row=1, column=1, sticky="nsew")
        preview_wrap.columnconfigure(0, weight=1)
        preview_wrap.rowconfigure(0, weight=1)
        self.calib_canvas = tk.Canvas(preview_wrap, bg="#101010", highlightthickness=0)
        self.calib_canvas.grid(row=0, column=0, sticky="nsew")
        self.calib_canvas.bind("<ButtonPress-1>", self._on_calib_canvas_press)
        self.calib_canvas.bind("<B1-Motion>", self._on_calib_canvas_drag)
        self.calib_canvas.bind("<ButtonRelease-1>", self._on_calib_canvas_release)

        self.stage_crop_frame = ttk.LabelFrame(self.calib_controls, text="Crop Calibration")
        self.stage_dark_frame = ttk.LabelFrame(self.calib_controls, text="Black Level Calibration")
        self.stage_flat_frame = ttk.LabelFrame(self.calib_controls, text="Flat Field Calibration")
        self.stage_geom_frame = ttk.LabelFrame(self.calib_controls, text="Geometry Calibration")

        self._build_stage_crop_controls()
        self._build_stage_dark_controls()
        self._build_stage_flat_controls()
        self._build_stage_geometry_controls()

        ttk.Label(f, textvariable=self.status_var, wraplength=980, justify="left").grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(8, 0)
        )

    def _build_main_page(self) -> None:
        f = self.page_main
        f.columnconfigure(0, weight=1)
        f.columnconfigure(1, weight=0)
        f.rowconfigure(0, weight=1)

        preview_wrap = ttk.LabelFrame(f, text="Hydra Preview")
        preview_wrap.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        preview_wrap.columnconfigure(0, weight=1)
        preview_wrap.rowconfigure(0, weight=1)
        self.main_canvas = tk.Canvas(preview_wrap, bg="#101010", highlightthickness=0)
        self.main_canvas.grid(row=0, column=0, sticky="nsew")

        side = ttk.LabelFrame(f, text="Main Controls")
        side.grid(row=0, column=1, sticky="ns")
        side.columnconfigure(0, weight=1)
        ttk.Radiobutton(side, text="Grid 4x4", variable=self.main_view_mode, value="grid").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 2))
        ttk.Radiobutton(side, text="Single Lens", variable=self.main_view_mode, value="single").grid(row=1, column=0, sticky="w", padx=8, pady=2)
        lens_values = [str(i) for i in range(1, 17)]
        self.main_lens_combo = ttk.Combobox(side, values=lens_values, textvariable=tk.StringVar(value="1"), width=6, state="readonly")
        self.main_lens_combo.grid(row=2, column=0, sticky="w", padx=8, pady=(2, 8))
        self.main_lens_combo.bind("<<ComboboxSelected>>", self._on_main_lens_change)
        ttk.Separator(side).grid(row=3, column=0, sticky="ew", padx=8, pady=(2, 6))
        ttk.Label(side, text="Live Camera Controls", font=("Helvetica", 10, "bold")).grid(row=4, column=0, sticky="w", padx=8, pady=(0, 2))
        self._build_main_gain_exposure_controls(side, row_start=5)
        ttk.Label(
            side,
            text="Snapshot always uses calibration Gain/Exposure,\nthen restores current live values.",
            wraplength=260,
            justify="left",
        ).grid(row=9, column=0, sticky="w", padx=8, pady=(4, 6))
        ttk.Button(side, text="Snapshot", command=self._snapshot).grid(row=10, column=0, sticky="ew", padx=8, pady=(6, 4))
        ttk.Button(side, text="Back To Choice", command=lambda: self._show_page("choice")).grid(row=11, column=0, sticky="ew", padx=8, pady=4)
        ttk.Button(side, text="Disconnect", command=self._disconnect).grid(row=12, column=0, sticky="ew", padx=8, pady=4)
        ttk.Label(side, textvariable=self.status_var, wraplength=260, justify="left").grid(row=13, column=0, sticky="w", padx=8, pady=(10, 8))

    def _build_stage_crop_controls(self) -> None:
        f = self.stage_crop_frame
        f.columnconfigure(0, weight=1)
        ttk.Label(
            f,
            text="Place chessboard 4x4 cells in view.\nDetect center seam (3x3 inner center) on all 16 lenses,\nthen adjust global offsets and save crop.",
            wraplength=250,
            justify="left",
        ).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        self._build_gain_exposure_controls(f, row_start=1, allow_lock=False)
        ttk.Button(f, text="Detect 16 Chess Centers", command=self._detect_crop_points).grid(row=6, column=0, sticky="ew", padx=8, pady=(4, 4))
        ttk.Checkbutton(f, text="Manual point drag", variable=self.manual_points_var).grid(row=7, column=0, sticky="w", padx=8, pady=(0, 6))
        self._add_offset_row(f, "Left", self.crop_left_var, 8, self._on_crop_offsets_changed)
        self._add_offset_row(f, "Right", self.crop_right_var, 9, self._on_crop_offsets_changed)
        self._add_offset_row(f, "Top", self.crop_top_var, 10, self._on_crop_offsets_changed)
        self._add_offset_row(f, "Bottom", self.crop_bottom_var, 11, self._on_crop_offsets_changed)
        ttk.Button(f, text="Save Crop And Continue", command=self._save_crop_and_continue).grid(row=12, column=0, sticky="ew", padx=8, pady=(8, 8))

    def _build_stage_dark_controls(self) -> None:
        f = self.stage_dark_frame
        f.columnconfigure(0, weight=1)
        ttk.Label(
            f,
            text="Set Exposure/Gain, then cover sensor and disable light.\nCreate dark map from RAW burst.",
            wraplength=250,
            justify="left",
        ).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        self._build_gain_exposure_controls(f, row_start=1, allow_lock=True)
        ttk.Label(f, textvariable=self.max_fps_var).grid(row=6, column=0, sticky="w", padx=8, pady=(4, 2))
        burst_row = ttk.Frame(f)
        burst_row.grid(row=7, column=0, sticky="ew", padx=8, pady=(2, 6))
        ttk.Label(burst_row, text="Burst frames").pack(side=tk.LEFT)
        ttk.Entry(burst_row, textvariable=self.dark_burst_var, width=8).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(f, text="Create Dark Map", command=self._start_dark_capture).grid(row=8, column=0, sticky="ew", padx=8, pady=(8, 8))

    def _build_stage_flat_controls(self) -> None:
        f = self.stage_flat_frame
        f.columnconfigure(0, weight=1)
        ttk.Label(
            f,
            text="Use uniform bright target (Spectralon). Exposure/Gain are locked from previous stage.",
            wraplength=250,
            justify="left",
        ).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        burst_row = ttk.Frame(f)
        burst_row.grid(row=1, column=0, sticky="ew", padx=8, pady=(2, 4))
        ttk.Label(burst_row, text="Burst frames").pack(side=tk.LEFT)
        ttk.Entry(burst_row, textvariable=self.flat_burst_var, width=8).pack(side=tk.LEFT, padx=(8, 0))
        sigma_row = ttk.Frame(f)
        sigma_row.grid(row=2, column=0, sticky="ew", padx=8, pady=(2, 8))
        ttk.Label(sigma_row, text="Low-pass sigma").pack(side=tk.LEFT)
        ttk.Entry(sigma_row, textvariable=self.flat_sigma_var, width=8).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(f, text="Create Flat Map", command=self._start_flat_capture).grid(row=3, column=0, sticky="ew", padx=8, pady=(8, 8))

    def _build_stage_geometry_controls(self) -> None:
        f = self.stage_geom_frame
        f.columnconfigure(0, weight=1)
        ttk.Label(
            f,
            text="Chessboard 4x4 cells => 3x3 inner corners.\nCapture 5 valid frames with board on all 16 lenses.",
            wraplength=250,
            justify="left",
        ).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        row_cfg = ttk.Frame(f)
        row_cfg.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 4))
        ttk.Label(row_cfg, text="Inner corners").pack(side=tk.LEFT)
        ttk.Entry(row_cfg, textvariable=self.geometry_cols_var, width=4).pack(side=tk.LEFT, padx=(8, 2))
        ttk.Label(row_cfg, text="x").pack(side=tk.LEFT)
        ttk.Entry(row_cfg, textvariable=self.geometry_rows_var, width=4).pack(side=tk.LEFT, padx=(2, 0))
        row_n = ttk.Frame(f)
        row_n.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 4))
        ttk.Label(row_n, text="Valid frames target").pack(side=tk.LEFT)
        ttk.Entry(row_n, textvariable=self.geometry_target_var, width=6).pack(side=tk.LEFT, padx=(8, 0))
        self.geometry_btn_var = tk.StringVar(value="Capture chess frame 1/5")
        ttk.Button(f, textvariable=self.geometry_btn_var, command=self._capture_geometry_frame).grid(
            row=3, column=0, sticky="ew", padx=8, pady=(8, 4)
        )
        ttk.Progressbar(f, maximum=100, variable=self.geometry_progress_var).grid(row=4, column=0, sticky="ew", padx=8, pady=(6, 2))
        ttk.Label(f, textvariable=self.geometry_progress_text_var, wraplength=250, justify="left").grid(
            row=5, column=0, sticky="w", padx=8, pady=(0, 8)
        )

    def _build_gain_exposure_controls(self, parent: ttk.Frame, row_start: int, allow_lock: bool) -> None:
        ttk.Label(parent, text="Gain").grid(row=row_start, column=0, sticky="w", padx=8, pady=(2, 0))
        gain_row = ttk.Frame(parent)
        gain_row.grid(row=row_start + 1, column=0, sticky="ew", padx=8, pady=(2, 6))
        gain_row.columnconfigure(0, weight=1)
        scale = ttk.Scale(gain_row, from_=self.gain_bounds[0], to=self.gain_bounds[1], variable=self.gain_var, orient=tk.HORIZONTAL, command=self._on_gain_slide)
        scale.grid(row=0, column=0, sticky="ew")
        ent = ttk.Entry(gain_row, textvariable=self.gain_entry_var, width=8)
        ent.grid(row=0, column=1, padx=(6, 0))
        ent.bind("<Return>", self._on_gain_entry_commit)

        ttk.Label(parent, text="Exposure (us)").grid(row=row_start + 2, column=0, sticky="w", padx=8, pady=(2, 0))
        exp_row = ttk.Frame(parent)
        exp_row.grid(row=row_start + 3, column=0, sticky="ew", padx=8, pady=(2, 6))
        exp_row.columnconfigure(0, weight=1)
        scale_e = ttk.Scale(
            exp_row,
            from_=self.exposure_bounds[0],
            to=self.exposure_bounds[1],
            variable=self.exposure_var,
            orient=tk.HORIZONTAL,
            command=self._on_exposure_slide,
        )
        scale_e.grid(row=0, column=0, sticky="ew")
        ent_e = ttk.Entry(exp_row, textvariable=self.exposure_entry_var, width=10)
        ent_e.grid(row=0, column=1, padx=(6, 0))
        ent_e.bind("<Return>", self._on_exposure_entry_commit)
        if not allow_lock:
            return

    def _build_main_gain_exposure_controls(self, parent: ttk.Frame, row_start: int) -> None:
        ttk.Label(parent, text="Gain").grid(row=row_start, column=0, sticky="w", padx=8, pady=(2, 0))
        gain_row = ttk.Frame(parent)
        gain_row.grid(row=row_start + 1, column=0, sticky="ew", padx=8, pady=(2, 6))
        gain_row.columnconfigure(0, weight=1)
        scale = ttk.Scale(
            gain_row,
            from_=self.gain_bounds[0],
            to=self.gain_bounds[1],
            variable=self.gain_var,
            orient=tk.HORIZONTAL,
            command=self._on_gain_slide,
        )
        scale.grid(row=0, column=0, sticky="ew")
        ent = ttk.Entry(gain_row, textvariable=self.gain_entry_var, width=8)
        ent.grid(row=0, column=1, padx=(6, 0))
        ent.bind("<Return>", self._on_gain_entry_commit)

        ttk.Label(parent, text="Exposure (us)").grid(row=row_start + 2, column=0, sticky="w", padx=8, pady=(2, 0))
        exp_row = ttk.Frame(parent)
        exp_row.grid(row=row_start + 3, column=0, sticky="ew", padx=8, pady=(2, 6))
        exp_row.columnconfigure(0, weight=1)
        scale_e = ttk.Scale(
            exp_row,
            from_=self.exposure_bounds[0],
            to=self.exposure_bounds[1],
            variable=self.exposure_var,
            orient=tk.HORIZONTAL,
            command=self._on_exposure_slide,
        )
        scale_e.grid(row=0, column=0, sticky="ew")
        ent_e = ttk.Entry(exp_row, textvariable=self.exposure_entry_var, width=10)
        ent_e.grid(row=0, column=1, padx=(6, 0))
        ent_e.bind("<Return>", self._on_exposure_entry_commit)

    def _add_offset_row(self, parent: ttk.Frame, label: str, var: tk.IntVar, row: int, callback) -> None:
        r = ttk.Frame(parent)
        r.grid(row=row, column=0, sticky="ew", padx=8, pady=(0, 3))
        r.columnconfigure(1, weight=1)
        ttk.Label(r, text=label).grid(row=0, column=0, sticky="w")
        sc = ttk.Scale(
            r,
            from_=1,
            to=300,
            orient=tk.HORIZONTAL,
            command=lambda value, v=var: self._on_offset_scale(v, value, callback),
        )
        sc.set(float(var.get()))
        sc.grid(row=0, column=1, sticky="ew", padx=(8, 6))
        ent = ttk.Entry(r, textvariable=var, width=7)
        ent.grid(row=0, column=2, sticky="e")
        ent.bind("<Return>", lambda _e: callback())

    def _on_offset_scale(self, var: tk.IntVar, value: str, callback) -> None:
        try:
            var.set(int(round(float(value))))
        except Exception:
            return
        callback()

    # ----------------------------- Navigation -----------------------------
    def _show_page(self, page: str) -> None:
        self.current_page = page
        if page == "connect":
            self.geometry("900x320")
            self.page_connect.tkraise()
        elif page == "choice":
            self.geometry("760x360")
            self.page_choice.tkraise()
        elif page == "calib":
            self.geometry("1320x860")
            self.page_calib.tkraise()
        elif page == "main":
            self.geometry("1320x860")
            self.page_main.tkraise()

    def _show_calibration_stage(self, stage: str) -> None:
        self.calib_stage = stage
        for fr in (self.stage_crop_frame, self.stage_dark_frame, self.stage_flat_frame, self.stage_geom_frame):
            fr.grid_remove()
        if stage == "crop":
            self.calib_title_var.set("Crop Calibration")
            self.stage_crop_frame.grid(row=0, column=0, sticky="nsew")
        elif stage == "dark":
            self.calib_title_var.set("Black Level Calibration")
            self.stage_dark_frame.grid(row=0, column=0, sticky="nsew")
        elif stage == "flat":
            self.calib_title_var.set("Flat Field Calibration")
            self.stage_flat_frame.grid(row=0, column=0, sticky="nsew")
        elif stage == "geometry":
            self.calib_title_var.set("Geometry Calibration")
            self.stage_geom_frame.grid(row=0, column=0, sticky="nsew")

    # ----------------------------- Camera connect -----------------------------
    def _on_camera_pick_selected(self, _event: tk.Event | None = None) -> None:
        label = self.camera_pick_var.get().strip()
        camera_id = self.camera_scan_display_to_id.get(label)
        if camera_id:
            self.camera_var.set(camera_id)

    @staticmethod
    def _parse_arv_list_output(text: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith("["):
                continue
            lo = line.lower()
            if lo.startswith("no device found"):
                continue
            if lo.startswith("option parsing failed"):
                continue
            if lo.startswith("error"):
                continue
            if lo.startswith("warning"):
                continue
            dev_id = line
            transport = "unknown"
            if " (" in line and line.endswith(")"):
                head, tail = line.rsplit(" (", 1)
                tail = tail[:-1].strip()
                if head.strip():
                    dev_id = head.strip()
                    transport = tail or "unknown"
            if dev_id in seen:
                continue
            seen.add(dev_id)
            out.append((dev_id, transport))
        return out

    def _scan_cameras(self) -> None:
        if self.camera_scan_running:
            self.status_var.set("Camera scan already running")
            return
        self.camera_scan_running = True
        interface = self.interface_var.get().strip()
        self.status_var.set("Scanning cameras...")
        threading.Thread(target=self._scan_cameras_worker, args=(interface,), daemon=True).start()

    def _scan_cameras_worker(self, interface: str) -> None:
        def emit(kind: str, payload: object) -> None:
            try:
                self.event_q.put_nowait((kind, payload))
            except Exception:
                pass

        try:
            arv_tool = shutil.which("arv-tool-0.8") or shutil.which("arv-tool")
            if not arv_tool:
                emit("camera_scan_done", {"ok": False, "error": "arv-tool not found"})
                return
            cmds: list[list[str]] = [[arv_tool]]
            if interface:
                cmds.insert(0, [arv_tool, f"--gv-discovery-interface={interface}"])
            merged: list[tuple[str, str]] = []
            seen: set[str] = set()
            for cmd in cmds:
                try:
                    out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, timeout=6.0)
                except Exception as exc:
                    out = str(exc)
                for dev_id, transport in self._parse_arv_list_output(out):
                    if dev_id in seen:
                        continue
                    seen.add(dev_id)
                    merged.append((dev_id, transport))
            emit("camera_scan_done", {"ok": True, "devices": merged})
        finally:
            emit("camera_scan_finish", None)

    def _connect(self) -> None:
        if self.worker and self.worker.is_alive():
            self.status_var.set("Already connected")
            return
        interface = self.interface_var.get().strip()
        camera_id = self.camera_var.get().strip()
        self._dbg(f"connect requested: interface={interface} camera={camera_id}")
        if not camera_id:
            self.status_var.set("Empty camera ID / IP")
            return
        if (not interface) and is_ipv4_literal(camera_id):
            self.status_var.set("Empty interface for GigE camera IP")
            return
        self.event_q = queue.Queue(maxsize=96)
        self.cmd_q = queue.Queue(maxsize=64)
        self.worker = CameraWorker(
            interface=(interface or "en10"),
            camera_ip=camera_id,
            event_q=self.event_q,
            cmd_q=self.cmd_q,
            packet_size=self.packet_size,
            packet_delay=self.packet_delay,
            buffers=24,
            debug=self.debug,
        )
        self.status_var.set("Connecting...")
        self.worker.start()

    def _disconnect(self) -> None:
        self._dbg("disconnect requested")
        if self.worker and self.worker.is_alive():
            self.worker.stop()
            self.status_var.set("Disconnecting...")
        self.connected = False
        self.last_frame = None
        self._show_page("connect")

    def _auto_find_fix(self) -> None:
        if self.auto_fix_running:
            self.status_var.set("Auto Find/Fix already running")
            return
        if not AUTO_FIX_AVAILABLE:
            self.status_var.set("Auto Find/Fix unavailable")
            return
        interface = self.interface_var.get().strip()
        if not interface:
            self.status_var.set("Empty interface")
            return
        self.auto_fix_running = True
        self.status_var.set("Auto Find/Fix started...")
        threading.Thread(target=self._auto_find_fix_worker, args=(interface,), daemon=True).start()

    def _auto_find_fix_worker(self, interface: str) -> None:
        def emit(kind: str, payload: object) -> None:
            try:
                self.event_q.put_nowait((kind, payload))
            except Exception:
                pass

        try:
            host_ip = get_interface_ipv4(interface)
            self._dbg(f"auto_find_fix: host_ip on {interface} = {host_ip}")
            if not host_ip:
                emit("status", f"Auto Find/Fix failed: no IPv4 on {interface}")
                emit("auto_fix_done", None)
                return
            netmask = get_interface_netmask(interface) or "255.255.255.0"
            target_ip = suggest_camera_ip(host_ip)

            cams = gvcp_discover(interface, duration=3.5, interval=0.25) if gvcp_discover else []
            self._dbg(f"auto_find_fix: discovered cameras count={len(cams)}")
            if not cams:
                emit("status", "No GVCP replies")
                emit("auto_fix_done", None)
                return
            cam = cams[0]
            mac = (cam.mac or "").lower()
            discovered_ip = cam.current_ip or cam.source_ip
            emit("status", f"Found {cam.model_name} at {discovered_ip}")

            need_force = (not same_subnet(discovered_ip, host_ip, netmask)) or (discovered_ip != target_ip)
            if need_force and mac and gvcp_force_ip:
                emit("status", f"Applying ForceIP {target_ip}")
                try:
                    gvcp_force_ip(interface=interface, target_mac=mac, ip=target_ip, subnet=netmask, gateway="0.0.0.0", timeout=1.2)
                    time.sleep(0.4)
                except Exception as exc:
                    emit("status", f"ForceIP error: {exc}")
                cams_after = gvcp_discover(interface, duration=2.8, interval=0.2) if gvcp_discover else []
                if cams_after:
                    picked = None
                    for c in cams_after:
                        if (c.mac or "").lower() == mac:
                            picked = c
                            break
                    if picked is None:
                        picked = cams_after[0]
                    discovered_ip = picked.current_ip or picked.source_ip
            emit("auto_fix_set_ip", discovered_ip)
            emit("status", f"Auto Find/Fix done: {discovered_ip}")
        except Exception as exc:
            emit("status", f"Auto Find/Fix failed: {exc}")
        finally:
            emit("auto_fix_done", None)

    # ----------------------------- Session / calibration filesystem -----------------------------
    def _browse_wavelength_mapping(self) -> None:
        path = filedialog.askopenfilename(
            title="Select wavelength mapping file (optional)",
            filetypes=[("Text files", "*.txt *.csv"), ("All files", "*.*")],
        )
        if not path:
            return
        self.wavelength_map_path_var.set(path)
        ok, msg = self._load_wavelength_mapping_from_path(path, silent=False)
        if ok:
            self.status_var.set(msg)
        else:
            self.status_var.set(f"Wavelength mapping error: {msg}")

    def _parse_wavelength_mapping_file(self, path: Path) -> tuple[list[dict[str, object]], list[str]]:
        if not path.exists():
            raise RuntimeError(f"File not found: {path}")
        entries: list[dict[str, object]] = []
        warnings: list[str] = []
        seen: set[int] = set()
        line_re = re.compile(r"^\s*([^:\s]+)\s*:\s*([-+]?\d+(?:\.\d+)?)\s*$")
        name_re = re.compile(r"(?i)(?:^|.*?)(?:crop_)?(\d+)_([rgb])(?:\.[a-z0-9_]+)?$")
        for lineno, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            m = line_re.match(line)
            if not m:
                warnings.append(f"line {lineno}: skipped (invalid format)")
                continue
            name = m.group(1).strip()
            try:
                wl = float(m.group(2))
            except Exception:
                warnings.append(f"line {lineno}: skipped (invalid wavelength)")
                continue
            nm = name_re.match(name)
            if not nm:
                warnings.append(f"line {lineno}: skipped (cannot parse lens/channel from '{name}')")
                continue
            lens = int(nm.group(1))
            ch = str(nm.group(2)).upper()
            if lens < 1 or lens > 16:
                warnings.append(f"line {lineno}: skipped (lens out of range 1..16)")
                continue
            ch_off = {"R": 0, "G": 1, "B": 2}[ch]
            channel_index = (lens - 1) * 3 + ch_off
            if channel_index in seen:
                warnings.append(f"line {lineno}: duplicate mapping for L{lens}_{ch}, skipped")
                continue
            seen.add(channel_index)
            entries.append(
                {
                    "line": lineno,
                    "source_name": name,
                    "lens": lens,
                    "channel": ch,
                    "label": f"L{lens}_{ch}",
                    "channel_index": channel_index,
                    "wavelength_nm": wl,
                }
            )
        if not entries:
            raise RuntimeError("No valid mapping rows were parsed")
        return entries, warnings

    def _load_wavelength_mapping_from_path(self, path_str: str, silent: bool = False) -> tuple[bool, str]:
        p = Path(path_str).expanduser()
        try:
            entries, warnings = self._parse_wavelength_mapping_file(p)
        except Exception as exc:
            if not silent:
                self._dbg(f"wavelength mapping parse failed: {exc}")
            self.wavelength_mapping_entries = []
            self.wavelength_mapping_source = None
            return False, str(exc)
        self.wavelength_mapping_entries = entries
        self.wavelength_mapping_source = str(p)
        msg = f"Wavelength mapping loaded: {len(entries)} channels from {p.name}"
        if warnings and not silent and self.debug:
            self._dbg("wavelength mapping warnings: " + "; ".join(warnings[:8]))
        return True, msg

    def _save_wavelength_mapping_to_session(self) -> None:
        if self.session_root is None:
            return
        payload: dict[str, object] = {
            "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "source_path": self.wavelength_mapping_source,
            "count": int(len(self.wavelength_mapping_entries)),
            "entries": self.wavelength_mapping_entries,
            "expected_labels": [f"L{i}_{ch}" for i in range(1, 17) for ch in ("R", "G", "B")],
            "source_order_note": "entries order follows file lines and is used for snapshot channel ordering",
        }
        self._save_json(self.session_root / "wavelength_mapping.json", payload)

    def _load_wavelength_mapping_from_session(self, session: Path) -> None:
        mapping_path = session / "wavelength_mapping.json"
        if not mapping_path.exists():
            return
        try:
            payload = json.loads(mapping_path.read_text(encoding="utf-8"))
            raw_entries = payload.get("entries", [])
            parsed: list[dict[str, object]] = []
            for e in raw_entries:
                if not isinstance(e, dict):
                    continue
                if "channel_index" not in e or "wavelength_nm" not in e or "label" not in e:
                    continue
                parsed.append(
                    {
                        "line": int(e.get("line", 0)),
                        "source_name": str(e.get("source_name", "")),
                        "lens": int(e.get("lens", 0)),
                        "channel": str(e.get("channel", "")),
                        "label": str(e.get("label")),
                        "channel_index": int(e.get("channel_index")),
                        "wavelength_nm": float(e.get("wavelength_nm")),
                    }
                )
            if parsed:
                self.wavelength_mapping_entries = parsed
                src = payload.get("source_path", "")
                self.wavelength_mapping_source = str(src) if src else str(mapping_path)
                self.wavelength_map_path_var.set(self.wavelength_mapping_source)
        except Exception as exc:
            if self.debug:
                self._dbg(f"failed to load session wavelength mapping: {exc}")

    def _start_calibration_session(self) -> None:
        ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
        self.session_root = self.output_dir / f"session_{ts}_calibration"
        self.session_root.mkdir(parents=True, exist_ok=True)
        self.wavelength_mapping_entries = []
        self.wavelength_mapping_source = None
        map_path = self.wavelength_map_path_var.get().strip()
        if map_path:
            ok, msg = self._load_wavelength_mapping_from_path(map_path, silent=False)
            if not ok:
                self.status_var.set(f"Wavelength mapping warning: {msg}")
            else:
                self._save_wavelength_mapping_to_session()
        self._show_page("calib")
        self._show_calibration_stage("crop")
        if self.wavelength_mapping_entries:
            self.status_var.set(
                "Crop calibration: detect 16 chessboard centers (4x4 board, center of 3x3 inner corners). "
                f"Wavelength map loaded ({len(self.wavelength_mapping_entries)} channels)."
            )
        else:
            self.status_var.set("Crop calibration: detect 16 chessboard centers (4x4 board, center of 3x3 inner corners)")

    def _load_existing_calibration(self) -> None:
        root = filedialog.askdirectory(title="Select calibration session folder")
        if not root:
            return
        session = Path(root)
        required = [
            session / "crop.json",
            session / "darkMap.npy",
            session / "flat_norm.npy",
            session / "geometry_calibration.json",
            session / "camera_settings.json",
            session / "H_lens.npy",
        ]
        missing = [p.name for p in required if not p.exists()]
        if missing:
            self.status_var.set(f"Missing calibration files: {', '.join(missing)}")
            return
        try:
            crop = json.loads((session / "crop.json").read_text(encoding="utf-8"))
            boxes_raw = crop.get("boxes", [])
            if len(boxes_raw) != 16:
                raise RuntimeError("crop.json has invalid boxes")
            self.crop_boxes = []
            for b in boxes_raw:
                self.crop_boxes.append(
                    {
                        "index": int(b["index"]),
                        "row": int(b["row"]),
                        "col": int(b["col"]),
                        "x": int(b["x"]),
                        "y": int(b["y"]),
                        "width": int(b["width"]),
                        "height": int(b["height"]),
                    }
                )
            self.dark_map = np.load(session / "darkMap.npy")
            self.flat_norm = np.load(session / "flat_norm.npy")
            self.geometry_h = np.load(session / "H_lens.npy")
            settings = json.loads((session / "camera_settings.json").read_text(encoding="utf-8"))
            self.locked_gain = float(settings.get("gain_db", self.gain_var.get()))
            self.locked_exposure = float(settings.get("exposure_us", self.exposure_var.get()))
            self.gain_var.set(self.locked_gain)
            self.exposure_var.set(self.locked_exposure)
            self.gain_entry_var.set(f"{self.locked_gain:.3f}")
            self.exposure_entry_var.set(f"{self.locked_exposure:.1f}")
            geo = json.loads((session / "geometry_calibration.json").read_text(encoding="utf-8"))
            self.reference_lens = int(geo.get("reference_lens", 0))
            self._load_wavelength_mapping_from_session(session)
            self.session_root = session
            self._show_page("main")
            self.status_var.set(f"Loaded calibration from {session}")
        except Exception as exc:
            self.status_var.set(f"Failed to load calibration: {exc}")

    def _save_json(self, path: Path, payload: dict[str, object]) -> None:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    # ----------------------------- Controls -----------------------------
    def _queue_command(self, cmd: str, value: object | None) -> None:
        if not (self.worker and self.worker.is_alive()):
            return
        try:
            self.cmd_q.put_nowait((cmd, value))
        except queue.Full:
            self.status_var.set("Command queue is full")

    def _on_gain_slide(self, _value: str) -> None:
        if not self._gain_control_allowed() and self.locked_gain is not None:
            self.gain_var.set(float(self.locked_gain))
            self.gain_entry_var.set(f"{float(self.locked_gain):.3f}")
            self.status_var.set("Gain is locked for current session")
            return
        v = max(self.gain_bounds[0], min(self.gain_bounds[1], float(self.gain_var.get())))
        self.gain_var.set(v)
        self.gain_entry_var.set(f"{v:.3f}")
        self._update_max_fps_label()
        if self._gain_control_allowed():
            self._queue_command("set_gain", v)

    def _on_exposure_slide(self, _value: str) -> None:
        if not self._gain_control_allowed() and self.locked_exposure is not None:
            self.exposure_var.set(float(self.locked_exposure))
            self.exposure_entry_var.set(f"{float(self.locked_exposure):.1f}")
            self.status_var.set("Exposure is locked for current session")
            return
        v = max(self.exposure_bounds[0], min(self.exposure_bounds[1], float(self.exposure_var.get())))
        self.exposure_var.set(v)
        self.exposure_entry_var.set(f"{v:.1f}")
        self._update_max_fps_label()
        if self._gain_control_allowed():
            self._queue_command("set_exposure", v)

    def _on_gain_entry_commit(self, _event: tk.Event) -> None:
        if not self._gain_control_allowed() and self.locked_gain is not None:
            self.gain_var.set(float(self.locked_gain))
            self.gain_entry_var.set(f"{float(self.locked_gain):.3f}")
            self.status_var.set("Gain is locked for current session")
            return
        try:
            v = float(self.gain_entry_var.get().strip())
        except ValueError:
            return
        self.gain_var.set(max(self.gain_bounds[0], min(self.gain_bounds[1], v)))
        self._on_gain_slide("")

    def _on_exposure_entry_commit(self, _event: tk.Event) -> None:
        if not self._gain_control_allowed() and self.locked_exposure is not None:
            self.exposure_var.set(float(self.locked_exposure))
            self.exposure_entry_var.set(f"{float(self.locked_exposure):.1f}")
            self.status_var.set("Exposure is locked for current session")
            return
        try:
            v = float(self.exposure_entry_var.get().strip())
        except ValueError:
            return
        self.exposure_var.set(max(self.exposure_bounds[0], min(self.exposure_bounds[1], v)))
        self._on_exposure_slide("")

    def _gain_control_allowed(self) -> bool:
        if self.current_page == "main":
            return True
        if self.current_page == "calib" and self.calib_stage in ("crop", "dark"):
            return True
        return not (self.locked_gain is not None and self.locked_exposure is not None)

    def _update_max_fps_label(self) -> None:
        exp = max(1.0, float(self.exposure_var.get()))
        fps = 1_000_000.0 / exp
        self.max_fps_var.set(f"Max FPS (exposure-only estimate): {fps:.2f}")

    # ----------------------------- RAW helpers -----------------------------
    def _decode_frame(self, frame: FramePacket) -> tuple["np.ndarray", str]:
        meta = frame.meta or {}
        fmt = meta.get("pixel_format_name") or meta.get("pixel_format") or frame.pixel_format
        fmt_name = pixel_format_to_name(fmt)
        arr = decode_buffer_to_ndarray(frame.raw, frame.width, frame.height, fmt)
        if self.force_u8_mode and arr.dtype != np.uint8:
            # Force 8-bit domain while preserving linear intensity values.
            arr = raw_to_u8(arr, fmt_name, autostretch=False)
        if self.debug:
            raw_len = len(frame.raw)
            self._dbg(
                f"decode: w={frame.width} h={frame.height} raw_bytes={raw_len} "
                f"fmt={fmt_name} out_dtype={arr.dtype} out_shape={tuple(arr.shape)}"
            )
        return arr, fmt_name

    def _get_latest_raw(self) -> tuple["np.ndarray", str] | None:
        if self.last_frame is None:
            return None
        try:
            return self._decode_frame(self.last_frame)
        except Exception as exc:
            self.status_var.set(f"Decode failed: {exc}")
            return None

    @staticmethod
    def _apply_crop_to_raw_boxes(raw_array: "np.ndarray", crop_boxes: list[dict[str, int]]) -> "np.ndarray | None":
        if not crop_boxes or len(crop_boxes) != 16:
            return None
        out: list[np.ndarray] = []
        for b in sorted(crop_boxes, key=lambda z: int(z["index"])):
            x = int(b["x"])
            y = int(b["y"])
            w = int(b["width"])
            h = int(b["height"])
            out.append(raw_array[y : y + h, x : x + w])
        try:
            return np.stack(out, axis=0)
        except Exception:
            return None

    def _apply_crop_to_raw(self, raw_array: "np.ndarray") -> "np.ndarray | None":
        return self._apply_crop_to_raw_boxes(raw_array, self.crop_boxes)

    def _apply_dark_flat(self, raw_lenses: "np.ndarray") -> "np.ndarray":
        work = raw_lenses.astype(np.float32)
        if self.dark_map is not None and self.dark_map.shape == raw_lenses.shape:
            work = work - self.dark_map.astype(np.float32)
        if self.flat_norm is not None and self.flat_norm.shape == raw_lenses.shape:
            safe = np.where(np.abs(self.flat_norm) < 1e-6, 1e-6, self.flat_norm)
            work = work / safe
        return work

    def _preview_rgb_menon(
        self,
        raw_array: "np.ndarray",
        fmt: str,
        autostretch: bool = False,
        pattern_override: str | None = None,
    ) -> "np.ndarray":
        return debayer_menon_rgb(
            raw_array,
            fmt,
            pattern_override=pattern_override,
            autostretch=autostretch,
        )

    def _preview_rgb_main_fast(
        self,
        raw_array: "np.ndarray",
        fmt: str,
        autostretch: bool = False,
        pattern_override: str | None = None,
    ) -> "np.ndarray":
        return debayer_fast_preview_rgb(
            raw_array,
            fmt,
            pattern_override=pattern_override,
            autostretch=autostretch,
        )

    def _sorted_crop_boxes(self) -> list[dict[str, int]]:
        if not self.crop_boxes:
            return []
        return sorted(self.crop_boxes, key=lambda z: int(z["index"]))

    # ----------------------------- Crop stage -----------------------------
    def _equal_bands(self, length: int, expected: int) -> list[tuple[int, int]]:
        bands: list[tuple[int, int]] = []
        step = max(1, length // expected)
        for i in range(expected):
            a = i * step
            b = length if i == expected - 1 else (i + 1) * step
            if b <= a:
                b = min(length, a + 1)
            bands.append((a, b))
        return bands

    def _find_bands_from_profile(self, profile: "np.ndarray", expected: int, min_len: int) -> list[tuple[int, int]]:
        if profile.size < expected:
            return self._equal_bands(int(profile.size), expected)
        smooth = np.asarray(profile, dtype=np.float32)
        k = max(7, int(round(profile.size * 0.015)))
        if (k % 2) == 0:
            k += 1
        kernel = np.ones((k,), dtype=np.float32) / float(k)
        smooth = np.convolve(smooth, kernel, mode="same")
        lo = float(np.min(smooth))
        hi = float(np.max(smooth))
        if hi <= lo + 1e-6:
            return self._equal_bands(int(profile.size), expected)
        thr = max(float(np.percentile(smooth, 55.0)), lo + 0.35 * (hi - lo))
        mask = (smooth >= thr).astype(np.uint8)
        close_k = max(5, int(round(profile.size * 0.01)))
        close_kernel = np.ones((close_k,), dtype=np.uint8)
        mask = (np.convolve(mask, close_kernel, mode="same") >= max(1, close_k // 2)).astype(np.uint8)

        runs: list[tuple[int, int, float]] = []
        i = 0
        n = int(mask.size)
        while i < n:
            if mask[i] == 0:
                i += 1
                continue
            s = i
            while i < n and mask[i] == 1:
                i += 1
            e = i
            if (e - s) >= min_len:
                runs.append((s, e, float(np.mean(smooth[s:e]))))

        if len(runs) < expected:
            return self._equal_bands(int(profile.size), expected)
        if len(runs) > expected:
            runs.sort(key=lambda x: (x[2], x[1] - x[0]), reverse=True)
            runs = runs[:expected]
        runs.sort(key=lambda x: x[0])
        bands = [(int(s), int(e)) for s, e, _m in runs]
        # Validate spacing/widths: reject pathological split (e.g. one thin top strip + duplicated first row).
        widths = np.asarray([max(1, b[1] - b[0]) for b in bands], dtype=np.float32)
        med_w = float(np.median(widths)) if widths.size else 1.0
        if med_w <= 1.0:
            return self._equal_bands(int(profile.size), expected)
        if float(np.min(widths)) < max(float(min_len), 0.60 * med_w) or float(np.max(widths)) > 1.95 * med_w:
            return self._equal_bands(int(profile.size), expected)
        centers = np.asarray([0.5 * (b[0] + b[1]) for b in bands], dtype=np.float32)
        diffs = np.diff(centers)
        if diffs.size:
            med_d = float(np.median(diffs))
            if med_d <= 1.0 or float(np.min(diffs)) < 0.50 * med_d or float(np.max(diffs)) > 1.85 * med_d:
                return self._equal_bands(int(profile.size), expected)
        return bands

    def _detect_peak_in_roi(self, roi_u8: "np.ndarray") -> tuple[float, float, float]:
        h, w = int(roi_u8.shape[0]), int(roi_u8.shape[1])
        if h <= 0 or w <= 0:
            return 0.0, 0.0, 0.0
        patch = roi_u8
        blur = cv2.GaussianBlur(patch, (0, 0), 3.0)
        hp = np.clip(patch.astype(np.float32) - blur.astype(np.float32), 0.0, None)
        hp_max = float(np.max(hp))
        if hp_max <= 1e-6:
            _mn0, max_v0, _mn_loc0, max_loc0 = cv2.minMaxLoc(patch)
            return float(max_loc0[0]), float(max_loc0[1]), float(max_v0)

        hp_u8 = np.clip((hp / hp_max) * 255.0, 0.0, 255.0).astype(np.uint8)
        thr = float(np.percentile(hp_u8, 99.6))
        thr = max(8.0, thr)
        _ret, bw = cv2.threshold(hp_u8, thr, 255, cv2.THRESH_BINARY)
        k = np.ones((3, 3), dtype=np.uint8)
        bw = cv2.morphologyEx(bw, cv2.MORPH_OPEN, k, iterations=1)

        cc = cv2.connectedComponentsWithStats(bw, connectivity=8)
        n_labels, labels, stats, centroids = cc
        roi_area = max(1, h * w)
        best_idx = -1
        best_score = -1e9
        best_conf = 0.0
        for i in range(1, int(n_labels)):
            area = int(stats[i, cv2.CC_STAT_AREA])
            if area < 1 or area > int(roi_area * 0.06):
                continue
            mask_i = labels == i
            local_max = float(np.max(hp[mask_i])) if np.any(mask_i) else 0.0
            score = local_max - 0.03 * float(area)
            if score > best_score:
                best_score = score
                best_idx = i
                best_conf = local_max

        if best_idx >= 0:
            cx, cy = centroids[best_idx]
            return float(cx), float(cy), float(best_conf)

        _mn, max_v, _mn_loc, max_loc = cv2.minMaxLoc(hp)
        if float(max_v) < 0.5:
            _mn2, max_v2, _mn_loc2, max_loc2 = cv2.minMaxLoc(patch)
            return float(max_loc2[0]), float(max_loc2[1]), float(max_v2)
        return float(max_loc[0]), float(max_loc[1]), float(max_v)

    def _find_chessboard_corners_crop(self, roi_u8: "np.ndarray", cols: int, rows: int) -> "np.ndarray | None":
        # Crop-stage detector: tuned for 16 boards in one full frame (one board per lens ROI).
        # This path intentionally does NOT reuse geometry detector to avoid cross-stage coupling.
        if cv2 is None:
            return None
        h, w = int(roi_u8.shape[0]), int(roi_u8.shape[1])
        if h < 14 or w < 14:
            return None

        candidates: list[np.ndarray] = [roi_u8]
        try:
            clahe_obj = cv2.createCLAHE(clipLimit=2.4, tileGridSize=(8, 8))
            clahe = clahe_obj.apply(roi_u8)
            candidates.append(clahe)
            blur = cv2.GaussianBlur(clahe, (0, 0), 1.0)
            sharp = cv2.addWeighted(clahe, 1.55, blur, -0.55, 0)
            candidates.append(sharp)
        except Exception:
            pass
        candidates.extend([cv2.bitwise_not(img) for img in list(candidates)])

        scales: list[float] = [1.0]
        if min(h, w) < 260:
            scales.append(2.0)

        best: np.ndarray | None = None
        best_score = -1e9
        for sc in scales:
            for img in candidates:
                if abs(sc - 1.0) > 1e-6:
                    ww = max(8, int(round(w * sc)))
                    hh = max(8, int(round(h * sc)))
                    probe = cv2.resize(img, (ww, hh), interpolation=cv2.INTER_LINEAR)
                    scale_back = 1.0 / sc
                else:
                    probe = img
                    scale_back = 1.0

                flags = int(getattr(cv2, "CALIB_CB_ADAPTIVE_THRESH", 0))
                flags |= int(getattr(cv2, "CALIB_CB_NORMALIZE_IMAGE", 0))
                flags |= int(getattr(cv2, "CALIB_CB_FILTER_QUADS", 0))
                try:
                    found, corners = cv2.findChessboardCorners(probe, (cols, rows), flags)
                except Exception:
                    found, corners = (False, None)
                if not found or corners is None or int(corners.shape[0]) != (cols * rows):
                    continue

                crit = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 35, 0.001)
                try:
                    corners = cv2.cornerSubPix(probe, corners, (5, 5), (-1, -1), crit)
                except Exception:
                    pass
                pts = corners.reshape(-1, 2).astype(np.float32)
                if abs(scale_back - 1.0) > 1e-6:
                    pts *= float(scale_back)

                cidx = (rows // 2) * cols + (cols // 2)
                cx = float(pts[cidx, 0])
                cy = float(pts[cidx, 1])
                # Crop-stage prior: board center should stay reasonably central in each lens ROI.
                if cx < 0.10 * w or cx > 0.90 * w or cy < 0.10 * h or cy > 0.90 * h:
                    continue
                spread = float(np.mean(np.linalg.norm(pts - pts[cidx], axis=1)))
                dx = cx - 0.5 * w
                dy = cy - 0.5 * h
                score = spread - 0.010 * float(dx * dx + dy * dy)
                if score > best_score:
                    best_score = score
                    best = pts
        return best

    def _detect_chess_center_in_roi(self, roi_u8: "np.ndarray") -> tuple[float, float, float] | None:
        # 4x4 chessboard cells => 3x3 inner corners, center is index 4.
        cols = 3
        rows = 3
        corners = self._find_chessboard_corners_crop(roi_u8, cols, rows)
        if corners is None or corners.shape[0] != (cols * rows):
            return None
        cidx = (rows // 2) * cols + (cols // 2)
        cx = float(corners[cidx, 0])
        cy = float(corners[cidx, 1])
        spread = float(np.mean(np.linalg.norm(corners - corners[cidx], axis=1)))
        return (cx, cy, spread)

    def _fit_center_grid_from_found(self, found: dict[int, tuple[float, float, float]]) -> dict[int, tuple[float, float, float]]:
        # Model center positions as bilinear function of (row, col) on the 4x4 lens lattice.
        # Bilinear term (c*r) better handles mild lattice warping than plain affine.
        if len(found) < 4:
            return {}
        rows = []
        xvals = []
        yvals = []
        for idx, (x, y, _c) in found.items():
            r = float(idx // 4)
            c = float(idx % 4)
            rows.append([1.0, c, r, c * r])
            xvals.append(float(x))
            yvals.append(float(y))
        A = np.asarray(rows, dtype=np.float32)
        bx = np.asarray(xvals, dtype=np.float32)
        by = np.asarray(yvals, dtype=np.float32)
        try:
            px, _resx, _rankx, _sx = np.linalg.lstsq(A, bx, rcond=None)
            py, _resy, _ranky, _sy = np.linalg.lstsq(A, by, rcond=None)
        except Exception:
            return {}
        out: dict[int, tuple[float, float, float]] = {}
        for idx in range(16):
            if idx in found:
                continue
            r = float(idx // 4)
            c = float(idx % 4)
            vx = float(px[0] + px[1] * c + px[2] * r + px[3] * c * r)
            vy = float(py[0] + py[1] * c + py[2] * r + py[3] * c * r)
            out[idx] = (vx, vy, 0.0)
        return out

    def _refine_center_near_hint(self, roi_u8: "np.ndarray", x_hint: float, y_hint: float, radius: int = 26) -> tuple[float, float, float]:
        if cv2 is None:
            return x_hint, y_hint, 0.0
        h, w = int(roi_u8.shape[0]), int(roi_u8.shape[1])
        if h <= 4 or w <= 4:
            return x_hint, y_hint, 0.0
        xh = int(max(0, min(w - 1, round(float(x_hint)))))
        yh = int(max(0, min(h - 1, round(float(y_hint)))))
        x0 = max(0, xh - radius)
        y0 = max(0, yh - radius)
        x1 = min(w, xh + radius + 1)
        y1 = min(h, yh + radius + 1)
        if x1 <= x0 + 2 or y1 <= y0 + 2:
            return float(xh), float(yh), 0.0
        patch = roi_u8[y0:y1, x0:x1]
        # Prefer true chess center via local robust call around hint.
        got = self._detect_chess_center_in_roi(patch)
        if got is not None:
            gx, gy, conf = got
            return float(x0 + gx), float(y0 + gy), float(conf)
        # If local chessboard lock failed, keep geometric hint (avoid drifting to unrelated bright structures).
        return float(xh), float(yh), 0.0

    def _detect_crop_points(self) -> None:
        if cv2 is None:
            self.status_var.set("OpenCV is required for chess-center detection")
            return
        got = self._get_latest_raw()
        if got is None:
            self.status_var.set("No frame for crop detection")
            return
        raw_arr, fmt = got
        gray_det = raw_to_u8(raw_arr, fmt, autostretch=True)
        h, w = int(gray_det.shape[0]), int(gray_det.shape[1])
        min_len_x = max(30, w // 20)
        min_len_y = max(30, h // 20)
        col_bands = self._find_bands_from_profile(gray_det.mean(axis=0), expected=4, min_len=min_len_x)
        row_bands = self._find_bands_from_profile(gray_det.mean(axis=1), expected=4, min_len=min_len_y)
        pts: list[tuple[float, float, float]] = []
        roi_boxes: list[tuple[int, int, int, int]] = []
        found_map: dict[int, tuple[float, float, float]] = {}
        missing: list[int] = []
        lens_idx = 0
        for r, (y0, y1) in enumerate(row_bands):
            for c, (x0, x1) in enumerate(col_bands):
                x0c = max(0, min(w - 1, int(x0)))
                x1c = max(x0c + 1, min(w, int(x1)))
                y0c = max(0, min(h - 1, int(y0)))
                y1c = max(y0c + 1, min(h, int(y1)))
                roi_boxes.append((x0c, y0c, x1c, y1c))
                roi = gray_det[y0c:y1c, x0c:x1c]
                got_center = self._detect_chess_center_in_roi(roi)
                if got_center is None:
                    missing.append(lens_idx + 1)
                    # Fallback keeps array shape consistent, but user is warned and save is blocked.
                    px, py, conf = self._detect_peak_in_roi(roi)
                    pts.append((x0c + px, y0c + py, conf))
                else:
                    px, py, conf = got_center
                    gx = x0c + px
                    gy = y0c + py
                    pts.append((gx, gy, conf))
                    found_map[lens_idx] = (gx, gy, conf)
                lens_idx += 1
        if self.debug:
            confs = np.asarray([p[2] for p in pts], dtype=np.float32)
            self._dbg(
                f"crop detect grid: row_bands={row_bands} col_bands={col_bands} "
                f"gray_min={int(gray_det.min())} gray_max={int(gray_det.max())} "
                f"conf(min/mean/max)={float(np.min(confs)):.2f}/{float(np.mean(confs)):.2f}/{float(np.max(confs)):.2f}"
            )
        if len(pts) != 16:
            self.status_var.set(f"Internal detection error: got {len(pts)} points instead of 16")
            return
        # Recovery pass: estimate missing centers from 4x4 lattice + local ROI refinement.
        recovered_count = 0
        if missing:
            preds = self._fit_center_grid_from_found(found_map)
            if self.crop_centers is not None and len(self.crop_centers) == 16:
                for midx in [m - 1 for m in missing]:
                    if midx not in preds:
                        preds[midx] = (float(self.crop_centers[midx, 0]), float(self.crop_centers[midx, 1]), 0.0)
            recovered: list[int] = []
            for midx1 in list(missing):
                midx = midx1 - 1
                if midx < 0 or midx >= 16 or midx not in preds:
                    continue
                x0c, y0c, x1c, y1c = roi_boxes[midx]
                roi = gray_det[y0c:y1c, x0c:x1c]
                px_hint = float(preds[midx][0] - x0c)
                py_hint = float(preds[midx][1] - y0c)
                px_hint = max(0.0, min(float(x1c - x0c - 1), px_hint))
                py_hint = max(0.0, min(float(y1c - y0c - 1), py_hint))
                px, py, conf = self._refine_center_near_hint(roi, px_hint, py_hint, radius=28)
                gx = float(x0c + px)
                gy = float(y0c + py)
                pts[midx] = (gx, gy, conf)
                found_map[midx] = (gx, gy, conf)
                recovered.append(midx1)
            if recovered:
                recovered_count = len(recovered)
                missing = [m for m in missing if m not in recovered]
                if self.debug:
                    self._dbg(f"crop detect recovery: recovered={recovered} remaining_missing={missing}")
        if missing:
            self.status_var.set(
                f"Chessboard center not found on lenses: {', '.join(str(i) for i in missing)}. "
                "Use 4x4 chessboard and keep it visible on all 16 lenses."
            )
            return
        ordered = np.asarray([[p[0], p[1]] for p in pts], dtype=np.float32)
        conf_mean = float(np.mean(np.asarray([p[2] for p in pts], dtype=np.float32)))
        self.crop_centers = ordered
        self._recompute_crop_limits(raw_arr.shape[1], raw_arr.shape[0])
        self._on_crop_offsets_changed()
        if recovered_count > 0:
            self.status_var.set(
                f"Detected 16 chess centers (recovered {recovered_count} by grid model). "
                f"Mean confidence={conf_mean:.2f}. Adjust offsets and save crop."
            )
        else:
            self.status_var.set(f"Detected 16 chess centers. Mean confidence={conf_mean:.2f}. Adjust offsets and save crop.")

    def _recompute_crop_limits(self, w: int, h: int) -> None:
        if self.crop_centers is None:
            return
        centers = self.crop_centers.reshape(4, 4, 2)
        left_vals: list[int] = []
        right_vals: list[int] = []
        top_vals: list[int] = []
        bottom_vals: list[int] = []
        for r in range(4):
            for c in range(4):
                cx = float(centers[r, c, 0])
                cy = float(centers[r, c, 1])
                left_bound = 0.0 if c == 0 else 0.5 * (cx + float(centers[r, c - 1, 0]))
                right_bound = (w - 1) if c == 3 else 0.5 * (cx + float(centers[r, c + 1, 0]))
                top_bound = 0.0 if r == 0 else 0.5 * (cy + float(centers[r - 1, c, 1]))
                bottom_bound = (h - 1) if r == 3 else 0.5 * (cy + float(centers[r + 1, c, 1]))
                left_vals.append(max(1, int(math.floor(cx - left_bound))))
                right_vals.append(max(1, int(math.floor(right_bound - cx))))
                top_vals.append(max(1, int(math.floor(cy - top_bound))))
                bottom_vals.append(max(1, int(math.floor(bottom_bound - cy))))
        self.crop_limits = {
            "left": max(1, min(left_vals)),
            "right": max(1, min(right_vals)),
            "top": max(1, min(top_vals)),
            "bottom": max(1, min(bottom_vals)),
        }

    def _on_crop_offsets_changed(self) -> None:
        if self.crop_centers is None or self.last_frame is None:
            return
        w = int(self.last_frame.width)
        h = int(self.last_frame.height)
        self._recompute_crop_limits(w, h)
        left = max(1, min(int(self.crop_left_var.get()), self.crop_limits["left"]))
        right = max(1, min(int(self.crop_right_var.get()), self.crop_limits["right"]))
        top = max(1, min(int(self.crop_top_var.get()), self.crop_limits["top"]))
        bottom = max(1, min(int(self.crop_bottom_var.get()), self.crop_limits["bottom"]))
        self.crop_left_var.set(left)
        self.crop_right_var.set(right)
        self.crop_top_var.set(top)
        self.crop_bottom_var.set(bottom)

        centers = self.crop_centers.reshape(4, 4, 2)
        boxes: list[dict[str, int]] = []
        idx = 0
        for r in range(4):
            for c in range(4):
                cx = float(centers[r, c, 0])
                cy = float(centers[r, c, 1])
                left_bound = 0.0 if c == 0 else 0.5 * (cx + float(centers[r, c - 1, 0]))
                right_bound = (w - 1) if c == 3 else 0.5 * (cx + float(centers[r, c + 1, 0]))
                top_bound = 0.0 if r == 0 else 0.5 * (cy + float(centers[r - 1, c, 1]))
                bottom_bound = (h - 1) if r == 3 else 0.5 * (cy + float(centers[r + 1, c, 1]))

                x0 = int(round(max(left_bound, cx - left)))
                x1 = int(round(min(right_bound, cx + right)))
                y0 = int(round(max(top_bound, cy - top)))
                y1 = int(round(min(bottom_bound, cy + bottom)))
                if x1 <= x0:
                    x1 = x0 + 1
                if y1 <= y0:
                    y1 = y0 + 1
                boxes.append(
                    {
                        "index": idx,
                        "row": r,
                        "col": c,
                        "x": x0,
                        "y": y0,
                        "width": x1 - x0 + 1,
                        "height": y1 - y0 + 1,
                    }
                )
                idx += 1
        self.crop_boxes = boxes
        self._render_preview(force=True)

    def _save_crop_and_continue(self) -> None:
        if self.session_root is None:
            self.status_var.set("No active calibration session")
            return
        if self.crop_centers is None or len(self.crop_boxes) != 16:
            self.status_var.set("Detect points and adjust crop first")
            return
        payload: dict[str, object] = {
            "stage": "crop_calibration",
            "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "image_width": int(self.last_frame.width if self.last_frame else 0),
            "image_height": int(self.last_frame.height if self.last_frame else 0),
            "order": "top_to_bottom_left_to_right",
            "centers_xy": self.crop_centers.tolist(),
            "offsets": {
                "left": int(self.crop_left_var.get()),
                "right": int(self.crop_right_var.get()),
                "top": int(self.crop_top_var.get()),
                "bottom": int(self.crop_bottom_var.get()),
            },
            "boxes": self.crop_boxes,
        }
        self._save_json(self.session_root / "crop.json", payload)
        self._show_calibration_stage("dark")
        self.status_var.set("Crop calibration saved. Proceed to dark map.")

    # ----------------------------- Dark stage -----------------------------
    def _start_dark_capture(self) -> None:
        if self.crop_boxes is None or len(self.crop_boxes) != 16:
            self.status_var.set("Crop calibration is required first")
            return
        if self.dark_capture_active:
            self.status_var.set("Dark capture already running")
            return
        try:
            target = max(1, int(self.dark_burst_var.get()))
        except Exception:
            self.status_var.set("Invalid burst count")
            return
        self._queue_command("set_gain", float(self.gain_var.get()))
        self._queue_command("set_exposure", float(self.exposure_var.get()))
        self.dark_frames = []
        self.frame_dedupe_id = None
        self.dark_capture_active = True
        self._dark_target = target
        self.status_var.set(f"Collecting dark burst: 0/{target}")

    def _consume_dark_frame(self, frame: FramePacket) -> None:
        if not self.dark_capture_active:
            return
        frame_id = int((frame.meta or {}).get("frame_id", -1))
        if frame_id >= 0 and frame_id == self.frame_dedupe_id:
            return
        self.frame_dedupe_id = frame_id
        try:
            raw_arr, _fmt = self._decode_frame(frame)
            lenses = self._apply_crop_to_raw(raw_arr)
            if lenses is None:
                return
            self.dark_frames.append(lenses.copy())
            count = len(self.dark_frames)
            self.status_var.set(f"Collecting dark burst: {count}/{self._dark_target}")
            if count < self._dark_target:
                return
            stack = np.stack(self.dark_frames, axis=0).astype(np.float32)
            mean_dark = stack.mean(axis=0)
            std_dark = stack.std(axis=0)
            if lenses.dtype == np.uint8:
                self.dark_map = np.clip(np.rint(mean_dark), 0, 255).astype(np.uint8)
            elif lenses.dtype == np.uint16:
                self.dark_map = np.clip(np.rint(mean_dark), 0, 65535).astype(np.uint16)
            else:
                self.dark_map = mean_dark.astype(np.float32)
            self.noise_map = std_dark.astype(np.float32)
            self.dark_capture_active = False
            self.locked_gain = float(self.gain_var.get())
            self.locked_exposure = float(self.exposure_var.get())
            if self.session_root is not None:
                np.save(self.session_root / "darkMap.npy", self.dark_map)
                np.save(self.session_root / "noiseMap.npy", self.noise_map)
                np.save(self.session_root / "dark_frames.npy", stack)
                self._save_json(
                    self.session_root / "camera_settings.json",
                    {
                        "exposure_us": self.locked_exposure,
                        "gain_db": self.locked_gain,
                        "locked_after_stage": "black_level",
                    },
                )
            self._show_calibration_stage("flat")
            self.status_var.set("Dark map created. Exposure/Gain locked for this session.")
        except Exception as exc:
            self.dark_capture_active = False
            self.status_var.set(f"Dark map failed: {exc}")

    # ----------------------------- Flat stage -----------------------------
    def _start_flat_capture(self) -> None:
        if self.dark_map is None:
            self.status_var.set("Create dark map first")
            return
        if self.flat_capture_active:
            self.status_var.set("Flat capture already running")
            return
        try:
            target = max(1, int(self.flat_burst_var.get()))
            sigma = max(0.1, float(self.flat_sigma_var.get()))
        except Exception:
            self.status_var.set("Invalid flat settings")
            return
        self.flat_frames = []
        self.frame_dedupe_id = None
        self.flat_capture_active = True
        self._flat_target = target
        self._flat_sigma = sigma
        self.status_var.set(f"Collecting flat burst: 0/{target}")

    def _consume_flat_frame(self, frame: FramePacket) -> None:
        if not self.flat_capture_active:
            return
        frame_id = int((frame.meta or {}).get("frame_id", -1))
        if frame_id >= 0 and frame_id == self.frame_dedupe_id:
            return
        self.frame_dedupe_id = frame_id
        try:
            raw_arr, _fmt = self._decode_frame(frame)
            lenses = self._apply_crop_to_raw(raw_arr)
            if lenses is None:
                return
            work = lenses.astype(np.float32) - self.dark_map.astype(np.float32)
            self.flat_frames.append(work.copy())
            count = len(self.flat_frames)
            self.status_var.set(f"Collecting flat burst: {count}/{self._flat_target}")
            if count < self._flat_target:
                return
            stack = np.stack(self.flat_frames, axis=0).astype(np.float32)
            flat_mean = stack.mean(axis=0)
            sigma = float(self._flat_sigma)
            if gaussian_filter is not None:
                flat_smooth = np.empty_like(flat_mean, dtype=np.float32)
                for i in range(flat_mean.shape[0]):
                    flat_smooth[i] = gaussian_filter(flat_mean[i], sigma=sigma)
            else:
                flat_smooth = flat_mean
            flat_norm = np.empty_like(flat_smooth, dtype=np.float32)
            for i in range(flat_smooth.shape[0]):
                m = float(np.mean(flat_smooth[i]))
                if abs(m) < 1e-6:
                    m = 1.0
                flat_norm[i] = flat_smooth[i] / m
            self.flat_raw_mean = flat_mean
            self.flat_norm = flat_norm
            self.flat_capture_active = False
            if self.session_root is not None:
                np.save(self.session_root / "flat_raw_mean.npy", self.flat_raw_mean.astype(np.float32))
                np.save(self.session_root / "flat_norm.npy", self.flat_norm.astype(np.float32))
                np.save(self.session_root / "flat_frames_minus_dark.npy", stack)
                self._save_json(
                    self.session_root / "flat_calibration.json",
                    {
                        "stage": "flat_field_calibration",
                        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "sigma": sigma,
                        "burst_count": int(stack.shape[0]),
                        "shape": list(flat_norm.shape),
                        "normalization": "per_lens_mean",
                    },
                )
            self._show_calibration_stage("geometry")
            self.geometry_capture_idx = 0
            self.geometry_corners = []
            self.geometry_focus_scores = []
            self.geometry_btn_var.set(f"Capture chess frame 1/{max(1, int(self.geometry_target_var.get()))}")
            self.status_var.set("Flat map created. Proceed to geometry calibration.")
        except Exception as exc:
            self.flat_capture_active = False
            self.status_var.set(f"Flat map failed: {exc}")

    # ----------------------------- Geometry stage -----------------------------
    def _find_chessboard_corners_robust(self, u8: "np.ndarray", cols: int, rows: int) -> "np.ndarray | None":
        if cv2 is None:
            return None
        h, w = int(u8.shape[0]), int(u8.shape[1])
        if h < 12 or w < 12:
            return None

        def _roi_candidates(img_u8: "np.ndarray") -> list[tuple["np.ndarray", int, int]]:
            rois: list[tuple["np.ndarray", int, int]] = [(img_u8, 0, 0)]
            hh, ww = int(img_u8.shape[0]), int(img_u8.shape[1])
            if hh < 20 or ww < 20:
                return rois
            try:
                blur = cv2.GaussianBlur(img_u8, (5, 5), 0.0)
                p = float(np.percentile(blur, 72.0))
                thr = max(8.0, min(245.0, p))
                _ret, bw = cv2.threshold(blur, thr, 255, cv2.THRESH_BINARY)
                k = np.ones((5, 5), dtype=np.uint8)
                bw = cv2.morphologyEx(bw, cv2.MORPH_CLOSE, k, iterations=1)
                bw = cv2.morphologyEx(bw, cv2.MORPH_OPEN, k, iterations=1)
                n, _labels, stats, _cent = cv2.connectedComponentsWithStats(bw, connectivity=8)
                items: list[tuple[float, int, int, int, int]] = []
                area_all = max(1, hh * ww)
                for i in range(1, int(n)):
                    x = int(stats[i, cv2.CC_STAT_LEFT])
                    y = int(stats[i, cv2.CC_STAT_TOP])
                    cw = int(stats[i, cv2.CC_STAT_WIDTH])
                    ch = int(stats[i, cv2.CC_STAT_HEIGHT])
                    area = int(stats[i, cv2.CC_STAT_AREA])
                    if area < int(0.01 * area_all) or area > int(0.90 * area_all):
                        continue
                    if cw < 10 or ch < 10:
                        continue
                    aspect = float(cw) / float(max(1, ch))
                    if aspect < 0.35 or aspect > 2.8:
                        continue
                    squareness = 1.0 - min(1.0, abs(aspect - 1.0))
                    score = float(area) * (0.6 + 0.4 * squareness)
                    items.append((score, x, y, cw, ch))
                items.sort(key=lambda t: t[0], reverse=True)
                for _score, x, y, cw, ch in items[:3]:
                    padx = max(8, int(round(cw * 0.28)))
                    pady = max(8, int(round(ch * 0.28)))
                    x0 = max(0, x - padx)
                    y0 = max(0, y - pady)
                    x1 = min(ww, x + cw + padx)
                    y1 = min(hh, y + ch + pady)
                    if (x1 - x0) < 14 or (y1 - y0) < 14:
                        continue
                    rois.append((img_u8[y0:y1, x0:x1], x0, y0))
            except Exception:
                pass
            return rois

        def _try_one(img_u8: "np.ndarray", scale_back: float, ox: int = 0, oy: int = 0) -> "np.ndarray | None":
            corners = None
            found = False
            # Use classic CPU detector first for stability on macOS.
            flags = int(getattr(cv2, "CALIB_CB_ADAPTIVE_THRESH", 0))
            flags |= int(getattr(cv2, "CALIB_CB_NORMALIZE_IMAGE", 0))
            flags |= int(getattr(cv2, "CALIB_CB_FILTER_QUADS", 0))
            try:
                found, corners = cv2.findChessboardCorners(img_u8, (cols, rows), flags)
            except Exception:
                found, corners = (False, None)
            if found and corners is not None:
                crit = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 35, 0.001)
                corners = cv2.cornerSubPix(img_u8, corners, (5, 5), (-1, -1), crit)

            # Optional fallback to SB only if explicitly enabled by env.
            if (not found) and hasattr(cv2, "findChessboardCornersSB") and os.environ.get("HYDRA_ENABLE_SB", "0") == "1":
                sb_flags = 0
                for name in ("CALIB_CB_NORMALIZE_IMAGE", "CALIB_CB_EXHAUSTIVE", "CALIB_CB_ACCURACY"):
                    sb_flags |= int(getattr(cv2, name, 0))
                try:
                    found, corners = cv2.findChessboardCornersSB(img_u8, (cols, rows), sb_flags)
                except Exception:
                    found, corners = (False, None)
            if not found or corners is None or int(corners.shape[0]) != (cols * rows):
                return None
            out = corners.reshape(-1, 2).astype(np.float32)
            if abs(scale_back - 1.0) > 1e-6:
                out *= float(scale_back)
            if ox or oy:
                out[:, 0] += float(ox)
                out[:, 1] += float(oy)
            return out

        # Candidate inputs: raw, equalized, sharpened, inverted variants.
        base = u8
        clahe = None
        sharp = None
        adapt = None
        try:
            clahe_obj = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            clahe = clahe_obj.apply(base)
            blur = cv2.GaussianBlur(clahe, (0, 0), 1.2)
            sharp = cv2.addWeighted(clahe, 1.6, blur, -0.6, 0)
            adapt = cv2.adaptiveThreshold(
                clahe,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                31,
                3,
            )
        except Exception:
            pass
        candidates: list[np.ndarray] = [base]
        if clahe is not None:
            candidates.append(clahe)
        if sharp is not None:
            candidates.append(sharp)
        if adapt is not None:
            candidates.append(adapt)
        candidates.extend([cv2.bitwise_not(img) for img in list(candidates)])

        # If board is small in ROI, upscaling often helps SB/classic detector.
        scales: list[float] = [1.0]
        if min(h, w) < 520:
            scales.append(2.0)
        if min(h, w) < 320:
            scales.append(3.0)

        for sc in scales:
            for img in candidates:
                for roi_img, ox, oy in _roi_candidates(img):
                    rh, rw = int(roi_img.shape[0]), int(roi_img.shape[1])
                    if abs(sc - 1.0) > 1e-6:
                        up = cv2.resize(roi_img, (int(round(rw * sc)), int(round(rh * sc))), interpolation=cv2.INTER_LINEAR)
                        c = _try_one(up, scale_back=(1.0 / sc), ox=ox, oy=oy)
                    else:
                        c = _try_one(roi_img, scale_back=1.0, ox=ox, oy=oy)
                    if c is not None:
                        return c
        return None

    def _find_chessboard_corners_geometry(self, u8: "np.ndarray", cols: int, rows: int) -> "np.ndarray | None":
        # Geometry-stage detector: one board per already-cropped lens image.
        # Kept isolated from crop-stage detector by dedicated method.
        return self._find_chessboard_corners_robust(u8, cols, rows)

    def _capture_geometry_frame(self) -> None:
        if cv2 is None:
            self.status_var.set("OpenCV is required for geometry calibration")
            return
        if self.dark_map is None or self.flat_norm is None:
            self.status_var.set("Dark/Flat maps are required first")
            return
        got = self._get_latest_raw()
        if got is None:
            self.status_var.set("No frame for geometry capture")
            return
        try:
            cols = max(2, int(self.geometry_cols_var.get()))
            rows = max(2, int(self.geometry_rows_var.get()))
            target = max(1, int(self.geometry_target_var.get()))
        except Exception:
            self.status_var.set("Invalid geometry settings")
            return
        raw_arr, fmt = got
        lenses = self._apply_crop_to_raw(raw_arr)
        if lenses is None:
            self.status_var.set("Crop configuration is invalid")
            return
        work = self._apply_dark_flat(lenses)
        corners_dict: dict[int, np.ndarray] = {}
        focus_scores = np.zeros((16,), dtype=np.float32)
        missing: list[int] = []
        for i in range(16):
            gray_f = work[i]
            u8 = raw_to_u8(np.clip(gray_f, 0, 65535).astype(np.uint16), fmt)
            u8_stretch = raw_to_u8(np.clip(gray_f, 0, 65535).astype(np.uint16), fmt, autostretch=True)
            # Fallback source: original RAW crop as uint8, useful if flat normalization weakens pattern contrast.
            raw_u8 = raw_to_u8(np.asarray(lenses[i]), fmt, autostretch=False)
            raw_u8_stretch = raw_to_u8(np.asarray(lenses[i]), fmt, autostretch=True)
            lap = cv2.Laplacian(u8, cv2.CV_32F)
            focus_scores[i] = float(lap.var())
            corners = self._find_chessboard_corners_geometry(u8, cols, rows)
            if corners is None:
                corners = self._find_chessboard_corners_geometry(u8_stretch, cols, rows)
            if corners is None:
                corners = self._find_chessboard_corners_geometry(raw_u8, cols, rows)
            if corners is None:
                corners = self._find_chessboard_corners_geometry(raw_u8_stretch, cols, rows)
            if corners is not None and corners.shape[0] == cols * rows:
                corners_dict[i] = corners.astype(np.float32)
            else:
                missing.append(i + 1)
                if self.debug:
                    self._dbg(
                        f"geom lens {i + 1}: chessboard not found; "
                        f"focus={focus_scores[i]:.2f} u8[min,max]=[{int(u8.min())},{int(u8.max())}] "
                        f"raw_u8[min,max]=[{int(raw_u8.min())},{int(raw_u8.max())}]"
                    )
        if missing:
            self.status_var.set(f"Chessboard not found on lenses: {', '.join(str(i) for i in missing)}")
            return
        self.geometry_corners.append(corners_dict)
        self.geometry_focus_scores.append(focus_scores)
        self.geometry_capture_idx += 1
        if self.geometry_capture_idx < target:
            self.geometry_btn_var.set(f"Capture chess frame {self.geometry_capture_idx + 1}/{target}")
            self.status_var.set(f"Valid geometry frame captured: {self.geometry_capture_idx}/{target}")
            return
        self.geometry_btn_var.set("Solving geometry...")
        self.geometry_progress_var.set(5.0)
        self.geometry_progress_text_var.set("Estimating homographies...")
        self.status_var.set("Geometry solve started")
        threading.Thread(target=self._solve_geometry_worker, args=(cols, rows), daemon=True).start()

    def _solve_geometry_worker(self, cols: int, rows: int) -> None:
        if cv2 is None:
            self._push_ui_event("status", "OpenCV is unavailable")
            return
        try:
            focus_stack = np.stack(self.geometry_focus_scores, axis=0).astype(np.float32)  # [n,16]
            focus_mean = focus_stack.mean(axis=0)
            order = np.argsort(focus_mean)
            ref = int(order[len(order) // 2])
            H = np.full((16, 3, 3), np.nan, dtype=np.float64)
            H[ref] = np.eye(3, dtype=np.float64)
            lens_info: list[dict[str, object]] = []
            for i in range(16):
                if i == ref:
                    lens_info.append({"lens_index": i, "status": "ok", "reference_lens": True})
                    self._push_ui_event("geometry_progress", {"value": 10 + (i + 1) * 5, "text": f"Lens {i + 1}/16"})
                    continue
                src_all: list[np.ndarray] = []
                dst_all: list[np.ndarray] = []
                for cap in self.geometry_corners:
                    src = cap.get(i)
                    dst = cap.get(ref)
                    if src is None or dst is None:
                        continue
                    if src.shape[0] != dst.shape[0] or src.shape[0] < 4:
                        continue
                    src_all.append(src)
                    dst_all.append(dst)
                if not src_all:
                    lens_info.append({"lens_index": i, "status": "insufficient_data"})
                    self._push_ui_event("geometry_progress", {"value": 10 + (i + 1) * 5, "text": f"Lens {i + 1}/16"})
                    continue
                src_pts = np.vstack(src_all).astype(np.float32)
                dst_pts = np.vstack(dst_all).astype(np.float32)
                h_mat, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 3.0)
                if h_mat is None:
                    lens_info.append({"lens_index": i, "status": "failed"})
                else:
                    H[i] = h_mat.astype(np.float64)
                    inliers = int(mask.sum()) if mask is not None else int(src_pts.shape[0])
                    lens_info.append({"lens_index": i, "status": "ok", "inliers": inliers, "points_used": int(src_pts.shape[0])})
                self._push_ui_event("geometry_progress", {"value": 10 + (i + 1) * 5, "text": f"Lens {i + 1}/16"})

            self.geometry_h = H
            self.reference_lens = ref
            if self.session_root is not None:
                np.save(self.session_root / "H_lens.npy", H)
                np.save(self.session_root / "focus_scores.npy", focus_stack)
                self._save_json(
                    self.session_root / "focus_scores.json",
                    {
                        "focus_score_metric": "variance_of_laplacian",
                        "mean_scores": focus_mean.tolist(),
                        "reference_lens": ref,
                    },
                )
                self._save_json(
                    self.session_root / "geometry_calibration.json",
                    {
                        "stage": "geometry_calibration",
                        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "reference_lens": ref,
                        "inner_corners": {"cols": cols, "rows": rows},
                        "captures_count": len(self.geometry_corners),
                        "lenses": lens_info,
                    },
                )
            self._push_ui_event("geometry_done", {"reference_lens": ref})
        except Exception as exc:
            self._push_ui_event("status", f"Geometry solve failed: {exc}")
            self._push_ui_event("geometry_progress", {"value": 0.0, "text": ""})

    # ----------------------------- Main snapshot -----------------------------
    def _on_main_lens_change(self, _event: tk.Event) -> None:
        try:
            self.main_lens_var.set(int(self.main_lens_combo.get()))
        except Exception:
            self.main_lens_var.set(1)
        self._render_preview(force=True)

    def _current_frame_id(self) -> int:
        if self.last_frame is None:
            return -1
        try:
            return int((self.last_frame.meta or {}).get("frame_id", -1))
        except Exception:
            return -1

    def _wait_for_frame_advance(self, previous_frame_id: int, timeout_s: float) -> bool:
        deadline = time.monotonic() + max(0.05, float(timeout_s))
        while time.monotonic() < deadline:
            try:
                self.update()
            except Exception:
                pass
            current = self._current_frame_id()
            if current > previous_frame_id:
                return True
            time.sleep(0.01)
        return False

    def _ordered_wavelength_mapping_for_snapshot(self) -> tuple[list[int], list[float], list[str], str]:
        if not self.wavelength_mapping_entries:
            return [], [], [], "none"
        entries = list(self.wavelength_mapping_entries)
        order_idx: list[int] = []
        order_wl: list[float] = []
        order_labels: list[str] = []
        seen: set[int] = set()
        sortable: list[tuple[float, int, str]] = []
        for e in entries:
            try:
                ch_idx = int(e.get("channel_index", -1))
                wl = float(e.get("wavelength_nm"))
                label = str(e.get("label", ""))
            except Exception:
                continue
            if ch_idx < 0 or ch_idx >= 48:
                continue
            if ch_idx in seen:
                continue
            seen.add(ch_idx)
            sortable.append((wl, ch_idx, label))
        # Snapshot channels must be saved in ascending wavelength order.
        sortable.sort(key=lambda x: (x[0], x[1]))
        for wl, ch_idx, label in sortable:
            order_idx.append(ch_idx)
            order_wl.append(wl)
            order_labels.append(label)
        source = str(self.wavelength_mapping_source or "session_wavelength_mapping") + " (sorted_by_wavelength_asc)"
        return order_idx, order_wl, order_labels, source

    def _snapshot(self) -> None:
        if self.last_frame is None:
            self.status_var.set("No frame for snapshot")
            return
        if self.crop_boxes is None or len(self.crop_boxes) != 16:
            self.status_var.set("Calibration is incomplete: crop data missing")
            return
        if self.dark_map is None or self.flat_norm is None or self.geometry_h is None:
            self.status_var.set("Calibration is incomplete: dark/flat/geometry missing")
            return
        live_gain_before = float(self.gain_var.get())
        live_exposure_before = float(self.exposure_var.get())
        snapshot_gain = float(self.locked_gain if self.locked_gain is not None else live_gain_before)
        snapshot_exposure = float(self.locked_exposure if self.locked_exposure is not None else live_exposure_before)
        switched_for_snapshot = False
        try:
            # Snapshot must use calibration camera settings, even if user changed live controls in main view.
            if self.worker and self.worker.is_alive():
                if (
                    abs(snapshot_gain - live_gain_before) > 1e-6
                    or abs(snapshot_exposure - live_exposure_before) > 1e-3
                ):
                    self.status_var.set(
                        f"Snapshot: applying calibration settings (Gain={snapshot_gain:.3f}, Exposure={snapshot_exposure:.1f} us)..."
                    )
                    prev_id = self._current_frame_id()
                    self._queue_command("set_exposure", snapshot_exposure)
                    self._queue_command("set_gain", snapshot_gain)
                    self._queue_command("refresh_controls", None)
                    wait_s = max(0.6, min(2.4, snapshot_exposure / 1_000_000.0 * 3.2 + 0.2))
                    self._wait_for_frame_advance(prev_id, timeout_s=wait_s)
                    switched_for_snapshot = True

            got = self._get_latest_raw()
            if got is None:
                return
            raw_arr, fmt = got
            raw_lenses = self._apply_crop_to_raw(raw_arr)
            if raw_lenses is None:
                self.status_var.set("Invalid crop data")
                return
            corrected = self._apply_dark_flat(raw_lenses)

            boxes_sorted = self._sorted_crop_boxes()
            lens_rgb = np.empty((16, corrected.shape[1], corrected.shape[2], 3), dtype=np.float32)
            for i in range(16):
                pattern_i = None
                if i < len(boxes_sorted):
                    bi = boxes_sorted[i]
                    pattern_i = bayer_pattern_for_crop(fmt, int(bi["x"]), int(bi["y"]))
                rgb_u8 = debayer_menon_rgb(
                    np.clip(corrected[i], 0, 65535).astype(np.uint16),
                    fmt,
                    pattern_override=pattern_i,
                )
                lens_rgb[i] = rgb_u8.astype(np.float32)

            ref_h = int(lens_rgb[self.reference_lens].shape[0])
            ref_w = int(lens_rgb[self.reference_lens].shape[1])
            warped = np.empty((16, ref_h, ref_w, 3), dtype=np.float32)
            for i in range(16):
                h_mat = self.geometry_h[i]
                if np.isnan(h_mat).any():
                    warped[i] = (
                        cv2.resize(lens_rgb[i], (ref_w, ref_h), interpolation=cv2.INTER_NEAREST)
                        if cv2 is not None
                        else lens_rgb[i]
                    )
                    continue
                if cv2 is not None:
                    warped[i] = cv2.warpPerspective(
                        lens_rgb[i], h_mat.astype(np.float32), (ref_w, ref_h), flags=cv2.INTER_LINEAR
                    )
                else:
                    warped[i] = lens_rgb[i]

            cube = np.empty((48, ref_h, ref_w), dtype=np.float32)
            for i in range(16):
                base = i * 3
                cube[base + 0] = warped[i, :, :, 0]
                cube[base + 1] = warped[i, :, :, 1]
                cube[base + 2] = warped[i, :, :, 2]

            mapping_idx, mapping_wl, mapping_labels, mapping_source = self._ordered_wavelength_mapping_for_snapshot()
            cube_out = cube
            mapping_applied = False
            mapping_note = "default_channels_order"
            if mapping_idx:
                cube_out = cube[mapping_idx, :, :]
                mapping_applied = True
                if len(mapping_idx) == 48:
                    mapping_note = "full_48ch_reorder"
                else:
                    mapping_note = f"partial_reorder_{len(mapping_idx)}ch"

            if self.session_root is None:
                ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
                self.session_root = self.output_dir / f"session_{ts}_snapshot"
                self.session_root.mkdir(parents=True, exist_ok=True)
            cap_dir = self.session_root / "captures"
            cap_dir.mkdir(parents=True, exist_ok=True)
            stem = dt.datetime.now().strftime("snapshot_%Y-%m-%d_%H-%M-%S_%f")
            snap_dir = cap_dir / stem
            snap_dir.mkdir(parents=True, exist_ok=True)
            cube_path = snap_dir / "cube.npy"
            np.save(cube_path, cube_out)
            np.save(snap_dir / "raw_full.npy", raw_arr)
            np.save(snap_dir / "raw_lenses.npy", raw_lenses)
            np.save(snap_dir / "corrected_lenses.npy", corrected.astype(np.float32))
            np.save(snap_dir / "rgb_lenses.npy", warped.astype(np.float32))
            if mapping_idx:
                np.save(snap_dir / "wavelengths_nm.npy", np.asarray(mapping_wl, dtype=np.float32))
                (snap_dir / "wavelengths_order.txt").write_text(
                    "\n".join(f"{wl:.6f}" for wl in mapping_wl) + "\n",
                    encoding="utf-8",
                )
                self._save_json(
                    snap_dir / "wavelengths.json",
                    {
                        "order_labels": mapping_labels,
                        "wavelengths_nm": mapping_wl,
                        "channel_indices_from_default_cube": mapping_idx,
                        "source": mapping_source,
                    },
                )
            self._save_json(
                snap_dir / "snapshot.json",
                {
                    "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "format": "cube_48_h_w",
                    "channels_order_default": "[L1_R, L1_G, L1_B, ... L16_B]",
                    "shape": list(cube_out.shape),
                    "dtype": str(cube_out.dtype),
                    "reference_lens": int(self.reference_lens),
                    "source_pixel_format": fmt,
                    "snapshot_capture_gain_db": snapshot_gain,
                    "snapshot_capture_exposure_us": snapshot_exposure,
                    "live_gain_before_snapshot_db": live_gain_before,
                    "live_exposure_before_snapshot_us": live_exposure_before,
                    "wavelength_mapping": {
                        "applied": mapping_applied,
                        "source": mapping_source if mapping_applied else None,
                        "count": int(len(mapping_idx)),
                        "note": mapping_note,
                        "labels_preview": mapping_labels[:8] if mapping_applied else [],
                    },
                    "files": {
                        "cube": "cube.npy",
                        "raw_full": "raw_full.npy",
                        "raw_lenses": "raw_lenses.npy",
                        "corrected_lenses": "corrected_lenses.npy",
                        "rgb_lenses": "rgb_lenses.npy",
                        "wavelengths_nm": "wavelengths_nm.npy" if mapping_applied else None,
                        "wavelengths_txt": "wavelengths_order.txt" if mapping_applied else None,
                        "wavelengths_json": "wavelengths.json" if mapping_applied else None,
                    },
                },
            )
            self.status_var.set(f"Snapshot saved: {snap_dir}")
        finally:
            if switched_for_snapshot and self.worker and self.worker.is_alive():
                self._queue_command("set_exposure", live_exposure_before)
                self._queue_command("set_gain", live_gain_before)
                self._queue_command("refresh_controls", None)

    # ----------------------------- Render -----------------------------
    def _fit_rgb_for_canvas(self, rgb: "np.ndarray", canvas: tk.Canvas, nearest: bool = True) -> tuple[tk.PhotoImage, tuple[float, float, float]]:
        cw = max(1, int(canvas.winfo_width()))
        ch = max(1, int(canvas.winfo_height()))
        h, w = int(rgb.shape[0]), int(rgb.shape[1])
        scale = min(cw / max(1, w), ch / max(1, h))
        dw = max(1, int(round(w * scale)))
        dh = max(1, int(round(h * scale)))
        img = Image.fromarray(rgb, mode="RGB")
        resample = Image.Resampling.NEAREST if nearest else Image.Resampling.BILINEAR
        if dw != w or dh != h:
            img = img.resize((dw, dh), resample=resample)
        photo = ImageTk.PhotoImage(img)
        x0 = 0.5 * (cw - dw)
        y0 = 0.5 * (ch - dh)
        return photo, (x0, y0, scale)

    def _draw_crop_overlay(self) -> None:
        if self.current_page != "calib" or self.calib_stage != "crop":
            return
        if self._calib_display_map is None:
            return
        self.calib_canvas.delete("overlay")
        x0, y0, s = self._calib_display_map
        if self.crop_centers is not None:
            for i, pt in enumerate(self.crop_centers):
                cx = x0 + float(pt[0]) * s
                cy = y0 + float(pt[1]) * s
                self.calib_canvas.create_oval(cx - 3, cy - 3, cx + 3, cy + 3, outline="#00ff88", width=2, tags="overlay")
                self.calib_canvas.create_text(cx + 8, cy - 8, text=str(i + 1), fill="#00ff88", anchor="sw", tags="overlay")
        for b in self.crop_boxes:
            rx0 = x0 + float(b["x"]) * s
            ry0 = y0 + float(b["y"]) * s
            rx1 = x0 + float(b["x"] + b["width"]) * s
            ry1 = y0 + float(b["y"] + b["height"]) * s
            self.calib_canvas.create_rectangle(rx0, ry0, rx1, ry1, outline="#ffaa00", width=1, tags="overlay")

    def _queue_render_request(self) -> None:
        if self.last_frame is None:
            return
        self._render_submit_seq += 1
        req = {
            "seq": int(self._render_submit_seq),
            "frame": self.last_frame,
            "page": str(self.current_page),
            "stage": str(self.calib_stage),
            "main_view_mode": str(self.main_view_mode.get()),
            "main_lens": int(self.main_lens_var.get()),
            "crop_boxes": [dict(b) for b in self._sorted_crop_boxes()],
            "dark_map": self.dark_map,
            "flat_norm": self.flat_norm,
            "force_u8_mode": bool(self.force_u8_mode),
        }
        while True:
            try:
                self._render_in_q.get_nowait()
            except queue.Empty:
                break
        try:
            self._render_in_q.put_nowait(req)
        except Exception:
            pass

    def _render_worker_loop(self) -> None:
        while not self._render_stop_event.is_set():
            try:
                req = self._render_in_q.get(timeout=0.12)
            except queue.Empty:
                continue
            if req is None:
                continue
            try:
                result = self._build_preview_rgb_for_request(req)
            except Exception as exc:
                result = {
                    "seq": int(req.get("seq", 0)),
                    "page": str(req.get("page", "")),
                    "stage": str(req.get("stage", "")),
                    "error": f"{exc}",
                }
            while True:
                try:
                    self._render_out_q.get_nowait()
                except queue.Empty:
                    break
            try:
                self._render_out_q.put_nowait(result)
            except Exception:
                pass

    def _build_preview_rgb_for_request(self, req: dict[str, object]) -> dict[str, object]:
        frame = req.get("frame")
        if not isinstance(frame, FramePacket):
            return {"seq": int(req.get("seq", 0)), "page": str(req.get("page", "")), "stage": str(req.get("stage", ""))}
        meta = frame.meta or {}
        fmt = meta.get("pixel_format_name") or meta.get("pixel_format") or frame.pixel_format
        fmt_name = pixel_format_to_name(fmt)
        raw_arr = decode_buffer_to_ndarray(frame.raw, frame.width, frame.height, fmt)
        if bool(req.get("force_u8_mode", True)) and raw_arr.dtype != np.uint8:
            raw_arr = raw_to_u8(raw_arr, fmt_name, autostretch=False)

        page = str(req.get("page", ""))
        stage = str(req.get("stage", ""))
        rgb: np.ndarray | None = None
        crop_boxes = req.get("crop_boxes") if isinstance(req.get("crop_boxes"), list) else []
        boxes_sorted = sorted(crop_boxes, key=lambda z: int(z["index"])) if crop_boxes else []
        dark_map = req.get("dark_map")
        flat_norm = req.get("flat_norm")

        if page == "calib":
            if stage == "crop":
                rgb = self._preview_rgb_main_fast(raw_arr, fmt_name, autostretch=False)
            else:
                lenses = self._apply_crop_to_raw_boxes(raw_arr, boxes_sorted)
                if lenses is None:
                    rgb = self._preview_rgb_main_fast(raw_arr, fmt_name, autostretch=False)
                else:
                    work = lenses
                    if stage in ("flat", "geometry") and isinstance(dark_map, np.ndarray) and isinstance(flat_norm, np.ndarray):
                        if dark_map.shape == lenses.shape and flat_norm.shape == lenses.shape:
                            work = lenses.astype(np.float32) - dark_map.astype(np.float32)
                            safe = np.where(np.abs(flat_norm) < 1e-6, 1e-6, flat_norm)
                            work = work / safe
                    elif stage in ("dark",) and isinstance(dark_map, np.ndarray):
                        if dark_map.shape == lenses.shape:
                            work = lenses.astype(np.float32) - dark_map.astype(np.float32)
                    rgb_lenses = np.empty((16, work.shape[1], work.shape[2], 3), dtype=np.uint8)
                    for i in range(16):
                        pattern_i = None
                        if i < len(boxes_sorted):
                            bi = boxes_sorted[i]
                            pattern_i = bayer_pattern_for_crop(fmt_name, int(bi["x"]), int(bi["y"]))
                        rgb_lenses[i] = self._preview_rgb_main_fast(
                            np.clip(work[i], 0, 65535).astype(np.uint16),
                            fmt_name,
                            autostretch=False,
                            pattern_override=pattern_i,
                        )
                    rgb = compose_grid16_rgb(rgb_lenses, gap=2)
        elif page == "main":
            lenses = self._apply_crop_to_raw_boxes(raw_arr, boxes_sorted)
            if lenses is None:
                rgb = self._preview_rgb_main_fast(raw_arr, fmt_name)
            else:
                if isinstance(dark_map, np.ndarray) and isinstance(flat_norm, np.ndarray):
                    if dark_map.shape == lenses.shape and flat_norm.shape == lenses.shape:
                        work = lenses.astype(np.float32) - dark_map.astype(np.float32)
                        safe = np.where(np.abs(flat_norm) < 1e-6, 1e-6, flat_norm)
                        work = work / safe
                    else:
                        work = lenses.astype(np.float32)
                else:
                    work = lenses.astype(np.float32)
                rgb_lenses = np.empty((16, work.shape[1], work.shape[2], 3), dtype=np.uint8)
                for i in range(16):
                    pattern_i = None
                    if i < len(boxes_sorted):
                        bi = boxes_sorted[i]
                        pattern_i = bayer_pattern_for_crop(fmt_name, int(bi["x"]), int(bi["y"]))
                    rgb_lenses[i] = self._preview_rgb_main_fast(
                        np.clip(work[i], 0, 65535).astype(np.uint16),
                        fmt_name,
                        pattern_override=pattern_i,
                    )
                if str(req.get("main_view_mode", "grid")) == "single":
                    idx = max(1, min(16, int(req.get("main_lens", 1)))) - 1
                    rgb = rgb_lenses[idx]
                else:
                    rgb = compose_grid16_rgb(rgb_lenses, gap=2)

        dark_hint = None
        try:
            raw_max_hint = float(raw_arr.max()) if raw_arr.size else 0.0
            if page == "calib" and stage == "crop" and raw_max_hint <= 4.0:
                dark_hint = "Crop preview is very dark (RAW max<=4). Increase Exposure/Gain."
        except Exception:
            pass

        return {
            "seq": int(req.get("seq", 0)),
            "page": page,
            "stage": stage,
            "rgb": rgb,
            "fmt": fmt_name,
            "raw_dtype": str(raw_arr.dtype),
            "raw_shape": tuple(raw_arr.shape),
            "raw_min": float(raw_arr.min()) if raw_arr.size else 0.0,
            "raw_max": float(raw_arr.max()) if raw_arr.size else 0.0,
            "dark_hint": dark_hint,
        }

    def _drain_render_results(self) -> None:
        latest: dict[str, object] | None = None
        while True:
            try:
                latest = self._render_out_q.get_nowait()
            except queue.Empty:
                break
        if latest is None:
            return
        seq = int(latest.get("seq", 0))
        if seq < self._render_applied_seq:
            return
        self._render_applied_seq = seq

        err = latest.get("error")
        if err:
            self.status_var.set(f"Preview render error: {err}")
            return
        page = str(latest.get("page", ""))
        stage = str(latest.get("stage", ""))
        if page != self.current_page:
            return
        if page == "calib" and stage != self.calib_stage:
            return
        rgb = latest.get("rgb")
        if not isinstance(rgb, np.ndarray):
            return

        if page == "calib":
            photo, m = self._fit_rgb_for_canvas(rgb, self.calib_canvas, nearest=True)
            self._calib_photo = photo
            self._calib_display_map = m
            self.calib_canvas.delete("all")
            self.calib_canvas.create_image(m[0], m[1], anchor="nw", image=self._calib_photo)
            self._draw_crop_overlay()
        elif page == "main":
            photo, m = self._fit_rgb_for_canvas(rgb, self.main_canvas, nearest=True)
            self._main_photo = photo
            self._main_display_map = m
            self.main_canvas.delete("all")
            self.main_canvas.create_image(m[0], m[1], anchor="nw", image=self._main_photo)

        hint = latest.get("dark_hint")
        if hint and (time.monotonic() - self._dark_preview_hint_ts) >= 2.0:
            self._dark_preview_hint_ts = time.monotonic()
            self.status_var.set(str(hint))

        if self.debug:
            now_log = time.monotonic()
            if (now_log - self._debug_last_render_log_ts) >= 1.0:
                self._debug_last_render_log_ts = now_log
                self._dbg(
                    f"render: page={page} stage={stage} fmt={latest.get('fmt')} "
                    f"dtype={latest.get('raw_dtype')} shape={latest.get('raw_shape')} "
                    f"raw_min={float(latest.get('raw_min', 0.0)):.2f} raw_max={float(latest.get('raw_max', 0.0)):.2f}"
                )
            try:
                rgb_min = int(rgb.min())
                rgb_max = int(rgb.max())
                if rgb_max <= 1:
                    self._dbg(f"render warning: rgb appears dark/flat (min={rgb_min}, max={rgb_max})")
            except Exception:
                pass

    def _render_preview(self, force: bool = False) -> None:
        if self.last_frame is None:
            return
        frame_id = int((self.last_frame.meta or {}).get("frame_id", -1))
        if (not force) and frame_id >= 0 and frame_id == self._last_rendered_frame_id:
            return
        now = time.monotonic()
        render_interval = self.render_interval_s
        if self.current_page == "calib" and self.calib_stage == "crop":
            render_interval = max(render_interval, 0.10)
        if (not force) and (now - self.last_render_ts) < render_interval:
            return
        self.last_render_ts = now
        if frame_id >= 0:
            self._last_rendered_frame_id = frame_id
        self._queue_render_request()

    # ----------------------------- Crop manual drag -----------------------------
    def _canvas_to_raw(self, x: float, y: float) -> tuple[float, float] | None:
        m = self._calib_display_map
        if m is None:
            return None
        ox, oy, s = m
        if s <= 0:
            return None
        return (x - ox) / s, (y - oy) / s

    def _on_calib_canvas_press(self, event: tk.Event) -> None:
        if self.calib_stage != "crop" or not self.manual_points_var.get() or self.crop_centers is None:
            return
        xy = self._canvas_to_raw(float(event.x), float(event.y))
        if xy is None:
            return
        x, y = xy
        d2 = ((self.crop_centers[:, 0] - x) ** 2 + (self.crop_centers[:, 1] - y) ** 2).astype(np.float32)
        idx = int(np.argmin(d2))
        if float(d2[idx]) <= 900.0:  # 30 px radius
            self._drag_center_idx = idx

    def _on_calib_canvas_drag(self, event: tk.Event) -> None:
        if self._drag_center_idx is None or self.crop_centers is None or self.last_frame is None:
            return
        xy = self._canvas_to_raw(float(event.x), float(event.y))
        if xy is None:
            return
        x = max(0.0, min(float(self.last_frame.width - 1), float(xy[0])))
        y = max(0.0, min(float(self.last_frame.height - 1), float(xy[1])))
        self.crop_centers[self._drag_center_idx, 0] = x
        self.crop_centers[self._drag_center_idx, 1] = y
        pts = self.crop_centers[np.argsort(self.crop_centers[:, 1])]
        rows = []
        for r in range(4):
            row = pts[r * 4 : (r + 1) * 4]
            row = row[np.argsort(row[:, 0])]
            rows.append(row)
        self.crop_centers = np.vstack(rows).astype(np.float32)
        self._on_crop_offsets_changed()

    def _on_calib_canvas_release(self, _event: tk.Event) -> None:
        self._drag_center_idx = None

    # ----------------------------- Event loop -----------------------------
    def _push_ui_event(self, kind: str, payload: object | None = None) -> None:
        try:
            self.event_q.put_nowait((kind, payload))
        except Exception:
            pass

    def _poll_events(self) -> None:
        handled = 0
        frame_updated = False
        while handled < 24:
            try:
                kind, payload = self.event_q.get_nowait()
            except queue.Empty:
                break
            handled += 1
            if kind == "status":
                self.status_var.set(str(payload))
            elif kind == "auto_fix_set_ip":
                self.camera_var.set(str(payload))
            elif kind == "auto_fix_done":
                self.auto_fix_running = False
            elif kind == "camera_scan_finish":
                self.camera_scan_running = False
            elif kind == "camera_scan_done":
                payload_d = dict(payload) if isinstance(payload, dict) else {}
                ok = bool(payload_d.get("ok"))
                if not ok:
                    self.status_var.set(f"Camera scan failed: {payload_d.get('error', 'unknown error')}")
                else:
                    devices = payload_d.get("devices") if isinstance(payload_d.get("devices"), list) else []
                    disp: list[str] = []
                    mapping: dict[str, str] = {}
                    for item in devices:
                        if not (isinstance(item, (list, tuple)) and len(item) >= 1):
                            continue
                        dev_id = str(item[0])
                        transport = str(item[1]) if len(item) > 1 else "unknown"
                        label = f"{dev_id} ({transport})"
                        disp.append(label)
                        mapping[label] = dev_id
                    self.camera_scan_display_to_id = mapping
                    self.camera_pick_combo.configure(values=disp)
                    if disp:
                        self.camera_pick_var.set(disp[0])
                        self.camera_var.set(mapping[disp[0]])
                        self.status_var.set(f"Found {len(disp)} camera(s). Selected: {mapping[disp[0]]}")
                    else:
                        self.camera_pick_var.set("")
                        self.status_var.set("No cameras found by Aravis scan")
            elif kind == "connected":
                self.connected = True
                info = dict(payload) if isinstance(payload, dict) else {}
                self.camera_info = info
                if self.debug:
                    self._dbg(f"connected event: {json.dumps(info, ensure_ascii=False, default=str)}")
                controls = info.get("controls", {}) if isinstance(info, dict) else {}
                if isinstance(controls, dict):
                    if "gain" in controls:
                        self.gain_var.set(float(controls["gain"]))
                        self.gain_entry_var.set(f"{float(controls['gain']):.3f}")
                    if "exposure" in controls:
                        self.exposure_var.set(float(controls["exposure"]))
                        self.exposure_entry_var.set(f"{float(controls['exposure']):.1f}")
                    if "gain_min" in controls and "gain_max" in controls:
                        self.gain_bounds = (float(controls["gain_min"]), float(controls["gain_max"]))
                    if "exposure_min" in controls and "exposure_max" in controls:
                        self.exposure_bounds = (float(controls["exposure_min"]), float(controls["exposure_max"]))
                vendor = str(info.get("vendor", ""))
                model = str(info.get("model", ""))
                serial = str(info.get("serial", ""))
                pix = str(info.get("pixel_format", ""))
                self.connection_info_var.set(f"{vendor} {model}\nSerial: {serial}\nPixelFormat: {pix}")
                self.status_var.set("Connected")
                self._show_page("choice")
            elif kind == "controls":
                controls = dict(payload) if isinstance(payload, dict) else {}
                if "gain" in controls:
                    self.gain_var.set(float(controls["gain"]))
                    self.gain_entry_var.set(f"{float(controls['gain']):.3f}")
                if "exposure" in controls:
                    self.exposure_var.set(float(controls["exposure"]))
                    self.exposure_entry_var.set(f"{float(controls['exposure']):.1f}")
            elif kind == "frame" and isinstance(payload, FramePacket):
                self.last_frame = payload
                frame_updated = True
                if self.debug:
                    self._debug_frame_count += 1
                    if (self._debug_frame_count % 30) == 1:
                        pmeta = payload.meta or {}
                        self._dbg(
                            f"frame event: #{self._debug_frame_count} "
                            f"w={payload.width} h={payload.height} pf=0x{payload.pixel_format:08x} "
                            f"bytes={len(payload.raw)} frame_id={pmeta.get('frame_id')} "
                            f"status={pmeta.get('status_int')}"
                        )
                self._consume_dark_frame(payload)
                self._consume_flat_frame(payload)
            elif kind == "geometry_progress":
                if isinstance(payload, dict):
                    self.geometry_progress_var.set(float(payload.get("value", 0.0)))
                    self.geometry_progress_text_var.set(str(payload.get("text", "")))
            elif kind == "geometry_done":
                self.geometry_progress_var.set(100.0)
                self.geometry_progress_text_var.set("Done")
                self.status_var.set(f"Geometry calibration complete. Reference lens: {self.reference_lens + 1}")
                self._show_page("main")
            elif kind == "error":
                self.status_var.set(f"Camera error: {payload}")
            elif kind == "disconnected":
                if self.connected:
                    self.status_var.set("Camera disconnected")
                self.connected = False

        if frame_updated:
            self._render_preview()
        self._drain_render_results()

        self.after(self.ui_poll_ms, self._poll_events)

    # ----------------------------- Lifecycle -----------------------------
    def _on_close(self) -> None:
        try:
            self._render_stop_event.set()
            try:
                self._render_in_q.put_nowait(None)
            except Exception:
                pass
            if self._render_thread.is_alive():
                self._render_thread.join(timeout=0.25)
            self._disconnect()
        finally:
            self.after(120, self.destroy)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hydra Baumer wizard GUI")
    parser.add_argument("--interface", default="en10", help="GigE interface name")
    parser.add_argument("--camera", default="", help="Camera IP or Aravis device id")
    parser.add_argument("--output-dir", default="capture", help="Output directory for sessions and snapshots")
    parser.add_argument("--packet-size", type=int, default=DEFAULT_PACKET_SIZE, help="GevSCPSPacketSize")
    parser.add_argument("--packet-delay", type=int, default=DEFAULT_PACKET_DELAY, help="GevSCPD")
    parser.add_argument("--preview-fps", type=float, default=DEFAULT_PREVIEW_FPS, help="Target preview FPS")
    parser.add_argument("--ui-poll-ms", type=int, default=DEFAULT_UI_POLL_MS, help="UI poll interval")
    parser.add_argument("--debug", action="store_true", help="Enable verbose terminal debug prints")
    u8_group = parser.add_mutually_exclusive_group()
    u8_group.add_argument("--force-u8", dest="force_u8", action="store_true", help="Force uint8 processing in GUI pipeline")
    u8_group.add_argument("--no-force-u8", dest="force_u8", action="store_false", help="Keep source bit depth in GUI pipeline")
    parser.set_defaults(force_u8=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.debug:
        print(
            f"[hydra-debug] launch args: interface={args.interface} camera={args.camera} "
            f"output_dir={args.output_dir} preview_fps={args.preview_fps} "
            f"force_u8={args.force_u8}",
            flush=True,
        )
    app = HydraWizardApp(
        interface=args.interface,
        camera_ip=args.camera,
        output_dir=Path(args.output_dir),
        packet_size=args.packet_size,
        packet_delay=args.packet_delay,
        preview_fps=args.preview_fps,
        ui_poll_ms=args.ui_poll_ms,
        debug=bool(args.debug),
        force_u8_mode=bool(args.force_u8),
    )
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
