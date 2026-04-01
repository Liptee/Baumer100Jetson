#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import queue
import re
import signal
import shutil
import subprocess
import threading
import time
from pathlib import Path

cv2 = None


def require_cv2():
    global cv2
    if cv2 is not None:
        return cv2
    try:
        import cv2 as _cv2  # type: ignore
    except Exception as exc:
        raise SystemExit(f"OpenCV is required: {exc}")
    cv2 = _cv2
    return cv2


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tn = threading.current_thread().name
    print(f"[{ts}] [{tn}] {msg}", flush=True)


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


def configure_uvc_mode(cap, req_w: int, req_h: int, target_fps: float) -> tuple[int, int, float]:
    cv = require_cv2()
    try:
        cap.set(cv.CAP_PROP_CONVERT_RGB, 0.0)
    except Exception:
        pass
    # Prefer native monochrome path from camera (GREY 8-bit).
    try:
        cap.set(cv.CAP_PROP_FOURCC, cv.VideoWriter_fourcc(*"GREY"))
    except Exception:
        pass
    cap.set(cv.CAP_PROP_FRAME_WIDTH, float(req_w))
    cap.set(cv.CAP_PROP_FRAME_HEIGHT, float(req_h))
    cap.set(cv.CAP_PROP_FPS, float(target_fps))
    for _ in range(4):
        cap.read()
    got_w = int(cap.get(cv.CAP_PROP_FRAME_WIDTH) or 0)
    got_h = int(cap.get(cv.CAP_PROP_FRAME_HEIGHT) or 0)
    got_fps = float(cap.get(cv.CAP_PROP_FPS) or 0.0)
    return got_w, got_h, got_fps


def measure_read_fps(cap, sample_s: float = 0.4, max_frames: int = 300) -> tuple[float, int, int]:
    t0 = time.monotonic()
    n = 0
    w = 0
    h = 0
    while (time.monotonic() - t0) < sample_s and n < max_frames:
        ret, frm = cap.read()
        if not ret or frm is None:
            continue
        if hasattr(frm, "shape") and len(frm.shape) >= 2:
            h = int(frm.shape[0])
            w = int(frm.shape[1])
        n += 1
    dtm = max(1e-6, time.monotonic() - t0)
    return float(n / dtm), w, h


def try_open_uvc(
    target_fps: float,
    serial_hint: str = "",
    requested_device: str = "",
    fixed_mode: tuple[int, int] | None = None,
    auto_mode: bool = False,
):
    cv = require_cv2()
    if auto_mode:
        mode_candidates = [
            (2048, 1536),
            (1920, 1080),
            (1600, 1200),
            (1280, 1024),
            (1280, 960),
            (1280, 720),
            (1024, 768),
            (800, 600),
            (640, 480),
        ]
    else:
        fm = fixed_mode or (640, 480)
        mode_candidates = [(max(16, int(fm[0])), max(16, int(fm[1])))]
    devices = [requested_device] if requested_device else find_uvc_candidates(serial_hint)
    log(f"UVC devices: {devices}")
    best = None
    for dev in devices:
        cap = cv.VideoCapture(dev, cv.CAP_V4L2)
        if not cap.isOpened():
            try:
                cap.release()
            except Exception:
                pass
            continue
        try:
            cap.set(cv.CAP_PROP_BUFFERSIZE, 2)
        except Exception:
            pass
        local_best = None
        for req_w, req_h in mode_candidates:
            got_w, got_h, fps_prop = configure_uvc_mode(cap, req_w, req_h, target_fps)
            fps_est, frm_w, frm_h = measure_read_fps(cap, sample_s=0.35)
            if frm_w > 0 and frm_h > 0:
                got_w, got_h = frm_w, frm_h
            area = max(0, got_w * got_h)
            log(
                f"probe {dev}: req={req_w}x{req_h}@{target_fps:.1f}, "
                f"got={got_w}x{got_h}, fps_prop={fps_prop:.1f}, fps_est={fps_est:.1f}"
            )
            if area <= 0 or fps_est <= 1.0:
                continue
            candidate = {
                "dev": dev,
                "req_w": req_w,
                "req_h": req_h,
                "w": got_w,
                "h": got_h,
                "fps_prop": fps_prop,
                "fps_est": fps_est,
                "cap": cap,
            }
            if local_best is None:
                local_best = candidate
                continue
            old_area = int(local_best["w"]) * int(local_best["h"])
            if (fps_est > float(local_best["fps_est"]) + 2.0) or (
                abs(fps_est - float(local_best["fps_est"])) <= 2.0 and area > old_area
            ):
                local_best = candidate
            if fps_est >= target_fps * 0.95:
                local_best = candidate
                break
        if local_best is None:
            cap.release()
            continue
        if best is None:
            best = local_best
        else:
            old_area = int(best["w"]) * int(best["h"])
            new_area = int(local_best["w"]) * int(local_best["h"])
            if (float(local_best["fps_est"]) > float(best["fps_est"]) + 2.0) or (
                abs(float(local_best["fps_est"]) - float(best["fps_est"])) <= 2.0 and new_area > old_area
            ):
                try:
                    best["cap"].release()
                except Exception:
                    pass
                best = local_best
            else:
                cap.release()
    if best is None:
        return None
    cap = best["cap"]
    configure_uvc_mode(cap, int(best["req_w"]), int(best["req_h"]), target_fps)
    fps_est, ww, hh = measure_read_fps(cap, sample_s=0.5)
    if ww > 0 and hh > 0:
        best["w"] = ww
        best["h"] = hh
    best["fps_est"] = fps_est
    log(f"selected: {best['dev']} {best['w']}x{best['h']} est_fps={best['fps_est']:.1f}")
    return best


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


def set_controls_cv2(cap, exposure_us: float, gain: float) -> None:
    cv = require_cv2()
    # Try both common conventions for manual exposure.
    for v in (1.0, 0.25):
        try:
            cap.set(cv.CAP_PROP_AUTO_EXPOSURE, v)
        except Exception:
            pass
    # Different drivers expect different scale for CAP_PROP_EXPOSURE.
    for ex in (float(exposure_us), float(exposure_us) / 100.0):
        try:
            cap.set(cv.CAP_PROP_EXPOSURE, ex)
        except Exception:
            pass
    try:
        cap.set(cv.CAP_PROP_GAIN, float(gain))
    except Exception:
        pass
    try:
        got_ex = float(cap.get(cv.CAP_PROP_EXPOSURE))
    except Exception:
        got_ex = float("nan")
    try:
        got_gain = float(cap.get(cv.CAP_PROP_GAIN))
    except Exception:
        got_gain = float("nan")
    log(f"cv2 controls readback: exposure={got_ex}, gain={got_gain}")


def _gst_has_element(name: str) -> bool:
    inspect = shutil.which("gst-inspect-1.0")
    if not inspect:
        return False
    try:
        cp = subprocess.run(
            [inspect, name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=2.0,
        )
        return cp.returncode == 0
    except Exception:
        return False


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
        last_log = 0.0
        while True:
            elapsed = time.monotonic() - t0
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
            size_b = float(out_path.stat().st_size if out_path.exists() else 0)
            frames_est = float(size_b / float(frame_bytes))
            fps_est = float(frames_est / elapsed_total)
            # On some Jetson builds gst-launch may return non-zero on interrupt
            # even after valid EOS/write. Accept by data/elapsed evidence.
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


def record_gst_mjpeg(
    device: str,
    out_path: Path,
    duration_s: float,
    fps: float,
    width: int,
    height: int,
    fps_hint: float = 0.0,
) -> tuple[Path, dict[str, float]]:
    gst = shutil.which("gst-launch-1.0")
    if not gst:
        raise RuntimeError("gst-launch-1.0 not found")

    fps_candidates: list[int] = []

    def add_fps(v: float) -> None:
        iv = int(round(float(v)))
        if iv > 0 and iv not in fps_candidates:
            fps_candidates.append(iv)

    # First, trust camera-reported discrete fps for this exact mode.
    for vv in _v4l2_supported_fps(device, int(width), int(height), "GREY"):
        add_fps(vv)
    add_fps(fps)
    add_fps(fps_hint)
    add_fps(120)
    add_fps(100)
    add_fps(90)
    add_fps(80)
    add_fps(60)
    add_fps(30)

    used_fps = 0
    t0 = time.monotonic()
    last_err = "unknown"
    has_nv = _gst_has_element("nvv4l2h264enc") and _gst_has_element("nvvidconv")
    has_x264 = _gst_has_element("x264enc")
    encoder_modes = ["jetson-h264-hw", "x264-sw", "raw-mkv"]
    for enc_mode in encoder_modes:
        if enc_mode == "jetson-h264-hw" and not has_nv:
            continue
        if enc_mode == "x264-sw" and not has_x264:
            continue
        for fps_i in fps_candidates:
            common = [
                gst,
                "-e",
                "v4l2src",
                f"device={device}",
                "io-mode=2",
                "do-timestamp=true",
                "!",
                f"video/x-raw,format=GRAY8,width={int(width)},height={int(height)},framerate={fps_i}/1",
                "!",
                "queue",
                "max-size-buffers=1024",
                "leaky=downstream",
                "!",
                "videoconvert",
                "!",
            ]
            if enc_mode == "jetson-h264-hw":
                tail = [
                    "video/x-raw,format=I420",
                    "!",
                    "nvvidconv",
                    "!",
                    "video/x-raw(memory:NVMM),format=NV12",
                    "!",
                    "nvv4l2h264enc",
                    "maxperf-enable=1",
                    "control-rate=1",
                    "bitrate=40000000",
                    "iframeinterval=120",
                    "idrinterval=120",
                    "insert-sps-pps=true",
                    "!",
                    "h264parse",
                    "!",
                    "matroskamux",
                ]
            elif enc_mode == "x264-sw":
                tail = [
                    "video/x-raw,format=I420",
                    "!",
                    "x264enc",
                    "speed-preset=ultrafast",
                    "tune=zerolatency",
                    "bitrate=40000",
                    "key-int-max=120",
                    "!",
                    "h264parse",
                    "!",
                    "matroskamux",
                ]
            else:
                tail = [
                    "video/x-raw,format=I420",
                    "!",
                    "matroskamux",
                ]
            cmd = common + tail + [
                "!",
                "filesink",
                f"location={str(out_path)}",
                "sync=false",
            ]
            log(f"gst start (enc={enc_mode}, fps={fps_i}): {' '.join(cmd)}")
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

            attempt_t0 = time.monotonic()
            ok = True
            last_log = 0.0
            while True:
                now = time.monotonic()
                elapsed = now - attempt_t0
                if elapsed >= float(duration_s):
                    break
                rc = proc.poll()
                if rc is not None:
                    ok = False
                    last_err = f"gst exited early with code {rc} at fps={fps_i} enc={enc_mode}"
                    break
                if elapsed - last_log >= 1.0:
                    last_log = elapsed
                    size_mb = 0.0
                    try:
                        size_mb = out_path.stat().st_size / (1024.0 * 1024.0)
                    except Exception:
                        pass
                    log(f"gst telemetry: elapsed={elapsed:.1f}s size={size_mb:.1f}MB")
                time.sleep(0.05)

            if ok:
                try:
                    proc.send_signal(signal.SIGINT)
                except Exception:
                    pass
                try:
                    rc = proc.wait(timeout=15.0)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        rc = proc.wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        rc = proc.wait(timeout=3.0)
                if reader is not None:
                    reader.join(timeout=2.0)
                if rc in (0, 130):
                    used_fps = fps_i
                    log(f"gst mode selected: enc={enc_mode}, fps={used_fps}")
                    break
                last_err = f"gst failed with code {rc} at fps={fps_i} enc={enc_mode}"
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
        if used_fps > 0:
            break

    if used_fps <= 0:
        raise RuntimeError(last_err)

    elapsed_total = max(1e-6, time.monotonic() - t0)
    size_b = float(out_path.stat().st_size if out_path.exists() else 0)
    est_write_fps = float(size_b / max(1.0, (width * height))) / elapsed_total
    stats = {
        "elapsed_s": elapsed_total,
        "read": 0.0,
        "enq": 0.0,
        "written": 0.0,
        "q_drop": 0.0,
        "read_fail": 0.0,
        "write_fail": 0.0,
        "read_fps_avg": float(used_fps),
        "write_fps_avg": est_write_fps,
        "backend_gst": 1.0,
        "capture_fps_used": float(used_fps),
    }
    log(f"gst done: capture_fps_used={used_fps}")
    return out_path, stats


def open_writer(path: Path, w: int, h: int, fps: float):
    cv = require_cv2()
    location = str(path).replace('"', '\\"')
    gst = (
        "appsrc is-live=true block=false do-timestamp=true format=time ! "
        "queue leaky=downstream max-size-buffers=32 ! "
        f"video/x-raw,format=BGR,width={w},height={h},framerate={int(round(fps))}/1 ! "
        "videoconvert ! video/x-raw,format=I420 ! "
        "nvvidconv ! video/x-raw(memory:NVMM),format=NV12 ! "
        "nvv4l2h264enc maxperf-enable=1 preset-level=1 control-rate=1 bitrate=25000000 iframeinterval=100 idrinterval=100 insert-sps-pps=true ! "
        "h264parse ! qtmux ! "
        f'filesink location="{location}" sync=false'
    )
    wr = cv.VideoWriter(gst, cv.CAP_GSTREAMER, 0, fps, (w, h), True)
    if wr.isOpened():
        return wr, "jetson-nvv4l2-uvc", path
    avi_path = path.with_suffix(".avi")
    wr = cv.VideoWriter(str(avi_path), cv.VideoWriter_fourcc(*"MJPG"), fps, (w, h), True)
    if wr.isOpened():
        return wr, "opencv-mjpg-avi", avi_path
    wr = cv.VideoWriter(str(path), cv.VideoWriter_fourcc(*"mp4v"), fps, (w, h), True)
    if wr.isOpened():
        return wr, "opencv-mp4v", path
    return None, "writer-open-failed", path


def record_headless(
    cap,
    out_path: Path,
    duration_s: float,
    target_fps: float,
    queue_size: int,
) -> tuple[Path, dict[str, float]]:
    cv = require_cv2()
    w = int(cap.get(cv.CAP_PROP_FRAME_WIDTH) or 0)
    h = int(cap.get(cv.CAP_PROP_FRAME_HEIGHT) or 0)
    if w <= 0 or h <= 0:
        ret, frm = cap.read()
        if not ret or frm is None:
            raise RuntimeError("cannot read initial frame for writer setup")
        h = int(frm.shape[0])
        w = int(frm.shape[1])
    writer, enc, actual_path = open_writer(out_path, w, h, target_fps)
    if writer is None:
        raise RuntimeError("failed to open writer")
    log(f"writer opened: enc={enc}, size={w}x{h}, fps={target_fps:.1f}, out={actual_path}")

    q: queue.Queue = queue.Queue(maxsize=max(8, int(queue_size)))
    stop_event = threading.Event()
    read_done = threading.Event()
    stats = {
        "read": 0.0,
        "read_fail": 0.0,
        "enq": 0.0,
        "q_drop": 0.0,
        "written": 0.0,
        "write_fail": 0.0,
        "read_fps": 0.0,
        "write_fps": 0.0,
    }
    lock = threading.Lock()

    def writer_loop() -> None:
        nonlocal writer
        w_count = 0
        w_window = 0
        t_win = time.monotonic()
        while not stop_event.is_set() or not q.empty() or not read_done.is_set():
            try:
                frame = q.get(timeout=0.08)
            except queue.Empty:
                if read_done.is_set() and q.empty():
                    break
                continue
            try:
                writer.write(frame)
                w_count += 1
                w_window += 1
            except Exception:
                with lock:
                    stats["write_fail"] += 1.0
            now = time.monotonic()
            if (now - t_win) >= 1.0:
                with lock:
                    stats["write_fps"] = w_window / (now - t_win)
                    stats["written"] = float(w_count)
                w_window = 0
                t_win = now
        with lock:
            stats["written"] = float(w_count)

    wt = threading.Thread(target=writer_loop, name="writer", daemon=True)
    wt.start()

    t0 = time.monotonic()
    read_window = 0
    t_win = t0
    while (time.monotonic() - t0) < float(duration_s):
        ret, frame = cap.read()
        if not ret or frame is None:
            with lock:
                stats["read_fail"] += 1.0
            continue
        with lock:
            stats["read"] += 1.0
        try:
            q.put_nowait(frame)
            with lock:
                stats["enq"] += 1.0
        except queue.Full:
            with lock:
                stats["q_drop"] += 1.0
        read_window += 1
        now = time.monotonic()
        if (now - t_win) >= 1.0:
            with lock:
                stats["read_fps"] = read_window / (now - t_win)
                log(
                    "telemetry: "
                    f"read_fps={stats['read_fps']:.1f} write_fps={stats['write_fps']:.1f} "
                    f"q={q.qsize()}/{q.maxsize} enq={int(stats['enq'])} wr={int(stats['written'])} "
                    f"drop={int(stats['q_drop'])} read_fail={int(stats['read_fail'])}"
                )
            read_window = 0
            t_win = now

    capture_elapsed = max(1e-6, time.monotonic() - t0)
    read_done.set()
    stop_event.set()
    while wt.is_alive():
        log(f"writer draining: q={q.qsize()}/{q.maxsize}, written={int(stats['written'])}")
        wt.join(timeout=1.0)
    try:
        writer.release()
    except Exception:
        pass

    elapsed = max(1e-6, time.monotonic() - t0)
    with lock:
        stats["capture_elapsed_s"] = capture_elapsed
        stats["elapsed_s"] = elapsed
        stats["read_fps_avg"] = stats["read"] / capture_elapsed
        stats["write_fps_avg"] = stats["written"] / capture_elapsed
        stats["write_fps_overall"] = stats["written"] / elapsed
        stats["queue_left"] = float(q.qsize())
    return actual_path, stats


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
    p.add_argument("--auto-mode", action="store_true", help="Probe multiple camera modes and auto-select one")
    p.add_argument("--snapshot-dir", default="capture", help="Output directory")
    p.add_argument("--queue-size", type=int, default=512, help="Frame queue size")
    p.add_argument(
        "--backend",
        choices=["auto", "gst-raw", "gst-mjpeg", "opencv"],
        default="auto",
        help="Recording backend: auto (prefer gst-raw), gst-raw (recommended), opencv",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.snapshot_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    serial_hint = extract_serial_hint(args.camera_id)
    log(
        f"start: duration={args.duration:.2f}s target_fps={args.target_fps:.1f} "
        f"exposure_us={args.exposure_us:.1f} gain={args.gain:.2f} "
        f"crop(l/r/t/b)=({int(args.crop_left)}/{int(args.crop_right)}/{int(args.crop_top)}/{int(args.crop_bottom)}) "
        f"roi=({args.roi_x if args.roi_x is not None else '-'},"
        f"{args.roi_y if args.roi_y is not None else '-'}) "
        f"roi_center={args.roi_center} serial_hint={serial_hint or '-'}"
    )

    backend = str(args.backend)
    if backend == "auto":
        backend = "gst-raw" if shutil.which("gst-launch-1.0") else "opencv"
    if backend == "gst-mjpeg":
        backend = "gst-raw"
    log(f"record backend: {backend}")

    # Fast path: raw GStreamer capture, preserves 8/16-bit.
    if backend == "gst-raw":
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
            duration_s=float(args.duration),
            device=dev,
        )
        log(
            "done: "
            f"file={saved_path} pixel={pixel_code} "
            f"capture_fps_used={stats.get('capture_fps_used', 0.0):.1f} "
            f"size_mb={(float(stats.get('file_size_bytes', 0.0)) / (1024.0 * 1024.0)):.1f}"
        )
        return 0

    # Compatibility path: OpenCV capture/record.
    if str(args.pixel_format).lower() == "y16":
        log("warn: opencv backend does not guarantee native Y16 preservation; use --backend gst-raw")

    opened = try_open_uvc(
        args.target_fps,
        serial_hint=serial_hint,
        requested_device=args.device,
        fixed_mode=(int(args.width), int(args.height)),
        auto_mode=bool(args.auto_mode),
    )
    if opened is None:
        log("failed: no suitable UVC camera mode")
        return 2
    cap = opened["cap"]
    dev = str(opened["dev"])
    w = int(opened["w"])
    h = int(opened["h"])
    fps_est = float(opened["fps_est"])
    log(f"camera connected: dev={dev}, mode={w}x{h}, est_fps={fps_est:.1f}")

    # Apply controls (best-effort).
    set_roi_v4l2(dev, args.roi_x, args.roi_y, str(args.roi_center))
    set_controls_v4l2(dev, args.exposure_us, args.gain)
    set_controls_cv2(cap, args.exposure_us, args.gain)

    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = out_dir / f"headless_{ts}.mp4"
    try:
        saved_path, stats = record_headless(
            cap=cap,
            out_path=out_path,
            duration_s=float(args.duration),
            target_fps=float(args.target_fps),
            queue_size=int(args.queue_size),
        )
    finally:
        try:
            cap.release()
        except Exception:
            pass

    log(
        "done: "
        f"file={saved_path} "
        f"read={int(stats['read'])} enq={int(stats['enq'])} written={int(stats['written'])} "
        f"drop={int(stats['q_drop'])} read_fail={int(stats['read_fail'])} write_fail={int(stats['write_fail'])} "
        f"read_fps_avg={stats['read_fps_avg']:.1f} write_fps_avg={stats['write_fps_avg']:.1f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
