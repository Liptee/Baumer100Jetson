#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import queue
import re
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
        # Prefer MJPG at camera side for USB throughput.
        try:
            cap.set(cv.CAP_PROP_FOURCC, cv.VideoWriter_fourcc(*"MJPG"))
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

    read_done.set()
    stop_event.set()
    wt.join(timeout=max(4.0, duration_s * 1.2))
    if wt.is_alive():
        log("writer still draining after timeout")
    try:
        writer.release()
    except Exception:
        pass

    elapsed = max(1e-6, time.monotonic() - t0)
    with lock:
        stats["elapsed_s"] = elapsed
        stats["read_fps_avg"] = stats["read"] / elapsed
        stats["write_fps_avg"] = stats["written"] / elapsed
        stats["queue_left"] = float(q.qsize())
    return actual_path, stats


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Headless UVC recorder for Jetson/TIS camera")
    p.add_argument("--duration", type=float, default=5.0, help="Recording duration in seconds")
    p.add_argument("--target-fps", type=float, default=100.0, help="Target FPS")
    p.add_argument("--exposure-us", type=float, default=9500.0, help="Exposure in microseconds")
    p.add_argument("--gain", type=float, default=1.0, help="Gain value")
    p.add_argument("--camera-id", default="", help="Camera id/serial hint (optional)")
    p.add_argument("--device", default="", help="Force /dev/videoX (optional)")
    p.add_argument("--width", type=int, default=640, help="Requested capture width (default: 640)")
    p.add_argument("--height", type=int, default=480, help="Requested capture height (default: 480)")
    p.add_argument("--auto-mode", action="store_true", help="Probe multiple camera modes and auto-select one")
    p.add_argument("--snapshot-dir", default="capture", help="Output directory")
    p.add_argument("--queue-size", type=int, default=512, help="Frame queue size")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.snapshot_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    serial_hint = extract_serial_hint(args.camera_id)
    log(
        f"start: duration={args.duration:.2f}s target_fps={args.target_fps:.1f} "
        f"exposure_us={args.exposure_us:.1f} gain={args.gain:.2f} serial_hint={serial_hint or '-'}"
    )

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
