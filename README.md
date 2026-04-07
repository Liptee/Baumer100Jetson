# Baumer USB Recorder + FastAPI Service

Проект для записи RAW-видео с USB-камеры (GRAY8/Y16) и управления записью через FastAPI.

## Что внутри

- `tools/baumer_record_headless.py` — запись RAW из CLI
- `tools/baumer_api_service.py` — FastAPI API (`start/status/stop/logs`)
- `run_baumer_service.sh` — запуск API-сервиса (uvicorn)
- `create_service.sh` — установка systemd unit

## Установка (Raspberry Pi OS / Ubuntu)

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip v4l-utils gstreamer1.0-tools gstreamer1.0-plugins-good ffmpeg
```

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Проверка камеры

```bash
v4l2-ctl --list-devices
v4l2-ctl -d /dev/video0 --list-formats-ext
```

## Ручная запись (CLI)

```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --target-fps 100 --min-fps 100 --max-fps 120 \
  --duration 5 --exposure-us 9500 --gain 1
```

## FastAPI локально

```bash
source .venv/bin/activate
python -m uvicorn baumer_api_service:app --app-dir tools --host 0.0.0.0 --port 8000
```

Проверка:
```bash
curl http://127.0.0.1:8000/api/health
```

### API: запуск записи

```bash
curl -X POST "http://127.0.0.1:8000/api/record/start" \
  -H "Content-Type: application/json" \
  -d '{
    "backend": "gst-raw",
    "pixel_format": "gray8",
    "width": 1024,
    "height": 768,
    "target_fps": 120,
    "min_fps": 100,
    "max_fps": 120,
    "duration": 10,
    "exposure_us": 9500,
    "gain": 1,
    "roi_center": "off",
    "roi_x": 512,
    "roi_y": 384,
    "crop_top": 100,
    "crop_bottom": 100
  }'
```

### API: статус / логи / остановка

```bash
curl "http://127.0.0.1:8000/api/record/status"
curl "http://127.0.0.1:8000/api/record/logs?tail=200"
curl -X POST "http://127.0.0.1:8000/api/record/stop"
```

## Установка как systemd service

```bash
chmod +x create_service.sh run_baumer_service.sh
./create_service.sh
```

Настройки:
```bash
sudo nano /etc/default/baumer_recorder
```

Управление:
```bash
sudo systemctl restart baumer_recorder.service
sudo systemctl status baumer_recorder.service
sudo journalctl -u baumer_recorder.service -f
```

