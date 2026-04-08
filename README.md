# Baumer USB Recorder + FastAPI

Headless-проект для записи RAW-видео с USB-камеры (режимы `GRAY8` и `Y16`) и управления записью через FastAPI.
Также поддерживается параллельная запись MAVLink-телеметрии с полетного контроллера по USB/UART.

## Файлы

- `tools/baumer_record_headless.py` — CLI-рекордер RAW
- `tools/baumer_api_service.py` — API-сервис (`/api/record/start|status|stop|logs`)
- `run_baumer_service.sh` — запуск uvicorn
- `create_service.sh` — установка systemd-сервиса
- `example_update_and_restart_service.sh` — пример update/restart

## Установка (Raspberry Pi OS / Ubuntu)

```bash
sudo apt update
sudo apt install -y \
  python3 python3-venv python3-pip \
  v4l-utils \
  gstreamer1.0-tools gstreamer1.0-plugins-good \
  ffmpeg
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

## Ручной запуск (CLI)

### GRAY8, 1024x768

```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --target-fps 120 --min-fps 100 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1
```

### GRAY8 + MAVLink telemetry (CUAV/PX4/ArduPilot)

```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --target-fps 120 --min-fps 100 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1 \
  --telemetry-enable \
  --telemetry-device /dev/ttyACM0 \
  --telemetry-baud 115200
```

Если `--telemetry-device` не указан, скрипт пробует автопоиск (`/dev/serial/by-id`, затем `/dev/ttyACM*`, `/dev/ttyUSB*`).

### Y16, 1024x768

```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format y16 \
  --width 1024 --height 768 \
  --target-fps 120 --min-fps 100 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1
```

### Crop (software)

Пример `1024x768 -> 1024x568`:

```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --crop-top 100 --crop-bottom 100 \
  --target-fps 120 --min-fps 120 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1
```

### ROI (hardware, V4L2)

```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --roi-center off --roi-x 512 --roi-y 384 \
  --target-fps 120 --min-fps 100 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1
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

## API

### Старт записи

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
    "duration": 600,
    "exposure_us": 9500,
    "gain": 1,
    "roi_center": "off",
    "roi_x": 512,
    "roi_y": 384,
    "crop_top": 100,
    "crop_bottom": 100,
    "telemetry_enable": true,
    "telemetry_device": "/dev/ttyACM0",
    "telemetry_baud": 115200,
    "telemetry_wait_heartbeat": 5.0,
    "telemetry_msg_types": "ATTITUDE,GPS_RAW_INT,GLOBAL_POSITION_INT",
    "telemetry_max_rate_hz": 0
  }'
```

### Статус

```bash
curl "http://127.0.0.1:8000/api/record/status"
```

### Логи

```bash
curl "http://127.0.0.1:8000/api/record/logs?tail=200"
```

### Остановка записи

```bash
curl -X POST "http://127.0.0.1:8000/api/record/stop"
```

Важно: для корректного sidecar-файла `.raw.json` запись нужно останавливать через `/api/record/stop` (graceful stop).

## Формат выходных файлов

В `capture/` создаются:

- `*.raw` — сырые кадры подряд
- `*.raw.json` — метаданные (`width`, `height`, `bytes_per_pixel`, `fps_from_size`, и т.д.)
- `*.raw.timestamps.csv` — timestamp на каждый кадр (`mono_ns`, `unix_ns`, `utc_iso`)
- `*.raw.timestamps.json` — метаданные timestamp-интерполяции
- `*.raw.telemetry.jsonl` — поток MAVLink сообщений с timestamp
- `*.raw.telemetry.csv` — плоский CSV телеметрии
- `*.raw.telemetry.meta.json` — метаданные телеметрии

## Просмотр RAW

Пример для `GRAY8`:

```bash
ffplay -f rawvideo -pixel_format gray -video_size 1024x768 -framerate 100.8 capture/file.raw
```

Конвертация в mp4:

```bash
ffmpeg -f rawvideo -pixel_format gray -video_size 1024x768 -framerate 100.8 \
  -i capture/file.raw -c:v libx264 -pix_fmt yuv420p capture/preview.mp4
```

## Установка как systemd service

```bash
chmod +x create_service.sh run_baumer_service.sh
./create_service.sh
```

После установки:

```bash
sudo systemctl status baumer_recorder.service
sudo journalctl -u baumer_recorder.service -f
```

## Конфиг сервиса

Файл:

```bash
sudo nano /etc/default/baumer_recorder
```

Основные переменные:

- `API_HOST`, `API_PORT` — адрес API
- `BACKEND`, `PIXEL_FORMAT`
- `WIDTH`, `HEIGHT`
- `TARGET_FPS`, `MIN_FPS`, `MAX_FPS`
- `DURATION`
- `EXPOSURE_US`, `GAIN`
- `ROI_CENTER`, `ROI_X`, `ROI_Y`
- `CROP_TOP`, `CROP_BOTTOM`, `CROP_LEFT`, `CROP_RIGHT`
- `CAMERA_ID`, `DEVICE`
- `SNAPSHOT_DIR`
- `TELEMETRY_ENABLE`, `TELEMETRY_DEVICE`, `TELEMETRY_BAUD`
- `TELEMETRY_WAIT_HEARTBEAT`, `TELEMETRY_MSG_TYPES`, `TELEMETRY_MAX_RATE_HZ`

Перезапуск после изменения конфига:

```bash
sudo systemctl restart baumer_recorder.service
```
