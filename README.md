# Baumer USB Recorder (Headless)

Минимальный проект для записи RAW-видео с USB-камеры без GUI.

Основной скрипт:
- `tools/baumer_record_headless.py`

Сервисный обвес:
- `run_baumer_service.sh`
- `create_service.sh`
- `example_update_and_restart_service.sh`

## Установка (Raspberry Pi OS / Ubuntu)

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip v4l-utils gstreamer1.0-tools gstreamer1.0-plugins-good ffmpeg
```

Создать venv:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Проверка камеры

```bash
v4l2-ctl --list-devices
v4l2-ctl -d /dev/video0 --list-formats-ext
```

## Ручной запуск

### 8-bit (GRAY8), 1024x768
```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --target-fps 100 --min-fps 100 --max-fps 120 \
  --duration 5 --exposure-us 9500 --gain 1
```

### 16-bit (Y16), 1024x768
```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format y16 \
  --width 1024 --height 768 \
  --target-fps 100 --min-fps 100 --max-fps 120 \
  --duration 5 --exposure-us 9500 --gain 1
```

### Обрезка (software crop) сверху/снизу
Пример: `1024x768 -> 1024x568`.
```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --crop-top 100 --crop-bottom 100 \
  --target-fps 120 --min-fps 120 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1
```

### ROI (hardware, через V4L2 controls)
```bash
python tools/baumer_record_headless.py \
  --backend gst-raw \
  --pixel-format gray8 \
  --width 1024 --height 768 \
  --roi-center off --roi-x 512 --roi-y 384 \
  --target-fps 120 --min-fps 100 --max-fps 120 \
  --duration 10 --exposure-us 9500 --gain 1
```

## Формат выхода

В `capture/` создаются:
- `*.raw` — сырые кадры подряд
- `*.raw.json` — метаданные (размер, fps, bytes_per_pixel, расчетное число кадров)

## Просмотр `.raw`

Пример для `GRAY8`, `1024x768`, `100.8 fps`:
```bash
ffplay -f rawvideo -pixel_format gray -video_size 1024x768 -framerate 100.8 capture/file.raw
```

## Установка как systemd-сервис

1. Сделать скрипты исполняемыми:
```bash
chmod +x create_service.sh run_baumer_service.sh example_update_and_restart_service.sh
```

2. Установить и запустить сервис:
```bash
./create_service.sh
```

3. Настройки сервиса:
```bash
sudo nano /etc/default/baumer_recorder
```

4. Перезапуск:
```bash
sudo systemctl restart baumer_recorder.service
```

5. Логи:
```bash
sudo journalctl -u baumer_recorder.service -f
```

