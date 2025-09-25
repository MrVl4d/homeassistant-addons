#!/usr/bin/env python3
import os
import signal
import shlex
import json
import time
import threading
import subprocess
import datetime
from typing import Optional

try:
    import requests  # installed via py3-requests
except Exception:  # pragma: no cover
    requests = None

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rtsp-recorder")

STOP_EVENT = threading.Event()

def _signal_handler(signum, frame):
    STOP_EVENT.set()

for _sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(_sig, _signal_handler)

def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _post_event(event_url: Optional[str], template: Optional[str], payload: dict) -> None:
    if not event_url or requests is None:
        return
    try:
        if template:
            body = template.format(
                stream=payload.get("stream", ""),
                url=payload.get("url", ""),
                file=payload.get("file", ""),
                path=payload.get("path", ""),
                timestamp=payload.get("timestamp", ""),
                success=str(payload.get("success", False)).lower(),
            )
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            requests.post(event_url, data=body, headers=headers, timeout=5)
        else:
            requests.post(event_url, json=payload, timeout=5)
    except Exception as e:
        log.warning("Failed to POST event for '%s': %s", payload.get("stream"), e)

def read_options() -> dict:
    with open("/data/options.json", "r", encoding="utf-8") as f:
        return json.load(f)

def record_stream_loop(
    name: str,
    url: str,
    expected_duration: float,
    setpts: float,
    output_dir: str,
    event_url: Optional[str],
    use_hwaccel: bool = False,
    hw_device: str = "/dev/dri/renderD128",
    global_quality: int = 39,
    max_filesize_mb: int = 0,
    event_body_template: Optional[str] = None,
) -> None:

    while not STOP_EVENT.is_set():
        # таймстамп без DeprecationWarning (TZ-aware)
        start_ts = datetime.datetime.now(datetime.UTC)

        # длительность реальной записи = ожидаемая_длина_клипа * setpts
        try:
            record_seconds = float(expected_duration) * float(setpts)
        except Exception:
            record_seconds = float(expected_duration)

        # Дата-папка: /media/camera/YYYY.MM.DD
        day_folder = start_ts.strftime("%Y.%m.%d")
        day_path = os.path.join(output_dir, day_folder)
        ensure_directory(day_path)

        # Имя файла: <name>_YYYYMMDD_HHMMSS.mp4
        safe_name = name.replace(" ", "_")
        timestamp_str = start_ts.strftime("%Y%m%d_%H%M%S")
        filename = f"{safe_name}_{timestamp_str}.mp4"
        full_path = os.path.join(day_path, filename)

        # Формирование команды ffmpeg
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-nostdin",
            "-fflags", "+genpts+discardcorrupt",
        ]
        if use_hwaccel:
            cmd += [
                "-hwaccel_flags", "allow_profile_mismatch",
                "-hwaccel", "vaapi",
                "-hwaccel_device", hw_device,
                "-hwaccel_output_format", "vaapi",
            ]
        cmd += ["-i", url]

        if use_hwaccel:
            cmd += ["-c:v", "h264_vaapi", "-global_quality", str(global_quality)]
        else:
            cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23"]

        # setpts: пользователь задаёт множитель скорости (1=норм)
        try:
            inv = 1.0 / float(setpts)
        except Exception:
            inv = 1.0
        cmd += ["-filter:v", f"setpts={inv:.6f}*PTS"]

        if max_filesize_mb and int(max_filesize_mb) > 0:
            cmd += ["-fs", f"{int(max_filesize_mb)}M"]

        # Обрубить по времени реальной записи
        if record_seconds and record_seconds > 0:
            cmd += ["-t", f"{record_seconds:.3f}"]

        # Лог: полная команда
        logged_cmd = " ".join(shlex.quote(x) for x in cmd + [full_path])
        log.info("Started recording worker for '%s' at %s", name, start_ts.isoformat())
        log.info("ffmpeg command: %s", logged_cmd)

        success = False
        rc = None
        try:
            proc = subprocess.Popen(
                cmd + [full_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            # ожидаем завершения, но реагируем на STOP_EVENT
            while True:
                try:
                    rc = proc.wait(timeout=1.0)
                    break
                except subprocess.TimeoutExpired:
                    if STOP_EVENT.is_set():
                        # Попросим ffmpeg корректно завершить файл
                        try:
                            proc.send_signal(signal.SIGINT)  # ffmpeg закрывает файл
                        except Exception:
                            pass
                        # немного подождать
                        try:
                            rc = proc.wait(timeout=5.0)
                        except subprocess.TimeoutExpired:
                            proc.terminate()
                            try:
                                rc = proc.wait(timeout=3.0)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                rc = proc.wait()
                        break

            success = (rc == 0)

        except Exception as e:
            log.exception("FFmpeg error for '%s': %s", name, e)
            success = False
            rc = -1

        file_ok = os.path.exists(full_path) and os.path.getsize(full_path) > 0

        payload = {
            "stream": name,
            "url": url,
            "file": os.path.basename(full_path),
            "path": full_path,
            "timestamp": start_ts.isoformat(),
            "success": bool(success and file_ok),
            "stopped": STOP_EVENT.is_set(),
            "return_code": rc,
        }
        _post_event(event_url, event_body_template, payload)

        if STOP_EVENT.is_set():
            break

        # быстрый ре-трай при неуспехе
        if not success:
            time.sleep(1.0)

def main() -> None:
    config = read_options()

    streams = config.get("streams") or []
    default_setpts = float(config.get("default_setpts") or 1)
    default_expected_duration = float(config.get("default_expected_duration") or 60)
    output_dir = str(config.get("output_dir") or "/media/camera")
    event_url: Optional[str] = config.get("event_url")
    event_body_template: Optional[str] = config.get("event_body_template") or None

    use_hwaccel = bool(config.get("use_hwaccel", False))
    hw_device = str(config.get("hw_device") or "/dev/dri/renderD128")
    global_quality = int(config.get("global_quality") or 39)

    ensure_directory(output_dir)

    threads = []
    for idx, stream_cfg in enumerate(streams):
        url = stream_cfg["url"]
        name = stream_cfg.get("name", f"Stream{idx+1}")
        expected_duration = float(stream_cfg.get("expected_duration", default_expected_duration))
        setpts = float(stream_cfg.get("setpts", default_setpts))
        max_filesize_mb = int(stream_cfg.get("max_filesize_mb", 0))  # ← теперь пер-стрим

        t = threading.Thread(
            target=record_stream_loop,
            args=(
                name,
                url,
                expected_duration,
                setpts,
                output_dir,
                event_url,
                use_hwaccel,
                hw_device,
                global_quality,
                max_filesize_mb,
                event_body_template,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
