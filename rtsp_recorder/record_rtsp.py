#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import glob
import shlex
import signal
import logging
import threading
import subprocess
import datetime
from typing import Optional

# requests is installed via Alpine package py3-requests
try:
    import requests
except Exception:  # pragma: no cover
    requests = None

# -----------------------------------------------------------------------------
# logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rtsp-recorder")

# -----------------------------------------------------------------------------
# shutdown coordination
# -----------------------------------------------------------------------------
STOP_EVENT = threading.Event()

def _signal_handler(signum, frame):
    STOP_EVENT.set()

for _sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(_sig, _signal_handler)

# -----------------------------------------------------------------------------
# utils
# -----------------------------------------------------------------------------
def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def read_options() -> dict:
    with open("/data/options.json", "r", encoding="utf-8") as f:
        return json.load(f)

def _redact_url(u: str) -> str:
    """
    rtsp://user:pass@host -> rtsp://user:***@host
    leaves anything else untouched
    """
    try:
        if "://" in u and "@" in u:
            scheme, rest = u.split("://", 1)
            auth, host = rest.split("@", 1)
            if ":" in auth:
                user, _pw = auth.split(":", 1)
                return f"{scheme}://{user}:***@{host}"
    except Exception:
        pass
    return u

def _pipe_to_log(pipe, level=logging.INFO, prefix="ffmpeg"):
    """Stream a pipe line-by-line into our logger."""
    try:
        for line in iter(pipe.readline, ''):
            if not line:
                break
            line = line.rstrip()
            if line:
                log.log(level, "%s: %s", prefix, line)
    except Exception:
        pass
    finally:
        try:
            pipe.close()
        except Exception:
            pass

def _post_event(event_url: Optional[str], template: Optional[str], payload: dict) -> None:
    """POST webhook either as JSON or form-encoded via template."""
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

def _pick_va_device(explicit: Optional[str]) -> Optional[str]:
    """Pick a VAAPI render device (/dev/dri/renderD*)."""
    if explicit and os.path.exists(explicit):
        return explicit
    cands = sorted(glob.glob("/dev/dri/renderD*"))
    return cands[0] if cands else None

# -----------------------------------------------------------------------------
# worker
# -----------------------------------------------------------------------------
def record_stream_loop(
    name: str,
    url: str,
    expected_duration: float,
    setpts: float,
    output_dir: str,
    event_url: Optional[str],
    use_hwaccel: bool = False,                 # when true: VAAPI decode + encode
    hw_device: str = "/dev/dri/renderD128",
    global_quality: int = 39,
    max_filesize_mb: int = 0,                  # per-stream
    event_body_template: Optional[str] = None,
    ffmpeg_show_output: bool = False,
) -> None:

    while not STOP_EVENT.is_set():
        # timezone-aware UTC (Python 3.12)
        start_ts = datetime.datetime.now(datetime.UTC)

        # real-time record length = desired output length * speed multiplier
        try:
            record_seconds = float(expected_duration) * float(setpts)
        except Exception:
            record_seconds = float(expected_duration)

        # /media/camera/YYYY.MM.DD
        day_folder = start_ts.strftime("%Y.%m.%d")
        day_path = os.path.join(output_dir, day_folder)
        ensure_directory(day_path)

        safe_name = name.replace(" ", "_")
        ts_str = start_ts.strftime("%Y.%m.%d_%H:%M:%S")
        filename = f"{safe_name}_{ts_str}.mp4"
        full_path = os.path.join(day_path, filename)

        # ---------------------------------------------------------------------
        # build ffmpeg cmd (match user's VAAPI flags)
        # VAAPI decode + encode when use_hwaccel == True:
        #   -hwaccel_flags allow_profile_mismatch
        #   -hwaccel vaapi
        #   -hwaccel_device <dev>
        #   -hwaccel_output_format vaapi
        # ---------------------------------------------------------------------
        loglevel = "info" if ffmpeg_show_output else "warning"

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", loglevel,
            "-nostdin",
            "-fflags", "+genpts+discardcorrupt",
            "-rtsp_transport", "tcp",
        ]

        va_dev = None
        if use_hwaccel:
            va_dev = _pick_va_device(hw_device if hw_device else None)
            if va_dev:
                cmd += [
                    "-hwaccel_flags", "allow_profile_mismatch",
                    "-hwaccel", "vaapi",
                    "-hwaccel_device", va_dev,
                    "-hwaccel_output_format", "vaapi",
                ]
            else:
                log.warning("VAAPI device not found under /dev/dri; falling back to software for '%s'", name)

        # input
        cmd += ["-i", url]

        # setpts factor
        try:
            inv = 1.0 / float(setpts)
        except Exception:
            inv = 1.0

        # filters & encoder
        if use_hwaccel and va_dev:
            # VAAPI decode -> CPU setpts -> VAAPI encode
            vf = f"setpts={inv:.6f}*PTS"
            cmd += ["-filter:v", vf]
            cmd += ["-c:v", "h264_vaapi", "-global_quality", str(global_quality)]
        else:
            # full software path
            cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23"]
            cmd += ["-filter:v", f"setpts={inv:.6f}*PTS"]

        if max_filesize_mb and int(max_filesize_mb) > 0:
            cmd += ["-fs", f"{int(max_filesize_mb)}M"]

        if record_seconds and record_seconds > 0:
            cmd += ["-t", f"{record_seconds:.3f}"]

        # ---------------------------------------------------------------------
        # logging
        # ---------------------------------------------------------------------
        log_cmd = cmd.copy()
        # redact URL in logged command (after "-i")
        try:
            i_idx = log_cmd.index("-i")
            if i_idx + 1 < len(log_cmd):
                log_cmd[i_idx + 1] = _redact_url(log_cmd[i_idx + 1])
        except Exception:
            pass

        logged_cmd = " ".join(shlex.quote(x) for x in log_cmd + [full_path])
        log.info("Started recording worker for '%s' at %s", name, start_ts.isoformat())
        log.info("ffmpeg command: %s", logged_cmd)

        # ---------------------------------------------------------------------
        # run ffmpeg with graceful stop handling
        # ---------------------------------------------------------------------
        success = False
        rc = None
        try:
            proc = subprocess.Popen(
                cmd + [full_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # line buffered for live mirroring
            )

            # mirror output if requested
            stdout_t = stderr_t = None
            if ffmpeg_show_output:
                if proc.stdout:
                    stdout_t = threading.Thread(
                        target=_pipe_to_log,
                        args=(proc.stdout, logging.INFO, "ffmpeg out"),
                        daemon=True,
                    )
                    stdout_t.start()
                if proc.stderr:
                    stderr_t = threading.Thread(
                        target=_pipe_to_log,
                        args=(proc.stderr, logging.INFO, "ffmpeg err"),
                        daemon=True,
                    )
                    stderr_t.start()

            # wait loop with stop support
            while True:
                try:
                    rc = proc.wait(timeout=1.0)
                    break
                except subprocess.TimeoutExpired:
                    if STOP_EVENT.is_set():
                        # ask ffmpeg to finalize the file
                        try:
                            proc.send_signal(signal.SIGINT)  # graceful finalize
                        except Exception:
                            pass
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

            # if not verbose, at least capture the tail of stderr on error
            if rc != 0 and not ffmpeg_show_output and proc.stderr:
                try:
                    err_tail = proc.stderr.read()[-4000:]
                    if err_tail:
                        log.warning("ffmpeg stderr for '%s':\n%s", name, err_tail)
                except Exception:
                    pass

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

        # quick retry on failure
        if not success:
            time.sleep(1.0)

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
def main() -> None:
    cfg = read_options()

    streams = cfg.get("streams") or []
    default_setpts = float(cfg.get("default_setpts") or 1)
    default_expected_duration = float(cfg.get("default_expected_duration") or 60)
    output_dir = str(cfg.get("output_dir") or "/media/camera")

    event_url: Optional[str] = cfg.get("event_url")
    event_body_template: Optional[str] = cfg.get("event_body_template") or None

    use_hwaccel = bool(cfg.get("use_hwaccel", False))
    hw_device = str(cfg.get("hw_device") or "/dev/dri/renderD128")
    global_quality = int(cfg.get("global_quality") or 39)
    ffmpeg_show_output = bool(cfg.get("ffmpeg_show_output", False))

    ensure_directory(output_dir)

    threads = []
    for idx, stream_cfg in enumerate(streams):
        url = stream_cfg["url"]
        name = stream_cfg.get("name", f"Stream{idx+1}")
        expected_duration = float(stream_cfg.get("expected_duration", default_expected_duration))
        setpts = float(stream_cfg.get("setpts", default_setpts))
        max_filesize_mb = int(stream_cfg.get("max_filesize_mb", 0))  # per stream

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
            kwargs={"ffmpeg_show_output": ffmpeg_show_output},
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    # if started manually, make sure stdout is not fully buffered
    try:
        os.environ["PYTHONUNBUFFERED"] = "1"
    except Exception:
        pass
    main()
