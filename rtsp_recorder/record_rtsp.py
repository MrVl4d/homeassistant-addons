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

# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rtsp-recorder")

try:
    import requests
except Exception:
    requests = None

# -------------- HW accel + external ffmpeg (Variant B) ------------------------
FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")
HWACCEL_MODE = os.environ.get("HWACCEL_MODE", "auto").lower()  # auto|vaapi|drm|none

def _is_amd64_vaapi() -> bool:
    try:
        return (os.uname().machine in ("x86_64","amd64")) and os.path.exists("/dev/dri/renderD128")
    except Exception:
        return False

def _is_pi5_media() -> bool:
    try:
        return (os.uname().machine in ("aarch64","arm64")) and any(os.path.exists(p) for p in ("/dev/media0","/dev/media1","/dev/media2"))
    except Exception:
        return False

def _pick_hw_mode() -> str:
    if HWACCEL_MODE in ("vaapi","drm","none"):
        return HWACCEL_MODE
    if _is_amd64_vaapi():
        return "vaapi"
    if _is_pi5_media():
        return "drm"
    return "none"

def _probe_codec(ffprobe_bin: str, rtsp_url: str) -> Optional[str]:
    try:
        out = subprocess.check_output([ffprobe_bin, "-v", "error", "-select_streams", "v:0",
                                       "-show_entries", "stream=codec_name", "-of", "default=nk=1:nw=1", rtsp_url],
                                       stderr=subprocess.STDOUT, timeout=7)
        c = out.decode("utf-8","ignore").strip().lower()
        return {"h265":"hevc","hevc":"hevc","h264":"h264"}.get(c, None)
    except Exception:
        return None
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# options
# -----------------------------------------------------------------------------
def read_options() -> dict:
    p = "/data/options.json"
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    # fallback for manual runs (env/config-less)
    return {
        "streams": [],
        "default_setpts": 10,
        "default_expected_duration": 60,
        "output_dir": "/media/camera",
        "event_url": "",
        "event_body_template": "",
        "use_hwaccel": True,
        "hw_device": "/dev/dri/renderD128",
        "global_quality": 24,
        "ffmpeg_show_output": False,
    }

# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _redact_url(u: str) -> str:
    """Hide password in RTSP URL for logs."""
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
            try:
                # strip trailing newline for clean log lines
                s = line.rstrip("\r\n")
            except Exception:
                s = line
            if s:
                logging.log(level, "%s: %s", prefix, s)
    except Exception:
        pass
    finally:
        try:
            pipe.close()
        except Exception:
            pass

def _post_event(event_url: Optional[str], payload: dict, template: Optional[str], log: logging.Logger):
    if not event_url or not requests:
        return
    try:
        if template:
            # template is application/x-www-form-urlencoded body
            body = template.format(
                stream=payload.get("stream", ""),
                path=payload.get("path", ""),
                started_at=payload.get("started_at", ""),
                speed_factor=payload.get("speed_factor", ""),
                hwaccel_mode=payload.get("hwaccel_mode", ""),
                codec_in=payload.get("codec_in", ""),
                success=str(payload.get("success", False)).lower(),
                timestamp=payload.get("timestamp", ""),
            )
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            requests.post(event_url, data=body, headers=headers, timeout=5)
        else:
            requests.post(event_url, json=payload, timeout=5)
    except Exception as e:
        log.warning("Failed to POST event for '%s': %s", payload.get("stream"), e)

def _pick_va_device(explicit: Optional[str]) -> Optional[str]:
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
    event_body_template: Optional[str],
    use_hwaccel: bool = False,
    hw_device: Optional[str] = None,
    max_filesize_mb: Optional[int] = None,
    global_quality: int = 24,
    ffmpeg_show_output: bool = False,
) -> None:

    while True:
        start_ts = datetime.datetime.now().astimezone()
        try:
            record_seconds = float(expected_duration) * float(setpts)
        except Exception:
            record_seconds = float(expected_duration)

        day_folder = start_ts.strftime("%Y.%m.%d")
        day_path = os.path.join(output_dir, day_folder)
        ensure_directory(day_path)

        safe_name = name.replace(" ", "_")
        ts_str = start_ts.strftime("%Y.%m.%d_%H:%M:%S")
        filename = f"{safe_name}_{ts_str}.mp4"
        full_path = os.path.join(day_path, filename)

        # Detect input codec & choose hw mode
        codec = _probe_codec(FFPROBE_BIN, url)
        picked_hw = _pick_hw_mode()

        # base ffmpeg
        loglevel = "info" if ffmpeg_show_output else "warning"
        cmd = [
            FFMPEG_BIN,
            "-hide_banner",
            "-loglevel", loglevel,
            "-nostdin",
            "-fflags", "+genpts+discardcorrupt",
            "-rtsp_transport", "tcp",
        ]

        va_dev = None
        if use_hwaccel and picked_hw == "vaapi":
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
        drm_in = ["-hwaccel", "drm"] if (use_hwaccel and picked_hw == "drm" and codec == "hevc") else []
        cmd += drm_in + ["-i", url]

        # setpts factor
        try:
            inv = 1.0 / float(setpts)
        except Exception:
            inv = 1.0

        cmd += ["-an"]
        
        # filters & encoder
        if use_hwaccel and picked_hw == "vaapi" and va_dev:
            # VAAPI decode -> CPU setpts -> VAAPI encode
            vf = f"setpts={inv:.6f}*PTS"
            cmd += ["-filter:v", vf]
            cmd += ["-c:v", "h264_vaapi", "-global_quality", str(global_quality)]
        elif picked_hw == "drm":
            # RPi5: HEVC hw-decode (via -hwaccel drm added on input) -> CPU setpts -> SW encode
            cmd += [
                "-filter:v", f"setpts={inv:.6f}*PTS",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-movflags", "+faststart"
            ]
        else:
            # full software path
            cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23"]

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
        log.info("ffmpeg: %s", logged_cmd)

        # ---------------------------------------------------------------------
        # run ffmpeg
        # ---------------------------------------------------------------------
        proc = None
        try:
            proc = subprocess.Popen(
                cmd + [full_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            threads = []
            if proc.stdout:
                t_out = threading.Thread(target=_pipe_to_log, args=(proc.stdout, logging.INFO, f"ffmpeg[{name}]"))
                t_out.daemon = True
                t_out.start()
                threads.append(t_out)
            if proc.stderr:
                t_err = threading.Thread(target=_pipe_to_log, args=(proc.stderr, logging.WARNING, f"ffmpeg[{name}]"))
                t_err.daemon = True
                t_err.start()
                threads.append(t_err)

            deadline = time.time() + float(record_seconds or 0)
            while True:
                rc = proc.poll()
                if rc is not None:
                    break
                if record_seconds and time.time() >= deadline:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    # give it a moment to flush and close the file
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                    break
                time.sleep(0.2)

            for t in threads:
                try:
                    t.join(timeout=1)
                except Exception:
                    pass

            rc = proc.returncode if proc else None
            ok = (rc == 0) and os.path.exists(full_path)
            # event
            _post_event(event_url, {
                "stream": name,
                "path": full_path,
                "started_at": start_ts.isoformat(),
                "speed_factor": setpts,
                "hwaccel_mode": picked_hw,
                "codec_in": codec or "",
                "success": ok,
                "timestamp": datetime.datetime.now().astimezone().isoformat(),
            }, event_body_template, log)

        except Exception as e:
            log.error("Recording failed for '%s': %s", name, e)
            _post_event(event_url, {
                "stream": name,
                "path": full_path,
                "started_at": start_ts.isoformat(),
                "speed_factor": setpts,
                "hwaccel_mode": picked_hw,
                "codec_in": codec or "",
                "success": False,
                "timestamp": datetime.datetime.now().astimezone().isoformat(),
            }, event_body_template, log)

        finally:
            try:
                if proc and proc.stdout:
                    proc.stdout.close()
            except Exception:
                pass
            try:
                if proc and proc.stderr:
                    proc.stderr.close()
            except Exception:
                pass

        # small pause before next segment
        time.sleep(0.25)

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
def main() -> None:
    cfg = read_options()

    streams = cfg.get("streams") or []
    default_setpts = float(cfg.get("default_setpts") or 1)
    default_expected_duration = float(cfg.get("default_expected_duration") or 60)
    output_dir = cfg.get("output_dir") or "/media/camera"
    event_url = cfg.get("event_url") or ""
    event_body_template = cfg.get("event_body_template") or ""
    use_hwaccel = bool(cfg.get("use_hwaccel")) if cfg.get("use_hwaccel") is not None else True
    hw_device = cfg.get("hw_device")
    global_quality = int(cfg.get("global_quality") or 24)
    ffmpeg_show_output = bool(cfg.get("ffmpeg_show_output"))

    ensure_directory(output_dir)

    threads = []
    for item in streams:
        url = item.get("url")
        if not url:
            continue
        name = item.get("name") or url
        expected_duration = float(item.get("expected_duration") or default_expected_duration)
        setpts = float(item.get("setpts") or default_setpts)
        max_filesize_mb = item.get("max_filesize_mb")
        log.info("Starting stream '%s' -> out=%s exp=%ss setpts=%s hw=%s", name, output_dir, expected_duration, setpts, "on" if use_hwaccel else "off")
        t = threading.Thread(
            target=record_stream_loop,
            args=(
                name,
                url,
                expected_duration,
                setpts,
                output_dir,
                event_url,
                event_body_template,
                use_hwaccel,
                hw_device,
                max_filesize_mb,
                global_quality,
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
