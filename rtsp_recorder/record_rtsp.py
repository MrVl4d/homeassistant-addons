#!/usr/bin/env python3
"""
RTSP Recorder
==============

This module is the core of the Home Assistant RTSP Recorder add-on. It reads
configuration from `/data/options.json` (provided by the Supervisor at
container startup), then spawns a worker thread for each configured stream.
Each worker thread repeatedly records a segment from its RTSP source using
``ffmpeg``, applies a configurable time scaling using the ``setpts`` filter,
saves the resulting video into the configured output directory and finally
sends an HTTP POST request to a configured URL to signal that the segment has
been created.

Configuration
-------------

Options are defined in ``config.yaml`` and passed to the container by the
Supervisor. The available keys are:

``streams``
    A list of stream definitions. Each item must contain at least a ``url`` key
    pointing at the RTSP source. The optional ``name`` key will be used in
    output filenames and event payloads. If omitted, the index of the stream
    in the list is used instead. ``expected_duration`` specifies the desired
    length of the output video file in seconds (defaults to the global
    ``default_expected_duration``). ``setpts`` specifies a factor applied to
    the timestamps of the recorded video. The actual recording time will be
    ``expected_duration * setpts`` so that after applying the speed-up or
    slow-down, the resulting clip has the expected duration.

``default_setpts``
    Global fallback ``setpts`` factor applied to all streams when not set per
    stream. A value of 1.0 means real-time playback, 2.0 means the video will
    play twice as fast and therefore record for twice as long to achieve the
    same output length.

``default_expected_duration``
    Global fallback length for each output clip in seconds when not set per
    stream. The actual recording time will be this value multiplied by the
    ``setpts`` factor.

``output_dir``
    Directory where all recorded clips will be stored. The directory will be
    created if it does not exist. Default is ``/media/rtsp-recordings`` which
    maps to the user's media folder when the add-on is configured with
    ``media:rw`` in ``config.yaml``.

``event_url``
    If set, an HTTP POST request will be sent to this URL after each clip is
    created. The payload includes the stream name, the original RTSP URL, the
    filename and full path of the created file, and a UTC timestamp. If unset
    or null, no notification is sent.

The script uses Python's threading module to run independent loops for each
stream. ``ffmpeg`` must be installed in the container; see the Dockerfile for
installation details.
"""

import json
import os
import sys
import time
import threading
import subprocess
import logging
import datetime
from typing import Any, Dict, List, Optional

try:
    import requests  # type: ignore
except ImportError:
    # If requests is missing, log a clear error. The Dockerfile installs it by default.
    requests = None  # type: ignore


CONFIG_FILE = "/data/options.json"


def load_config(path: str = CONFIG_FILE) -> Dict[str, Any]:
    """Load and parse the JSON options provided by the Home Assistant Supervisor.

    If the file does not exist or cannot be parsed, the function logs an error
    and exits the process. Default values are applied according to the schema
    defined in ``config.yaml``.
    """
    if not os.path.exists(path):
        logging.error("Configuration file %s not found", path)
        sys.exit(1)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as err:
        logging.error("Failed to load JSON configuration from %s: %s", path, err)
        sys.exit(1)

    # Apply defaults. These keys mirror those defined in config.yaml
    config: Dict[str, Any] = {}
    config["streams"] = data.get("streams", [])
    config["default_setpts"] = float(data.get("default_setpts", 1.0))
    config["default_expected_duration"] = int(data.get("default_expected_duration", 60))
    config["output_dir"] = data.get("output_dir") or "/media/rtsp-recordings"
    config["event_url"] = data.get("event_url") or None
    return config


def ensure_directory(path: str) -> None:
    """Ensure that the given directory exists. Create it if it does not."""
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as err:
        logging.error("Could not create output directory %s: %s", path, err)
        # Let the error propagate; failing early is better than silently not recording.
        raise


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
    """Continuously record segments from a single RTSP stream.

    This function runs forever until the Python process is killed. For each
    iteration it calculates the recording time needed to achieve the desired
    output duration after applying the ``setpts`` factor, invokes ``ffmpeg``
    to record the clip, and optionally sends an HTTP POST to the configured
    event endpoint.

    Parameters
    ----------
    name: str
        Friendly name used for logging, filenames and event payloads.
    url: str
        RTSP URL of the stream to record.
    expected_duration: float
        Desired length of the output video file in seconds.
    setpts: float
        Factor applied to the video timestamps. The recording time is
        ``expected_duration * setpts``. A value greater than 1 accelerates
        playback (shorter output), while a value less than 1 slows it down.
    output_dir: str
        Directory to which the clip is saved.
    event_url: Optional[str]
        If provided, an HTTP POST is sent after each clip with details about
        the recording.
    """
    while True:
        start_ts = datetime.datetime.utcnow()
        # Compute how long to record in real-time. For example, if the user
        # wants a 30s clip with setpts=2 (double speed), record for 60s.
        record_duration = expected_duration * setpts
        # Compose filename and full path. Include timestamp to avoid collisions.
        # Files are stored in a per-day subfolder using the UTC date in YYYY.MM.DD
        # format. A safe version of the stream name is prepended to the filename
        # to distinguish streams. For example: /media/camera/2025.09.07/cam1_20250907_123456.mp4
        date_str = start_ts.strftime("%Y.%m.%d")
        day_dir = os.path.join(output_dir, date_str)
        try:
            os.makedirs(day_dir, exist_ok=True)
        except Exception as err:
            logging.error("Could not create directory %s: %s", day_dir, err)
        timestamp_str = start_ts.strftime("%Y%m%d_%H%M%S")
        safe_name = name.replace(" ", "_") if name else "stream"
        filename = f"{safe_name}_{timestamp_str}.mp4"
        output_path = os.path.join(day_dir, filename)

        # Build ffmpeg command. The options used here aim for compatibility
        # across many streams and minimise CPU usage. Users can adjust ffmpeg
        # behaviour by modifying this script if needed.
        cmd: List[str] = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
        ]

        # When hardware acceleration is enabled, configure VA-API. This includes
        # specifying the device, allowing profile mismatch and setting the
        # output format. The extra -fflags option helps ffmpeg handle bad
        # streams by generating PTS for each frame and discarding corrupt
        # packets. See https://ffmpeg.org/ffmpeg.html for details.
        if use_hwaccel:
            cmd += [
                "-hwaccel_flags", "allow_profile_mismatch",
                "-hwaccel", "vaapi",
                "-hwaccel_device", hw_device,
                "-hwaccel_output_format", "vaapi",
                "-fflags", "+genpts+discardcorrupt",
            ]

        # Transport and input
        cmd += [
            "-rtsp_transport", "tcp",
            "-i", url,
            "-t", str(record_duration),
        ]

        # Apply the setpts filter to speed up or slow down the video. The
        # expression uses the same factor regardless of whether VA-API is used.
        cmd += ["-vf", f"setpts=PTS/{setpts}"]

        # Video encoding options. Use VA-API when requested; otherwise fall
        # back to software encoding with libx264.
        if use_hwaccel:
            cmd += [
                "-c:v", "h264_vaapi",
                "-global_quality", str(global_quality),
            ]
        else:
            cmd += [
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-crf", "23",
            ]

        # Audio: copy the stream if present.
        cmd += ["-c:a", "copy"]

        # Optional file size limit. Only include this flag when configured.
        if max_filesize_mb and max_filesize_mb > 0:
            cmd += ["-fs", f"{max_filesize_mb}M"]

        # Output path goes last.
        cmd.append(output_path)

        logging.info(
            "Recording stream '%s' for %.2fs (target %.2fs, setpts %.2f) to %s",
            name,
            record_duration,
            expected_duration,
            setpts,
            output_path,
        )
        ffmpeg_success = False
        try:
            subprocess.run(cmd, check=True)
            ffmpeg_success = True
        except subprocess.CalledProcessError as err:
            logging.error("ffmpeg returned error for stream '%s': %s", name, err)
        except FileNotFoundError:
            logging.error(
                "ffmpeg executable not found. Ensure ffmpeg is installed in the container."
            )
            # If ffmpeg is missing there's no point continuing; exit the process.
            os._exit(1)
        except Exception as err:
            logging.exception("Unexpected error while recording '%s': %s", name, err)

        # Notify via HTTP event if configured, regardless of success. Even failed
        # recordings may be of interest to a downstream service.
        if event_url and requests is not None:
            payload_dict = {
                "stream": name,
                "url": url,
                "file": filename,
                "path": output_path,
                "timestamp": start_ts.isoformat() + "Z",
                "success": ffmpeg_success,
            }
            try:
                if event_body_template:
                    # Create a form-encoded body from the template. Use format_map so
                    # missing keys will not raise a KeyError.
                    try:
                        body = event_body_template.format(**payload_dict)
                    except Exception as fmt_err:
                        logging.error(
                            "Failed to format event_body_template '%s': %s", event_body_template, fmt_err
                        )
                        body = ""
                    headers = {"Content-Type": "application/x-www-form-urlencoded"}
                    response = requests.post(
                        event_url, data=body, headers=headers, timeout=10
                    )
                else:
                    response = requests.post(
                        event_url, json=payload_dict, timeout=10
                    )
                logging.info(
                    "HTTP event sent for '%s', status=%s", name, response.status_code
                )
            except Exception as err:
                logging.error("Failed to send HTTP event for '%s': %s", name, err)
        # Short delay to avoid immediate restart in case of extremely short durations.
        time.sleep(1)


def main() -> None:
    """Program entry point. Load configuration and spawn worker threads."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    config = load_config()
    streams: List[Dict[str, Any]] = config.get("streams", [])
    if not streams:
        logging.error(
            "No streams configured. Please define at least one stream in the add-on options."
        )
        # Keep the container alive to allow editing of options in UI, but do not start any workers.
        while True:
            time.sleep(60)

    default_setpts: float = float(config.get("default_setpts", 1.0))
    default_duration: float = float(config.get("default_expected_duration", 60))
    output_dir: str = str(config.get("output_dir"))
    ensure_directory(output_dir)
    event_url: Optional[str] = config.get("event_url")
    event_body_template: Optional[str] = config.get("event_body_template") or None
    # Hardware acceleration configuration
    use_hwaccel: bool = bool(config.get("use_hwaccel", False))
    hw_device: str = str(config.get("hw_device") or "/dev/dri/renderD128")
    global_quality: int = int(config.get("global_quality") or 39)
    max_filesize_mb: int = int(config.get("max_filesize_mb") or 0)

    # Start a worker thread for each configured stream.
    threads: List[threading.Thread] = []
    for idx, stream_cfg in enumerate(streams):
        url = stream_cfg.get("url")
        if not url:
            logging.warning(
                "Stream configuration at index %d is missing a 'url'; skipping", idx
            )
            continue
        name = stream_cfg.get("name") or f"stream{idx+1}"
        # Use per-stream overrides or fall back to defaults
        setpts = float(stream_cfg.get("setpts") or default_setpts)
        expected_duration = float(
            stream_cfg.get("expected_duration") or default_duration
        )
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
        logging.info(
            "Started recording worker for '%s' (url=%s, expected_duration=%ss, setpts=%s)",
            name,
            url,
            expected_duration,
            setpts,
        )

    # Keep main thread alive while worker threads run in the background. Use join
    # on a never-ending thread to wait forever. If all workers die unexpectedly,
    # exit gracefully.
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Received SIGTERM, shutting down...")
    except Exception as err:
        logging.error("Unexpected error in main loop: %s", err)


if __name__ == "__main__":
    main()