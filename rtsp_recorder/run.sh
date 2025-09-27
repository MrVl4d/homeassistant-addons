#!/bin/sh
set -euo pipefail

term_handler() {
  if [ -n "${CHILD_PID:-}" ]; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
  fi
}
trap term_handler TERM INT

mkdir -p /media/camera
# Variant B: if host ffmpeg/ffprobe/libs are bind-mounted (e.g., /custom-ffmpeg),
# wire them up so record_rtsp.py uses the correct binaries.
if [ -d "/custom-ffmpeg/lib" ]; then
  export LD_LIBRARY_PATH="/custom-ffmpeg/lib:${LD_LIBRARY_PATH:-}"
fi
if [ -x "/custom-ffmpeg/bin/ffmpeg" ]; then
  export PATH="/custom-ffmpeg/bin:${PATH}"
  export FFMPEG_BIN="/custom-ffmpeg/bin/ffmpeg"
fi
if [ -x "/custom-ffmpeg/bin/ffprobe" ]; then
  export FFPROBE_BIN="/custom-ffmpeg/bin/ffprobe"
fi

# Allow optional HWACCEL_MODE override (auto|vaapi|drm|none)
export HWACCEL_MODE="${HWACCEL_MODE:-auto}"

/usr/local/bin/record_rtsp.py &
CHILD_PID=$!
wait "$CHILD_PID"
