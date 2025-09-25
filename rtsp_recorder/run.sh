#!/bin/sh
set -euo pipefail

term_handler() {
  if [ -n "${CHILD_PID:-}" ]; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
  fi
}
trap term_handler TERM INT

mkdir -p /media/camera
/usr/local/bin/record_rtsp.py &
CHILD_PID=$!
wait "$CHILD_PID"
