#!/bin/sh
#
# Entry point for the RTSP Recorder add-on. This script is executed by the
# Home Assistant Supervisor when the container starts. It simply invokes
# the Python program that performs all of the recording and notification
# logic. You could add additional shell logic here if desired (for example
# to generate runtime configuration files) but keeping it simple makes the
# behaviour predictable.

set -euo pipefail

echo "Starting RTSP Recorder add-on..."

# Ensure the recordings directory exists. The Python script also creates
# its output directory if necessary, but performing the creation here
# produces clearer log messages when the add-on starts.
CONFIG_PATH="/data/options.json"
OUTPUT_DIR=$(python3 -c "import json, os, sys; d=json.load(open('$CONFIG_PATH')).get('output_dir'); print(d if d else '/media/rtsp-recordings')" 2>/dev/null || true)

if [ -z "$OUTPUT_DIR" ]; then
  OUTPUT_DIR="/media/rtsp-recordings"
fi
mkdir -p "$OUTPUT_DIR"

# Execute the recorder. It runs indefinitely until the container is
# stopped by the Supervisor.
exec python3 /usr/local/bin/record_rtsp.py