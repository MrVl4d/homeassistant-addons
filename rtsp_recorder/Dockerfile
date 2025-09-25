ARG BUILD_FROM
FROM $BUILD_FROM

# Install python3, ffmpeg and pip. ffmpeg is used to capture RTSP streams.
# Requests is installed via pip for sending HTTP events. We intentionally
# avoid using bashio in this image and instead parse the add-on options
# directly from /data/options.json in Python.
RUN apk add --no-cache \
      python3 \
      py3-pip \
      ffmpeg \
      intel-media-sdk \
      intel-media-driver && \
    pip3 install --no-cache --upgrade \
      requests

# Copy our runtime script and the Python program into the image. The
# working directory remains the default root (/). The script reads
# configuration from /data/options.json and stores recordings in a
# configurable directory (defaulting to /media/rtsp-recordings).
COPY run.sh /run.sh
COPY record_rtsp.py /usr/local/bin/record_rtsp.py

# Ensure the shell script is executable.
RUN chmod a+x /run.sh /usr/local/bin/record_rtsp.py

# When the container starts the Home Assistant Supervisor executes
# /run.sh. The script handles execution of the Python process.
CMD [ "/run.sh" ]