ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

# Python + requests из apk (без pip), ffmpeg, Intel VA-API
RUN apk add --no-cache \
      python3 \
      py3-pip \
      py3-requests \
      ffmpeg \
      intel-media-sdk \
      intel-media-driver

COPY run.sh /run.sh
COPY record_rtsp.py /usr/local/bin/record_rtsp.py

RUN chmod a+x /run.sh /usr/local/bin/record_rtsp.py

CMD [ "/run.sh" ]
