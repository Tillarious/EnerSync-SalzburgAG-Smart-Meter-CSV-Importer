ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM ${BUILD_FROM}

RUN apk add --no-cache python3 py3-pip tzdata bash inotify-tools jq py3-pandas py3-dateutil py3-requests py3-watchdog py3-websocket-client

WORKDIR /app
COPY run.sh /run.sh
COPY app.py /app/app.py

RUN chmod +x /run.sh
CMD [ "/run.sh" ]
