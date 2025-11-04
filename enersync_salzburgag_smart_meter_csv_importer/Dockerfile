ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM ${BUILD_FROM}

# Unbuffered python logs + sane defaults
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# You don’t actually use pip anymore, so py3-pip is optional
# Add ca-certificates in case your HA base URL is HTTPS
RUN apk add --no-cache \
      python3 tzdata bash inotify-tools jq ca-certificates \
      py3-pandas py3-dateutil py3-requests py3-websocket-client

WORKDIR /app
COPY run.sh /run.sh
COPY app.py /app/app.py

# Ensure LF line endings and executable bit
RUN dos2unix /run.sh || true && chmod +x /run.sh

CMD ["/run.sh"]
