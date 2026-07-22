FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app
WORKDIR /app

# Copy pre-built Linux binary
# Built via: make build-gateway-linux
COPY --chmod=0555 .docker-bin/gateway ./gateway

# Sample queue configuration; the gateway reads it on startup via
# QUEUE_CONFIG_PATH (set in docker-compose.yml).
COPY --chmod=0444 service/submitqueue/gateway/server/queues.yaml ./queues.yaml

EXPOSE 8080

CMD ["/app/gateway"]
