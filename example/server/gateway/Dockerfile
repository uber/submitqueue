FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Copy pre-built Linux binary
# Built via: make build-gateway-linux
COPY .docker-bin/gateway ./gateway

# Sample queue configuration; the gateway reads it on startup via
# QUEUE_CONFIG_PATH (set in docker-compose.yml).
COPY example/server/gateway/queues.yaml ./queues.yaml

EXPOSE 8080

CMD ["./gateway"]
