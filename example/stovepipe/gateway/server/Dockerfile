FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Copy pre-built Linux binary (host path is stovepipe-* to avoid clobbering main .docker-bin/gateway).
# Built via: make build-stovepipe-gateway-linux
COPY .docker-bin/stovepipe-gateway ./gateway

# Reuse SubmitQueue sample queue YAML; compose sets QUEUE_CONFIG_PATH for parity with main gateway image.
COPY example/server/gateway/queues.yaml ./queues.yaml

EXPOSE 8080

CMD ["./gateway"]
