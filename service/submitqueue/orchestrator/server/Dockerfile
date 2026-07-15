FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app
WORKDIR /app

# Copy pre-built Linux binary
# Built via: make build-orchestrator-linux
COPY --chmod=0555 .docker-bin/orchestrator ./orchestrator

EXPOSE 8080

CMD ["/app/orchestrator"]
