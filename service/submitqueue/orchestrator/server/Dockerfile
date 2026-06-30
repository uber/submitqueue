FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Copy pre-built Linux binary
# Built via: make build-orchestrator-linux
COPY .docker-bin/orchestrator ./orchestrator

EXPOSE 8080

CMD ["./orchestrator"]
