FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Built via: make build-stovepipe-orchestrator-linux
COPY .docker-bin/stovepipe-orchestrator ./orchestrator

EXPOSE 8080

CMD ["./orchestrator"]
