FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Copy pre-built Linux binary.
# Built via: make build-stovepipe-linux
COPY .docker-bin/stovepipe ./stovepipe
COPY service/stovepipe/server/queues.yaml ./queues.yaml

EXPOSE 8080

CMD ["./stovepipe"]
