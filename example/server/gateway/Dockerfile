FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Copy pre-built Linux binary
# Built via: make build-gateway-linux
COPY .docker-bin/gateway ./gateway

EXPOSE 8080

CMD ["./gateway"]
