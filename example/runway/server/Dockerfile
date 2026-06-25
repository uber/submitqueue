FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /root/

# Built via: make build-runway-linux
COPY .docker-bin/runway ./runway

EXPOSE 8080

CMD ["./runway"]
