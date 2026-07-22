FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app
WORKDIR /app

# Built via: make build-runway-linux
COPY --chmod=0555 .docker-bin/runway ./runway

EXPOSE 8080

CMD ["/app/runway"]
