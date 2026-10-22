FROM debian:bookworm-slim

# git is a runtime dependency: the fake reads targets out of a repository by
# running it.
RUN apt-get update && apt-get install -y ca-certificates git && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app
WORKDIR /app

# Built via: make build-tangofake-linux
COPY .docker-bin/tangofake ./tangofake
RUN chmod 0555 ./tangofake

EXPOSE 8080

CMD ["/app/tangofake"]
