FROM debian:bookworm-slim

# git is a runtime dependency, not a build one: the git merger shells out to it
# to fetch, apply, and push. Without it the service still starts, but only the
# noop merger works — a git merge target fails at startup instead.
RUN apt-get update && apt-get install -y ca-certificates git && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app
WORKDIR /app

# Built via: make build-runway-linux
COPY --chmod=0555 .docker-bin/runway ./runway

EXPOSE 8080

CMD ["/app/runway"]
