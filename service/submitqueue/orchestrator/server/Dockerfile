FROM debian:bookworm-slim

# git is a runtime dependency, not a build one: the git change provider shells
# out to it to read what a change touched. Without it the service still starts,
# but a queue configured with that provider fails at startup instead.
RUN apt-get update && apt-get install -y ca-certificates git && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app \
    # The repository copies the git change provider owns. Created here so that a
    # named volume mounted over it starts with a mode the service can write:
    # Docker seeds an empty volume from the image, and the container's user is
    # deployment-configurable (SQ_CONTAINER_USER), so a root-owned directory
    # would leave a non-root service unable to provision. Group- and
    # world-writable for that reason, which costs nothing on a path that exists
    # to be mounted over.
    && mkdir -p /var/submitqueue/changerepos && chmod 0777 /var/submitqueue/changerepos
WORKDIR /app

# Copy pre-built Linux binary
# Built via: make build-orchestrator-linux
COPY --chmod=0555 .docker-bin/orchestrator ./orchestrator

EXPOSE 8080

CMD ["/app/orchestrator"]
