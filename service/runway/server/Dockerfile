FROM debian:bookworm-slim

# git is a runtime dependency, not a build one: the git lander shells out to it
# to fetch, apply, and push. Without it the service still starts, but only the
# noop lander works — a git land target fails at startup instead.
RUN apt-get update && apt-get install -y ca-certificates git && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app && chmod 0755 /app \
    # The working trees the git lander owns. Created here so that a named volume
    # mounted over it starts with a mode the service can write: Docker seeds an
    # empty volume from the image, and the container's user is deployment-
    # configurable (SQ_CONTAINER_USER), so a root-owned directory would leave a
    # non-root service unable to provision its checkout. Group- and
    # world-writable for that reason, which costs nothing on a path that exists
    # to be mounted over.
    && mkdir -p /var/runway/checkouts && chmod 0777 /var/runway/checkouts
WORKDIR /app

# Built via: make build-runway-linux
COPY --chmod=0555 .docker-bin/runway ./runway

EXPOSE 8080

CMD ["/app/runway"]
