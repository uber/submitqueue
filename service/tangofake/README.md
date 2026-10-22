# Fake Tango server

A runnable stand-in for a Tango server, for running the SubmitQueue Tango conflict analyzer without a build system. It serves Tango's real gRPC protocol and derives the changed targets from `TARGET` files in a git repository, so the analyzer exercises the same client path it uses against a real Tango. The target-file format and how a change maps to targets are described in the package doc of [`tangofake.go`](tangofake.go).

## Running it

It runs as a container like the other services here. Build the Linux binary with `make build-tangofake-linux`, then bring it up with [`server/docker-compose.yml`](server/docker-compose.yml), pointing `SQ_TANGOFAKE_REPO_DIR` at a git repository that holds the `TARGET` files. The server listens on port 8080 in the container.

Outside a container, `bazel run //service/tangofake/server -- -repo <path>` serves the same repository on a free port, which it logs.

## Flags

- `-repo` is the path to the git repository holding the `TARGET` files. It is required, and the repository must contain every commit a change URI names.
- `-address` is the gRPC listen address and defaults to `127.0.0.1:0`.
