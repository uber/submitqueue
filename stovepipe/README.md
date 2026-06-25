# Stovepipe

Stovepipe is currently a single Ping-only service. Its layout:

- `controller/` — business logic (transport-agnostic). Currently exposes the `Ping` RPC.

The wire contract lives under `api/stovepipe/` (`proto/` for the `.proto` source, `protopb/` for the committed generated stubs). Entities, extensions, and the orchestration pipeline will be added back as the service grows.
