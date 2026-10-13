# SubmitQueue web reference host

A local/demo Next.js host for the SubmitQueue module, and the composition root that chooses every implementation (like a Go service's `server/main.go`). Not a production authentication or deployment contract.

| Path | Holds |
|---|---|
| `src/server/` | Wiring, free of Next.js: environment config, demo Basic auth, the Connect gateway client and transport, pino, opt-in Prometheus, and `resolveReferenceWebHost` |
| `src/next/` | Everything Next.js-specific: catch-all page and metadata, the per-request auth check, navigation provider (`next/link` without prefetch, transition-aware refresh), readiness route |
| `src/app/` | Routes: one optional catch-all page, `/healthz`, layouts, not-found and unauthorized pages |
| `src/proxy.ts` | Challenges anonymous browsers with HTTP Basic auth |

Two Next.js specifics: `next.config.ts` sets `htmlLimitedBots: /.*/` so page titles resolve before streaming, and Next leaves pino unbundled, so the image's runtime layer declares it.

| Variable | Meaning |
|---|---|
| `SUBMITQUEUE_GATEWAY_URL` | Gateway URL (TLS unless plaintext is allowed) |
| `SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT` | `true` to allow local h2c |
| `SUBMITQUEUE_WEB_TOKEN` | Basic-auth password for user `test` (demo default `test`) |
| `SUBMITQUEUE_WEB_CURSOR_SECRET` | Key that signs pagination cursors (demo default `local-demo-cursor-secret`) |
| `SUBMITQUEUE_WEB_METRICS_PORT` | Optional Prometheus port |
| `LOG_LEVEL` | pino level, default `info` |

The OCI image is built by Bazel without a Dockerfile (`:image`, loaded as `submitqueue-web-service:latest` by `make web-image-load`), so local Compose, CI, and E2E run the same artifact.

```bash
./tool/bazel test //web/service/submitqueue:all
make local-submitqueue-start
```
