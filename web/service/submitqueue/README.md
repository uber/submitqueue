# SubmitQueue web reference host

This Next.js application demonstrates how to host `@submitqueue/web-submitqueue`. It is intentionally local/demo wiring rather than a production authentication or deployment contract.

The host owns the `/<queue>` landing page and `/<queue>/request/<full-request-id>` detail route, the fixed `demo-queue` selection, HTTP Basic authentication, server-side Connect transport, readiness checks, and environment validation. Request IDs remain readable and are passed to the gateway without interpreting their internal format. `/healthz` validates configuration and performs a bounded gateway ping so Compose readiness reflects the host's only upstream. The browser never connects to the gRPC gateway directly.

`/` lists the configured queues instead of redirecting to the demo queue. Request detail defaults to Summary and uses `?view=history` for shareable History navigation. The host supplies Next-specific refresh completion, dynamic rendering, authentication on every protected route, and CSS; none of those framework decisions live in the reusable library.

GitHub, Phabricator, and git change pages mirror the canonical change URI's authority/path. The host adapter validates those provider-specific paths; React components do not parse URIs. Fake-provider `sq-files`/`sq-fake` query hints remain in the submitted backend values but are excluded from UI labels and navigation URLs.

The default queue and change views use a rolling 24-hour window recalculated on every refresh; there are no `from`/`to` URL parameters. Older queue pages carry the stable gateway window inside a signed, queue-scoped `page` cursor, and their Refresh button returns to the live first page.

Pinned GitHub/Phabricator pages use exact-URI summary lookup. Across-version and git demo pages scan queue receipts in the displayed rolling window, up to ten gateway pages per history page. When more queue requests remain, the host marks the results as page-scoped and offers **Continue history scan** instead of failing or claiming complete history. Signed cursors bind the stable receipt window and gateway continuation to the queue, change, and pinned version. Older pages pause automatic refresh; Refresh and **Latest history** return to a fresh live window. A looping gateway cursor still fails. Git scans also match raw demo URIs carrying file hints without requiring those hints in the browser URL. This bounded adapter is not an unlimited logical-change history API.

Required configuration:

| Variable | Meaning |
|---|---|
| `SUBMITQUEUE_WEB_TOKEN` | Nonempty HTTP Basic password for username `test`; local demo default is `test` |
| `SUBMITQUEUE_GATEWAY_URL` | Gateway URL used by the server-side Connect transport |
| `SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT` | Must be `true` to permit local h2c instead of TLS |

`//web/service/submitqueue:standalone` builds the Next standalone output, `:server` assembles its runtime, `:image` creates the Linux/amd64 OCI image, and `:image_load` loads `submitqueue-web-service:latest` into Docker.

There is no web Dockerfile. Bazel layers the standalone server and pinned Node runtime directly with `js_image_layer` and `oci_image`, so local Compose, CI, and E2E use the same artifact.

```bash
make web-check
make web-image-load
make local-submitqueue-start
```
