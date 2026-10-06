# SubmitQueue web reference host

The Next.js demo host owns HTTP Basic authentication, gateway transport, dynamic rendering, readiness, refresh integration, and deployment. Page routes are added in the following stack changes.

Set `SUBMITQUEUE_WEB_TOKEN` to a nonempty password for username `test`, configure `SUBMITQUEUE_GATEWAY_URL`, and explicitly enable `SUBMITQUEUE_GATEWAY_ALLOW_PLAINTEXT=true` only for local h2c. The local demo uses password `test`.

`/healthz` validates configuration and pings the gateway. Bazel targets `:standalone`, `:server`, `:image`, and `:image_load` build and package the host; there is no web Dockerfile.
