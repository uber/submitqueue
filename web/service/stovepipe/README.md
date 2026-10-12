# Stovepipe reference host

This Next.js host mounts the Stovepipe UI and supplies a server-side gRPC client, Basic authentication and JSON logging. The backend must support `List` and `GetRequestHistoryByID`, including the current summary in history responses.

## Local startup

Run `make local-stovepipe-web-start` to build and start the backend and web UI. Compose uses its private network for backend calls, waits for `/healthz`, and publishes the web UI on a free port. Open the printed URL and sign in as `test` with password `test`, or the configured `STOVEPIPE_WEB_TOKEN`. Run `make local-stovepipe-stop` to stop the stack.

For a separate backend or development server, configure the environment below and run `./tool/bazel run //web/service/stovepipe:dev`. Next.js prints its free-port URL. Bazel stages a writable development directory; rerun after source edits, or use a Bazel watcher to synchronize changes.

| Variable | Purpose |
| --- | --- |
| `STOVEPIPE_URL` | Backend origin; the local overlay sets `http://stovepipe-service:8080`. |
| `STOVEPIPE_ALLOW_PLAINTEXT` | Set `true` for local HTTP/2 without TLS. |
| `STOVEPIPE_WEB_TOKEN` | Required Basic authentication password; the local overlay defaults to `test`. |
| `STOVEPIPE_WEB_QUEUES` | JSON array of queue objects with unique `name` values matching the backend. The local overlay supplies the demo queues. |
| `LOG_LEVEL` | JSON logging level; defaults to `info`. |

A queue such as `monorepo/main` appears at `/monorepo%2Fmain`; request `7` appears at `/monorepo%2Fmain/request/7`.

## Validation and images

Run `./tool/bazel test //web:check` for library, host, lint and package checks. Run `./tool/bazel test //web/test/e2e/stovepipe:web_test` for browser checks against a real backend. Build the deployment image with `./tool/bazel build //web/service/stovepipe:image`, or load it into Docker with `./tool/bazel run //web/service/stovepipe:image_load`.

To retain the seeded browser-test stack for inspection, add `--test_env=SKIP_CLEANUP=true --nocache_test_results` to the browser-test command. Its log prints the project and web URL. Stop it with `REPO_ROOT="$PWD" ./tool/bazel run @rules_docker_compose//docker_compose -- -f "$PWD/service/stovepipe/docker-compose.yml" -f "$PWD/web/service/stovepipe/docker-compose.yml" -f "$PWD/web/test/e2e/stovepipe/docker-compose.yml" -p <printed-project> down -v`.
