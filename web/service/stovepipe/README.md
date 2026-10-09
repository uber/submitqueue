# Stovepipe reference host

This Next.js host mounts the Stovepipe package and supplies a server-side gRPC client, Basic authentication and JSON logging. Its backend must support `List`, `GetProjectStatusByURI` and `GetRequestHistoryByID`.

Start the backend with `make local-stovepipe-start`. Find its published gRPC port with `docker compose -f service/stovepipe/docker-compose.yml -p stovepipe port stovepipe-service 8080`. Set `STOVEPIPE_URL` to that origin, `STOVEPIPE_ALLOW_PLAINTEXT=true` for local HTTP/2, `STOVEPIPE_WEB_TOKEN` to the demo password, and `STOVEPIPE_WEB_QUEUES` to a JSON array of objects with `name` and `projects` fields. Queue names must match the backend configuration. Every queue needs at least one project ID.

Run `./tool/bazel run //web/service/stovepipe:dev` to bind a free port. Open the address printed by Next.js and sign in as `test` with the configured password. A queue such as `monorepo/main` is available at `/monorepo%2Fmain`; its change page is at `/monorepo%2Fmain/change/<percent-escaped-uri>`. Bazel stages the app in a writable development directory; rerun after source edits, or use a Bazel watcher to synchronize changes.

`./tool/bazel test //web:check` validates both domains and the host. `./tool/bazel build //web/service/stovepipe:image` builds the deployable image, and `./tool/bazel run //web/service/stovepipe:image_load` loads it into Docker. `web/test/e2e/stovepipe` contains the browser checks against a real Stovepipe backend.

`./tool/bazel test //web/test/e2e/stovepipe:web_test --test_env=SKIP_CLEANUP=true --nocache_test_results` keeps the seeded browser-test stack running for manual inspection. The test log prints its project name and web URL. Stop it with `REPO_ROOT=. STOVEPIPE_WEB_QUEUES='[]' docker compose -f service/stovepipe/docker-compose.yml -f web/test/e2e/stovepipe/docker-compose.yml -p <printed-project> down -v`.
