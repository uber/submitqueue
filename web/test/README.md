# Web tests

- [`e2e/`](e2e/): the real stack (Bazel-built Go services, web image, MySQL) in Docker, driven by Playwright with axe checks. `make web-e2e-test`; set `SKIP_CLEANUP=true` to keep the stack for debugging.
- [`package/`](package/): checks the built packages as a consumer sees them: tarball contents and import boundaries, a typecheck and a plain-Node render through `createSubmitQueueWeb`, and React 18 render and hydration in `react18/`.

Unit tests live next to the code they cover.
