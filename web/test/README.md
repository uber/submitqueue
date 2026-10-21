# Web tests

- [`e2e/`](e2e/): the real stack (Bazel-built Go services, web image, MySQL) in Docker, driven by Playwright with axe checks. `make web-e2e-test`; set `SKIP_CLEANUP=true` to keep the stack for debugging.
- [`package/`](package/): checks the built packages as a consumer sees them: tarball contents, portable BaseUI imports, import boundaries, consumer typechecks, and the server entries running directly in Node. UI rendering runs through Vite, including React 18 SSR and hydration with light, dark, and custom host themes in `react18/`.

Unit tests live next to the code they cover.
