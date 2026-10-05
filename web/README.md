# SubmitQueue web workspace

Bazel is the canonical build and test interface for the web workspace. It pins Node 22, translates the pnpm lockfile into Bazel npm repositories, generates and drift-checks the protobuf TypeScript API, compiles and packages both public packages, builds the Next.js reference host and OCI image, and runs Playwright with a Bazel-managed Chromium.

## Modules

| Path | Purpose |
|---|---|
| [`api/`](api/) | Generated TypeScript bindings for the published SubmitQueue protobuf API |
| [`submitqueue/`](submitqueue/) | Reusable request models, gateway loaders, components, paths, polling, and test fakes |
| [`service/submitqueue/`](service/submitqueue/) | Local/demo Next.js reference host, authentication, gateway transport, and Bazel OCI image |
| [`test/e2e/`](test/e2e/) | Real-stack Playwright and axe coverage using Bazel-built services and Chromium |
| [`package_test/`](package_test/) | Package-tarball contents and external-consumer export checks |
| [`tool/`](tool/) | Starlark rules used by the web build, currently protobuf generation |

```bash
make web-build
make web-check
make web-e2e-test  # requires Docker
make web-proto
```

The deployable image is `//web/service/submitqueue:image`; `make web-image-load` loads it into Docker as `submitqueue-web-service:latest`. `make local-submitqueue-start` performs that load before Compose starts the local stack.

The web image has no Dockerfile. `js_image_layer` collects the standalone Next server and its Bazel-managed Node runtime, and `oci_image` places those layers over the pinned Node base image. Keeping the image definition in Bazel avoids a second build path that would reinstall dependencies and rebuild Next outside the validated target graph. Local development still uses the Go services' Dockerfiles, while the web E2E target packages their static Linux binaries into test-only scratch images so its required stack has no live Dockerfile or APT inputs.

The real-stack browser test lives at `web/test/e2e/`. It starts the Bazel-built Go services and web image, applies the real schemas, submits requests through the gateway, and checks HTTP Basic authentication, queue ordering, encoded SQID navigation, lifecycle history, and axe accessibility.

The pnpm workspace remains for optional editor integration and direct local iteration. Run `make web-install` only when a local editor or pnpm command needs physical `node_modules`; Bazel and CI do not require it.
