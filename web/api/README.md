# TypeScript API

`@submitqueue/api` contains the committed Protobuf-ES bindings consumed by the web library and reference host. The source contracts remain the repository's published `.proto` files under `api/`; generated TypeScript under `src/gen/` must not be edited by hand.

`//web/api:generated` runs Buf and `protoc-gen-es` hermetically. `//web/api:generate` copies that output into `src/gen/`, while `//web/api:generate_test` fails when the committed files drift. `//web/api:lib` compiles JavaScript and declarations, `//web/api:pkg` creates the npm package, and `//web/api:test` verifies the generated API surface.

```bash
make web-proto
./tool/bazel test //web/api:generate_test //web/api:test
./tool/bazel build //web/api:pkg
```

The package exposes domain-specific entry points such as `@submitqueue/api/submitqueue/gateway` rather than a broad root export.
