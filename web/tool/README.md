# Web build tooling

This directory contains repository-owned Starlark needed by the web target graph.

`generate_api.bzl` defines the hermetic Protobuf-ES generation action. It stages only the declared `.proto` inputs, creates a Buf generation template that points at the Bazel-provided `protoc-gen-es`, and emits a declared output directory consumed by the drift, compile, and package targets in `web/api/`.

Keep orchestration here only when existing Bazel rules cannot express it. Package compilation, tests, Next builds, OCI images, and browsers use their upstream rules directly.
