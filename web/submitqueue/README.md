# SubmitQueue web library

`@submitqueue/web-submitqueue` owns serializable models, gateway loaders, safe timestamps, status/error presentation, route helpers, refresh controls, and deterministic gateway fakes.

The root entry point is client-safe; `./server` contains server-only gateway mapping, and `./testing` contains test fixtures. React, Connect, and Protobuf-ES are peer contracts. The package has no Next.js dependency.

Hosts own routes, credentials, queue selection, transport creation, deadlines, dynamic rendering, and deployment. `AutoRefresh` takes a host callback whose promise settles after refreshing finishes.

Bazel owns compilation, unit/type tests, package tarballs, and isolated package-consumer checks. Page components are added in the following stack changes.
