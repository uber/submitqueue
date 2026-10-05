# Package consumer checks

This package validates the artifacts that downstream consumers receive rather than importing workspace source directly.

`//web/package_test:pack_contents_test` checks the Bazel-created npm tarballs for the generated gateway API and the library's root, `./server`, and `./testing` declaration files. `//web/package_test:consumer_typecheck_typecheck_test` links the packaged outputs into an isolated TypeScript consumer and verifies all public entry points.

These checks complement the Next reference-host build: the package tests enforce publication shape, while the host proves framework integration.
