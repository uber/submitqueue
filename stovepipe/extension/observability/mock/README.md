# observability mocks

Generated gomock mocks for the `observability.Reporter` and `observability.Factory` interfaces, used by controller and pipeline tests.

Mocks are **checked in** and produced by [mockgen](https://github.com/uber-go/mock) from the `//go:generate` directive on `observability.go`. After changing the interface, run `make mocks` to regenerate, then `make gazelle` to update `BUILD.bazel`, and commit the result.
