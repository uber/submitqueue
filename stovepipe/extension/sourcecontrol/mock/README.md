# sourcecontrol mocks

Generated gomock mock for the `sourcecontrol.SourceControl` interface, used by controller and pipeline tests.

Mocks are **checked in** and produced by [mockgen](https://github.com/uber-go/mock) from the `//go:generate` directive on `sourcecontrol.go`. After changing the interface, run `make mocks` to regenerate, then `make gazelle` to update `BUILD.bazel`, and commit the result.
