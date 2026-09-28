# Tools Directory

This directory contains tooling scripts and configurations for the submitqueue repository.

## Bazel Wrapper

The `tool/bazel` script is a Python-based Bazelisk implementation that:
- Reads `.bazelversion` from the repository root
- Automatically downloads and caches the correct Bazel version
- Delegates all commands to that version

### Usage

```bash
# Use the wrapper directly
./tool/bazel build //...

# Or add tool/ to your PATH (via .envrc with direnv)
bazel build //...
```

### Version Management

The Bazel version is controlled by `.bazelversion` at the repository root. Update that file to change the Bazel version used by the wrapper.

## Git sandbox

`tool/gitsandbox` creates the bare repository that `make local-submitqueue-start PROVIDER=git` merges into. It runs before the stack starts, because Runway clones that repository at boot and fails if the target does not already exist. The result is one seed commit on the target branch. Running it again leaves an existing repository unchanged, so a restart keeps commits that earlier runs landed. The Makefile invokes it; `bazel run //tool/gitsandbox -- -sandbox-dir <dir>` is the direct form.

## Proto generation

`tool/proto` is the Bazel codegen for the committed protobuf Go stubs. `make proto` builds `//tool/proto:generated` and copies each package's output into its `protopb/` directory. The package list and per-source outputs live in `tool/proto/BUILD.bazel`. Change the `.proto` sources and regenerate; do not edit the generated files. `make clean-proto` removes those stubs, and `make proto` writes them again.

## Linters

`tool/linter` holds the checkers behind `make lint`. Each one is a small program with its own Makefile target:

- `licenseheader` checks Apache license headers (`make lint-license`); `make license-fix` writes missing ones.
- `binaryfile` fails when a binary file is tracked (`make lint-binary`).
- `messageid` fails when a queue message is constructed outside `platform/publish` and the queue backends (`make lint-message-id`).
- `queueshard` fails when a schema's primary key does not lead with its shard column, or a secondary index reaches across shards (`make lint-queue-shard`).

## Adding New Tools

When adding new tools to this directory:

1. Create the script in `tool/`
2. Make it executable: `chmod +x tool/<script-name>`
3. Add it to `tool/BUILD.bazel` if it needs to be referenced by Bazel rules
4. Document it in this README

## Environment Setup

This directory is added to PATH via `.envrc` (for direnv users), allowing you to run `bazel` commands without prefixing with `./tool/`.
