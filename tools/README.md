# Tools Directory

This directory contains tooling scripts and configurations for the submitqueue repository.

## Bazel Wrapper

The `tools/bazel` script is a Python-based Bazelisk implementation that:
- Reads `.bazelversion` from the repository root
- Automatically downloads and caches the correct Bazel version
- Delegates all commands to that version

### Usage

```bash
# Use the wrapper directly
./tools/bazel build //...

# Or add tools/ to your PATH (via .envrc with direnv)
bazel build //...
```

### Version Management

The Bazel version is controlled by `.bazelversion` at the repository root. Update that file to change the Bazel version used by the wrapper.

## Adding New Tools

When adding new tools to this directory:

1. Create the script in `tools/`
2. Make it executable: `chmod +x tools/<script-name>`
3. Add it to `tools/BUILD.bazel` if it needs to be referenced by Bazel rules
4. Document it in this README

## Environment Setup

This directory is added to PATH via `.envrc` (for direnv users), allowing you to run `bazel` commands without prefixing with `./tools/`.
