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

## Documentation site

`tool/docsite` holds the MkDocs Material configuration that renders `doc/` as the site published at https://uber.github.io/submitqueue/. The site reads `doc/` in place; `doc/index.md` is its landing page. Links that leave `doc/` (code, package READMEs, `AGENTS.md`) are rewritten to GitHub by `hooks/repo_links.py`, so the strict build still fails on a broken link between pages. The nav is generated from the directory tree; `hooks/nav_titles.py` maps directory names to section titles (`howto` → Guides, `rfc` → Design (RFCs)), and `overrides/home.html` renders the landing-page hero. MkDocs runs under Bazel with a hermetic Python toolchain and locked dependencies: `make docs-serve` previews the site locally, `make docs-build` runs the strict build into `tool/docsite/build`, and `//tool/docsite:site_test` runs that strict build as part of `make test`. To change dependency versions, edit `requirements.txt` and run `bazel run //tool/docsite:requirements.update` to regenerate `requirements_lock.txt`. `.github/workflows/docs.yml` builds the site on pull requests and deploys it from `main`.

## Git sandbox

`tool/gitsandbox` creates the bare repository that `make local-submitqueue-start PROVIDER=git` merges into. It runs before the stack starts, because Runway clones that repository at boot and fails if the target does not already exist. The result is one seed commit on the target branch. Running it again leaves an existing repository unchanged, so a restart keeps commits that earlier runs landed. The Makefile invokes it; `bazel run //tool/gitsandbox -- -sandbox-dir <dir>` is the direct form.

## TLC model checking

`tool/tlc` model-checks the TLA+ specs under `spec/` (see `doc/rfc/tla-plus.md`). The TLA+ tools JAR is pinned in `MODULE.bazel` and runs on the JDK Bazel downloads, so no local Java install is needed. Each spec has a `matrix.json` naming its fixed and varied constants, the invariants and temporal properties to check, and the expected verdict for every combination. `tlc_matrix_test` in `tool/tlc/defs.bzl` turns a matrix into a test that `make test` runs and that fails when a verdict changes. `bazel run //tool/tlc -- spec/submitqueue/landoutcome/matrix.json` prints the table.

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
