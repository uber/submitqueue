# Contributing to SubmitQueue

Thank you for your interest in contributing to SubmitQueue!

## Getting Started

See [doc/howto/DEVELOPMENT.md](doc/howto/DEVELOPMENT.md) for prerequisites, setup, and troubleshooting.

## Development Workflow

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, following the project conventions.
3. Add or update tests as appropriate.
4. Run `make gazelle` if you added or removed Go files.
5. Run `make test` to ensure unit tests pass.
6. Commit with a clear, descriptive message.
7. Open a pull request against `main`.

## Code Style

See [CLAUDE.md](CLAUDE.md) for code style conventions.

## Testing

See [doc/howto/TESTING.md](doc/howto/TESTING.md) for the full testing guide.

Key commands:

```bash
make test                # Unit tests
make integration-test    # Integration tests (Docker-based, auto-builds binaries)
make e2e-test            # End-to-end tests
```

## Pull Request Guidelines

- Keep PRs focused on a single change.
- Include tests for new functionality.
- Ensure all existing tests pass (`bazel test //...`).
- Follow the existing code style and patterns (see [CLAUDE.md](CLAUDE.md) for detailed conventions).
- Fill out the PR template with a description, motivation, and test plan.

## Code Review

All submissions require review before merging. We use GitHub pull requests for this purpose. A maintainer will review your PR and may request changes.

## Reporting Issues

Use [GitHub Issues](https://github.com/uber/submitqueue/issues) to report bugs or request features. Please use the provided [issue templates](.github/ISSUE_TEMPLATE/) and check existing issues before creating a new one.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## License

By contributing to SubmitQueue, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
