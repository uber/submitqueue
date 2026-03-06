# Contributing to SubmitQueue

Thank you for your interest in contributing to SubmitQueue! Whether you are reporting a bug, suggesting a feature, improving documentation, or writing code, your contribution is welcome.

## Getting Started

1. Read the [Development Setup](doc/howto/DEVELOPMENT.md) guide for prerequisites, building, and running tests.
2. Review the [Architecture Guide](CLAUDE.md) to understand project layout, conventions, and code style.
3. Check the [Testing Guide](doc/howto/TESTING.md) for testing patterns and requirements.

## Development Workflow

1. Fork the repository and clone your fork.
2. Create a feature branch from `main`:
   ```bash
   git checkout -b yourname/short-description
   ```
   Use branch naming like `yourname/short-description` or `fix/issue-123`.
3. Make your changes, following the project conventions.
4. Add or update tests as appropriate.
5. Run `make gazelle` if you added or removed Go files.
6. Run `make test` to ensure unit tests pass.
7. Commit with a clear, descriptive message.
8. Push to your fork:
   ```bash
   git push origin yourname/short-description
   ```
9. Open a pull request against `main`.

## Pull Request Guidelines

- Keep PRs focused on a single change.
- Reference related issues: use `Closes #123` for fixes or `Part of #123` for incremental work.
- Include tests for new functionality.
- Ensure all existing tests pass (`make test`).
- Ensure CI passes before requesting review.
- Follow the existing code style and patterns described in the [Architecture Guide](CLAUDE.md).
- Fill out the PR template with a description, motivation, and test plan.

## Code Review

All submissions require review before merging. We use GitHub pull requests for this purpose. A maintainer will review your PR and may request changes.

## Reporting Issues

Use [GitHub Issues](https://github.com/uber/submitqueue/issues) to report bugs or request features. Please use the provided [issue templates](.github/ISSUE_TEMPLATE/) and check existing issues before creating a new one.

## Communication

- **Bug reports and feature requests** — [GitHub Issues](https://github.com/uber/submitqueue/issues)
- **Questions and discussions** — open an issue with the `question` label

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## License

By contributing to SubmitQueue, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
