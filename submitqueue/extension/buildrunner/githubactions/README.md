# GitHub Actions BuildRunner

`githubactions` implements `buildrunner.BuildRunner` with GitHub Actions
`workflow_dispatch`. It is intended to prove the SubmitQueue BuildRunner
architecture against a common CI system without adding local state.

Its HTTP client and GitHub Actions-specific facts (run status/conclusion
vocabulary, run id encoding) live in
[`platform/extension/buildrunner/githubactions`](../../../../platform/extension/buildrunner/githubactions/README.md),
shared with `stovepipe`'s own GitHub Actions backend.

## How it works

1. `Trigger` dispatches the configured workflow on a trusted ref, usually
   `main`, and returns GitHub's workflow run ID as the SubmitQueue build ID.
2. SubmitQueue passes these workflow inputs:
   - `sq_base_uris`
   - `sq_head_uris`
   - `sq_queue`
   - `sq_metadata`
3. `Status` calls GitHub's get-workflow-run endpoint with that run ID.
4. `Cancel` calls GitHub's cancel-workflow-run endpoint with that run ID.

## Minimal workflow

Create a workflow on the target repository's default branch:

```yaml
name: SubmitQueue CI
run-name: SubmitQueue ${{ inputs.sq_queue }}

on:
  workflow_dispatch:
    inputs:
      sq_base_uris:
        required: true
        type: string
      sq_head_uris:
        required: true
        type: string
      sq_queue:
        required: true
        type: string
      sq_metadata:
        required: false
        type: string

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Inspect SubmitQueue payload
        run: |
          echo '${{ inputs.sq_base_uris }}'
          echo '${{ inputs.sq_head_uris }}'
          echo '${{ inputs.sq_queue }}'
      # Prototype: add a script here that applies sq_base_uris, then
      # sq_head_uris, then runs the repository's real CI command.
```

The workflow definition should live on a trusted ref. The untrusted changes
should be represented by `sq_base_uris` and `sq_head_uris` and applied inside
the job.

## Integrator configuration

A server wiring this backend should provide the GitHub API client and runner
configuration equivalent to:

```sh
BUILD_RUNNER=githubactions
GITHUB_BASE_URL=https://api.github.com
GITHUB_TOKEN=<token with actions:read/actions:write>
GITHUB_ACTIONS_OWNER=uber
GITHUB_ACTIONS_REPO=submitqueue
GITHUB_ACTIONS_WORKFLOW=submitqueue-ci.yml
GITHUB_ACTIONS_REF=main
GITHUB_ACTIONS_EXTRA_INPUTS=runner=ubuntu-latest,test_command=make test
```

`GITHUB_ACTIONS_REF` defaults to `main`. `GITHUB_ACTIONS_EXTRA_INPUTS` is
optional comma-separated `key=value` data copied into every dispatch request;
use it for workflow-specific knobs like runner labels or test commands.

## Practical setup notes

- Use a trusted workflow definition from the target repository's default branch.
- Keep workflow permissions minimal. The job may apply untrusted changes before
  running tests, so avoid broad secrets in this workflow.
- The backend proves the BuildRunner architecture. It does not prescribe how
  your workflow materializes `sq_base_uris` and `sq_head_uris`; wire that to the
  repository's existing patch/PR application logic.
