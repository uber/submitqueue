# Base Revision Extension

Vendor-agnostic source of the commit a queue's new batches are analyzed against.

## Interfaces

### Resolver

Returns the full commit SHA of the tip of a queue's target branch, as of the call. A queue without a target is an error, and so is any failure to read the tip. A caller must not fall back to an earlier value, because the revision names the code a batch is measured against. Implementations must be safe for concurrent use.

## Implementations

- `git` fetches the target branch into a clone through any `gitworkspace.Factory`, so where git runs is the deployment's choice. Calls that share a repository are serialized, because concurrent fetches into one clone can fail on a ref lock.
- `github` reads the branch from the GitHub REST API, so it needs no clone. It takes an HTTP client that already carries the API root and credentials, as the GitHub change provider does.

A deployer can supply its own implementation, for example one backed by an internal source control service.

The Tango conflict analyzer is the only consumer today. The orchestrator wiring picks the implementation from the queue's change provider.
