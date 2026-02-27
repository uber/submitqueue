# Build Manager

The BuildManager extension provides a clean abstraction for integrating with CI/CD systems like BuildKite, Jenkins, and others. It allows the Orchestrator service to schedule builds, poll their status, and cancel running builds without being coupled to any specific CI provider.

## Interface

### BuildManager

Schedules builds, polls their status, and cancels running builds.

```go
type BuildManager interface {
    Schedule(ctx context.Context, queueName string, changes []entity.BuildChange) (string, error)
    Poll(ctx context.Context, buildID string) (entity.BuildStatus, entity.BuildMetadata, error)
    CancelBuild(ctx context.Context, buildID string) error
    Close() error
}
```

- **Schedule**: Submits changes to the CI provider for processing. Returns a unique build ID.
- **Poll**: Retrieves the current status and metadata of a build.
- **CancelBuild**: Requests cancellation of a build (asynchronous, does not wait for completion).
- **Close**: Gracefully shuts down the build manager and releases resources.

### Entities

```go
type BuildChange struct {
    Change entity.Change   // List of URIs with provider encoded in schema
    Action ChangeAction    // "apply" or "validate"
}

type BuildMetadata map[string]string  // Implementation-defined metadata (e.g., build URL, duration)
```

### Errors

- **ErrBuildNotFound**: Returned by Poll and CancelBuild when the build doesn't exist.
- **ErrInvalidRequest**: Returned by Schedule when validation fails.

## Usage

```go
mgr := buildkite.NewBuildManager(config)
defer mgr.Close()

// Schedule a build
changes := []entity.BuildChange{
    {Change: entity.Change{URIs: []string{"github://uber/repo/pull/123/abc"}}, Action: entity.ChangeActionValidate},
}
buildID, err := mgr.Schedule(ctx, "my-queue", changes)

// Poll for status
status, metadata, err := mgr.Poll(ctx, buildID)
if status.IsTerminal() {
    fmt.Println("Build finished:", status)
}

// Cancel if needed
err = mgr.CancelBuild(ctx, buildID)
```

## Implementing a Backend

1. Create `extension/build/{backend}/` directory
2. Implement the `BuildManager` interface
3. Map `entity.BuildChange` actions to backend-specific job configurations
4. Handle build status mapping to `entity.BuildStatus` values

**Thread-safety**: All implementations must be thread-safe and support concurrent operations.

**Singleton Design**: Implementations are long-lived singletons (one per build provider) initialized at service startup, similar to Storage and other extension components. They should manage connection pooling, caching, and other resources for the lifetime of the service.
