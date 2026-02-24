# BuildManager Extension

Vendor-agnostic interface for managing builds with external CI/CD providers.

## Overview

The BuildManager extension provides a clean abstraction for integrating with CI/CD systems like BuildKite, Jenkins, and others. It allows the Orchestrator service to schedule builds, poll their status, and cancel running builds without being coupled to any specific CI provider.

## Interfaces

### BuildManager

Main interface for interacting with CI providers.

```go
type BuildManager interface {
    // Schedule a new build with the CI provider for testing a batch
    ScheduleBuild(
        ctx context.Context,
        baseSHA string,
        speculatedBatchesToBeApplied []entity.BatchDependent,
        batchToBeTest entity.Batch,
        repoURL string,
        branch string,
        pipelineID string,
        sqid string,
        env map[string]string,
        message string,
    ) (BuildID, error)

    // Poll current status of a build
    Poll(ctx context.Context, id BuildID) (BuildStatus, error)

    // Cancel a running or queued build
    CancelBuild(ctx context.Context, id BuildID) error
}
```

**Thread-safety**: All implementations must be thread-safe and support concurrent operations.

## Types

### ScheduleBuild Parameters

**baseSHA** (string, required): SHA of the main/base branch - the starting point for applying batches

**speculatedBatchesToBeApplied** ([]entity.BatchDependent, optional): Batches that were applied on main before the test batch.

**batchToBeTest** (entity.Batch, required): The batch being tested.

**repoURL** (string, required): Repository URL (e.g., "https://github.com/uber/submitqueue")

**branch** (string, required): Target branch name (e.g., "main")

**pipelineID** (string, required): Provider-specific pipeline identifier
- BuildKite: "organization/pipeline-slug"
- Jenkins: "job-name"
- Mock: any string

**sqid** (string, required): SubmitQueue request ID for correlation and tracing

**env** (map[string]string, optional): Additional environment variables to pass to CI

**message** (string, optional): Human-readable build description

### BuildID

Unique identifier for a build using a URI-like format:

```go
type BuildID string

// Constructor to create a BuildID from provider and ID components
func NewBuildID(provider, id string) BuildID

// Parser to extract provider and ID from a BuildID
func ParseBuildID(buildID BuildID) (provider string, id string, err error)

// String returns the BuildID as a string ("provider://id" format)
func (b BuildID) String() string
```

**Format**: `"provider://id"`

**Examples**:
- `"buildkite://uber/submitqueue-ci/123"`
- `"jenkins://456"`
- `"mock://1"`

**Usage**:
```go
// Create a BuildID
buildID := entitybuild.NewBuildID("buildkite", "uber/submitqueue-ci/123")
fmt.Println(buildID.String()) // "buildkite://uber/submitqueue-ci/123"

// Parse a BuildID
provider, id, err := entitybuild.ParseBuildID(buildID)
// provider = "buildkite"
// id = "uber/submitqueue-ci/123"
```

### BuildStatus

Output from polling a build:

```go
type BuildStatus struct {
    ID           BuildID
    State        BuildState        // Current execution state
    QueuedAt     int64            // Unix milliseconds
    StartedAt    int64            // Unix milliseconds
    FinishedAt   int64            // Unix milliseconds
    WebURL       string           // Link to build UI
    LogsURL      string           // Link to logs
    ErrorMessage string           // Error details for failures
    Metadata     map[string]string // Provider-specific metadata (never nil)
}
```

**Metadata guarantee**: The `Metadata` field is never nil. Implementations must always initialize it to at least an empty map. Consumers can safely iterate over it without nil checks.

### BuildState

Enum representing build execution state:

```go
type BuildState string

const (
    BuildStateUnknown    BuildState = ""          // Sentinel value
    BuildStateQueued     BuildState = "queued"    // Scheduled but not started
    BuildStateRunning    BuildState = "running"   // Currently executing
    BuildStatePassed     BuildState = "passed"    // Completed successfully (terminal)
    BuildStateFailed     BuildState = "failed"    // Completed with failures (terminal)
    BuildStateCancelled  BuildState = "cancelled" // Cancelled before completion (terminal)
    BuildStateBlocked    BuildState = "blocked"   // Waiting for manual approval
)

// IsTerminal returns true for passed/failed/cancelled states
func (s BuildState) IsTerminal() bool
```

## Error Handling

The extension defines sentinel errors following the SubmitQueue pattern:

- **`ErrBuildNotFound`** - Build doesn't exist or was deleted
- **`ErrBuildNotCancellable`** - Build has already finished
- **`ErrProviderUnavailable`** - CI provider unreachable or experiencing errors
- **`ErrInvalidRequest`** - Request validation failed

Each error has helper functions:
- `Is{Error}(err)` - Check if error is of specific type
- `Wrap{Error}(err)` - Wrap provider-specific errors

Example:
```go
status, err := buildMgr.Poll(ctx, buildID)
if build.IsBuildNotFound(err) {
    // Handle missing build
}
```

## Usage

### In-Memory Implementation

For testing without external dependencies, use the in-memory implementation:

```go
import (
    "github.com/uber/submitqueue/extension/build/inmemory"
    "github.com/uber/submitqueue/entity"
)

// Create in-memory build manager
mgr := inmemory.NewInMemoryBuildManager(inmemory.Params{
    BuildDelay: 100 * time.Millisecond, // How long simulated builds take
})

// Create test batch
testBatch := entity.Batch{
    ID:      "queue-1/batch/5",
    Queue:   "queue-1",
    Contains: []string{"queue-1/10", "queue-1/11"},
    State:   entity.BatchStateUnknown,
    Version: 1,
}

// Create base batches (already applied on main)
baseBatches := []entity.BatchDependent{
    {BatchID: "queue-1/batch/1"},
    {BatchID: "queue-1/batch/2"},
}

// Schedule build
buildID, err := mgr.ScheduleBuild(
    ctx,
    "abc123def456",  // baseSHA (main branch HEAD)
    baseBatches,     // base batches
    testBatch,       // batch to test
    "https://github.com/uber/submitqueue",
    "main",
    "test-pipeline",
    "queue-1/10",
    map[string]string{"CUSTOM_VAR": "value"},
    "Testing batch queue-1/batch/5",
)
if err != nil {
    // Handle error
}

// Poll until complete
for {
    status, err := mgr.Poll(ctx, buildID)
    if err != nil {
        // Handle error
    }

    if status.State.IsTerminal() {
        fmt.Printf("Build finished: %s\n", status.State)
        fmt.Printf("Build metadata: %+v\n", status.Metadata)
        break
    }

    time.Sleep(1 * time.Second)
}
```

**Deterministic Testing**: The mock supports predictable behavior:
- Batch ID containing `"fail"` → `BuildStateFailed`
- Batch ID containing `"block"` → `BuildStateBlocked`
- All other batches → `BuildStatePassed`

**State Change Callbacks**: For deterministic testing without `time.Sleep`, use callbacks:

```go
// Create channel to track state changes
stateChanges := make(chan build.BuildState, 10)

mgr := inmemory.NewInMemoryBuildManager(inmemory.Params{
    BuildDelay: 100 * time.Millisecond,
    OnStateChange: func(buildID string, state build.BuildState) {
        stateChanges <- state
    },
})

testBatch := entity.Batch{
    ID: "queue-1/batch/1",
    Queue: "queue-1",
    Contains: []string{"queue-1/1"},
}

buildID, _ := mgr.ScheduleBuild(ctx, "abc123", []entity.BatchDependent{}, testBatch,
    "https://github.com/uber/submitqueue", "main", "test", "queue-1/1", nil, "")

// Wait for running state
state := <-stateChanges  // Will receive BuildStateRunning

// Wait for terminal state
state = <-stateChanges  // Will receive BuildStatePassed/Failed/Cancelled
```

**In-Memory Implementation Details**:
- PipelineID format: Any string (not validated)
- BuildID format: Sequential numbers ("1", "2", "3", etc.)
- BuildID.String() format: `"mock://1"`, `"mock://2"`, etc.

### GoMock Mocks

For unit testing with gomock, use the generated mock:

```go
import (
    "testing"

    "github.com/uber/submitqueue/extension/build/mock"
    "go.uber.org/mock/gomock"
)

func TestMyController(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    mockBuildMgr := mock.NewMockBuildManager(ctrl)

    // Set up expectations
    mockBuildMgr.EXPECT().
        ScheduleBuild(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
                     gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
                     gomock.Any(), gomock.Any()).
        Return(entitybuild.NewBuildID("mock", "1"), nil)

    // Test your code that uses the mock
}
```

### Batch Metadata

BuildManager uses batch information to enrich CI builds with metadata:

**Environment Variables**: Automatically added to the build environment:
- `SQ_BASE_SHA`: SHA of the main branch
- `SQ_BASE_BATCHES`: Comma-separated list of base batch IDs (e.g., "queue-1/batch/1,queue-1/batch/2")
- `SQ_TEST_BATCH`: ID of the batch being tested

**Build Message**: Automatically generated description like:
"Testing batch queue-1/batch/5 on top of queue-1/batch/1, queue-1/batch/2"

**Metadata Map**: BuildStatus.Metadata includes:
- `base_sha`: Same as baseSHA parameter
- `base_batches`: Same as SQ_BASE_BATCHES
- `test_batch`: ID of the batch being tested
- `repo`: Repository URL
- `branch`: Target branch
- `sqid`: SubmitQueue request ID

### Cancelling Builds

```go
err := mgr.CancelBuild(ctx, buildID)
if build.IsBuildNotCancellable(err) {
    // Build already finished
} else if err != nil {
    // Other error
}
```

## Implementing a New Provider

To add support for a new CI provider:

1. **Create provider directory**: `extension/build/{provider}/`

2. **Implement BuildManager interface**:
   ```go
   package jenkins

   import "github.com/uber/submitqueue/extension/build"

   type Params struct {
       // Provider-specific configuration
       BaseURL    string
       Username   string
       APIToken   string
       Logger     *zap.Logger
       MetricsScope tally.Scope
   }

   func NewBuildManager(params Params) (build.BuildManager, error) {
       // Validate params
       // Create HTTP client
       // Return implementation
   }
   ```

3. **Map provider states to BuildState enum**:
   - Map provider's state values to the standard `BuildState` constants
   - Use `BuildStateUnknown` for unexpected states

4. **Initialize BuildStatus with non-nil Metadata**:
   - Always initialize `Metadata` field to at least an empty map: `map[string]string{}`
   - Never return a BuildStatus with nil Metadata - this is a documented guarantee
   - Populate with provider-specific information (commit SHA, author, test counts, etc.)

5. **Handle provider errors**:
   - 404 errors → `build.WrapBuildNotFound()`
   - 5xx errors → `build.WrapProviderUnavailable()`
   - Validation errors → `build.WrapInvalidRequest()`

6. **Define PipelineID format**:
   - Document the expected format in provider README
   - Examples:
     - BuildKite: `"organization/pipeline-slug"`
     - Jenkins: `"job-name"`
     - Mock: `"any-string"` (not validated)

7. **Add tests**:
   - Unit tests with mock HTTP server
   - Validation tests for all required fields
   - Error mapping tests
   - Thread-safety tests

8. **Update BUILD.bazel**:
   ```bazel
   go_library(
       name = "jenkins",
       srcs = ["jenkins.go"],
       importpath = "github.com/uber/submitqueue/extension/build/jenkins",
       visibility = ["//visibility:public"],
       deps = [
           "//extension/build",
           "@org_uber_go_zap//:zap",
           "@com_github_uber_go_tally_v4//:tally",
       ],
   )
   ```

## Architecture

### Extension Pattern

Following the established SubmitQueue extension pattern:

```
extension/build/
├── build_manager.go    # Interface definition
├── types.go            # Request/Status/ID types
├── errors.go           # Sentinel errors
├── README.md           # This file
├── BUILD.bazel         # Bazel configuration
├── mock/               # Generated gomock mocks
│   ├── build_manager.go  # Generated by mockgen
│   └── BUILD.bazel
├── inmemory/           # In-memory implementation for testing
│   ├── in_memory_build_manager.go
│   ├── in_memory_build_manager_test.go
│   └── BUILD.bazel
└── {provider}/         # Provider implementations (future)
    ├── {provider}.go
    ├── {provider}_test.go
    └── BUILD.bazel
```

### Design Principles

1. **Vendor-agnostic**: Interface doesn't leak provider-specific details
2. **Immutable types**: BuildRequest and BuildStatus are value types
3. **Thread-safe**: All implementations support concurrent operations
4. **Error transparency**: Sentinel errors for common failure modes
5. **No persistent state**: BuildManager doesn't manage connections or require cleanup

## Future Enhancements

Potential improvements not in the current implementation:

- **Webhook support**: Accept push notifications from CI providers instead of polling
- **Build artifacts**: Track and retrieve build artifacts
- **Retry logic**: Automatic retry for transient failures
- **Batch operations**: Schedule/poll/cancel multiple builds at once
- **Streaming logs**: Real-time log streaming from builds
- **Build caching**: Cache build status to reduce API calls
