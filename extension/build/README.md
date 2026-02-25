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
        head string,
        base []string,
        jobName string,
    ) (string, error)

    // Poll current status of a build
    Poll(ctx context.Context, id string) (entity.BuildStatus, error)

    // Cancel a running or queued build
    CancelBuild(ctx context.Context, id string) error
}
```

**Thread-safety**: All implementations must be thread-safe and support concurrent operations.

## Types

### ScheduleBuild Parameters

**head** (string, required): BatchID of the batch being tested

**base** ([]string, required): List of BatchIDs (in order) that have been applied on main. Order matters.

**jobName** (string, required): Pipeline/job name to be called on the CI provider
- BuildKite: "organization/pipeline-slug"
- Jenkins: "job-name"

### Build ID Format

Build IDs returned by `ScheduleBuild` should use a URI-like format: `"provider://id"`

**Examples**:
- `"buildkite://uber/submitqueue-ci/123"`
- `"jenkins://456"`
- `"mock://1"`

This format allows the implementation to encode both the provider name and provider-specific build identifier in a single string.

### Build State Enum

```go
type BuildStatus string

const (
    BuildStatusUnknown    BuildStatus = ""          // Sentinel value
    BuildStatusQueued     BuildStatus = "queued"    // Scheduled but not started
    BuildStatusRunning    BuildStatus = "running"   // Currently executing
    BuildStatusPassed     BuildStatus = "passed"    // Completed successfully (terminal)
    BuildStatusFailed     BuildStatus = "failed"    // Completed with failures (terminal)
    BuildStatusCancelled  BuildStatus = "cancelled" // Cancelled before completion (terminal)
)

// IsTerminal returns true for passed/failed/cancelled states
func (s BuildStatus) IsTerminal() bool
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
        ScheduleBuild(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
        Return("mock://1", nil)

    // Test your code that uses the mock
}
```

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

3. **Map provider states to entity.BuildStatus enum**:
   - Map provider's state values to the standard `entity.BuildStatus` constants
   - Use `entity.BuildStatusUnknown` for unexpected states

4. **Handle provider errors**:
   - 404 errors → `build.WrapBuildNotFound()`
   - 5xx errors → `build.WrapProviderUnavailable()`
   - Validation errors → `build.WrapInvalidRequest()`

5. **Define jobName format**:
   - Document the expected format in provider README
   - Examples:
     - BuildKite: `"organization/pipeline-slug"`
     - Jenkins: `"job-name"`

6. **Add tests**:
   - Unit tests with mock HTTP server
   - Validation tests for all required fields
   - Error mapping tests
   - Thread-safety tests

7. **Update BUILD.bazel**:
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
├── errors.go           # Sentinel errors
├── README.md           # This file
├── BUILD.bazel         # Bazel configuration
├── mock/               # Generated gomock mocks
│   ├── build_manager.go  # Generated by mockgen
│   ├── build_manager_test.go
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
