# BuildManager Extension

Vendor-agnostic interface for managing builds with external CI/CD providers.

## Overview

The BuildManager extension provides a clean abstraction for integrating with CI/CD systems like BuildKite, Jenkins, and others. It allows the Orchestrator service to schedule builds, poll their status, and cancel running builds without being coupled to any specific CI provider.

## Interfaces

### BuildManager

Main interface for interacting with CI providers.

```go
type BuildManager interface {
    // Schedule submits a list of changes to the CI provider for processing
    Schedule(
        ctx context.Context,
        queueName string,
        changes []entity.BuildChange,
    ) (string, error)

    // Poll retrieves the current status of a build from the CI provider
    Poll(ctx context.Context, buildID string) (entity.BuildStatus, entity.BuildMetadata, error)

    // CancelBuild requests cancellation of a build (asynchronous operation)
    CancelBuild(ctx context.Context, buildID string) error

    // Close gracefully shuts down the build manager
    Close() error
}
```

**Thread-safety**: All implementations must be thread-safe and support concurrent operations.

**Implementation Design**: Implementations may be designed as heavy singletons (with connection pooling and caching) or lightweight instances created on-demand, depending on the CI provider's requirements and implementation strategy.

## Types

### BuildChange

Represents a code change to be processed by the build system.

```go
type BuildChange struct {
    // ChangeID is the unique identifier for this change (diff ID, PR number, etc.)
    ChangeID string
    // Action specifies what operation to perform on this change
    Action ChangeAction
}
```

### ChangeAction

Defines the action to perform on a change.

```go
type ChangeAction string

const (
    ChangeActionUnknown   ChangeAction = ""         // Sentinel value
    ChangeActionApply     ChangeAction = "apply"    // Apply the change to the target branch
    ChangeActionValidate  ChangeAction = "validate" // Run validation/testing without applying
)
```

### Schedule Parameters

**queueName** (string, required): Name of the queue processing these changes. Used to look up job configuration from queue config.

**changes** ([]entity.BuildChange, required): List of changes to process. Each change includes:
- **ChangeID**: Unique identifier (e.g., "D12345" for Phabricator, "42" for GitHub PR)
- **Action**: What to do with the change (validate or apply)

Order of changes may be significant for dependencies.

### Build ID Format

Build IDs returned by `Schedule` should use a URI-like format: `"provider://id"`

**Examples**:
- `"buildkite://uber/submitqueue-ci/123"`
- `"jenkins://456"`
- `"mock://1"`

This format allows the implementation to encode both the provider name and provider-specific build identifier in a single string.

### Build Status Enum

```go
type BuildStatus string

const (
    BuildStatusUnknown    BuildStatus = ""           // Sentinel value
    BuildStatusAccepted   BuildStatus = "accepted"   // Accepted by CI provider
    BuildStatusSucceeded  BuildStatus = "succeeded"  // Completed successfully (terminal)
    BuildStatusFailed     BuildStatus = "failed"     // Completed with failures (terminal)
    BuildStatusCancelled  BuildStatus = "cancelled"  // Cancelled before completion (terminal)
)

// IsTerminal returns true for succeeded/failed/cancelled states
func (s BuildStatus) IsTerminal() bool
```

**Build Lifecycle**: `accepted` → `succeeded`/`failed`/`cancelled`

### Build Metadata

The `Poll` method returns `entity.BuildMetadata` containing additional metadata about the build. The specific keys and values are implementation-defined, but common examples include:

**Common metadata keys:**
- `build_url` - Direct link to the build in the CI provider's UI
- `commit_sha` - Git commit SHA being tested
- `duration_ms` - Build duration in milliseconds
- `started_at` - Build start timestamp
- `finished_at` - Build completion timestamp
- `error_message` - Error details for failed builds

Implementations may include additional provider-specific metadata. Consumers should handle missing keys gracefully.

## Error Handling

The extension defines sentinel errors following the SubmitQueue pattern:

- **`ErrBuildNotFound`** - Build doesn't exist or was deleted
- **`ErrBuildNotCancellable`** - Build cannot be cancelled (implementation-specific)
- **`ErrInvalidRequest`** - Request validation failed

Each error has helper functions:
- `Is{Error}(err)` - Check if error is of specific type
- `Wrap{Error}(err)` - Wrap provider-specific errors

Example:
```go
status, metadata, err := buildMgr.Poll(ctx, buildID)
if build.IsBuildNotFound(err) {
    // Handle missing build
}
```

## Usage

### Basic Workflow

```go
// 1. Schedule a build with changes
changes := []entity.BuildChange{
    {ChangeID: "D12345", Action: entity.ChangeActionApply},
    {ChangeID: "D12346", Action: entity.ChangeActionValidate},
}

buildID, err := buildMgr.Schedule(ctx, "my-queue", changes)
if err != nil {
    // Handle error
}

// 2. Poll for build status
status, metadata, err := buildMgr.Poll(ctx, buildID)
if err != nil {
    // Handle error
}

// Access build metadata
buildURL := metadata["build_url"]
commitSHA := metadata["commit_sha"]

// 3. Check if build is done
if status.IsTerminal() {
    // Build finished: status is succeeded, failed, or cancelled
}
```

### GoMock Mocks

For unit testing with gomock, use the generated mock:

```go
import (
    "testing"

    "github.com/uber/submitqueue/entity"
    "github.com/uber/submitqueue/extension/build/mock"
    "go.uber.org/mock/gomock"
)

func TestMyController(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    mockBuildMgr := mock.NewMockBuildManager(ctrl)

    // Set up expectations
    changes := []entity.BuildChange{
        {ChangeID: "D12345", Action: entity.ChangeActionValidate},
    }

    mockBuildMgr.EXPECT().
        Schedule(gomock.Any(), "test-queue", changes).
        Return("mock://1", nil)

    // Test your code that uses the mock
}
```

### Cancelling Builds

Cancellation is **asynchronous** - the method initiates the cancellation request and returns immediately. Use `Poll` to check if the build has transitioned to `BuildStatusCancelled`.

```go
err := mgr.CancelBuild(ctx, buildID)
if build.IsBuildNotCancellable(err) {
    // Build cannot be cancelled (implementation-specific)
} else if err != nil {
    // Other error
}

// Poll to verify cancellation
status, _ := mgr.Poll(ctx, buildID)
if status == entity.BuildStatusCancelled {
    // Cancellation successful
}
```

**Note**: The implementation decides how to handle cancellation requests for builds in terminal states (succeeded, failed, cancelled). It may return an error, silently ignore the request, or handle it in a provider-specific way.

## Implementing a New Provider

To add support for a new CI provider:

1. **Create provider directory**: `extension/build/{provider}/`

2. **Implement BuildManager interface**:
   ```go
   package jenkins

   import "github.com/uber/submitqueue/extension/build"

   type Params struct {
       // Provider-specific configuration
       BaseURL      string
       Username     string
       APIToken     string
       QueueConfig  QueueConfigStore // For looking up job names
       Logger       *zap.Logger
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
   - Include mapping for `BuildStatusAccepted` if provider supports it
   - Use `entity.BuildStatusUnknown` for unexpected states

4. **Handle provider errors**:
   - 404 errors → `build.WrapBuildNotFound()`
   - Validation errors → `build.WrapInvalidRequest()`

5. **Implement Schedule method**:
   - Look up job name from queue config using `queueName` parameter
   - Handle both `ChangeActionApply` and `ChangeActionValidate` actions
   - Create appropriate builds/jobs for each change
   - Return unique build ID

6. **Implement CancelBuild**:
   - Make asynchronous cancellation request to CI provider
   - Return immediately after initiating request
   - Decide how to handle terminal state cancellations

7. **Choose implementation design**:
   - **Singleton**: If provider needs connection pooling, auth token caching, rate limiting
   - **Lightweight**: If provider SDK handles resource management or implementation is stateless

8. **Add tests**:
   - Unit tests with mock HTTP server
   - Validation tests for all required fields
   - Error mapping tests
   - Thread-safety tests
   - Test both validate and apply actions

9. **Update BUILD.bazel**:
   ```bazel
   go_library(
       name = "jenkins",
       srcs = ["jenkins.go"],
       importpath = "github.com/uber/submitqueue/extension/build/jenkins",
       visibility = ["//visibility:public"],
       deps = [
           "//entity",
           "//extension/build",
           "//extension/queueconfig",
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
2. **Change-focused**: Operates on individual changes (diff IDs, PR numbers) with explicit actions
3. **Thread-safe**: All implementations support concurrent operations
4. **Error transparency**: Sentinel errors for common failure modes
5. **Implementation flexibility**: Supports both heavy singleton and lightweight on-demand designs
6. **Asynchronous cancellation**: CancelBuild initiates request and returns immediately

## Future Enhancements

Potential improvements not in the current implementation:

- **Webhook support**: Accept push notifications from CI providers instead of polling
- **Build artifacts**: Track and retrieve build artifacts
- **Retry logic**: Automatic retry for transient failures
- **Batch operations**: Poll/cancel multiple builds at once
- **Streaming logs**: Real-time log streaming from builds
- **Build caching**: Cache build status to reduce API calls
- **Partial cancellation**: Cancel individual changes in a multi-change build
