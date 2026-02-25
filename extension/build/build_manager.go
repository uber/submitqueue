package build

//go:generate mockgen -source=build_manager.go -destination=mock/build_manager.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// BuildManager is a vendor-agnostic interface for managing builds with external CI/CD providers.
// Implementations provide integration with specific CI systems (BuildKite, Jenkins, etc.)
// to schedule builds, poll their status, and cancel running builds.
//
// Implementations may be designed as heavy singletons (with connection pooling and caching)
// or lightweight instances created on-demand, depending on the CI provider's requirements
// and implementation strategy.
//
// All implementations must be thread-safe and support concurrent operations.
type BuildManager interface {
	// Schedule submits a list of changes to the CI provider for processing.
	// Each change specifies an action (validate or apply) to perform.
	//
	// The implementation is responsible for:
	//   - Looking up the job name from the queue configuration
	//   - Creating appropriate builds/jobs for each change based on its action
	//   - Handling dependencies between changes (order may be significant)
	//
	// Parameters:
	//   - ctx: Request context for cancellation and timeouts
	//   - queueName: Name of the queue processing these changes. Used to look up job configuration.
	//   - changes: List of changes to process. Order may be significant for dependencies.
	//
	// Returns:
	//   - string: Unique build ID that can be used with Poll and CancelBuild methods
	//   - error: ErrInvalidRequest if validation fails
	Schedule(ctx context.Context, queueName string, changes []entity.BuildChange) (string, error)

	// Poll retrieves the current status of a build from the CI provider.
	// This is a synchronous call that queries the provider's API.
	//
	// Parameters:
	//   - id: Build ID string
	//
	// Returns:
	//   - BuildStatus: Current state of the build
	//   - map[string]string: Additional metadata about the build (e.g., build URL, commit SHA, duration)
	//   - error: ErrBuildNotFound if the build doesn't exist
	Poll(ctx context.Context, id string) (entity.BuildStatus, map[string]string, error)

	// CancelBuild requests cancellation of a build.
	//
	// This operation is asynchronous and does not wait for the cancellation to complete.
	// The implementation should initiate the cancellation request with the CI provider
	// and return immediately. Use Poll to check if the build has transitioned to
	// BuildStatusCancelled.
	//
	// The implementation decides how to handle cancellation requests for builds in
	// terminal states (passed, failed, cancelled). It may return an error, silently
	// ignore the request, or handle it in a provider-specific way.
	//
	// Parameters:
	//   - id: Build ID string
	//
	// Returns:
	//   - error: ErrBuildNotFound if the build doesn't exist,
	//            ErrBuildNotCancellable if the build cannot be cancelled (implementation-specific)
	CancelBuild(ctx context.Context, id string) error

	// Close gracefully shuts down the build manager.
	// Implementations should cancel pending requests, close HTTP clients, and clean up resources.
	// After Close is called, all other methods should return errors.
	// Close is idempotent and safe to call multiple times.
	Close() error
}
