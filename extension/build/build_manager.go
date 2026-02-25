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
// All implementations must be thread-safe and support concurrent operations.
type BuildManager interface {
	// Schedule submits a list of changes to the CI provider for processing.
	// Each change specifies an action (validate or apply) to perform.
	//
	// The implementation is responsible for:
	//   - Looking up the job name from the queue configuration
	//   - Creating appropriate builds/jobs for each change based on its action
	//   - Handling dependencies between changes (order may be significant)
	//   - Tracking build IDs internally for subsequent Poll/CancelBuild calls
	//
	// Parameters:
	//   - ctx: Request context for cancellation and timeouts
	//   - queueName: Name of the queue processing these changes. Used to look up job configuration.
	//   - changes: List of changes to process. Order may be significant for dependencies.
	//
	// Returns:
	//   - error: ErrInvalidRequest if validation fails, ErrProviderUnavailable if CI provider is unreachable
	Schedule(ctx context.Context, queueName string, changes []entity.BuildChange) error

	// Poll retrieves the current status of a build from the CI provider.
	// This is a synchronous call that queries the provider's API.
	//
	// Parameters:
	//   - id: Build ID string
	//
	// Returns:
	//   - BuildStatus: Current state of the build
	//   - error: ErrBuildNotFound if the build doesn't exist, ErrProviderUnavailable if CI provider is unreachable
	Poll(ctx context.Context, id string) (entity.BuildStatus, error)

	// CancelBuild requests cancellation of a queued or running build.
	// Builds that have already completed cannot be cancelled.
	//
	// Parameters:
	//   - id: Build ID string
	//
	// Returns:
	//   - error: ErrBuildNotFound if the build doesn't exist,
	//            ErrBuildNotCancellable if the build has already finished,
	//            ErrProviderUnavailable if the CI provider is unreachable
	CancelBuild(ctx context.Context, id string) error

	// Close gracefully shuts down the build manager.
	// Implementations should cancel pending requests, close HTTP clients, and clean up resources.
	// After Close is called, all other methods should return errors.
	// Close is idempotent and safe to call multiple times.
	Close() error
}
