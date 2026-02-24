package build

//go:generate go run go.uber.org/mock/mockgen -source=build_manager.go -destination=mock/build_manager.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
	entitybuild "github.com/uber/submitqueue/entity/build"
)

// BuildManager is a vendor-agnostic interface for managing builds with external CI/CD providers.
// Implementations provide integration with specific CI systems (BuildKite, Jenkins, etc.)
// to schedule builds, poll their status, and cancel running builds.
//
// All implementations must be thread-safe and support concurrent operations.
type BuildManager interface {
	// ScheduleBuild creates a new build with the CI provider for testing a batch.
	//
	// The baseSHA represents the starting point (main branch HEAD).
	// The speculatedBatchesToBeApplied are batches already applied on main.
	// The batchToBeTest is the batch being tested on top of those base batches.
	//
	// Batch information is used to:
	//   - Generate descriptive build messages
	//   - Add environment variables for CI scripts
	//   - Populate metadata for tracing and debugging
	//
	// Returns:
	//   - BuildID: Unique identifier for the scheduled build
	//   - error: ErrInvalidRequest if validation fails, ErrProviderUnavailable if CI provider is unreachable
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
	) (entitybuild.BuildID, error)

	// Poll retrieves the current status of a build from the CI provider.
	// This is a synchronous call that queries the provider's API.
	//
	// Returns:
	//   - BuildStatus: Current state, timestamps, URLs, and metadata for the build
	//   - error: ErrBuildNotFound if the build doesn't exist, ErrProviderUnavailable if the CI provider is unreachable
	Poll(ctx context.Context, id entitybuild.BuildID) (entitybuild.BuildStatus, error)

	// CancelBuild requests cancellation of a queued or running build.
	// Builds that have already completed cannot be cancelled.
	//
	// Returns:
	//   - error: ErrBuildNotFound if the build doesn't exist,
	//            ErrBuildNotCancellable if the build has already finished,
	//            ErrProviderUnavailable if the CI provider is unreachable
	CancelBuild(ctx context.Context, id entitybuild.BuildID) error

	// Close gracefully shuts down the build manager.
	// Implementations should cancel pending requests, close HTTP clients, and clean up resources.
	// After Close is called, all other methods should return errors.
	// Close is idempotent and safe to call multiple times.
	Close() error
}
