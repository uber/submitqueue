package build

import (
	"errors"
	"fmt"
)

// ErrBuildNotFound is returned when a build does not exist in the CI provider.
// This can occur if:
//   - The build ID is invalid or malformed
//   - The build was deleted from the provider
//   - The build never existed
var ErrBuildNotFound = errors.New("build not found")

// IsBuildNotFound returns true if any error in the error chain is ErrBuildNotFound.
func IsBuildNotFound(err error) bool {
	return errors.Is(err, ErrBuildNotFound)
}

// WrapBuildNotFound wraps ErrBuildNotFound with the original error from the build provider.
// This preserves the original error details while marking it as a "not found" error.
func WrapBuildNotFound(err error) error {
	return fmt.Errorf("%w: %w", ErrBuildNotFound, err)
}

// ErrBuildNotCancellable is returned when attempting to cancel a build that cannot be cancelled.
// This occurs when:
//   - The build has already finished (passed, failed, or cancelled)
//   - The provider does not support cancellation for this build type
var ErrBuildNotCancellable = errors.New("build not cancellable")

// IsBuildNotCancellable returns true if any error in the error chain is ErrBuildNotCancellable.
func IsBuildNotCancellable(err error) bool {
	return errors.Is(err, ErrBuildNotCancellable)
}

// WrapBuildNotCancellable wraps ErrBuildNotCancellable with the original error from the build provider.
func WrapBuildNotCancellable(err error) error {
	return fmt.Errorf("%w: %w", ErrBuildNotCancellable, err)
}

// ErrProviderUnavailable is returned when the CI provider is unreachable or experiencing errors.
// This can occur due to:
//   - Network connectivity issues
//   - Provider service outages (5xx errors)
//   - Authentication failures (invalid API tokens)
//   - Rate limiting
var ErrProviderUnavailable = errors.New("provider unavailable")

// IsProviderUnavailable returns true if any error in the error chain is ErrProviderUnavailable.
func IsProviderUnavailable(err error) bool {
	return errors.Is(err, ErrProviderUnavailable)
}

// WrapProviderUnavailable wraps ErrProviderUnavailable with the original error from the build provider.
func WrapProviderUnavailable(err error) error {
	return fmt.Errorf("%w: %w", ErrProviderUnavailable, err)
}

// ErrInvalidRequest is returned when ScheduleBuild parameters fail validation.
// This can occur when:
//   - Required parameters are missing (baseSHA, batchToBeTest.ID, repoURL, branch, pipelineID, sqid)
//   - Parameter values are malformed (invalid URLs, empty strings, etc.)
var ErrInvalidRequest = errors.New("invalid request")

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// WrapInvalidRequest wraps ErrInvalidRequest with a descriptive error message.
func WrapInvalidRequest(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
}
