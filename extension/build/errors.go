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
// The specific conditions are implementation-defined and may include:
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

// ErrInvalidRequest is returned when Schedule parameters fail validation.
// This can occur when:
//   - queueName is empty or invalid
//   - changes list is empty
//   - changes contain invalid ChangeIDs or Actions
var ErrInvalidRequest = errors.New("invalid request")

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// WrapInvalidRequest wraps ErrInvalidRequest with a descriptive error message.
func WrapInvalidRequest(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
}
