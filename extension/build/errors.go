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

// ErrInvalidRequest is returned when Schedule parameters fail validation.
// This can occur when:
//   - queueName is empty or invalid
//   - changes list is empty
//   - changes contain invalid Change entities or Actions
var ErrInvalidRequest = errors.New("invalid request")

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// WrapInvalidRequest wraps ErrInvalidRequest with a descriptive error message.
func WrapInvalidRequest(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
}
