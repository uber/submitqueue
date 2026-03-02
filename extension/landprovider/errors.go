package landprovider

import (
	"errors"
	"fmt"
)

// ErrLandRejected is returned by LandProvider implementations when the land operation
// was attempted but rejected due to the changes themselves (e.g., merge conflict, policy
// violation). This is a terminal failure — retrying will not help.
// Infrastructure errors (network timeout, API unavailable) should be returned as plain
// errors so the consumer can retry.
var ErrLandRejected = errors.New("land rejected")

// ErrAlreadyLanded is returned by LandProvider implementations when the changes have
// already been landed (e.g., PR already merged). The extension reports this as a domain
// fact; the controller decides whether to treat it as success.
var ErrAlreadyLanded = errors.New("already landed")

// IsLandRejected returns true if any error in the error chain is an ErrLandRejected.
func IsLandRejected(err error) bool {
	return errors.Is(err, ErrLandRejected)
}

// IsAlreadyLanded returns true if any error in the error chain is an ErrAlreadyLanded.
func IsAlreadyLanded(err error) bool {
	return errors.Is(err, ErrAlreadyLanded)
}

// WrapLandRejected wraps ErrLandRejected with a descriptive reason from the land provider.
func WrapLandRejected(err error) error {
	return fmt.Errorf("%w: %w", ErrLandRejected, err)
}
