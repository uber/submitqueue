package mergechecker

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/entity"
)

// ErrUnmergeable is returned by Check when the request's changes are not mergeable.
var ErrUnmergeable = errors.New("request is not mergeable")

// MergeChecker predicts whether a request's changes can merge cleanly.
type MergeChecker interface {
	// Check is a fail-fast validation that optimistically assesses the
	// mergeability of the request. A nil result does not guarantee
	// that the changes will apply cleanly at merge finalization time.
	// Returns ErrUnmergeable if the changes are not mergeable.
	Check(ctx context.Context, request entity.Request) error
}
