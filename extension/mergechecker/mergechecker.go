package mergechecker

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// MergeChecker predicts whether a request's changes can merge cleanly.
type MergeChecker interface {
	// Check is a fail-fast validation that optimistically assesses the
	// mergeability of the request. A positive result does not guarantee
	// that the changes will apply cleanly at merge finalization time.
	Check(ctx context.Context, request entity.Request) (Result, error)
}

// Result holds the outcome of a merge check.
type Result struct {
	// Mergeable is true if the request's changes are expected to merge cleanly.
	Mergeable bool
}
