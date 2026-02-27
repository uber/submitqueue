package mergechecker

//go:generate mockgen -source=mergechecker.go -destination=mock/mergechecker_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// MergeChecker predicts whether a set of changes can merge cleanly.
type MergeChecker interface {
	// Check is a fail-fast mergeability check that optimistically assesses
	// whether the changes can be merged. A positive result does not
	// guarantee that the changes will apply cleanly at merge time.
	Check(ctx context.Context, queue string, change entity.Change) (Result, error)
}

// Result holds the outcome of a mergeability check.
type Result struct {
	// Mergeable is true if the request's changes are expected to merge cleanly.
	Mergeable bool
	// Reason is a human-readable explanation when Mergeable is false.
	// Empty when Mergeable is true.
	Reason string
}
