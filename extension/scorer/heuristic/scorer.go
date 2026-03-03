package heuristic

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/scorer"
)

// ValueFunc extracts a single numeric value from a Change for bucketing.
type ValueFunc func(context.Context, entity.Change) (int, error)

// Bucket defines a range [Min, Max] mapped to a probability Score.
type Bucket struct {
	// Min is the inclusive lower bound of the range.
	Min int
	// Max is the inclusive upper bound of the range.
	Max int
	// Score is the probability returned when the metric falls within this bucket.
	Score float64
}

// heuristicScorer computes a success probability by bucketing a metric extracted from a Change.
// It follows the Java HeuristicsBasedSuccessPredictor pattern.
type heuristicScorer struct {
	// buckets is the list of ranges to match against.
	buckets []Bucket
	// valueFunc extracts the numeric value from a Change.
	valueFunc ValueFunc
	// scope is the tally scope for emitting metrics.
	scope tally.Scope
}

// New creates a new heuristic Scorer with the given buckets and value function.
// Panics if valueFunc is nil.
func New(buckets []Bucket, valueFunc ValueFunc, scope tally.Scope) scorer.Scorer {
	if valueFunc == nil {
		panic("heuristic.New: valueFunc must not be nil")
	}
	return &heuristicScorer{
		buckets:   buckets,
		valueFunc: valueFunc,
		scope:     scope,
	}
}

// Score extracts the value from the change, then returns the probability score for the first
// bucket whose range [Min, Max] contains the value. Returns an error if no bucket matches.
func (s *heuristicScorer) Score(ctx context.Context, change entity.Change) (ret float64, retErr error) {
	op := metrics.Begin(s.scope, "score")
	defer func() { op.Complete(retErr) }()
	value, err := s.valueFunc(ctx, change)
	if err != nil {
		return 0, err
	}
	for _, b := range s.buckets {
		if value >= b.Min && value <= b.Max {
			return b.Score, nil
		}
	}
	return 0, fmt.Errorf("no bucket matches value %d", value)
}
