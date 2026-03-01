package heuristic

import (
	"context"
	"fmt"

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
}

// New creates a new heuristic Scorer with the given buckets and value function.
// Panics if valueFunc is nil.
func New(buckets []Bucket, valueFunc ValueFunc) scorer.Scorer {
	if valueFunc == nil {
		panic("heuristic.New: valueFunc must not be nil")
	}
	return &heuristicScorer{
		buckets:   buckets,
		valueFunc: valueFunc,
	}
}

// Score extracts the value from the change, then returns the probability score for the first
// bucket whose range [Min, Max] contains the value. Returns an error if no bucket matches.
func (s *heuristicScorer) Score(ctx context.Context, change entity.Change) (float64, error) {
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
