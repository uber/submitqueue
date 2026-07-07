// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package heuristic

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
)

// ValueFunc extracts a single numeric value from a batch of changes for bucketing.
type ValueFunc func(context.Context, entity.BatchChanges) (int, error)

// Bucket defines a range [Min, Max] mapped to a probability Score.
type Bucket struct {
	// Min is the inclusive lower bound of the range.
	Min int
	// Max is the inclusive upper bound of the range.
	Max int
	// Score is the probability returned when the metric falls within this bucket.
	Score float64
}

// heuristicScorer computes a success probability by bucketing a metric extracted from a batch of changes.
// It follows the Java HeuristicsBasedSuccessPredictor pattern.
type heuristicScorer struct {
	// resolver resolves the batch identity into its detailed changes.
	resolver changeset.Resolver
	// buckets is the list of ranges to match against.
	buckets []Bucket
	// valueFunc extracts the numeric value from a batch of changes.
	valueFunc ValueFunc
	// scope is the tally scope for emitting metrics.
	scope tally.Scope
}

// New creates a new heuristic Scorer with the given resolver, buckets and value function.
// Panics if valueFunc is nil.
func New(resolver changeset.Resolver, buckets []Bucket, valueFunc ValueFunc, scope tally.Scope) scorer.Scorer {
	if valueFunc == nil {
		panic("heuristic.New: valueFunc must not be nil")
	}
	return &heuristicScorer{
		resolver:  resolver,
		buckets:   buckets,
		valueFunc: valueFunc,
		scope:     scope,
	}
}

// Score resolves the batch's changes, extracts the metric, then returns the probability
// score for the first bucket whose range [Min, Max] contains the value. Returns an error
// if no bucket matches.
func (s *heuristicScorer) Score(ctx context.Context, batch entity.Batch) (ret float64, retErr error) {
	op := metrics.Begin(s.scope, "score")
	defer func() { op.Complete(retErr) }()
	changes, err := s.resolver.DetailedForBatch(ctx, batch)
	if err != nil {
		return 0, err
	}
	value, err := s.valueFunc(ctx, changes)
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
