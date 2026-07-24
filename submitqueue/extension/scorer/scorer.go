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

package scorer

//go:generate mockgen -source=scorer.go -destination=mock/scorer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Scorer computes the probability that a batch ultimately succeeds, based on
// its changes.
type Scorer interface {
	// Score returns a probability between 0.0 and 1.0 that the given batch
	// ultimately succeeds — reaches its terminal Succeeded state with its
	// changes landed, rather than Failed or Cancelled. A passing build is
	// necessary but not sufficient: a batch whose build already passed can
	// still fail to merge, so this is the probability of the final outcome,
	// not of the build alone. It is handed the batch identity and resolves the
	// batch's changes itself through an injected changeset.Resolver.
	//
	// Callers may score every batch a queue is waiting on, so implementations
	// should be cheap: a speculation run scores each batch at most once, but it
	// does not carry results over to the next run, so anything expensive to
	// compute belongs behind the implementation's own cache.
	Score(ctx context.Context, batch entity.Batch) (float64, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Scorer serves.
	QueueName string
}

// Factory builds the Scorer for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Scorer for the given queue.
	For(cfg Config) (Scorer, error)
}
