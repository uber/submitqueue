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

// Package predictor defines how likely a batch is to succeed, given both what
// it changes and what has happened to it so far. A Scorer prices the change; a
// Predictor is built over one and revises its price with the batch's observed
// progress.
package predictor

//go:generate mockgen -source=predictor.go -destination=mock/predictor_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Probability is how likely an outcome is, from 0.0 to 1.0.
type Probability float64

// Predictor estimates a batch's final outcome.
type Predictor interface {
	// Predict returns how likely the batch is to reach Succeeded with its
	// changes landed. A passing build is necessary but not sufficient.
	//
	// paths is the batch's own build progress, zero-valued for a batch nothing
	// has speculated on. Callers may predict every batch a queue waits on, so
	// anything expensive belongs behind the implementation's own cache.
	Predict(ctx context.Context, batch entity.Batch, paths entity.SpeculationPathSet) (Probability, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Predictor serves.
	QueueName string
}

// Factory builds the Predictor for a queue. Implementations inject what they
// need at construction, including the Scorer whose price they revise.
type Factory interface {
	// For returns the Predictor for the given queue.
	For(cfg Config) (Predictor, error)
}
