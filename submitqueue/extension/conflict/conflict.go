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

package conflict

//go:generate mockgen -source=conflict.go -destination=mock/conflict_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Analyzer detects conflicts between a candidate batch and the batches
// already in flight, so the speculation layer can decide which batches can
// safely advance in parallel.
type Analyzer interface {
	// Analyze returns the subset of inFlight batches that conflict with
	// batch, each paired with the type of conflict detected. An empty
	// result means batch does not conflict with any in-flight batch.
	//
	// Callers should not include batch itself in inFlight; terminal batches
	// should be filtered out before calling. A non-nil error indicates the
	// analysis itself failed (infrastructure issue) and should be treated as
	// retryable by the caller.
	Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]entity.Conflict, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Analyzer serves.
	QueueName string
}

// Factory builds the Analyzer for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Analyzer for the given queue.
	For(cfg Config) (Analyzer, error)
}
