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

// ConflictType classifies why two batches are considered to conflict.
// New values may be added as more sophisticated analyzers are introduced.
type ConflictType string

const (
	// ConflictTypeUnknown is the unreachable zero value, set by default when
	// the structure is initialized. It should never be seen in the system.
	ConflictTypeUnknown ConflictType = ""
	// ConflictTypeConservative means the analyzer treated the batches as
	// conflicting because it could not prove otherwise, without identifying a
	// specific reason. Used by conservative analyzers that serialize
	// everything by default.
	ConflictTypeConservative ConflictType = "conservative"
	// ConflictTypeTargetOverlap means the two batches modify one or more of
	// the same build targets and may therefore interfere with each other.
	ConflictTypeTargetOverlap ConflictType = "target_overlap"
)

// Conflict reports a single conflict between the analyzed batch and one of
// the in-flight batches.
type Conflict struct {
	// BatchID is the ID of the in-flight batch that conflicts with the
	// analyzed batch.
	BatchID string
	// Type classifies the conflict. A single (analyzed, in-flight) pair may
	// be reported with multiple Conflict entries when different conflict
	// types apply.
	Type ConflictType
}

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
	Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]Conflict, error)
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
