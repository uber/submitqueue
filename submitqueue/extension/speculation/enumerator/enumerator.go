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

package enumerator

//go:generate mockgen -source=enumerator.go -destination=mock/enumerator_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Enumerator builds the speculation tree for a batch: the set of candidate
// speculation paths to consider, each scored with its predicted success
// probability.
//
// Enumeration answers "what futures are possible" for a batch. It is
// deliberately dumb: it mechanically lists candidate paths from the dependency
// batches it is handed and attaches a Score to each. It does not decide which
// paths to build — that is the selector's job (see
// extension/speculation/selector) — and it does not decide how far back to
// speculate: the controller trims the dependency list by speculation depth
// before calling Enumerate.
type Enumerator interface {
	// Enumerate returns the speculation tree for the batch identified by batchID,
	// given its dependency batches in arrival order. Each returned path carries a
	// Base/Head split and a predicted success Score; the returned paths leave
	// Status unset (the controller stamps it on persist).
	//
	// Path scores are derived from the dependency batches' Score field (the
	// per-batch success probability set by the score stage), so no separate
	// scoring backend is needed. The combination formula is the implementation's
	// concern.
	//
	// Enumeration is pure and deterministic: the same (batchID, deps) always
	// yields the same tree, so callers may regenerate safely.
	Enumerate(ctx context.Context, batchID string, deps []entity.Batch) (entity.SpeculationTree, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (including behavioral
// knobs such as speculation depth) is injected at construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Enumerator serves.
	QueueName string
}

// Factory builds the Enumerator for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Enumerator for the given queue.
	For(cfg Config) (Enumerator, error)
}
