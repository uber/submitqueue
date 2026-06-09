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
// speculation paths to consider.
//
// Enumeration answers "what futures are possible" for a batch. It is
// deliberately dumb and purely structural: it mechanically lists candidate
// Base/Head paths from the dependency batches it is handed and nothing else. It
// does not score paths — that is the scorer's job (see
// extension/speculation/scorer), which the controller re-runs on every
// respeculate — it does not decide which paths to build — that is the selector's
// job (see extension/speculation/selector) — it does not set path status, and it
// does not decide how far back to speculate: the controller gates on the
// dependency limit and hands Enumerate exactly the active dependencies to
// speculate over.
type Enumerator interface {
	// Enumerate returns the speculation tree structure for the batch identified
	// by batchID, given its active dependency batches in arrival order. Each
	// returned path carries a Base/Head split only: Score and Status are left
	// unset — the controller stamps Status on persist and calls the scorer to
	// fill Score.
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
