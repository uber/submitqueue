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

// Enumerator lists the candidate speculation paths for a batch — the raw
// material the controller assembles into the batch's speculation tree.
//
// Enumeration answers "what futures are possible" for a batch. It is
// deliberately dumb and purely structural: it mechanically lists candidate
// Base/Head paths from the dependency batches it is handed and nothing else. It
// does not score paths — that is the scorer's job (see
// extension/speculation/scorer), which the controller re-runs on every
// respeculate — it does not decide which paths to build — that is the selector's
// job (see extension/speculation/selector) — and it does not decide how far
// back to speculate: the controller gates on the dependency limit and hands
// Enumerate exactly the active dependencies to speculate over.
//
// Its output is structure only. Everything else about a path — its identity,
// status, and score — is owned by the controller, which wraps each returned
// path into the persisted tree entry (entity.SpeculationPathInfo).
type Enumerator interface {
	// Enumerate returns the candidate speculation paths for the batch, given its
	// active dependency batches ordered oldest-first (queue arrival order). That
	// ordering is load-bearing: a path's Base is an assumed-good prefix of
	// predecessors applied in order, so the deps ordering fixes which Base
	// prefixes exist and the order of batch IDs within each.
	//
	// Enumeration is pure and deterministic: the same (batch, deps) always
	// yields the same paths, so callers may regenerate safely. Returned paths
	// must not contain duplicates (same Head and same ordered Base).
	Enumerate(ctx context.Context, batch entity.Batch, deps []entity.Batch) ([]entity.SpeculationPath, error)
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
