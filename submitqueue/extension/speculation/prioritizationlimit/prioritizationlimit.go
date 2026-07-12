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

package prioritizationlimit

//go:generate mockgen -source=prioritizationlimit.go -destination=mock/prioritizationlimit_mock.go -package=mock

import "context"

// PrioritizationLimit is the "how much" policy that bounds how many builds a
// queue may run at once — the queue's concurrent-build budget.
//
// It is the prioritizer's companion: the prioritizer decides *which* of the
// queue's pending builds run (its ranking across all in-flight batches); the
// prioritization limit decides *how many* fit at once. It is the queue-wide
// resource knob, the ultimate cap on speculation's demand on CI.
//
// The value is dynamic: it may change between calls, so the prioritizer reads it
// each round rather than caching it.
//
// It is injected into the prioritizer at construction and called by it, never
// passed as a method parameter, keeping the prioritizer interface limit-free and
// stable.
type PrioritizationLimit interface {
	// Limit returns the current maximum number of concurrent builds for the
	// queue. The prioritizer admits at most this many candidates. It takes no
	// parameters; anything an implementation needs is injected at construction.
	Limit(ctx context.Context) (int, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything a policy needs to compute the limit (a
// capacity feed, cost budgets, config) is injected at construction by the
// integrator.
type Config struct {
	// QueueName identifies the queue this PrioritizationLimit serves.
	QueueName string
}

// Factory builds the PrioritizationLimit for a queue. Implementations are
// provided by integrators (and tests) and inject whatever signals they need at
// construction.
type Factory interface {
	// For returns the PrioritizationLimit for the given queue.
	For(cfg Config) (PrioritizationLimit, error)
}
