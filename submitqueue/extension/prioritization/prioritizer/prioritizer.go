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

package prioritizer

//go:generate mockgen -source=prioritizer.go -destination=mock/prioritizer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Prioritizer is the queue-wide policy that rations a shared build budget across
// every in-flight batch in a queue.
//
// Selection is per batch and blind to other batches, so it cannot ration a
// shared budget: if every batch selected generously, their combined demand could
// swamp CI. Prioritization closes that gap. It sees every selected build across
// all of the queue's in-flight batches, ranks them (by each build's score, plus
// any fairness or tie-break policy), and admits only the subset that fits the
// queue's concurrent-build budget.
//
// It lives at the build stage — the one place all of the queue's selected paths
// converge and the build budget is known — not in the per-batch speculate stage.
// It is the queue-wide enforcer: selection expresses desire per batch,
// prioritization reconciles that desire against supply. It is constructed with
// its prioritization limit and applies it itself.
//
// The store is the source of truth, and the prioritizer is bound to its queue at
// construction (Config.QueueName). So it takes no arguments: it reads whatever it
// needs for the whole queue from storage — the pending builds and each build's
// path score — through read access injected at its Factory, ranks them, and
// returns the admitted subset. It never writes; dispatching the admitted builds
// is the controller's job.
type Prioritizer interface {
	// Prioritize reads the queue's pending builds and their path scores from
	// storage and returns the subset admitted to run now, ranked by score plus any
	// fairness policy and capped by the prioritization limit. Builds not returned
	// are left pending for a later round.
	Prioritize(ctx context.Context) ([]entity.Build, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (read access to the
// build and tree stores, its prioritization limit, fairness policy, capacity
// signals) is injected at construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Prioritizer serves.
	QueueName string
}

// Factory builds the Prioritizer for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Prioritizer for the given queue.
	For(cfg Config) (Prioritizer, error)
}
