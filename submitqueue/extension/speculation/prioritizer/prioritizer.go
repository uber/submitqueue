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
// shared budget: if every batch promoted generously, their combined demand could
// swamp CI. Prioritization closes that gap. It sees every path across all of the
// queue's in-flight batches that is running or wants to run, ranks them (by each
// path's score, plus any fairness or tie-break policy), and admits only the
// subset that fits the queue's concurrent-build budget. Selection expresses
// desire per batch; prioritization reconciles that desire against supply. It is
// constructed with its prioritization limit and applies it itself.
//
// The controller hands it the queue's candidate paths directly — every path that
// is Selected (wants a slot) or Prioritized/Building (holds a slot), each
// carrying its identity (ID) and score. The prioritizer returns sparse
// decisions, each naming a path by that ID: Promote to admit a pending path,
// Cancel to preempt a running one; paths it omits are left as-is, and it
// returns at most one decision per path (the controller treats conflicting
// duplicates as a policy bug and skips them). Whether it preempts at all is
// its own policy — a sticky-slots implementation
// never emits Cancel for a running path and only fills free slots, while a
// preemptive one ranks running and pending together and may Cancel a running
// path to admit a higher-scored one. It never writes: the controller maps each
// decision to a status transition (Promote → Prioritized, Cancel → Cancelling),
// applied per tree under that tree's optimistic lock, and enacts it, staying
// the single writer.
type Prioritizer interface {
	// Prioritize ranks the queue's candidate paths and returns the decisions to
	// apply: Promote for paths admitted to run now, Cancel for running paths it
	// chooses to preempt. Omitted paths are left as-is. Ranking is by score —
	// a probability in [0, 1] per the path scorer's contract — plus any
	// fairness policy, bounded by the prioritization limit.
	Prioritize(ctx context.Context, candidates []entity.SpeculationPathInfo) ([]entity.PathDecision, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (its prioritization
// limit, fairness policy, capacity signals) is injected at construction by the
// integrator.
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
