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

package selector

//go:generate mockgen -source=selector.go -destination=mock/selector_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Selector decides what the controller should do with each path in a batch's
// speculation tree.
//
// Selection is the policy: it answers "which futures do we spend build resources
// on, and how many, right now". It reads the batch's tree — each path's
// controller-stamped Status (Candidate / Selected / Prioritized / Building /
// Passed / Failed / Cancelling / Cancelled) and Score — and returns a decision
// per path it wants to act on.
//
// The controller hands the selector the batch's speculation tree directly — the
// subject it decides over. Its only output is decisions (Promote / Cancel),
// each naming a path by its ID (entity.SpeculationPathInfo.ID); it never
// writes Status. The controller maps each decision to a status
// transition — Promote → Selected, Cancel → Cancelling (or Cancelled) — applied
// under the tree's optimistic lock, and persists it, staying the single writer
// of tree state. This keeps the selector a deterministic policy over the tree it
// is given. Policy knobs such as a top-K limit or budget belong to the
// implementation's construction, not this method.
type Selector interface {
	// Select returns the decisions to take for the given tree, at most one per
	// path (the controller treats conflicting duplicates as a policy bug and
	// skips them). Returning multiple Promote decisions selects several paths
	// for building in parallel — each waits for the prioritizer to clear it
	// under the queue's build budget before a build is dispatched. An empty
	// result means nothing should be done right now. Paths the selector has no
	// opinion on are simply omitted (leave-as-is).
	Select(ctx context.Context, tree entity.SpeculationTree) ([]entity.PathDecision, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; policy knobs such as a top-K cap or build budget are
// injected at construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Selector serves.
	QueueName string
}

// Factory builds the Selector for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Selector for the given queue.
	For(cfg Config) (Selector, error)
}
