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
// on, and how many, right now". It reads the tree — including each path's controller-stamped
// Status (Candidate / Building / Passed / Failed / Cancelled) and Score — and
// returns an action per path it wants to act on.
//
// The selector's only output is actions; it never writes Status. The controller
// owns every Status write (into the store) and feeds the up-to-date tree back in
// on the next call, so the tree is the selector's complete input. This keeps the
// selector a pure, deterministic policy. Policy knobs such as a top-K limit or
// budget belong to the implementation's construction, not this method.
type Selector interface {
	// Select returns the actions to take for the given tree. Returning multiple
	// Build decisions dispatches several speculative builds in parallel; an empty
	// result means nothing should be done right now. Paths the selector has no
	// opinion on are simply omitted (leave-as-is).
	Select(ctx context.Context, tree entity.SpeculationTree) ([]entity.SpeculationPathDecision, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (including policy
// knobs such as a top-K cap or build budget) is injected at construction by the
// integrator.
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
