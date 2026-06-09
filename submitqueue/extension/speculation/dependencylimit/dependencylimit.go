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

package dependencylimit

//go:generate mockgen -source=dependencylimit.go -destination=mock/dependencylimit_mock.go -package=mock

import "context"

// DependencyLimit is the "how much" policy that bounds how many active
// (in-flight, non-terminal) dependencies a batch may speculate over.
//
// It is the eligibility gate for speculation: a batch becomes eligible to
// enumerate only when its count of active dependencies is at or below the
// current limit; otherwise it waits, and is admitted later as predecessors land
// and leave the active set. The limit is a bound, not a trim — nothing is
// dropped from a batch's base.
//
// The value is dynamic: it may change between calls — not only when a
// dependency lands — so a change alone can newly admit a waiting batch, and the
// controller re-consults it on every respeculate rather than caching it.
//
// This limit is the exception among the speculation limits: it gates eligibility
// *before* enumeration and needs active-dependency reconciliation, which is
// controller orchestration — so the controller holds and applies it, rather than
// it being injected into a decision seam. The enumerator stays pure.
type DependencyLimit interface {
	// Limit returns the current maximum number of active dependencies a batch
	// may speculate over. The controller compares a batch's active-dependency
	// count against this to decide eligibility. It takes no parameters; anything
	// an implementation needs is injected at construction.
	Limit(ctx context.Context) (int, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything a policy needs to compute the limit (a
// capacity feed, historical metrics, config) is injected at construction by the
// integrator.
type Config struct {
	// QueueName identifies the queue this DependencyLimit serves.
	QueueName string
}

// Factory builds the DependencyLimit for a queue. Implementations are provided
// by integrators (and tests) and inject whatever signals they need at
// construction.
type Factory interface {
	// For returns the DependencyLimit for the given queue.
	For(cfg Config) (DependencyLimit, error)
}
