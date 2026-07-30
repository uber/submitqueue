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

package speculator

//go:generate mockgen -source=speculator.go -destination=mock/speculator_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Speculator decides which speculation paths to build and which running ones to
// cancel, within the queue's build budget. It is the only speculation extension the
// speculate controller calls. It can never express a verdict: whether a batch
// merges or fails is fixed by the facts and computed by the controller, so a
// swapped-in Speculator changes which paths run, never a batch's outcome.
type Speculator interface {
	// Speculate proposes this run's path actions.
	//
	// Arguments:
	//   - batches: every in-flight batch of the queue, plus any finalized batch
	//     still referenced as a dependency by an in-flight one. Each carries its
	//     dependency list and state.
	//   - pathSets: every materialized path set for those batches, holding live
	//     and recently finished entries, so an implementation can avoid
	//     re-proposing a path that already passed or failed.
	//
	// Returns:
	//   - the proposed actions: a build funds a path, a cancel preempts an
	//     in-flight one. A path to be left as-is has no entry, so an empty
	//     result means the queue needs no changes. Order carries no meaning.
	//   - error: reserved for infrastructure failures — a failed dependency of
	//     the implementation, or ctx cancellation/expiry, which is passed
	//     through unwrapped enough for errors.Is(err, context.Canceled) and
	//     context.DeadlineExceeded to hold. Per the platform/errs rules the
	//     error is plain (never classified here), and "nothing worth doing" is
	//     an empty result, not an error. On error the actions are nil and the
	//     run is abandoned; the next dirty signal retries with fresh state.
	Speculate(ctx context.Context, batches []entity.Batch, pathSets []entity.SpeculationPathSet) ([]entity.Speculation, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Speculator serves.
	QueueName string
}

// Factory builds the Speculator for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need — budget, depth bound,
// clock, and any extra data — at construction.
type Factory interface {
	// For returns the Speculator for the given queue.
	For(cfg Config) (Speculator, error)
}
