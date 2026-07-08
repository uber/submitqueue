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

package selectionlimit

//go:generate mockgen -source=selectionlimit.go -destination=mock/selectionlimit_mock.go -package=mock

import "context"

// SelectionLimit is the "how much" policy that bounds how many paths a batch may
// build in parallel.
//
// It is the selector's companion: the selector decides *which* of a batch's
// paths are worth building (its ranking); the selection limit decides *how many*
// of them may run at once. Separating the two keeps selector logic free of
// resource accounting and lets the bound scale with build resources without
// touching that logic.
//
// The value is dynamic: it may change between calls, so the selector reads it
// each pass rather than caching it.
//
// Unlike the dependency limit, this limit is injected into the seam that uses it
// — the selector is constructed with it and calls it itself — never passed as a
// method parameter, keeping the selector interface limit-free and stable.
type SelectionLimit interface {
	// Limit returns the current maximum number of paths a batch may build in
	// parallel. The selector caps its Promote decisions at this. It takes no
	// parameters; anything an implementation needs is injected at construction.
	Limit(ctx context.Context) (int, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything a policy needs to compute the limit (a
// capacity feed, historical metrics, config) is injected at construction by the
// integrator.
type Config struct {
	// QueueName identifies the queue this SelectionLimit serves.
	QueueName string
}

// Factory builds the SelectionLimit for a queue. Implementations are provided by
// integrators (and tests) and inject whatever signals they need at construction.
type Factory interface {
	// For returns the SelectionLimit for the given queue.
	For(cfg Config) (SelectionLimit, error)
}
