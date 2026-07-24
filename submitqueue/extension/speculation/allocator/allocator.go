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

// Package allocator defines the budget-spending composition point used by the
// standard Speculator. An Allocator spends the queue's build budget over a
// candidate iterator. It is not a controller-facing extension — an alternate
// Speculator need not use or expose this split.
package allocator

//go:generate mockgen -source=allocator.go -destination=mock/allocator_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
)

// Allocator decides how to spend the queue's build budget over the generator's
// candidate stream, returning the build and cancel actions to take this run. How
// it rations the budget across new candidates and paths already in flight — and
// whether it preempts running builds — is the implementation's policy.
type Allocator interface {
	// Allocate returns the build and cancel actions to take. It reconciles the
	// candidate stream against the queue's current path sets by path ID: a
	// candidate matching an in-flight path stays funded rather than starting a
	// new attempt, and one matching a path whose build already finished is
	// skipped — the generator enumerates the whole space, so suppressing
	// finished paths is the Allocator's job.
	//
	// A cancelled or expired ctx aborts with its error and no actions.
	Allocate(ctx context.Context, pathSets []entity.SpeculationPathSet, iter generator.Iterator) ([]entity.Speculation, error)
}
