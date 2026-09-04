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

package landchecker

//go:generate mockgen -source=landchecker.go -destination=mock/landchecker_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// LandChecker predicts whether a request's changes can land cleanly.
type LandChecker interface {
	// Check is a fail-fast landability check that optimistically assesses
	// whether the request's changes can be landed. It is handed the request
	// identity and reads request.Change itself. A positive result does not
	// guarantee that the changes will apply cleanly at land time.
	Check(ctx context.Context, request entity.Request) (entity.LandCheckResult, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this LandChecker serves.
	QueueName string
}

// Factory builds the LandChecker for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the LandChecker for the given queue.
	For(cfg Config) (LandChecker, error)
}
