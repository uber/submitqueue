// Copyright (c) 2026 Uber Technologies, Inc.
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

package queuepause

//go:generate mockgen -source=queuepause.go -destination=mock/queuepause_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Store is the source of per-queue pause state. It reports the switches and
// decides nothing; enforcement lives in the services that resolve it.
type Store interface {
	// Get returns the current pause state of a queue. A queue with no entry
	// returns the zero state (not paused) and no error. A non-nil error means
	// the backend could not be read, not that the queue is unpaused; a caller
	// that enforces a switch must not fail open on it. Implementations must be
	// safe for concurrent use.
	Get(ctx context.Context, queue string) (entity.QueuePause, error)
}
