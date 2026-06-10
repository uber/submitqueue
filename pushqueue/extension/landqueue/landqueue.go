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

package landqueue

//go:generate mockgen -source=landqueue.go -destination=mock/landqueue_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/pushqueue/entity"
)

// Preparer performs pre-push preparation for a set of land items.
// Implementations are typically backed by a VCS extension. The Queue
// receives a Preparer at construction time and may invoke it between
// Enqueue and Wait to pipeline preparation with queue wait time.
type Preparer interface {
	Prepare(ctx context.Context, target entity.QueueTarget, items []entity.LandItem) error
}

// Queue serializes access to a landing target, ensuring only one request
// pushes at a time per (address, target) pair.
//
// Contract:
//   - Enqueue is idempotent: re-enqueueing the same request is a no-op.
//   - Wait blocks until this request is head-of-queue AND preparation
//     (via the Preparer provided at construction) has completed.
//     Must respect ctx cancellation.
//   - Dequeue removes this request from the queue. Must be called exactly
//     once per successful Enqueue (use defer).
//   - Implementations must detect stale queue heads and evict them after a timeout.
//   - Implementations decide when to call Preparer.Prepare — during the
//     wait (pipelined) or synchronously before Wait returns (simple).
type Queue interface {
	Enqueue(ctx context.Context, target entity.QueueTarget, items []entity.LandItem) error
	Wait(ctx context.Context, target entity.QueueTarget) error
	Dequeue(ctx context.Context, target entity.QueueTarget) error
}
