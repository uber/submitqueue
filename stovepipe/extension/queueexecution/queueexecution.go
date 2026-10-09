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

// Package queueexecution defines the read-only queue-level pause observation seam.
package queueexecution

//go:generate mockgen -source=queueexecution.go -destination=mock/queueexecution_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Reader observes the queue-wide pause control without changing admission or policy.
// Narrower stage or partition controls are outside this summary.
type Reader interface {
	// GetQueueExecutionState returns the current queue-level control. Failures do not imply running or disabled.
	GetQueueExecutionState(ctx context.Context, queue string) (entity.QueueExecutionState, error)
}
