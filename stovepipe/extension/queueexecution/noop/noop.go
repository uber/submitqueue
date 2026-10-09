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

// Package noop observes a host with no queue-level pause control.
package noop

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueexecution"
)

type reader struct{}

// New returns the explicit unpaused observation for an ungated host.
func New() queueexecution.Reader { return reader{} }

func (reader) GetQueueExecutionState(ctx context.Context, _ string) (entity.QueueExecutionState, error) {
	if err := ctx.Err(); err != nil {
		return entity.QueueExecutionStateUnknown, err
	}
	return entity.QueueExecutionStateRunning, nil
}
