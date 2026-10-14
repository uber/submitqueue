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

package main

import (
	"context"

	"github.com/uber/submitqueue/platform/extension/consumergate"
	"github.com/uber/submitqueue/submitqueue/extension/queuepause"
	"go.uber.org/zap"
)

var (
	_ consumergate.Gate  = queuePauseGate{}
	_ consumergate.Entry = frozenEntry{}
)

// queuePauseGate closes the gate for a queue while its pause state freezes
// processing, and otherwise defers to the gate it wraps.
type queuePauseGate struct {
	logger *zap.SugaredLogger
	pauses queuepause.Store
	next   consumergate.Gate
}

func newQueuePauseGate(logger *zap.SugaredLogger, pauses queuepause.Store, next consumergate.Gate) queuePauseGate {
	return queuePauseGate{logger: logger, pauses: pauses, next: next}
}

// Enter reads the pause state of the delivery's queue, which is its tenant. A
// failed read freezes the queue: the consumer treats a gate error as open, so
// returning it would unfreeze a queue whenever the pause backend is down.
func (g queuePauseGate) Enter(ctx context.Context, key consumergate.Key) (consumergate.Entry, error) {
	state, err := g.pauses.Get(ctx, key.Partition.Tenant)
	if err != nil {
		g.logger.Errorw("failed to read queue pause state, freezing queue", "queue", key.Partition.Tenant, "error", err)
		return frozenEntry{}, nil
	}
	if state.FreezeProcessing {
		return frozenEntry{}, nil
	}
	return g.next.Enter(ctx, key)
}

// frozenEntry is a blocked entry that keeps no parked record.
type frozenEntry struct{}

func (frozenEntry) Blocked() bool { return true }

func (frozenEntry) Park(context.Context, consumergate.DeliveryDescriptor) error { return nil }

func (frozenEntry) Unpark(context.Context, consumergate.DeliveryDescriptor) error { return nil }
