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

package hook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const testDLQTopicKey = "hook_dlq"

func dlqDelivery(ctrl *gomock.Controller, payload []byte, f *failure.Failure) *consumermock.MockDelivery {
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage("msg-1", payload, "batch-778", nil)).AnyTimes()
	d.EXPECT().Attempt().Return(4).AnyTimes()
	d.EXPECT().Metadata().Return(map[string]string{
		"dlq.original_topic": "hook",
		"dlq.failure_count":  "3",
		"dlq.last_error":     "boom",
	}).AnyTimes()
	if f == nil {
		d.EXPECT().Failure().Return(failure.Failure{}, false).AnyTimes()
	} else {
		d.EXPECT().Failure().Return(*f, true).AnyTimes()
	}
	return d
}

func newDLQController(scope tally.Scope) *DLQController {
	return NewDLQController(zap.NewNop().Sugar(), scope, testDLQTopicKey, "submitqueue-hook-dlq")
}

// The DLQ consumer has no DLQ of its own and treats every error as retryable, so
// anything but an ack loops the message forever. Whatever the payload, the
// reconciler must ack.
func TestDLQControllerAlwaysAcks(t *testing.T) {
	attributed := failure.New("hook boom", failure.Subject{Type: "batch", ID: "batch-778"})

	cases := map[string]struct {
		payload []byte
		failure *failure.Failure
	}{
		"decodable event with attribution": {payload: hookPayload(t, validEvent(t)), failure: &attributed},
		"decodable event unattributed":     {payload: hookPayload(t, validEvent(t))},
		"undecodable payload":              {payload: []byte("{definitely not json"), failure: &attributed},
		"empty payload":                    {payload: []byte{}},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c := newDLQController(tally.NewTestScope("test", nil))

			require.NoError(t, c.Process(context.Background(), dlqDelivery(ctrl, tt.payload, tt.failure)))
		})
	}
}

// The dropped-event counter is the only signal that a side effect was lost —
// nothing else in the system notices a comment that never posted — so it is the
// reconciler's actual output, tagged for attribution.
func TestDLQControllerCountsDroppedEvents(t *testing.T) {
	t.Run("attributed to the decoded envelope", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		scope := tally.NewTestScope("test", nil)

		require.NoError(t, newDLQController(scope).Process(
			context.Background(), dlqDelivery(ctrl, hookPayload(t, validEvent(t)), nil)))

		counter, ok := scope.Snapshot().Counters()["test.hook_dlq_controller.reconcile.events_dropped+event_type=batch.failed,source=submitqueue"]
		require.True(t, ok)
		assert.Equal(t, int64(1), counter.Value())
	})

	t.Run("counted even when the envelope cannot be read", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		scope := tally.NewTestScope("test", nil)

		require.NoError(t, newDLQController(scope).Process(
			context.Background(), dlqDelivery(ctrl, []byte("{definitely not json"), nil)))

		counter, ok := scope.Snapshot().Counters()["test.hook_dlq_controller.reconcile.events_dropped+event_type=unknown,source=unknown"]
		require.True(t, ok)
		assert.Equal(t, int64(1), counter.Value())
	})
}

func TestDLQControllerIdentity(t *testing.T) {
	c := newDLQController(tally.NewTestScope("test", nil))

	assert.Equal(t, basehook.TopicKey(testDLQTopicKey), c.TopicKey())
	assert.Equal(t, "submitqueue-hook-dlq", c.ConsumerGroup())
}
