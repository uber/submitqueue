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

package mysql

// Contract integration tests for testutil.StageHold. These pin the
// partition-lease semantics that StageHold relies on, so a future change to the
// lease algebra fails THIS test loudly instead of hanging e2e tests.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/test/testutil"
)

// StageHoldContractSuite pins the partition-lease semantics testutil.StageHold
// relies on: a phantom lease with a far-future renewal timestamp prevents
// delivery for the held partition without affecting other partitions, and
// deleting the phantom row resumes delivery.
type StageHoldContractSuite struct {
	suite.Suite
	ctx   context.Context
	stack *testutil.ComposeStack
	log   *testutil.TestLogger
}

func TestStageHoldContract(t *testing.T) {
	suite.Run(t, new(StageHoldContractSuite))
}

func (s *StageHoldContractSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		s.ctx,
		"docker-compose.yml",
		"ext-messagequeue-stagehold",
	)

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	t.Cleanup(func() {
		s.log.Logf("Tearing down StageHold contract test suite")
	})
}

// TestStageHold_BlocksHeldPartition_AllowsOthers verifies the core contract:
//   - A pre-planted hold starves the held partition: messages published there
//     are never delivered while the hold is active.
//   - Messages to other partitions on the same topic and consumer group are
//     delivered normally (proves the hold is per-partition, not per-topic).
//   - After Release(), the held partition's messages are delivered.
//
// The test is deterministic (no time.Sleep): delivery is signaled via channels,
// and non-delivery is proven by observing that the subscriber completed at least
// one full discovery+poll cycle without delivering the held message.
func (s *StageHoldContractSuite) TestStageHold_BlocksHeldPartition_AllowsOthers() {
	t := s.T()

	db, err := s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err)

	testutil.ApplySchema(t, s.log, db, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	topic := "stagehold_contract_topic"
	consumerGroup := "stagehold-cg"
	heldPartition := "partition-held"
	freePartition := "partition-free"

	// Plant the hold BEFORE publishing or subscribing.
	hold, err := testutil.NewStageHold(s.log, db, consumerGroup, topic, heldPartition, t.Cleanup)
	require.NoError(t, err)

	signalCh := make(chan queueMySQL.HookSignal, 100)
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
		OnSignal:     signalCh,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", consumerGroup)
	subConfig.PollIntervalMs = 100
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish msg1 to the HELD partition, msg2 to the FREE partition.
	msg1 := entityqueue.NewMessage("held-msg-1", []byte("held"), heldPartition, nil)
	msg2 := entityqueue.NewMessage("free-msg-2", []byte("free"), freePartition, nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg1))
	require.NoError(t, publisher.Publish(s.ctx, topic, msg2))

	// msg2 (free partition) should be delivered.
	d2 := receiveWithTimeout(t, deliveryChan)
	assert.Equal(t, "free-msg-2", d2.Message().ID, "free partition message should be delivered")
	require.NoError(t, d2.Ack(s.ctx))
	t.Logf("Received and acked free partition message")

	// Publish msg3 to the free partition and await it — proves at least one
	// more full poll/discovery cycle has elapsed while the held partition
	// stayed starved.
	msg3 := entityqueue.NewMessage("free-msg-3", []byte("free-2"), freePartition, nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg3))

	d3 := receiveWithTimeout(t, deliveryChan)
	assert.Equal(t, "free-msg-3", d3.Message().ID, "second free partition message should be delivered")
	require.NoError(t, d3.Ack(s.ctx))
	t.Logf("Received and acked second free partition message (proves poll cycles elapsed)")

	// Assert msg1 (held partition) has NOT been delivered. After two successful
	// deliveries from the free partition, the subscriber has completed at least
	// two full poll cycles — if the held partition were accessible, msg1 would
	// have appeared.
	assertNoDelivery(t, deliveryChan, signalCh, queueMySQL.SignalDeliveryCheck, 2)
	t.Logf("Confirmed: held partition message not delivered while hold is active")

	// Release the hold: the subscriber's next discovery tick re-acquires the
	// partition and delivers msg1.
	hold.Release()
	t.Logf("Hold released; awaiting held partition delivery")

	d1 := receiveWithTimeout(t, deliveryChan)
	assert.Equal(t, "held-msg-1", d1.Message().ID, "held partition message should be delivered after release")
	require.NoError(t, d1.Ack(s.ctx))
	t.Logf("Received and acked held partition message after release")

	// Prove no more messages remain.
	assertNoDelivery(t, deliveryChan, signalCh, queueMySQL.SignalDeliveryCheck, 2)
	t.Logf("StageHold contract verified: hold blocks, release resumes, other partitions unaffected")
}

// TestStageHold_ReleaseIsIdempotent verifies that calling Release() multiple
// times is safe (no error, no side effect beyond the first call).
func (s *StageHoldContractSuite) TestStageHold_ReleaseIsIdempotent() {
	t := s.T()

	db, err := s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err)

	testutil.ApplySchema(t, s.log, db, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	hold, err := testutil.NewStageHold(s.log, db, "idempotent-cg", "idempotent-topic", "pk", t.Cleanup)
	require.NoError(t, err)

	// Release three times — should not panic or error.
	hold.Release()
	hold.Release()
	hold.Release()

	t.Logf("Verified: Release() is idempotent")
}
