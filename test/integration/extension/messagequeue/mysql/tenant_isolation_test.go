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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"go.uber.org/zap/zaptest"
)

func (s *SQLQueueIntegrationSuite) tenantTopicRowCount(t *testing.T, table, tenant, topic string) int {
	t.Helper()
	var count int
	err := s.db.QueryRowContext(
		s.ctx,
		"SELECT COUNT(*) FROM "+table+" WHERE tenant = ? AND topic = ?",
		tenant,
		topic,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

func (s *SQLQueueIntegrationSuite) TestTenantIsolationWithEqualMessageIdentities() {
	t := s.T()

	const (
		tenantA       = "tenant-a"
		tenantB       = "tenant-b"
		topic         = "tenant_isolation_topic"
		partitionKey  = "共享-partition"
		messageID     = "共享-message"
		consumerGroup = "tenant-isolation-consumer"
	)
	signalCh := make(chan queueMySQL.HookSignal, 100)

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
		Tenants:      []string{tenantA, tenantB},
		OnSignal:     signalCh,
	})
	require.NoError(t, err)
	defer q.Close()

	cfg := testSubConfig("tenant-isolation-worker", consumerGroup)
	cfg.VisibilityTimeoutMs = cfg.LeaseDurationMs * 10
	deliveries, err := q.Subscriber().Subscribe(s.ctx, topic, cfg)
	require.NoError(t, err)

	for _, tenant := range []string{tenantA, tenantB} {
		msg := entityqueue.NewMessage(messageID, []byte(tenant), partitionKey, nil)
		msg.Tenant = tenant
		require.NoError(t, q.Publisher().Publish(s.ctx, topic, msg))
	}

	received := make(map[string]string, 2)
	receivedDeliveries := make(map[string]extqueue.Delivery, 2)
	receiveN(t, deliveries, 2, func(delivery extqueue.Delivery, _ int) {
		msg := delivery.Message()
		received[msg.Tenant] = string(msg.Payload)
		receivedDeliveries[msg.Tenant] = delivery
	})
	assert.Equal(t, map[string]string{tenantA: tenantA, tenantB: tenantB}, received)

	for _, table := range []string{
		"queue_messages",
		"queue_delivery_state",
		"queue_offsets",
		"queue_partition_leases",
		"queue_subscriber_heartbeats",
	} {
		for _, tenant := range []string{tenantA, tenantB} {
			assert.Equal(t, 1, s.tenantTopicRowCount(t, table, tenant, topic), "%s rows for %s", table, tenant)
		}
	}

	require.NoError(t, receivedDeliveries[tenantB].Postpone(s.ctx, cfg.LeaseDurationMs*10))
	require.NoError(t, receivedDeliveries[tenantA].Ack(s.ctx))
	waitForCondition(t, signalCh, func() bool {
		return s.tenantTopicRowCount(t, "queue_messages", tenantA, topic) == 0
	}, "acked tenant message was garbage-collected")

	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_messages", tenantB, topic))
	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_delivery_state", tenantB, topic))
	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_offsets", tenantB, topic))
	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_partition_leases", tenantB, topic))
	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_subscriber_heartbeats", tenantB, topic))

	var tenantBOffset int64
	err = s.db.QueryRowContext(
		s.ctx,
		"SELECT offset_acked FROM queue_offsets WHERE tenant = ? AND topic = ? AND partition_key = ? AND consumer_group = ?",
		tenantB,
		topic,
		partitionKey,
		consumerGroup,
	).Scan(&tenantBOffset)
	require.NoError(t, err)
	assert.Zero(t, tenantBOffset)
}

func (s *SQLQueueIntegrationSuite) TestTenantIsolationWhenMovingToDLQ() {
	t := s.T()

	const (
		tenantA      = "tenant-dlq-a"
		tenantB      = "tenant-dlq-b"
		topic        = "tenant_isolation_dlq_topic"
		partitionKey = "shared-partition"
		messageID    = "shared-message"
	)

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
		Tenants:      []string{tenantA, tenantB},
	})
	require.NoError(t, err)
	defer q.Close()

	cfg := testSubConfig("tenant-dlq-worker", "tenant-dlq-consumer")
	cfg.VisibilityTimeoutMs = cfg.LeaseDurationMs * 10
	deliveries, err := q.Subscriber().Subscribe(s.ctx, topic, cfg)
	require.NoError(t, err)

	for _, tenant := range []string{tenantA, tenantB} {
		msg := entityqueue.NewMessage(messageID, []byte(tenant), partitionKey, nil)
		msg.Tenant = tenant
		require.NoError(t, q.Publisher().Publish(s.ctx, topic, msg))
	}

	receivedDeliveries := make(map[string]extqueue.Delivery, 2)
	receiveN(t, deliveries, 2, func(delivery extqueue.Delivery, _ int) {
		receivedDeliveries[delivery.Message().Tenant] = delivery
	})

	reason := failure.New("tenant-scoped failure", failure.Subject{Type: "message", ID: messageID})
	require.NoError(t, receivedDeliveries[tenantA].Reject(s.ctx, reason))

	assert.Zero(t, s.tenantTopicRowCount(t, "queue_messages", tenantA, topic))
	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_messages", tenantA, topic+"_dlq"))
	assert.Equal(t, 1, s.tenantTopicRowCount(t, "queue_messages", tenantB, topic))
	assert.Zero(t, s.tenantTopicRowCount(t, "queue_messages", tenantB, topic+"_dlq"))

	require.NoError(t, receivedDeliveries[tenantB].Ack(s.ctx))
}
