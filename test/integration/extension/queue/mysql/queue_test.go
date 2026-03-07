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

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
	queueMySQL "github.com/uber/submitqueue/extension/queue/mysql"
	queueAdmin "github.com/uber/submitqueue/extension/queue/mysql/ctl/lib"
	"github.com/uber/submitqueue/test/testutil"
)

// testTimeout is the safety-net duration for channel waits in integration tests.
const testTimeout = 10 * time.Second

// Timing constants for rebalance tests. Keep poll/lease intervals short to make
// tests converge fast, but lease duration long enough that active subscribers
// don't expire each other.
const (
	rebalancePollIntervalMs         = 100
	rebalanceLeaseRenewalIntervalMs = 200
	rebalanceLeaseDurationMs        = 1000
	rebalanceConvergeTimeout        = 10 * time.Second
	rebalanceConvergePollInterval   = 200 * time.Millisecond
)

type SQLQueueIntegrationSuite struct {
	suite.Suite
	ctx   context.Context
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestSQLQueueIntegration(t *testing.T) {
	suite.Run(t, new(SQLQueueIntegrationSuite))
}

func (s *SQLQueueIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting SQL Queue integration test suite using docker-compose")

	// Use docker-compose to start MySQL (schema applied programmatically)
	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		s.ctx,
		"docker-compose.yml",
		"ext-queue-sql", // Test context for meaningful container names
	)

	// Start the compose stack (MySQL only, no schema)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to MySQL using utility
	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	s.log.Logf("Connected to MySQL for queue testing")

	// Apply schemas programmatically from directory (queue has 3 schema files)
	schemaDir := testutil.SchemaDir("extension/queue/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	s.log.Logf("Schemas applied successfully")

	t.Cleanup(func() {
		if s.db != nil {
			s.log.Logf("Closing MySQL connection")
			s.db.Close()
		}
	})

	s.log.Logf("SQL Queue integration test suite ready")
}

func (s *SQLQueueIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down SQL Queue integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// receiveWithTimeout receives a single delivery from the channel with a timeout.
// Returns the delivery or fails the test on timeout.
func receiveWithTimeout(t *testing.T, deliveryChan <-chan extqueue.Delivery, timeout time.Duration) extqueue.Delivery {
	t.Helper()
	select {
	case delivery := <-deliveryChan:
		return delivery
	case <-time.After(timeout):
		t.Fatalf("Timeout waiting for delivery after %v", timeout)
		return nil
	}
}

// receiveNWithTimeout receives N deliveries from the channel with a timeout.
// Calls the provided handler for each delivery.
func receiveNWithTimeout(
	t *testing.T,
	deliveryChan <-chan extqueue.Delivery,
	count int,
	timeout time.Duration,
	handler func(delivery extqueue.Delivery, index int),
) {
	t.Helper()
	deadline := time.After(timeout)
	for i := 0; i < count; i++ {
		select {
		case delivery := <-deliveryChan:
			handler(delivery, i)
		case <-deadline:
			t.Fatalf("Timeout waiting for message %d/%d after %v", i+1, count, timeout)
		}
	}
}

func (s *SQLQueueIntegrationSuite) TestPublishAndSubscribe() {
	t := s.T()

	// Create queue
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "test_topic"

	// Subscribe first with config
	subConfig := extqueue.DefaultSubscriptionConfig("test-worker-1", "test-consumer")
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish messages with various metadata scenarios
	msg1 := queue.NewMessage("msg-1", []byte("hello"), "partition-1", map[string]string{
		"key1":     "value1",
		"key2":     "value2",
		"trace_id": "abc123",
	})

	msg2 := queue.NewMessage("msg-2", []byte("world"), "partition-1", nil)

	err = publisher.Publish(s.ctx, topic, msg1)
	require.NoError(t, err)

	err = publisher.Publish(s.ctx, topic, msg2)
	require.NoError(t, err)

	t.Logf("Published 2 messages")

	// Receive and ack messages
	receiveNWithTimeout(t, deliveryChan, 2, 5*time.Second, func(delivery extqueue.Delivery, index int) {
		msg := delivery.Message()
		t.Logf("Received message: id=%s payload=%s", msg.ID, string(msg.Payload))

		if index == 0 {
			// Verify message content
			assert.Equal(t, "msg-1", msg.ID)
			assert.Equal(t, []byte("hello"), msg.Payload)
			assert.Equal(t, "partition-1", msg.PartitionKey)

			// Verify metadata round-trip (published metadata preserved exactly)
			assert.Equal(t, 3, len(msg.Metadata), "Should have 3 metadata keys")
			assert.Equal(t, "value1", msg.Metadata["key1"])
			assert.Equal(t, "value2", msg.Metadata["key2"])
			assert.Equal(t, "abc123", msg.Metadata["trace_id"])
		} else {
			// Verify message with nil metadata
			assert.Equal(t, "msg-2", msg.ID)
			assert.Equal(t, []byte("world"), msg.Payload)
			assert.NotNil(t, msg.Metadata, "Metadata should be initialized (not nil)")
			assert.Equal(t, 0, len(msg.Metadata), "Empty metadata should have 0 keys")
		}

		// Ack the message
		err := delivery.Ack(s.ctx)
		require.NoError(t, err)
	})

	t.Logf("Successfully received and acked 2 messages with metadata verified")
}

func (s *SQLQueueIntegrationSuite) TestSubscriberPerPartitionIsolation() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "subscriber_isolation_topic"

	// Subscribe with short poll interval for fast test
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "isolation-consumer")
	subConfig.PollIntervalMs = 100
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish 1 message to partition-a and 1 to partition-b
	msgA := queue.NewMessage("iso-msg-a", []byte("data-a"), "partition-a", nil)
	msgB := queue.NewMessage("iso-msg-b", []byte("data-b"), "partition-b", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msgA))
	require.NoError(t, publisher.Publish(s.ctx, topic, msgB))
	t.Logf("Published 1 message to partition-a and 1 to partition-b")

	// Receive first delivery — hold it without acking (simulates slow processing)
	first := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("First delivery received: partition=%s id=%s (holding without ack)",
		first.Message().PartitionKey, first.Message().ID)

	// Receive second delivery — should arrive promptly even though first is unacked.
	// If subscriber had head-of-line blocking, this would time out.
	second := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("Second delivery received: partition=%s id=%s",
		second.Message().PartitionKey, second.Message().ID)

	// Verify both partitions are represented
	partitions := map[string]bool{
		first.Message().PartitionKey:  true,
		second.Message().PartitionKey: true,
	}
	assert.True(t, partitions["partition-a"], "should have delivery from partition-a")
	assert.True(t, partitions["partition-b"], "should have delivery from partition-b")

	// Ack both
	require.NoError(t, first.Ack(s.ctx))
	require.NoError(t, second.Ack(s.ctx))

	t.Logf("Per-partition isolation verified: slow ack on one partition did not block the other")
}

func (s *SQLQueueIntegrationSuite) TestSubscriberPartitionOrderPreserved() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "subscriber_order_topic"
	partitionKey := "ordered-part"

	// Publish 5 messages to the same partition
	numMessages := 5
	publishedIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("order-msg-%03d", i)
		publishedIDs[i] = msgID
		msg := queue.NewMessage(msgID, []byte(fmt.Sprintf("payload-%d", i)), partitionKey, nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages to partition %s", numMessages, partitionKey)

	// Subscribe and receive all
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "order-consumer")
	subConfig.PollIntervalMs = 100
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	receivedIDs := make([]string, 0, numMessages)
	receiveNWithTimeout(t, deliveryChan, numMessages, testTimeout, func(delivery extqueue.Delivery, index int) {
		msgID := delivery.Message().ID
		receivedIDs = append(receivedIDs, msgID)
		t.Logf("Received: %s", msgID)
		require.NoError(t, delivery.Ack(s.ctx))
	})

	// Assert the received order matches publish order
	for i := 0; i < numMessages; i++ {
		assert.Equal(t, publishedIDs[i], receivedIDs[i],
			"Message at position %d out of order: expected %s, got %s",
			i, publishedIDs[i], receivedIDs[i])
	}

	t.Logf("Partition ordering verified: all %d messages received in publish order", numMessages)
}

func (s *SQLQueueIntegrationSuite) TestMultiplePartitions() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "multi_partition_topic"

	// Subscribe
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "multi-partition-consumer")
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish messages to different partitions
	partitions := []string{"part-A", "part-B", "part-C"}
	expectedCount := len(partitions) * 2 // 2 messages per partition

	for _, partition := range partitions {
		msg1 := queue.NewMessage(partition+"-msg-1", []byte("data-1"), partition, nil)
		msg2 := queue.NewMessage(partition+"-msg-2", []byte("data-2"), partition, nil)

		require.NoError(t, publisher.Publish(s.ctx, topic, msg1))
		require.NoError(t, publisher.Publish(s.ctx, topic, msg2))
	}

	t.Logf("Published %d messages across %d partitions", expectedCount, len(partitions))

	// Receive all messages
	receiveNWithTimeout(t, deliveryChan, expectedCount, 10*time.Second, func(delivery extqueue.Delivery, index int) {
		msg := delivery.Message()
		t.Logf("Received: partition=%s id=%s", msg.PartitionKey, msg.ID)
		require.NoError(t, delivery.Ack(s.ctx))
	})

	t.Logf("Successfully processed all %d messages", expectedCount)
}

func (s *SQLQueueIntegrationSuite) TestVisibilityTimeoutAndRetry() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "retry_topic"

	// Use short visibility timeout for faster test
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "retry-consumer")
	subConfig.VisibilityTimeoutMs = 2000 // 2 seconds
	subConfig.PollIntervalMs = 100       // 100 milliseconds

	// Subscribe
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish a message
	msg := queue.NewMessage("retry-msg", []byte("test"), "retry-partition", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg))

	t.Logf("Published message, expecting visibility timeout retry")

	// Test 1: ExtendVisibilityTimeout allows longer processing time
	t.Logf("Test 1: ExtendVisibilityTimeout")
	firstDelivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("First delivery: attempt=%d", firstDelivery.Attempt())
	assert.Equal(t, 1, firstDelivery.Attempt())

	// Extend visibility timeout by 3 seconds
	extensionDuration := 3 * time.Second
	t.Logf("Extending visibility timeout by %v", extensionDuration)
	err = firstDelivery.ExtendVisibilityTimeout(s.ctx, extensionDuration.Milliseconds())
	require.NoError(t, err)

	// Wait for original visibility timeout to expire (but not the extended timeout)
	t.Logf("Waiting for original visibility timeout (%v) - message should NOT reappear", time.Duration(subConfig.VisibilityTimeoutMs)*time.Millisecond)
	time.Sleep(time.Duration(subConfig.VisibilityTimeoutMs)*time.Millisecond + 200*time.Millisecond)

	// Message should NOT be redelivered yet (visibility was extended)
	select {
	case <-deliveryChan:
		t.Fatal("Message should not be redelivered yet - visibility was extended")
	case <-time.After(500 * time.Millisecond):
		t.Logf("✓ Confirmed: message not redelivered during extended visibility")
	}

	// Now ack the message successfully
	t.Logf("Acking message after extended processing time")
	require.NoError(t, firstDelivery.Ack(s.ctx))

	// Test 2: Visibility timeout retry when not acked
	t.Logf("Test 2: Visibility timeout retry")

	// Publish another message
	msg2 := queue.NewMessage("retry-msg-2", []byte("test2"), "retry-partition", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg2))

	// Receive first time
	secondDelivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("Second message delivery: attempt=%d", secondDelivery.Attempt())
	assert.Equal(t, 1, secondDelivery.Attempt())
	// Don't ack - let it become visible again

	// Wait for visibility timeout to expire
	t.Logf("Waiting for visibility timeout to expire...")
	time.Sleep(time.Duration(subConfig.VisibilityTimeoutMs)*time.Millisecond + 500*time.Millisecond)

	// Receive second time (retry)
	thirdDelivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("Retry delivery: attempt=%d", thirdDelivery.Attempt())
	assert.Greater(t, thirdDelivery.Attempt(), 1, "retry count should increase")
	assert.Equal(t, "retry-msg-2", thirdDelivery.Message().ID)
	// Ack this time
	require.NoError(t, thirdDelivery.Ack(s.ctx))

	t.Logf("Successfully tested ExtendVisibilityTimeout and visibility timeout retry")
}

func (s *SQLQueueIntegrationSuite) TestNackWithDelay() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "nack_topic"

	// Subscribe
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "nack-consumer")
	subConfig.PollIntervalMs = 100 // 100 milliseconds
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish message
	msg := queue.NewMessage("nack-msg", []byte("test"), "nack-partition", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg))

	// Receive and Nack with delay
	nackDelay := 2 * time.Second

	delivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("Received message, nacking with %s delay", nackDelay)
	nackErr := delivery.Nack(s.ctx, int64(nackDelay.Milliseconds()))
	require.NoError(t, nackErr)

	// Should NOT receive immediately
	select {
	case <-deliveryChan:
		t.Fatal("Message should not be visible immediately after Nack")
	case <-time.After(500 * time.Millisecond):
		t.Logf("Confirmed message is not visible immediately")
	}

	// Wait for nack delay to expire
	time.Sleep(nackDelay)

	// Should receive again now
	delivery2 := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("Received message again after nack delay")
	assert.Equal(t, "nack-msg", delivery2.Message().ID)
	require.NoError(t, delivery2.Ack(s.ctx))
}

func (s *SQLQueueIntegrationSuite) TestIdempotentPublish() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "idempotent_topic"

	// Subscribe
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "idempotent-consumer")
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish same message twice
	msg := queue.NewMessage("same-id", []byte("duplicate"), "same-partition", nil)

	err1 := publisher.Publish(s.ctx, topic, msg)
	require.NoError(t, err1)

	err2 := publisher.Publish(s.ctx, topic, msg)
	// Second publish should fail with duplicate key error since message already exists
	require.Error(t, err2, "duplicate publish should return error")

	t.Logf("Published same message twice - second attempt correctly rejected")

	// Should only receive once
	delivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	t.Logf("Received message: %s", delivery.Message().ID)
	require.NoError(t, delivery.Ack(s.ctx))

	// Verify no second message arrives
	select {
	case <-deliveryChan:
		t.Fatal("Received duplicate message - idempotency check failed")
	case <-time.After(1 * time.Second):
		t.Logf("Confirmed: only received message once (idempotency works)")
	}
}

func (s *SQLQueueIntegrationSuite) TestConcurrentPublishers() {
	t := s.T()

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	topic := "concurrent_topic"

	// Subscribe
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "concurrent-consumer")
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish from multiple goroutines
	numPublishers := 5
	messagesPerPublisher := 3
	totalMessages := numPublishers * messagesPerPublisher

	errChan := make(chan error, totalMessages)
	for i := 0; i < numPublishers; i++ {
		go func(publisherID int) {
			for j := 0; j < messagesPerPublisher; j++ {
				msg := queue.NewMessage(
					t.Name()+"-"+string(rune(publisherID))+"-"+string(rune(j)),
					[]byte("concurrent"),
					"concurrent-partition",
					nil,
				)
				errChan <- publisher.Publish(s.ctx, topic, msg)
			}
		}(i)
	}

	// Check all publishes succeeded
	for i := 0; i < totalMessages; i++ {
		require.NoError(t, <-errChan)
	}

	t.Logf("Published %d messages concurrently", totalMessages)

	// Receive all messages
	receiveNWithTimeout(t, deliveryChan, totalMessages, 10*time.Second, func(delivery extqueue.Delivery, index int) {
		require.NoError(t, delivery.Ack(s.ctx))
	})

	t.Logf("Received all %d concurrent messages", totalMessages)
}

func (s *SQLQueueIntegrationSuite) TestCrashRecovery() {
	t := s.T()

	q1, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)

	publisher := q1.Publisher()
	subscriber1 := q1.Subscriber()

	topic := "crash_topic"

	// Use short timeouts for faster test
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "crash-consumer")
	subConfig.VisibilityTimeoutMs = 2000    // 2 seconds
	subConfig.PollIntervalMs = 100          // 100 milliseconds
	subConfig.LeaseDurationMs = 3000        // 3 seconds - short lease for testing crash recovery
	subConfig.LeaseRenewalIntervalMs = 1000 // 1 second - must be less than LeaseDuration

	// Subscribe with first worker
	deliveryChan1, err := subscriber1.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish message
	msg := queue.NewMessage("crash-msg", []byte("test-crash"), "crash-partition", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg))

	// Worker 1 receives but doesn't ack (simulating crash)
	delivery1 := receiveWithTimeout(t, deliveryChan1, 5*time.Second)
	t.Logf("Worker 1 received message but crashing without ack")
	assert.Equal(t, "crash-msg", delivery1.Message().ID)

	// Simulate crash by closing q1
	q1.Close()
	t.Logf("Worker 1 crashed (queue closed)")

	// Wait for both visibility timeout AND partition lease to expire
	waitTime := time.Duration(subConfig.LeaseDurationMs+subConfig.VisibilityTimeoutMs)*time.Millisecond + time.Second
	t.Logf("Waiting %v for lease and visibility timeout to expire", waitTime)
	time.Sleep(waitTime)

	// Start worker 2 with same consumer group
	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q2.Close()

	subscriber2 := q2.Subscriber()

	subConfig2 := extqueue.DefaultSubscriptionConfig("worker-2", "crash-consumer")
	subConfig2.VisibilityTimeoutMs = 2000    // 2 seconds
	subConfig2.PollIntervalMs = 100          // 100 milliseconds
	subConfig2.LeaseDurationMs = 3000        // 3 seconds
	subConfig2.LeaseRenewalIntervalMs = 1000 // 1 second

	deliveryChan2, err := subscriber2.Subscribe(s.ctx, topic, subConfig2)
	require.NoError(t, err)

	// Worker 2 should receive the same message (recovery)
	delivery2 := receiveWithTimeout(t, deliveryChan2, 5*time.Second)
	t.Logf("Worker 2 recovered message: attempt=%d", delivery2.Attempt())
	assert.Equal(t, "crash-msg", delivery2.Message().ID)
	assert.Greater(t, delivery2.Attempt(), 1, "should be a retry after crash")

	// Worker 2 successfully acks
	require.NoError(t, delivery2.Ack(s.ctx))
	t.Logf("Crash recovery successful: message processed by worker 2")
}

func (s *SQLQueueIntegrationSuite) TestMultipleConsumerGroups() {
	t := s.T()

	topic := "multi_group_topic"

	// Create two different consumer groups
	q1, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q1.Close()

	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q2.Close()

	publisher := q1.Publisher()
	subscriber1 := q1.Subscriber()
	subscriber2 := q2.Subscriber()

	// Subscribe both groups
	subConfig1 := extqueue.DefaultSubscriptionConfig("worker-1", "group-A")
	deliveryChan1, err := subscriber1.Subscribe(s.ctx, topic, subConfig1)
	require.NoError(t, err)

	subConfig2 := extqueue.DefaultSubscriptionConfig("worker-1", "group-B")
	deliveryChan2, err := subscriber2.Subscribe(s.ctx, topic, subConfig2)
	require.NoError(t, err)

	// Publish messages
	numMessages := 3
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("msg-%d", i)
		messageIDs[i] = msgID
		msg := queue.NewMessage(msgID, []byte(fmt.Sprintf("data-%d", i)), "partition-1", nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages to topic", numMessages)

	// Both groups should receive all messages
	group1Messages := make(map[string]bool)
	group2Messages := make(map[string]bool)

	// Receive from group A
	receiveNWithTimeout(t, deliveryChan1, numMessages, 10*time.Second, func(delivery extqueue.Delivery, index int) {
		msgID := delivery.Message().ID
		t.Logf("Group A received: %s", msgID)
		group1Messages[msgID] = true
		require.NoError(t, delivery.Ack(s.ctx))
	})

	// Receive from group B
	receiveNWithTimeout(t, deliveryChan2, numMessages, 10*time.Second, func(delivery extqueue.Delivery, index int) {
		msgID := delivery.Message().ID
		t.Logf("Group B received: %s", msgID)
		group2Messages[msgID] = true
		require.NoError(t, delivery.Ack(s.ctx))
	})

	// Verify both groups got all messages
	assert.Equal(t, numMessages, len(group1Messages), "Group A should receive all messages")
	assert.Equal(t, numMessages, len(group2Messages), "Group B should receive all messages")

	for _, msgID := range messageIDs {
		assert.True(t, group1Messages[msgID], "Group A missing message: %s", msgID)
		assert.True(t, group2Messages[msgID], "Group B missing message: %s", msgID)
	}

	t.Logf("Both consumer groups independently received all %d messages", numMessages)
}

func (s *SQLQueueIntegrationSuite) TestMultipleWorkersInConsumerGroup() {
	t := s.T()

	topic := "multi_worker_topic"
	consumerGroup := "shared-group"

	// Create two workers in same consumer group
	q1, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q1.Close()

	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q2.Close()

	publisher := q1.Publisher()
	subscriber1 := q1.Subscriber()
	subscriber2 := q2.Subscriber()

	// Subscribe both workers
	subConfig1 := extqueue.DefaultSubscriptionConfig("worker-1", consumerGroup)
	deliveryChan1, err := subscriber1.Subscribe(s.ctx, topic, subConfig1)
	require.NoError(t, err)

	subConfig2 := extqueue.DefaultSubscriptionConfig("worker-2", consumerGroup)
	deliveryChan2, err := subscriber2.Subscribe(s.ctx, topic, subConfig2)
	require.NoError(t, err)

	// Publish messages to different partitions so they can be distributed
	numMessages := 10
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("msg-%d", i)
		messageIDs[i] = msgID
		// Use different partition keys to allow distribution
		partitionKey := fmt.Sprintf("partition-%d", i%3)
		msg := queue.NewMessage(msgID, []byte(fmt.Sprintf("data-%d", i)), partitionKey, nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages to topic across multiple partitions", numMessages)

	// Collect messages from both workers concurrently
	allMessages := make(map[string]int) // msgID -> count (should be 1 for each)
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(2)

	// Worker 1 receiver
	go func() {
		defer wg.Done()
		for {
			select {
			case delivery := <-deliveryChan1:
				msgID := delivery.Message().ID
				mu.Lock()
				allMessages[msgID]++
				mu.Unlock()
				t.Logf("Worker 1 received: %s (total received: %d)", msgID, len(allMessages))
				require.NoError(t, delivery.Ack(s.ctx))

				if len(allMessages) == numMessages {
					return
				}
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()

	// Worker 2 receiver
	go func() {
		defer wg.Done()
		for {
			select {
			case delivery := <-deliveryChan2:
				msgID := delivery.Message().ID
				mu.Lock()
				allMessages[msgID]++
				mu.Unlock()
				t.Logf("Worker 2 received: %s (total received: %d)", msgID, len(allMessages))
				require.NoError(t, delivery.Ack(s.ctx))

				if len(allMessages) == numMessages {
					return
				}
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()

	wg.Wait()

	// Verify all messages received exactly once
	assert.Equal(t, numMessages, len(allMessages), "Should receive all messages")

	for _, msgID := range messageIDs {
		count, exists := allMessages[msgID]
		assert.True(t, exists, "Missing message: %s", msgID)
		assert.Equal(t, 1, count, "Message %s received %d times (expected 1)", msgID, count)
	}

	t.Logf("Load balanced: %d messages distributed across 2 workers with no duplicates", numMessages)
}

func (s *SQLQueueIntegrationSuite) TestConcurrentSubscribers() {
	t := s.T()

	topic := "concurrent_subscribers_topic"
	consumerGroup := "concurrent-group"
	numSubscribers := 3
	messagesPerSubscriber := 5
	totalMessages := numSubscribers * messagesPerSubscriber

	// Create publisher
	pubQueue, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer pubQueue.Close()

	publisher := pubQueue.Publisher()

	// Create multiple concurrent subscribers
	var queues []extqueue.Queue
	var deliveryChans []<-chan extqueue.Delivery

	for i := 0; i < numSubscribers; i++ {
		q, err := queueMySQL.NewQueue(queueMySQL.Params{
			DB:           s.db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NoopScope,
		})
		require.NoError(t, err)
		queues = append(queues, q)

		subscriber := q.Subscriber()
		subConfig := extqueue.DefaultSubscriptionConfig(fmt.Sprintf("worker-%d", i), consumerGroup)
		deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
		require.NoError(t, err)
		deliveryChans = append(deliveryChans, deliveryChan)
	}

	// Cleanup all queues
	defer func() {
		for _, q := range queues {
			q.Close()
		}
	}()

	t.Logf("Started %d concurrent subscribers", numSubscribers)

	// Publish messages to multiple partitions
	for i := 0; i < totalMessages; i++ {
		msgID := fmt.Sprintf("concurrent-msg-%d", i)
		partitionKey := fmt.Sprintf("partition-%d", i%5)
		msg := queue.NewMessage(msgID, []byte(fmt.Sprintf("data-%d", i)), partitionKey, nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages", totalMessages)

	// Collect messages from all subscribers concurrently
	allMessages := make(map[string]int) // msgID -> count
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, deliveryChan := range deliveryChans {
		wg.Add(1)
		go func(workerID int, ch <-chan extqueue.Delivery) {
			defer wg.Done()
			workerMessages := 0
			for {
				select {
				case delivery := <-ch:
					msgID := delivery.Message().ID
					mu.Lock()
					allMessages[msgID]++
					totalReceived := len(allMessages)
					mu.Unlock()

					t.Logf("Worker %d received: %s (total unique: %d)", workerID, msgID, totalReceived)
					require.NoError(t, delivery.Ack(s.ctx))
					workerMessages++

					if totalReceived >= totalMessages {
						t.Logf("Worker %d processed %d messages", workerID, workerMessages)
						return
					}
				case <-time.After(10 * time.Second):
					t.Logf("Worker %d timeout after processing %d messages", workerID, workerMessages)
					return
				}
			}
		}(i, deliveryChan)
	}

	wg.Wait()

	// Verify all messages received exactly once
	assert.Equal(t, totalMessages, len(allMessages), "Should receive all messages")

	duplicates := 0
	for msgID, count := range allMessages {
		if count > 1 {
			t.Errorf("Message %s received %d times (duplicate!)", msgID, count)
			duplicates++
		}
	}

	assert.Equal(t, 0, duplicates, "Should have no duplicate messages")
	t.Logf("Concurrent subscribers test: %d messages processed by %d workers with no duplicates", totalMessages, numSubscribers)
}

func (s *SQLQueueIntegrationSuite) TestDeadLetterQueue() {
	t := s.T()

	topic := "dlq_topic"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	// Configure with low max attempts and DLQ enabled
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "dlq-consumer")
	subConfig.PollIntervalMs = 100       // 100 milliseconds
	subConfig.VisibilityTimeoutMs = 1000 // 1 second
	subConfig.Retry.MaxAttempts = 2      // Only 2 attempts before DLQ
	subConfig.DLQ.Enabled = true

	// Subscribe to main topic
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish a message that will fail
	msg := queue.NewMessage("poison-msg", []byte("poison"), "partition-1", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg))

	t.Logf("Published poison message, will nack repeatedly")

	// Receive and nack the message MaxAttempts times
	for attempt := 1; attempt <= subConfig.Retry.MaxAttempts; attempt++ {
		delivery := receiveWithTimeout(t, deliveryChan, 10*time.Second)
		t.Logf("Attempt %d: received message, nacking", delivery.Attempt())
		assert.Equal(t, attempt, delivery.Attempt())
		assert.Equal(t, "poison-msg", delivery.Message().ID)

		// Nack without delay to retry immediately
		require.NoError(t, delivery.Nack(s.ctx, 0))

		// Wait a bit for visibility timeout
		time.Sleep(time.Duration(subConfig.VisibilityTimeoutMs)*time.Millisecond + 200*time.Millisecond)
	}

	// After MaxAttempts, message should be moved to DLQ topic
	t.Logf("Message should be moved to DLQ after %d failed attempts", subConfig.Retry.MaxAttempts)

	// Should NOT receive on main topic anymore (message moved to DLQ)
	select {
	case <-deliveryChan:
		t.Fatal("Should not receive message on main topic after max retries (should be in DLQ)")
	case <-time.After(3 * time.Second):
		t.Logf("Confirmed: message removed from main topic")
	}

	// Subscribe to DLQ topic to consume the failed message
	dlqTopic := topic + subConfig.DLQ.TopicSuffix
	t.Logf("Subscribing to DLQ topic: %s", dlqTopic)

	dlqConfig := extqueue.DefaultSubscriptionConfig("worker-1", "dlq-consumer")
	dlqDeliveryChan, err := subscriber.Subscribe(s.ctx, dlqTopic, dlqConfig)
	require.NoError(t, err)

	// Receive the message from DLQ
	dlqDelivery := receiveWithTimeout(t, dlqDeliveryChan, 10*time.Second)
	assert.Equal(t, "poison-msg", dlqDelivery.Message().ID)
	assert.Equal(t, []byte("poison"), dlqDelivery.Message().Payload)
	assert.Equal(t, "partition-1", dlqDelivery.Message().PartitionKey)

	// Verify DLQ-specific metadata is exposed through delivery metadata
	metadata := dlqDelivery.Metadata()
	assert.Contains(t, metadata, "dlq.failed_at")
	assert.Contains(t, metadata, "dlq.failure_count")
	assert.Contains(t, metadata, "dlq.last_error")
	assert.Contains(t, metadata, "dlq.original_topic")

	// Verify values
	assert.Equal(t, topic, metadata["dlq.original_topic"])
	assert.Equal(t, fmt.Sprintf("%d", subConfig.Retry.MaxAttempts), metadata["dlq.failure_count"])
	assert.Equal(t, "exceeded retry limit", metadata["dlq.last_error"])

	failedAt := metadata["dlq.failed_at"]
	failedAtInt, err := strconv.ParseInt(failedAt, 10, 64)
	require.NoError(t, err)
	assert.Greater(t, failedAtInt, int64(0), "dlq.failed_at should be a valid timestamp")

	// Acknowledge the DLQ message
	require.NoError(t, dlqDelivery.Ack(s.ctx))

	t.Logf("DLQ test successful: poison message consumed from DLQ topic '%s' with metadata: %+v", dlqTopic, metadata)
}

func (s *SQLQueueIntegrationSuite) TestMessageOrderingWithinPartition() {
	t := s.T()

	topic := "ordering_topic"
	partitionKey := "ordered-partition"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	// Subscribe first
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "ordering-consumer")
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish messages with same partition key (should be ordered)
	numMessages := 10
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("msg-%03d", i)
		messageIDs[i] = msgID
		msg := queue.NewMessage(msgID, []byte(fmt.Sprintf("order-%d", i)), partitionKey, nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages to same partition: %s", numMessages, partitionKey)

	// Receive and verify ordering
	receivedOrder := make([]string, 0, numMessages)
	receiveNWithTimeout(t, deliveryChan, numMessages, 10*time.Second, func(delivery extqueue.Delivery, index int) {
		msgID := delivery.Message().ID
		receivedOrder = append(receivedOrder, msgID)
		t.Logf("Received in order: %s", msgID)
		require.NoError(t, delivery.Ack(s.ctx))
	})

	// Verify messages received in exact publish order
	for i := 0; i < numMessages; i++ {
		assert.Equal(t, messageIDs[i], receivedOrder[i],
			"Message at position %d out of order: expected %s, got %s",
			i, messageIDs[i], receivedOrder[i])
	}

	t.Logf("FIFO ordering verified: all %d messages received in exact publish order", numMessages)
}

func (s *SQLQueueIntegrationSuite) TestLateSubscriber() {
	t := s.T()

	topic := "late_subscriber_topic"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()

	// Publish messages BEFORE subscribing
	numMessages := 5
	messageIDs := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		msgID := fmt.Sprintf("early-msg-%d", i)
		messageIDs[i] = msgID
		msg := queue.NewMessage(msgID, []byte(fmt.Sprintf("data-%d", i)), "partition-1", nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages BEFORE subscribing", numMessages)

	// Now subscribe (late subscriber)
	subscriber := q.Subscriber()
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "late-consumer")
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)
	t.Logf("Late subscriber joined after messages published")

	// Late subscriber should receive all messages
	receivedMessages := make(map[string]bool)
	receiveNWithTimeout(t, deliveryChan, numMessages, 10*time.Second, func(delivery extqueue.Delivery, index int) {
		msgID := delivery.Message().ID
		receivedMessages[msgID] = true
		t.Logf("Late subscriber received: %s", msgID)
		require.NoError(t, delivery.Ack(s.ctx))
	})

	// Verify all messages received
	assert.Equal(t, numMessages, len(receivedMessages), "Should receive all pre-published messages")
	for _, msgID := range messageIDs {
		assert.True(t, receivedMessages[msgID], "Missing message: %s", msgID)
	}

	t.Logf("Late subscriber successfully received all %d pre-published messages", numMessages)
}

func (s *SQLQueueIntegrationSuite) TestEmptyTopicSubscribe() {
	t := s.T()

	topic := "empty_topic"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	subscriber := q.Subscriber()

	// Subscribe to empty topic (no messages published yet)
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "empty-consumer")
	subConfig.PollIntervalMs = 100 // 100 milliseconds
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)
	t.Logf("Subscribed to empty topic")

	// Should not receive anything immediately
	select {
	case <-deliveryChan:
		t.Fatal("Should not receive any messages from empty topic")
	case <-time.After(1 * time.Second):
		t.Logf("Confirmed: no messages on empty topic")
	}

	// Now publish a message
	publisher := q.Publisher()
	msg := queue.NewMessage("late-msg", []byte("data"), "partition-1", nil)
	require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	t.Logf("Published message to previously-empty topic")

	// Should now receive the message
	delivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	assert.Equal(t, "late-msg", delivery.Message().ID)
	require.NoError(t, delivery.Ack(s.ctx))

	t.Logf("Successfully received message published after subscription to empty topic")
}

func (s *SQLQueueIntegrationSuite) TestGracefulShutdownDuringProcessing() {
	t := s.T()

	topic := "shutdown_topic"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	// Subscribe
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "shutdown-consumer")
	subConfig.PollIntervalMs = 100 // 100 milliseconds
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Publish messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		msg := queue.NewMessage(fmt.Sprintf("msg-%d", i), []byte("data"), "partition-1", nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}
	t.Logf("Published %d messages", numMessages)

	// Receive one message but don't ack yet (in-flight)
	delivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	inFlightMsgID := delivery.Message().ID
	t.Logf("Received in-flight message: %s (not acked yet)", inFlightMsgID)

	// Close the queue while message is in-flight
	t.Logf("Closing queue with in-flight message...")
	err = q.Close()
	require.NoError(t, err)
	t.Logf("Queue closed successfully")

	// Drain any buffered messages from the channel without acking them
	// These messages were already fetched and marked invisible
	drained := 0
drainLoop:
	for {
		select {
		case msg, ok := <-deliveryChan:
			if !ok {
				// Channel closed - this is expected
				t.Logf("✓ Delivery channel closed after draining %d buffered messages (not acked)", drained)
				break drainLoop
			}
			drained++
			// Don't ack - let them become visible again after timeout
			t.Logf("Drained buffered message (not acked): %s", msg.Message().ID)
		case <-time.After(1 * time.Second):
			t.Logf("Delivery channel not closed after draining %d messages, may still be open", drained)
			break drainLoop
		}
	}

	// Don't try to ack the in-flight message - we want it to be redelivered
	// (Acking after close might succeed and delete the message)

	// Wait for visibility timeout to expire so messages become visible again
	// All messages (in-flight + buffered) were fetched and marked invisible
	t.Logf("Waiting for visibility timeout to expire (%v) so messages become visible again...", time.Duration(subConfig.VisibilityTimeoutMs)*time.Millisecond)
	time.Sleep(time.Duration(subConfig.VisibilityTimeoutMs)*time.Millisecond + 500*time.Millisecond)

	// Start new subscriber to verify all messages are redelivered
	t.Logf("Starting new subscriber to verify message recovery...")
	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q2.Close()

	subscriber2 := q2.Subscriber()
	subConfig2 := extqueue.DefaultSubscriptionConfig("worker-1", "shutdown-consumer")
	deliveryChan2, err := subscriber2.Subscribe(s.ctx, topic, subConfig2)
	require.NoError(t, err)

	// Receive all unprocessed messages (all should be redelivered after visibility timeout)
	receivedIDs := make(map[string]bool)
	expectedMessages := 1 + drained // in-flight + drained buffered messages
	if expectedMessages == 0 {
		expectedMessages = numMessages // fallback if nothing was drained
	}

	for i := 0; i < expectedMessages; i++ {
		delivery := receiveWithTimeout(t, deliveryChan2, 10*time.Second)
		msgID := delivery.Message().ID
		receivedIDs[msgID] = true
		t.Logf("Recovered message %d/%d: %s", i+1, expectedMessages, msgID)
		require.NoError(t, delivery.Ack(s.ctx))
	}

	// Verify the in-flight message was redelivered
	assert.True(t, receivedIDs[inFlightMsgID], "In-flight message should be redelivered")
	assert.GreaterOrEqual(t, len(receivedIDs), 1, "Should receive at least the in-flight message")

	t.Logf("Graceful shutdown test successful: %d messages recovered (including in-flight)", len(receivedIDs))
}

// --- Admin CLI (ctl) integration tests ---
// These tests use the publisher/subscriber to create real state,
// then verify it using AdminStore.

func (s *SQLQueueIntegrationSuite) TestAdmin_ListTopicsAfterPublish() {
	t := s.T()

	topic := "admin_list_topics_test"
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	// Publish messages
	publisher := q.Publisher()
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("msg-1", []byte("a"), "p1", nil)))
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("msg-2", []byte("b"), "p1", nil)))

	// Verify via AdminStore
	admin := queueAdmin.NewAdminStore(s.db)
	topics, err := admin.ListTopics(s.ctx)
	require.NoError(t, err)

	found := false
	for _, ti := range topics {
		if ti.Topic == topic {
			found = true
			assert.Equal(t, int64(2), ti.MessageCount)
		}
	}
	assert.True(t, found, "topic %q should appear in list-topics", topic)
}

func (s *SQLQueueIntegrationSuite) TestAdmin_TopicStatsAfterPublish() {
	t := s.T()

	topic := "admin_stats_test"
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("s1", []byte("x"), "p1", nil)))
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("s2", []byte("y"), "p2", nil)))
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("s3", []byte("z"), "p2", nil)))

	admin := queueAdmin.NewAdminStore(s.db)
	stats, err := admin.GetTopicStats(s.ctx, topic, "_dlq")
	require.NoError(t, err)

	assert.Equal(t, int64(3), stats.TotalMessages)
	assert.Equal(t, int64(3), stats.VisibleMessages)
	assert.Equal(t, int64(0), stats.InvisibleMessages)
	assert.Equal(t, int64(2), stats.PartitionCount) // p1, p2
	assert.Equal(t, int64(0), stats.DLQCount)

	t.Logf("Topic stats verified: total=%d visible=%d partitions=%d", stats.TotalMessages, stats.VisibleMessages, stats.PartitionCount)
}

func (s *SQLQueueIntegrationSuite) TestAdmin_InspectMessage() {
	t := s.T()

	topic := "admin_inspect_test"
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	metadata := map[string]string{"env": "test", "trace": "abc"}
	publisher := q.Publisher()
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("inspect-1", []byte("payload-data"), "p1", metadata)))

	admin := queueAdmin.NewAdminStore(s.db)
	detail, found, err := admin.InspectMessage(s.ctx, topic, "inspect-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "inspect-1", detail.ID)
	assert.Equal(t, topic, detail.Topic)
	assert.Equal(t, "p1", detail.PartitionKey)
	assert.Equal(t, []byte("payload-data"), detail.Payload)
	assert.Equal(t, "test", detail.Metadata["env"])
	assert.Equal(t, "abc", detail.Metadata["trace"])

	t.Logf("Inspect message verified: id=%s payload=%s metadata=%v", detail.ID, string(detail.Payload), detail.Metadata)
}

func (s *SQLQueueIntegrationSuite) TestAdmin_DeleteAndPurge() {
	t := s.T()

	topic := "admin_delete_test"
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("del-1", []byte("a"), "p1", nil)))
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("del-2", []byte("b"), "p1", nil)))
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("del-3", []byte("c"), "p1", nil)))

	admin := queueAdmin.NewAdminStore(s.db)

	// Delete single message
	affected, err := admin.DeleteMessage(s.ctx, topic, "del-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Verify it's gone
	_, found, err := admin.InspectMessage(s.ctx, topic, "del-1")
	require.NoError(t, err)
	assert.False(t, found)

	// Purge remaining
	affected, err = admin.PurgeTopic(s.ctx, topic)
	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)

	// Verify topic is empty
	msgs, err := admin.ListMessages(s.ctx, topic, "", 50)
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func (s *SQLQueueIntegrationSuite) TestAdmin_ConsumerLagAfterPartialAck() {
	t := s.T()

	topic := "admin_lag_test"
	consumerGroup := "lag-consumer"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	// Publish 5 messages to same partition
	for i := 0; i < 5; i++ {
		msg := queue.NewMessage(fmt.Sprintf("lag-%d", i), []byte("data"), "lag-partition", nil)
		require.NoError(t, publisher.Publish(s.ctx, topic, msg))
	}

	// Subscribe and ack only 2
	subConfig := extqueue.DefaultSubscriptionConfig("worker-1", consumerGroup)
	subConfig.PollIntervalMs = 100
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	receiveNWithTimeout(t, deliveryChan, 2, 5*time.Second, func(delivery extqueue.Delivery, index int) {
		require.NoError(t, delivery.Ack(s.ctx))
	})

	// Check consumer lag — should show lag > 0
	admin := queueAdmin.NewAdminStore(s.db)
	lags, err := admin.ConsumerLag(s.ctx, topic)
	require.NoError(t, err)
	require.NotEmpty(t, lags)

	var found bool
	for _, lag := range lags {
		if lag.ConsumerGroup == consumerGroup && lag.PartitionKey == "lag-partition" {
			found = true
			assert.Greater(t, lag.LatestOffset, lag.AckedOffset, "latest should be ahead of acked")
			assert.Greater(t, lag.Lag, int64(0), "lag should be positive with unacked messages")
			t.Logf("Consumer lag verified: acked=%d latest=%d lag=%d", lag.AckedOffset, lag.LatestOffset, lag.Lag)
		}
	}
	assert.True(t, found, "should find lag entry for consumer group %q", consumerGroup)
}

func (s *SQLQueueIntegrationSuite) TestAdmin_LeasesAndOffsets() {
	t := s.T()

	topic := "admin_leases_test"
	consumerGroup := "lease-consumer"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	// Publish and subscribe to create leases and offsets
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("lo-1", []byte("a"), "p1", nil)))

	subConfig := extqueue.DefaultSubscriptionConfig("admin-worker-1", consumerGroup)
	subConfig.PollIntervalMs = 100
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	// Ack the message to create offset entries
	delivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	require.NoError(t, delivery.Ack(s.ctx))

	admin := queueAdmin.NewAdminStore(s.db)

	// Verify leases are visible
	leases, err := admin.ListLeases(s.ctx)
	require.NoError(t, err)

	var leaseFound bool
	for _, l := range leases {
		if l.ConsumerGroup == consumerGroup && l.Topic == topic {
			leaseFound = true
			assert.Equal(t, "admin-worker-1", l.LeasedBy)
			assert.Greater(t, l.LeasedAt, int64(0))
			assert.Greater(t, l.LeaseRenewedAt, int64(0))
			t.Logf("Lease verified: group=%s topic=%s partition=%s leased_by=%s", l.ConsumerGroup, l.Topic, l.PartitionKey, l.LeasedBy)
		}
	}
	assert.True(t, leaseFound, "should find lease for consumer group %q", consumerGroup)

	// Verify offsets are visible
	offsets, err := admin.ListOffsets(s.ctx, consumerGroup)
	require.NoError(t, err)

	var offsetFound bool
	for _, o := range offsets {
		if o.Topic == topic {
			offsetFound = true
			assert.Greater(t, o.OffsetAcked, int64(0), "offset should be > 0 after ack")
			t.Logf("Offset verified: group=%s topic=%s partition=%s acked=%d", o.ConsumerGroup, o.Topic, o.PartitionKey, o.OffsetAcked)
		}
	}
	assert.True(t, offsetFound, "should find offset for consumer group %q", consumerGroup)
}

func (s *SQLQueueIntegrationSuite) TestAdmin_ResetOffsetAndReleaseLease() {
	t := s.T()

	topic := "admin_reset_test"
	consumerGroup := "reset-consumer"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q.Close()

	publisher := q.Publisher()
	subscriber := q.Subscriber()

	// Publish, subscribe, ack — creates offsets and leases
	require.NoError(t, publisher.Publish(s.ctx, topic, queue.NewMessage("r1", []byte("a"), "rp1", nil)))

	subConfig := extqueue.DefaultSubscriptionConfig("reset-worker", consumerGroup)
	subConfig.PollIntervalMs = 100
	deliveryChan, err := subscriber.Subscribe(s.ctx, topic, subConfig)
	require.NoError(t, err)

	delivery := receiveWithTimeout(t, deliveryChan, 5*time.Second)
	require.NoError(t, delivery.Ack(s.ctx))

	admin := queueAdmin.NewAdminStore(s.db)

	// Reset offset to 0
	affected, err := admin.ResetOffset(s.ctx, consumerGroup, topic, "rp1", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Verify offset was reset
	offsets, err := admin.ListOffsets(s.ctx, consumerGroup)
	require.NoError(t, err)
	for _, o := range offsets {
		if o.Topic == topic && o.PartitionKey == "rp1" {
			assert.Equal(t, int64(0), o.OffsetAcked, "offset should be reset to 0")
		}
	}

	// Release the lease
	affected, err = admin.ReleaseLease(s.ctx, consumerGroup, topic, "rp1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Verify lease is gone
	leases, err := admin.ListLeases(s.ctx)
	require.NoError(t, err)
	for _, l := range leases {
		if l.ConsumerGroup == consumerGroup && l.Topic == topic && l.PartitionKey == "rp1" {
			t.Errorf("lease should have been released but still exists")
		}
	}

	t.Logf("Reset offset and release lease verified")
}

// --- Rebalance integration tests ---

// getPartitionLeases queries the partition lease table and returns a map from
// subscriber name to the set of partition keys it owns for the given topic and
// consumer group.
func getPartitionLeases(db *sql.DB, topic, consumerGroup string) (map[string][]string, error) {
	rows, err := db.Query(
		"SELECT leased_by, partition_key FROM queue_partition_leases WHERE topic = ? AND consumer_group = ? ORDER BY leased_by, partition_key",
		topic, consumerGroup,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]string)
	for rows.Next() {
		var owner, pk string
		if err := rows.Scan(&owner, &pk); err != nil {
			return nil, err
		}
		result[owner] = append(result[owner], pk)
	}
	return result, nil
}

// rebalanceSubConfig returns a SubscriptionConfig tuned for fast rebalance tests.
func rebalanceSubConfig(subscriberName, consumerGroup string) extqueue.SubscriptionConfig {
	cfg := extqueue.DefaultSubscriptionConfig(subscriberName, consumerGroup)
	cfg.PollIntervalMs = rebalancePollIntervalMs
	cfg.LeaseRenewalIntervalMs = rebalanceLeaseRenewalIntervalMs
	cfg.LeaseDurationMs = rebalanceLeaseDurationMs
	return cfg
}

func (s *SQLQueueIntegrationSuite) TestRebalance_EvenDistribution() {
	t := s.T()

	topic := "rebalance_even_topic"
	consumerGroup := "rebalance-even-cg"
	partitions := []string{"pk-1", "pk-2", "pk-3", "pk-4"}

	// Publish one message per partition so they are discoverable.
	pubQ, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer pubQ.Close()

	for i, pk := range partitions {
		msg := queue.NewMessage(fmt.Sprintf("rb-even-%d", i), []byte("x"), pk, nil)
		require.NoError(t, pubQ.Publisher().Publish(s.ctx, topic, msg))
	}

	// S1: subscribe, should acquire all 4 partitions (only subscriber).
	q1, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q1.Close()

	_, err = q1.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig("s1", consumerGroup))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		return len(leases["s1"]) == 4
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "S1 should acquire all 4 partitions")

	// S2: subscribe. After rebalancing, each should own 2.
	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q2.Close()

	_, err = q2.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig("s2", consumerGroup))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		return len(leases["s1"]) == 2 && len(leases["s2"]) == 2
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "each subscriber should own exactly 2 partitions")

	t.Logf("Even distribution verified: 4 partitions split evenly across 2 subscribers")
}

func (s *SQLQueueIntegrationSuite) TestRebalance_SubscriberLeaves() {
	t := s.T()

	topic := "rebalance_leave_topic"
	consumerGroup := "rebalance-leave-cg"
	partitions := []string{"pk-1", "pk-2", "pk-3", "pk-4"}

	// Publish messages.
	pubQ, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer pubQ.Close()

	for i, pk := range partitions {
		msg := queue.NewMessage(fmt.Sprintf("rb-leave-%d", i), []byte("x"), pk, nil)
		require.NoError(t, pubQ.Publisher().Publish(s.ctx, topic, msg))
	}

	// S1 + S2 start, wait for 2+2 split.
	q1, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q1.Close()

	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	// no defer close — we close explicitly below

	_, err = q1.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig("s1", consumerGroup))
	require.NoError(t, err)
	_, err = q2.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig("s2", consumerGroup))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		return len(leases["s1"])+len(leases["s2"]) == 4 && len(leases["s1"]) == 2 && len(leases["s2"]) == 2
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "2+2 split should converge")

	// S2 leaves: close releases leases and deregisters heartbeat.
	require.NoError(t, q2.Close())

	// S1's discovery loop will detect orphaned (expired) partitions and acquire them.
	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		return len(leases["s1"]) == 4
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "S1 should reacquire all 4 partitions after S2 leaves")

	t.Logf("Subscriber leave verified: S1 owns all 4 partitions after S2 departed")
}

func (s *SQLQueueIntegrationSuite) TestRebalance_OddPartitions() {
	t := s.T()

	topic := "rebalance_odd_topic"
	consumerGroup := "rebalance-odd-cg"
	partitions := []string{"pk-1", "pk-2", "pk-3", "pk-4", "pk-5"}

	pubQ, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer pubQ.Close()

	for i, pk := range partitions {
		msg := queue.NewMessage(fmt.Sprintf("rb-odd-%d", i), []byte("x"), pk, nil)
		require.NoError(t, pubQ.Publisher().Publish(s.ctx, topic, msg))
	}

	q1, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q1.Close()

	q2, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer q2.Close()

	_, err = q1.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig("s1", consumerGroup))
	require.NoError(t, err)
	_, err = q2.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig("s2", consumerGroup))
	require.NoError(t, err)

	// maxPart = ceil(5/2) = 3. One gets 3, the other gets 2.
	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		total := len(leases["s1"]) + len(leases["s2"])
		max := len(leases["s1"])
		if len(leases["s2"]) > max {
			max = len(leases["s2"])
		}
		min := len(leases["s1"])
		if len(leases["s2"]) < min {
			min = len(leases["s2"])
		}
		return total == 5 && max == 3 && min == 2
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "5 partitions should split 3+2 across 2 subscribers")

	t.Logf("Odd partition distribution verified: 5 partitions split 3+2")
}

func (s *SQLQueueIntegrationSuite) TestRebalance_NoOrphans() {
	t := s.T()

	topic := "rebalance_orphan_topic"
	consumerGroup := "rebalance-orphan-cg"
	partitions := []string{"pk-1", "pk-2", "pk-3", "pk-4", "pk-5", "pk-6"}

	pubQ, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer pubQ.Close()

	for i, pk := range partitions {
		msg := queue.NewMessage(fmt.Sprintf("rb-orphan-%d", i), []byte("x"), pk, nil)
		require.NoError(t, pubQ.Publisher().Publish(s.ctx, topic, msg))
	}

	// 3 subscribers → 2 each.
	queues := make([]extqueue.Queue, 3)
	subNames := []string{"s1", "s2", "s3"}
	for i, name := range subNames {
		q, err := queueMySQL.NewQueue(queueMySQL.Params{
			DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
		})
		require.NoError(t, err)
		queues[i] = q
		_, err = q.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig(name, consumerGroup))
		require.NoError(t, err)
	}
	defer queues[0].Close()
	defer queues[1].Close()
	// queues[2] will be closed explicitly

	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		total := 0
		for _, pks := range leases {
			total += len(pks)
		}
		return total == 6
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "all 6 partitions should be assigned across 3 subscribers")

	// Remove S3 → remaining 2 should pick up orphans. maxPart = ceil(6/2) = 3.
	require.NoError(t, queues[2].Close())

	// S1/S2 discovery loops will detect orphaned (expired) partitions and acquire them.
	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		total := len(leases["s1"]) + len(leases["s2"])
		// s3 leases should be gone (released on close or expired)
		return total == 6 && len(leases["s3"]) == 0
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval, "remaining 2 subscribers should own all 6 partitions")

	t.Logf("No orphan partitions: all 6 reassigned after subscriber left")
}

func (s *SQLQueueIntegrationSuite) TestRebalance_MoreSubscribersThanPartitions() {
	t := s.T()

	topic := "rebalance_excess_topic"
	consumerGroup := "rebalance-excess-cg"
	partitions := []string{"pk-1", "pk-2"}

	pubQ, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	defer pubQ.Close()

	for i, pk := range partitions {
		msg := queue.NewMessage(fmt.Sprintf("rb-excess-%d", i), []byte("x"), pk, nil)
		require.NoError(t, pubQ.Publisher().Publish(s.ctx, topic, msg))
	}

	// 4 subscribers competing for 2 partitions. maxPart = ceil(2/4) = 1.
	subNames := []string{"s1", "s2", "s3", "s4"}
	var queues []extqueue.Queue
	for _, name := range subNames {
		q, err := queueMySQL.NewQueue(queueMySQL.Params{
			DB: s.db, Logger: zaptest.NewLogger(t), MetricsScope: tally.NoopScope,
		})
		require.NoError(t, err)
		queues = append(queues, q)
		_, err = q.Subscriber().Subscribe(s.ctx, topic, rebalanceSubConfig(name, consumerGroup))
		require.NoError(t, err)
	}
	defer func() {
		for _, q := range queues {
			q.Close()
		}
	}()

	require.Eventually(t, func() bool {
		leases, _ := getPartitionLeases(s.db, topic, consumerGroup)
		total := 0
		maxOwned := 0
		for _, pks := range leases {
			total += len(pks)
			if len(pks) > maxOwned {
				maxOwned = len(pks)
			}
		}
		return total == 2 && maxOwned <= 1
	}, rebalanceConvergeTimeout, rebalanceConvergePollInterval,
		"2 partitions across 4 subscribers: total=2, max per subscriber=1")

	t.Logf("More subscribers than partitions verified: 2 partitions, 4 subscribers, max 1 each")
}
