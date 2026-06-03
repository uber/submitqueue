package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	extqueue "github.com/uber/submitqueue/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/test/testutil"
)

// testController implements consumer.Controller for integration tests.
// Each test configures the processFunc to control behavior per partition.
type testController struct {
	name          string
	topicKey      consumer.TopicKey
	consumerGroup string
	processFunc   func(ctx context.Context, delivery consumer.Delivery) error
}

func (c *testController) Process(ctx context.Context, delivery consumer.Delivery) error {
	return c.processFunc(ctx, delivery)
}

func (c *testController) Name() string {
	return c.name
}

func (c *testController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

func (c *testController) ConsumerGroup() string {
	return c.consumerGroup
}

// testTimeout is the safety-net duration for channel waits in integration tests.
const testTimeout = 10 * time.Second

// stopTimeoutMs is the timeout in milliseconds for consumer.Stop().
const stopTimeoutMs = 10000

type ConsumerIntegrationSuite struct {
	suite.Suite
	ctx   context.Context
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestConsumerIntegration(t *testing.T) {
	suite.Run(t, new(ConsumerIntegrationSuite))
}

func (s *ConsumerIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Consumer integration test suite using docker-compose")

	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		s.ctx,
		"docker-compose.yml",
		"core-submitqueue-consumer",
	)

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	schemaDir := testutil.SchemaDir("extension/messagequeue/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	t.Cleanup(func() {
		if s.db != nil {
			s.db.Close()
		}
	})

	s.log.Logf("Consumer integration test suite ready")
}

func (s *ConsumerIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Consumer integration test suite")
}

// newQueue creates a new MySQL-backed queue for testing.
func (s *ConsumerIntegrationSuite) newQueue(t *testing.T) extqueue.Queue {
	t.Helper()
	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	return q
}

// newConsumer creates a consumer with a TopicRegistry wired to the given queue and topic.
func (s *ConsumerIntegrationSuite) newConsumer(t *testing.T, q extqueue.Queue, topicKey consumer.TopicKey, topicName string, consumerGroup string) consumer.Consumer {
	t.Helper()
	logger := zaptest.NewLogger(t).Sugar()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{
			Key:   topicKey,
			Name:  topicName,
			Queue: q,
			Subscription: extqueue.SubscriptionConfig{
				SubscriberName:         "test-worker",
				ConsumerGroup:          consumerGroup,
				PollIntervalMs:         100,
				BatchSize:              10,
				VisibilityTimeoutMs:    60000,
				LeaseRenewalIntervalMs: 2000,
				LeaseDurationMs:        5000,
				Retry: extqueue.RetryConfig{
					MaxAttempts:       3,
					InitialBackoffMs:  1000,
					MaxBackoffMs:      30000,
					BackoffMultiplier: 2.0,
				},
				DLQ: extqueue.DLQConfig{
					Enabled:     true,
					TopicSuffix: "_dlq",
				},
			},
		},
	})
	require.NoError(t, err)

	return consumer.New(logger, tally.NoopScope, registry)
}

func (s *ConsumerIntegrationSuite) TestConsumerPerPartitionIsolation() {
	t := s.T()

	topicKey := consumer.TopicKey("isolation-test")
	topicName := "consumer-isolation-topic"
	consumerGroup := "isolation-group"

	q := s.newQueue(t)
	defer q.Close()

	publisher := q.Publisher()

	// Channels for synchronizing the test with the controller
	partAStarted := make(chan struct{})   // signals partition-a processing began
	partAUnblock := make(chan struct{})   // unblocks partition-a processing
	partBProcessed := make(chan struct{}) // signals partition-b was processed

	ctrl := &testController{
		name:          "isolation-controller",
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
		processFunc: func(ctx context.Context, delivery consumer.Delivery) error {
			partition := delivery.Message().PartitionKey
			t.Logf("Controller processing: partition=%s id=%s", partition, delivery.Message().ID)

			if partition == "partition-a" {
				close(partAStarted)
				// Block until test unblocks us
				select {
				case <-partAUnblock:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else if partition == "partition-b" {
				close(partBProcessed)
			}
			return nil
		},
	}

	c := s.newConsumer(t, q, topicKey, topicName, consumerGroup)
	require.NoError(t, c.Register(ctrl))
	require.NoError(t, c.Start(s.ctx))

	// Publish to partition-a, wait for it to start blocking
	msgA := entityqueue.NewMessage("iso-a", []byte("data-a"), "partition-a", nil)
	require.NoError(t, publisher.Publish(s.ctx, topicName, msgA))

	select {
	case <-partAStarted:
		s.log.Logf("partition-a processing started (blocking)")
	case <-time.After(testTimeout):
		require.FailNow(t, "Timeout waiting for partition-a to start processing")
	}

	// Now publish to partition-b — should be processed even though partition-a is blocked
	msgB := entityqueue.NewMessage("iso-b", []byte("data-b"), "partition-b", nil)
	require.NoError(t, publisher.Publish(s.ctx, topicName, msgB))

	select {
	case <-partBProcessed:
		s.log.Logf("partition-b processed while partition-a was blocked")
	case <-time.After(testTimeout):
		require.FailNow(t, "Timeout waiting for partition-b: partition-a blocked it (no isolation)")
	}

	// Unblock partition-a and stop cleanly
	close(partAUnblock)
	require.NoError(t, c.Stop(stopTimeoutMs))

	s.log.Logf("Per-partition isolation verified at consumer level")
}

func (s *ConsumerIntegrationSuite) TestConsumerPartitionOrdering() {
	t := s.T()

	topicKey := consumer.TopicKey("ordering-test")
	topicName := "consumer-ordering-topic"
	consumerGroup := "ordering-group"

	q := s.newQueue(t)
	defer q.Close()

	publisher := q.Publisher()

	// Publish 5 messages to the same partition
	numMessages := 5
	publishedIDs := make([]string, numMessages)
	for i := range numMessages {
		msgID := fmt.Sprintf("order-%03d", i)
		publishedIDs[i] = msgID
		msg := entityqueue.NewMessage(msgID, []byte(fmt.Sprintf("payload-%d", i)), "single-partition", nil)
		require.NoError(t, publisher.Publish(s.ctx, topicName, msg))
	}
	s.log.Logf("Published %d messages to single-partition", numMessages)

	// Track processing order under a lock; signal allDone when complete.
	var mu sync.Mutex
	processedIDs := make([]string, 0, numMessages)
	allDone := make(chan struct{})

	ctrl := &testController{
		name:          "ordering-controller",
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
		processFunc: func(ctx context.Context, delivery consumer.Delivery) error {
			msgID := delivery.Message().ID
			t.Logf("Processing: %s", msgID)

			// Record this message's ID; when all expected messages
			// have been processed, signal completion via allDone.
			mu.Lock()
			processedIDs = append(processedIDs, msgID)
			done := len(processedIDs) == numMessages
			mu.Unlock()

			if done {
				close(allDone)
			}
			return nil
		},
	}

	c := s.newConsumer(t, q, topicKey, topicName, consumerGroup)
	require.NoError(t, c.Register(ctrl))
	require.NoError(t, c.Start(s.ctx))

	select {
	case <-allDone:
	case <-time.After(3 * testTimeout):
		require.FailNow(t, "Timeout waiting for all messages to be processed")
	}

	require.NoError(t, c.Stop(stopTimeoutMs))

	// Assert order matches publish order
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, processedIDs, numMessages)
	for i := range numMessages {
		require.Equal(t, publishedIDs[i], processedIDs[i],
			"Message at position %d out of order: expected %s, got %s",
			i, publishedIDs[i], processedIDs[i])
	}

	s.log.Logf("Partition ordering verified: all %d messages processed in FIFO order", numMessages)
}

func (s *ConsumerIntegrationSuite) TestConsumerMultiPartitionThroughput() {
	t := s.T()

	topicKey := consumer.TopicKey("throughput-test")
	topicName := "consumer-throughput-topic"
	consumerGroup := "throughput-group"

	q := s.newQueue(t)
	defer q.Close()

	publisher := q.Publisher()

	// Publish 1 message to each of 3 partitions
	numPartitions := 3
	for i := range numPartitions {
		partition := fmt.Sprintf("tp-partition-%d", i)
		msg := entityqueue.NewMessage(fmt.Sprintf("tp-msg-%d", i), []byte("data"), partition, nil)
		require.NoError(t, publisher.Publish(s.ctx, topicName, msg))
	}
	s.log.Logf("Published 1 message to each of %d partitions", numPartitions)

	// Barrier: each partition signals arrival, then waits for all partitions
	// to start before completing. Proves parallel execution without timing.
	var barrier sync.WaitGroup
	barrier.Add(numPartitions)

	var done sync.WaitGroup
	done.Add(numPartitions)

	ctrl := &testController{
		name:          "throughput-controller",
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
		processFunc: func(ctx context.Context, delivery consumer.Delivery) error {
			t.Logf("Processing partition=%s", delivery.Message().PartitionKey)
			barrier.Done() // signal: this partition started
			barrier.Wait() // block until all partitions are processing concurrently
			done.Done()
			return nil
		},
	}

	c := s.newConsumer(t, q, topicKey, topicName, consumerGroup)
	require.NoError(t, c.Register(ctrl))
	require.NoError(t, c.Start(s.ctx))

	// Wait for all partitions to complete
	allDone := make(chan struct{})
	go func() {
		done.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
	case <-time.After(3 * testTimeout):
		require.FailNow(t, "Timeout waiting for all partitions to be processed — partitions may not be running in parallel")
	}

	require.NoError(t, c.Stop(stopTimeoutMs))

	s.log.Logf("Multi-partition throughput verified: %d partitions processed concurrently", numPartitions)
}
