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

package e2e_test

// The queue observer is the suite's push-based signal plane. The pipeline
// already narrates itself onto the queue — the orchestrator publishes every
// customer-facing status transition to the log topic, and runway publishes its
// merge-conflict-check and merge answers to the signal topics. Queue delivery
// state is keyed by (consumer_group, topic, partition_key, offset), so consumer
// groups are independent cursors: the observer subscribes with its own
// test-owned group over the published mysql-queue port and receives a copy of
// every message, without stealing anything from the services' own groups and
// without any service change.
//
// Tests therefore block on pushed events ("the cancelled status was published",
// "runway answered the check for this request") instead of polling on a guessed
// interval. Bounded-ness comes from the suite context deadline, not per-wait
// timeout constants.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaymqpb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap"
)

// observerGroup is the consumer group all observer subscriptions use. The
// compose stack (and its queue database) is created fresh per suite run, so a
// fixed name cannot collide with an earlier run's offsets.
const observerGroup = "e2e-observer"

// pipelineEvent is one message observed on a pipeline topic, reduced to the
// fields tests correlate and assert on.
type pipelineEvent struct {
	// topic is the topic key the message was observed on.
	topic consumer.TopicKey
	// requestID correlates the event to a request: RequestLog.RequestID on the
	// log topic, MergeRequest.Id on merge-conflict-check (the request sqid), and
	// MergeResult.Id on the signal topics (the sqid for check signals, the batch
	// ID for merge signals).
	requestID string
	// status is the published request status. Log topic only.
	status entity.RequestStatus
	// outcome is runway's verdict. Signal topics only.
	outcome runwaymqpb.Outcome
	// stepIDs are the step correlation IDs carried by check/signal payloads.
	// The orchestrator sets each StepId to the request sqid, so merge-signal
	// events (whose top-level Id is the batch ID) match requests through here.
	stepIDs []string
}

// eventRecorder accumulates observed events and lets callers block until an
// event matching a predicate exists — scanning history first, so an await
// placed after the fact still succeeds.
type eventRecorder struct {
	mu      sync.Mutex
	events  []pipelineEvent
	changed chan struct{} // closed and replaced on every append
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{changed: make(chan struct{})}
}

func (r *eventRecorder) add(e pipelineEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	close(r.changed)
	r.changed = make(chan struct{})
}

// await returns the first recorded event matching match, blocking for new
// events until ctx expires. Events recorded before the call are considered.
func (r *eventRecorder) await(ctx context.Context, match func(pipelineEvent) bool) (pipelineEvent, error) {
	next := 0
	for {
		r.mu.Lock()
		for ; next < len(r.events); next++ {
			if match(r.events[next]) {
				e := r.events[next]
				r.mu.Unlock()
				return e, nil
			}
		}
		changed := r.changed
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return pipelineEvent{}, ctx.Err()
		case <-changed:
		}
	}
}

// observeController is a consumer.Controller that decodes deliveries from one
// topic into pipelineEvents. It always acks: an undecodable payload is logged
// and recorded as a bare event rather than nacked, because the observer must
// never influence delivery on the topics it watches.
type observeController struct {
	name     string
	topicKey consumer.TopicKey
	decode   func(payload []byte) (pipelineEvent, error)
	recorder *eventRecorder
	log      *testutil.TestLogger
}

var _ consumer.Controller = (*observeController)(nil)

func (c *observeController) Process(_ context.Context, d consumer.Delivery) error {
	e, err := c.decode(d.Message().Payload)
	if err != nil {
		c.log.Logf("observer: undecodable payload on %s: %v", c.topicKey, err)
	}
	e.topic = c.topicKey
	c.recorder.add(e)
	return nil
}

func (c *observeController) Name() string                { return c.name }
func (c *observeController) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *observeController) ConsumerGroup() string       { return observerGroup }

// queueObserver owns the observer consumer and the recorder tests await on.
type queueObserver struct {
	recorder *eventRecorder
	consumer consumer.Consumer
	queue    extqueue.Queue
}

// startQueueObserver subscribes test-owned consumer groups to the log topic and
// runway's check/signal topics on the given queue database, and starts pumping
// decoded events into the returned recorder. The *sql.DB stays owned by the
// caller; stop() releases only the observer's own resources.
func startQueueObserver(t *testing.T, log *testutil.TestLogger, ctx context.Context, queueDB *sql.DB) (*queueObserver, error) {
	t.Helper()

	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, fmt.Errorf("failed to create observer logger: %w", err)
	}

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           queueDB,
		Logger:       logger.Named("e2e-observer"),
		MetricsScope: tally.NoopScope,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create observer queue: %w", err)
	}

	recorder := newEventRecorder()

	// The observed topic set: every wire name must match what the services
	// register (see the orchestrator and runway server wiring). merge-signal
	// events carry the batch ID as MergeResult.Id and the request sqids as
	// StepIds. The internal "merge" stage topic is deliberately not observed —
	// it multiplexes payload kinds — the merge-signal answer is the useful fact.
	topics := []struct {
		key    consumer.TopicKey
		name   string
		decode func([]byte) (pipelineEvent, error)
	}{
		{topickey.TopicKeyLog, "log", decodeRequestLog},
		{runwaymq.TopicKeyMergeConflictCheck, "merge-conflict-check", decodeMergeRequest},
		{runwaymq.TopicKeyMergeConflictCheckSignal, "merge-conflict-check-signal", decodeMergeResult},
		{runwaymq.TopicKeyMergeSignal, "merge-signal", decodeMergeResult},
	}

	configs := make([]consumer.TopicConfig, 0, len(topics))
	for _, tc := range topics {
		// The observer is a pure tap: no DLQ (its controllers never nack, and a
		// dead-letter topic under a test-only group would be noise in the queue
		// the services share).
		sub := extqueue.DefaultSubscriptionConfig(observerGroup, observerGroup)
		sub.DLQ.Enabled = false
		configs = append(configs, consumer.TopicConfig{
			Key:          tc.key,
			Name:         tc.name,
			Queue:        q,
			Subscription: sub,
		})
	}

	registry, err := consumer.NewTopicRegistry(configs)
	if err != nil {
		q.Close()
		return nil, fmt.Errorf("failed to create observer topic registry: %w", err)
	}

	// No classifiers: observer controllers never return an error, so no
	// retry/classification policy is ever exercised.
	cons := consumer.New(logger.Sugar().Named("e2e-observer"), tally.NoopScope, registry, errs.NewClassifierProcessor())
	for _, tc := range topics {
		ctl := &observeController{
			name:     fmt.Sprintf("e2e-observer-%s", tc.name),
			topicKey: tc.key,
			decode:   tc.decode,
			recorder: recorder,
			log:      log,
		}
		if err := cons.Register(ctl); err != nil {
			q.Close()
			return nil, fmt.Errorf("failed to register observer controller for %s: %w", tc.name, err)
		}
	}

	if err := cons.Start(ctx); err != nil {
		q.Close()
		return nil, fmt.Errorf("failed to start observer consumer: %w", err)
	}

	log.Logf("Queue observer started (group %s)", observerGroup)
	return &queueObserver{recorder: recorder, consumer: cons, queue: q}, nil
}

// stop shuts down the observer consumer and its queue handles. The underlying
// *sql.DB belongs to the caller and is left open.
func (o *queueObserver) stop(timeoutMs int64) error {
	stopErr := o.consumer.Stop(timeoutMs)
	closeErr := o.queue.Close()
	if stopErr != nil {
		return stopErr
	}
	return closeErr
}

func decodeRequestLog(payload []byte) (pipelineEvent, error) {
	l, err := entity.RequestLogFromBytes(payload)
	if err != nil {
		return pipelineEvent{}, fmt.Errorf("request log: %w", err)
	}
	return pipelineEvent{requestID: l.RequestID, status: l.Status}, nil
}

func decodeMergeRequest(payload []byte) (pipelineEvent, error) {
	req := &runwaymq.MergeRequest{}
	if err := runwaymq.Unmarshal(payload, req); err != nil {
		return pipelineEvent{}, fmt.Errorf("merge request: %w", err)
	}
	e := pipelineEvent{requestID: req.GetId()}
	for _, s := range req.GetSteps() {
		e.stepIDs = append(e.stepIDs, s.GetStepId())
	}
	return e, nil
}

func decodeMergeResult(payload []byte) (pipelineEvent, error) {
	res := &runwaymq.MergeResult{}
	if err := runwaymq.Unmarshal(payload, res); err != nil {
		return pipelineEvent{}, fmt.Errorf("merge result: %w", err)
	}
	e := pipelineEvent{requestID: res.GetId(), outcome: res.GetOutcome()}
	for _, s := range res.GetSteps() {
		e.stepIDs = append(e.stepIDs, s.GetStepId())
	}
	return e, nil
}
