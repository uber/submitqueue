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

// Helpers for the Runway e2e suite: the client side of the land contract
// (publish a request, await the correlated result) plus the request builders
// the tests vary.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/landstrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
)

// Topic names Runway binds its topic keys to. The suite is the client on the
// other side of that contract, so it addresses topics by the same keys the
// service registers (see service/runway/server/main.go newTopicRegistry).
var (
	topicLand        = runwaymq.TopicKeyLand.String()
	topicLandSignal  = runwaymq.TopicKeyLandSignal.String()
	topicCheck       = runwaymq.TopicKeyLandConflictCheck.String()
	topicCheckSignal = runwaymq.TopicKeyLandConflictCheckSignal.String()
)

// reconciledPrefix is what the DLQ reconciler prepends to the reason of a
// result it republishes (see runway/controller/dlq). It is the only thing on
// the wire that distinguishes a failure the primary controller answered from
// one it abandoned to the dead-letter topic, so the suite reads it to tell the
// two paths apart. Asserting on the dead-letter row instead would race the
// queue's message GC, which reclaims the row once the reconciler acks it.
const reconciledPrefix = "dead-lettered: "

// reconciled reports whether a result came from the DLQ reconciler rather than
// from the primary controller.
func reconciled(result *runwaymq.LandResult) bool {
	return strings.HasPrefix(result.GetReason(), reconciledPrefix)
}

// baseURI is a well-formed change URI. Tests append a "sq-fake=<token>" marker
// to steer the fake lander; without one it lands cleanly.
const baseURI = "github://github.example.com/uber/runway-e2e/pull/1/abcdef0123456789abcdef0123456789abcdef01"

// Marker tokens the fake lander recognizes (runway/extension/lander/fake).
const (
	markerConflict = "land-conflict"
	markerInvalid  = "land-invalid"
	markerError    = "land-error"
)

// markedURI returns baseURI carrying the given fake-lander marker token.
func markedURI(token string) string {
	return baseURI + "?sq-fake=" + token
}

// observedResult is a LandResult as it arrived on a signal topic, with the
// envelope fields the tests assert on alongside the payload.
type observedResult struct {
	result       *runwaymq.LandResult
	messageID    string
	partitionKey string
}

// observer consumes one signal topic on behalf of the suite, standing in for
// the client that published the request. Deliveries are decoded and acked as
// they arrive and retained in arrival order, so a test can await its own
// correlation id without losing results belonging to another.
//
// Only the test goroutine touches an observer, so it needs no locking.
type observer struct {
	topic string
	ch    <-chan extqueue.Delivery
	seen  []observedResult
}

// mark returns a cursor into the results observed so far, for asserting later
// on exactly what arrived after some point (see newSince).
func (o *observer) mark() int { return len(o.seen) }

// newSince returns the results observed after the given cursor.
func (o *observer) newSince(mark int) []observedResult { return o.seen[mark:] }

// await blocks until a result carrying the given correlation id has arrived on
// the topic, returning it. Results for other ids are retained, so awaiting them
// later still works. There is no timeout by design: Bazel's test timeout is the
// only deadline.
func (o *observer) await(t *testing.T, ctx context.Context, id string) observedResult {
	t.Helper()

	for i := range o.seen {
		if o.seen[i].result.GetId() == id {
			return o.seen[i]
		}
	}

	for {
		delivery, ok := <-o.ch
		require.True(t, ok, "delivery channel for %s closed while awaiting %s", o.topic, id)
		require.NotNil(t, delivery, "nil delivery on %s", o.topic)

		msg := delivery.Message()
		result := &runwaymq.LandResult{}
		require.NoError(t, runwaymq.Unmarshal(msg.Payload, result),
			"failed to decode result on %s", o.topic)
		require.NoError(t, delivery.Ack(ctx), "failed to ack result on %s", o.topic)

		o.seen = append(o.seen, observedResult{
			result:       result,
			messageID:    msg.ID,
			partitionKey: msg.PartitionKey,
		})
		if result.GetId() == id {
			return o.seen[len(o.seen)-1]
		}
	}
}

// observe subscribes to a signal topic under the suite's own consumer group.
// Signal topics have no other consumer, so the suite reads them in full.
func (s *RunwayE2ESuite) observe(topic string) *observer {
	t := s.T()

	config := extqueue.DefaultSubscriptionConfig("runway-e2e-observer", "runway-e2e-observer")
	// The defaults (30s lease, 60s visibility) only slow teardown down here:
	// the suite acks every delivery as it decodes it.
	config.VisibilityTimeoutMs = 2000
	config.LeaseDurationMs = 3000
	config.LeaseRenewalIntervalMs = 1000

	ch, err := s.queue.Subscriber().Subscribe(s.ctx, topic, config)
	require.NoError(t, err, "failed to subscribe to %s", topic)

	return &observer{topic: topic, ch: ch}
}

// publish sends a land request to one of Runway's inbound topics, partitioned
// by queue name exactly as the orchestrator publishes it.
func (s *RunwayE2ESuite) publish(topic string, request *runwaymq.LandRequest) {
	t := s.T()

	payload, err := runwaymq.Marshal(request)
	require.NoError(t, err, "failed to marshal land request")

	s.publishRaw(topic, request.GetId(), request.GetQueueName(), payload)
}

// publishRaw sends arbitrary bytes to a topic, for the malformed-payload case.
func (s *RunwayE2ESuite) publishRaw(topic, id, partitionKey string, payload []byte) {
	t := s.T()

	msg := entityqueue.NewMessage(id, payload, partitionKey, nil)
	require.NoError(t, s.queue.Publisher().Publish(s.ctx, topic, msg),
		"failed to publish %s to %s", id, topic)
	s.log.Logf("published %s to %s (partition %s)", id, topic, partitionKey)
}

// landRequest builds a request with a unique correlation id. Ids must be
// unique across the suite: the queue deduplicates publishes on
// (topic, partition, id), and the correlation id is the message id on both the
// request and its result, so a reused id silently drops a publish.
func (s *RunwayE2ESuite) landRequest(queue string, steps ...*runwaymq.LandStep) *runwaymq.LandRequest {
	s.seq++
	return &runwaymq.LandRequest{
		Id:        fmt.Sprintf("%s/%d", queue, s.seq),
		QueueName: queue,
		Steps:     steps,
	}
}

// step builds one land step applying the given URIs with REBASE.
func step(id string, uris ...string) *runwaymq.LandStep {
	return &runwaymq.LandStep{
		StepId:   id,
		Change:   &changepb.Change{Uris: uris},
		Strategy: strategypb.Strategy_REBASE,
	}
}
