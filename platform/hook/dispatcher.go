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

// Package hook holds the consumer side of the hooks framework: the dispatcher
// that turns hook events on a queue into hook.Hook calls, and the reconciler for
// the events that never made it.
//
// The dispatcher is domain-neutral. Each domain runs its own hook topic and its
// own instance of this stage — "per-domain" is about the topic and the wiring,
// not about the logic, which is the same everywhere: decode, validate, invoke.
// The domain-specific parts are the topic name the host maps the key to, and the
// hook it wires.
//
// The contract this stage consumes is api/base/hook; the hooks it invokes
// implement platform/extension/hook.
package hook

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	hookext "github.com/uber/submitqueue/platform/extension/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// dispatchOp is the metric operation name shared by every emit in this file.
const dispatchOp = "dispatch"

// unknownTagValue stands in for an envelope field that could not be read, so a
// metric series exists for events that failed before they could be attributed.
const unknownTagValue = "unknown"

// Dispatcher consumes hook events and hands each to the host's hook.
type Dispatcher struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	hook          hookext.Hook
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*Dispatcher)(nil)

// NewDispatcher builds the hook dispatcher for a host. A host with no
// integrations wires the noop hook rather than skipping the stage, so that "off"
// and "lost" stay distinguishable.
func NewDispatcher(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	h hookext.Hook,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Dispatcher {
	name := string(topicKey) + "_controller"
	return &Dispatcher{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		hook:          h,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process decodes the delivery's hook event, validates it, and invokes the
// host's hook. Returns nil to ack, or an error to nack (retry) / reject (DLQ).
//
// A hook that does not care about this event returns nil, so an ack here means
// "no hook still has work to do with it", not "something acted on it".
func (d *Dispatcher) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	event := &basehook.HookEvent{}
	if err := basehook.Unmarshal(msg.Payload, event); err != nil {
		metrics.NamedCounter(d.metricsScope, dispatchOp, "deserialize_errors", 1)
		// Non-retryable: bytes that are not a hook event will not become one.
		return fmt.Errorf("failed to deserialize hook event: %w", err)
	}

	if err := basehook.Validate(event); err != nil {
		metrics.NamedCounter(d.metricsScope, dispatchOp, "invalid_events", 1)
		// Non-retryable: nothing downstream can supply an envelope field the
		// publisher omitted. Dead-lettering it is what keeps a malformed event
		// visible instead of silently acked.
		return fmt.Errorf("refusing to dispatch malformed hook event: %w", err)
	}

	tags := []metrics.Tag{
		metrics.NewTag("source", event.GetSource()),
		metrics.NewTag("event_type", event.GetType()),
	}

	if err := d.hook.Handle(ctx, event); err != nil {
		metrics.NamedCounter(d.metricsScope, dispatchOp, "hook_errors", 1, tags...)
		return fmt.Errorf("hook %s failed to handle event %s: %w", d.hook.Name(), event.GetId(), err)
	}

	metrics.NamedCounter(d.metricsScope, dispatchOp, "handled", 1, tags...)
	d.logger.Debugw("dispatched hook event",
		"event_id", event.GetId(),
		"source", event.GetSource(),
		"event_type", event.GetType(),
		"version", event.GetVersion(),
		"hook", d.hook.Name(),
	)
	return nil
}

// Name returns the controller name for logging and metrics.
func (d *Dispatcher) Name() string {
	return string(d.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (d *Dispatcher) TopicKey() consumer.TopicKey {
	return d.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (d *Dispatcher) ConsumerGroup() string {
	return d.consumerGroup
}
