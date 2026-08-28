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

// Package hook holds the domain-neutral mechanics of the hooks framework: the
// controller that turns hook events on a queue into hook.Hook calls, the
// reconciler for the events that never made it, and the helper a producer
// publishes an event through.
//
// The controller is domain-neutral. Each domain runs its own hook topic and its
// own instance of this stage — "per-domain" is about the topic and the wiring,
// not about the logic, which is the same everywhere: decode, validate, resolve,
// invoke. The domain-specific parts are the topic name the host maps the key to
// and the hooks its resolver returns.
//
// The contract this stage consumes is api/base/hook; the hooks it invokes
// implement platform/extension/hook.
package hook

import (
	"context"
	"fmt"
	"sync"

	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	hookext "github.com/uber/submitqueue/platform/extension/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// dispatchOp is the metric operation name shared by every emit in this file.
const dispatchOp = "dispatch"

// unknownTagValue stands in for an envelope field that could not be read, so a
// metric series exists for events that failed before they could be attributed.
const unknownTagValue = "unknown"

// Controller consumes hook events and runs the hooks each one resolves to.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	hooks         hookext.Hooks
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*Controller)(nil)

// NewController builds the hook controller for a host. A host registers the
// stage even when it has no integrations yet — a resolver that returns no hooks
// still acks, so "off" and "lost" stay distinguishable.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	hooks hookext.Hooks,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	name := string(topicKey) + "_controller"
	return &Controller{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		hooks:         hooks,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process decodes the delivery's hook event, validates it, and runs every hook
// the resolver returns for it. Returns nil to ack, or an error to nack (retry) /
// reject (DLQ).
//
// An ack means "no hook still has work to do with this event", not "something
// acted on it": a hook that does not care returns nil, and an event no hook
// resolves to is acked unhandled.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	event := &basehook.HookEvent{}
	if err := basehook.Unmarshal(msg.Payload, event); err != nil {
		metrics.NamedCounter(c.metricsScope, dispatchOp, "deserialize_errors", 1)
		// Non-retryable: bytes that are not a hook event will not become one.
		return fmt.Errorf("failed to deserialize hook event: %w", err)
	}

	if err := basehook.Validate(event); err != nil {
		metrics.NamedCounter(c.metricsScope, dispatchOp, "invalid_events", 1)
		// Non-retryable: nothing downstream can supply an envelope field the
		// publisher omitted. Dead-lettering it is what keeps a malformed event
		// visible instead of silently acked.
		return fmt.Errorf("refusing to dispatch malformed hook event: %w", err)
	}

	hooks := c.hooks.For(event)

	// Hooks are independent side effects on separate systems, so they run
	// concurrently and all of them run even after one fails: neither a slow nor a
	// broken integration can hold up the others. Failures carry the name of the
	// hook that raised them and are grouped, so the classifier weighs each one.
	failures := make([]error, len(hooks))
	var wg sync.WaitGroup
	for i, h := range hooks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Handle(ctx, event); err != nil {
				metrics.NamedCounter(c.metricsScope, dispatchOp, "hook_errors", 1,
					metrics.NewTag("source", event.GetSource()),
					metrics.NewTag("event_type", event.GetType()),
					metrics.NewTag("hook", h.Name()),
				)
				failures[i] = fmt.Errorf("hook %s: %w", h.Name(), err)
			}
		}()
	}
	wg.Wait()
	if err := errs.Group(failures...); err != nil {
		return fmt.Errorf("failed to dispatch hook event %s: %w", event.GetId(), err)
	}

	metrics.NamedCounter(c.metricsScope, dispatchOp, "handled", 1,
		metrics.NewTag("source", event.GetSource()),
		metrics.NewTag("event_type", event.GetType()),
	)
	c.logger.Debugw("dispatched hook event",
		"event_id", event.GetId(),
		"source", event.GetSource(),
		"event_type", event.GetType(),
		"version", event.GetVersion(),
		"hooks", len(hooks),
	)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
