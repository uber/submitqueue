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

// Package pipeline provides a typed engine for assembling queue-driven
// service pipelines from declarative data. A service declares its topology
// as a []Stage[D] table and its dependencies as a Deps struct; Construct
// builds all consumers, registers controllers, pairs DLQ stages, and
// returns a single lifecycle.Component the host drives with Start/Stop.
package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	"github.com/uber/submitqueue/platform/lifecycle"
	"go.uber.org/zap"
)

// timeNow is a hook for tests to control time. Production uses time.Now.
var timeNow = time.Now

// Stage is one row of a service's topology table. D is the service's Deps type.
type Stage[D any] struct {
	// Key is the stage's logical topic key (e.g. topickey.TopicKeyStart).
	// The engine maps it to a physical topic name via the TopicNames option.
	Key consumer.TopicKey

	// Name is the physical topic name for this stage (e.g. "start").
	// Used as the default when no TopicNames override is provided.
	Name string

	// ConsumerGroup is the consumer group suffix for this stage's subscription
	// (e.g. "orchestrator-start").
	ConsumerGroup string

	// New builds the stage's controller from the service's Deps. The engine
	// calls it once, eagerly, inside Construct — so a nil/missing dependency
	// fails at boot with the stage's name on it, never mid-delivery.
	New func(D) (consumer.Controller, error)

	// DLQ, when non-nil, declares "this stage dead-letters". The engine then
	// derives the paired DLQ topic (<topic>_dlq, retry budget, DLQ-of-DLQ
	// disabled) AND registers this reconciler on the DLQ consumer. Declaring
	// one without getting the other is impossible — that's the invariant.
	DLQ func(D) (consumer.Controller, error)
}

// PublishOnlyTopic declares a topic the service publishes to but does not
// consume. The engine registers it in the TopicRegistry so controllers
// can publish to it, but creates no subscription or controller.
type PublishOnlyTopic struct {
	// Key is the logical topic key.
	Key consumer.TopicKey

	// Name is the physical topic name.
	Name string
}

// options holds the resolved configuration for a Construct call.
type options struct {
	topicNames      map[consumer.TopicKey]string
	classifiers     []errs.Classifier
	publishOnly     []PublishOnlyTopic
	extraComponents []lifecycle.Component
}

// Option configures a Construct call.
type Option func(*options)

// TopicNames provides a mapping from logical topic keys to physical topic
// names. Keys not present in the map fall back to the Stage.Name default.
func TopicNames(m map[consumer.TopicKey]string) Option {
	return func(o *options) { o.topicNames = m }
}

// Classifiers sets the error classifiers for the primary consumer's
// ErrorProcessor. DLQ consumers always use AlwaysRetryableProcessor.
func Classifiers(c ...errs.Classifier) Option {
	return func(o *options) { o.classifiers = c }
}

// PublishOnly adds topics the service publishes to but does not consume.
func PublishOnly(topics ...PublishOnlyTopic) Option {
	return func(o *options) { o.publishOnly = append(o.publishOnly, topics...) }
}

// ExtraComponents adds lifecycle components that are started before
// consumers and stopped after them.
func ExtraComponents(c ...lifecycle.Component) Option {
	return func(o *options) { o.extraComponents = append(o.extraComponents, c...) }
}

// dlqTopicKey returns the DLQ topic key for a primary stage key.
// Matches the convention in submitqueue/orchestrator/controller/dlq.TopicKey.
const dlqTopicSuffix = "_dlq"

func dlqTopicKey(primary consumer.TopicKey) consumer.TopicKey {
	return consumer.TopicKey(string(primary) + dlqTopicSuffix)
}

// Construct is the single assembly function for a queue-driven service.
// It builds the topic registry, creates primary and DLQ consumers,
// eagerly constructs all controllers, and returns a lifecycle.Component
// that starts and stops everything in the correct order.
//
// The returned Component starts in this order:
//  1. Extra components (infrastructure)
//  2. Primary consumer (work-accepting)
//  3. DLQ consumer (reconciliation)
//
// Stop reverses the order: DLQ consumer drains first, then primary, then
// infrastructure.
func Construct[D any](
	logger *zap.SugaredLogger,
	scope tally.Scope,
	queue extqueue.Queue,
	subscriberName string,
	deps D,
	stages []Stage[D],
	opts ...Option,
) (lifecycle.Component, error) {
	if len(stages) == 0 {
		return nil, fmt.Errorf("pipeline: at least one stage is required")
	}

	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	// Build topic configs for the registry.
	configs, err := buildTopicConfigs(queue, subscriberName, stages, o)
	if err != nil {
		return nil, err
	}

	registry, err := consumer.NewTopicRegistry(configs)
	if err != nil {
		return nil, fmt.Errorf("pipeline: failed to create topic registry: %w", err)
	}

	// Create the primary consumer with user-provided classifiers.
	primaryProcessor := errs.NewClassifierProcessor(o.classifiers...)
	primary := consumer.New(logger, scope, registry, primaryProcessor)

	// Create the DLQ consumer with always-retryable processor.
	dlq := consumer.New(logger, scope, registry, errs.AlwaysRetryableProcessor)

	hasDLQ := false

	// Eagerly construct and register all controllers.
	for _, s := range stages {
		ctl, err := s.New(deps)
		if err != nil {
			return nil, fmt.Errorf("pipeline: stage %s: failed to create controller: %w", s.Key, err)
		}
		if err := primary.Register(ctl); err != nil {
			return nil, fmt.Errorf("pipeline: stage %s: failed to register controller: %w", s.Key, err)
		}

		if s.DLQ != nil {
			rec, err := s.DLQ(deps)
			if err != nil {
				return nil, fmt.Errorf("pipeline: stage %s dlq: failed to create controller: %w", s.Key, err)
			}
			if err := dlq.Register(rec); err != nil {
				return nil, fmt.Errorf("pipeline: stage %s dlq: failed to register controller: %w", s.Key, err)
			}
			hasDLQ = true
		}
	}

	// Compose the lifecycle group.
	members := make([]lifecycle.Component, 0, len(o.extraComponents)+2)
	members = append(members, o.extraComponents...)
	members = append(members, &consumerComponent{name: "primary", c: primary})
	if hasDLQ {
		members = append(members, &consumerComponent{name: "dlq", c: dlq})
	}

	return lifecycle.NewGroup(members...), nil
}

// buildTopicConfigs constructs the []consumer.TopicConfig from stages and options.
func buildTopicConfigs[D any](
	queue extqueue.Queue,
	subscriberName string,
	stages []Stage[D],
	o *options,
) ([]consumer.TopicConfig, error) {
	// Pre-size: each stage gets a primary config + optional DLQ config,
	// plus publish-only topics.
	configs := make([]consumer.TopicConfig, 0, 2*len(stages)+len(o.publishOnly))

	for _, s := range stages {
		topicName := resolveTopicName(s.Key, s.Name, o.topicNames)

		configs = append(configs, consumer.TopicConfig{
			Key:   s.Key,
			Name:  topicName,
			Queue: queue,
			Subscription: extqueue.DefaultSubscriptionConfig(
				subscriberName, s.ConsumerGroup,
			),
		})

		if s.DLQ != nil {
			configs = append(configs, consumer.TopicConfig{
				Key:   dlqTopicKey(s.Key),
				Name:  topicName + dlqTopicSuffix,
				Queue: queue,
				Subscription: extqueue.DLQSubscriptionConfig(
					subscriberName, s.ConsumerGroup+"-dlq",
				),
			})
		}
	}

	for _, p := range o.publishOnly {
		topicName := resolveTopicName(p.Key, p.Name, o.topicNames)
		configs = append(configs, consumer.TopicConfig{
			Key:   p.Key,
			Name:  topicName,
			Queue: queue,
		})
	}

	return configs, nil
}

// resolveTopicName returns the override name if present, otherwise the default.
func resolveTopicName(key consumer.TopicKey, defaultName string, overrides map[consumer.TopicKey]string) string {
	if overrides != nil {
		if name, ok := overrides[key]; ok {
			return name
		}
	}
	return defaultName
}

// consumerComponent adapts consumer.Consumer to lifecycle.Component.
// Consumer.Stop takes a timeoutMs int64; Component.Stop takes a context.
// We derive the timeout from the context's deadline if set, defaulting to 30s.
type consumerComponent struct {
	name string
	c    consumer.Consumer
}

func (a *consumerComponent) Start(ctx context.Context) error {
	return a.c.Start(ctx)
}

func (a *consumerComponent) Stop(ctx context.Context) error {
	const defaultStopTimeoutMs = 30000
	timeoutMs := int64(defaultStopTimeoutMs)
	if deadline, ok := ctx.Deadline(); ok {
		remaining := deadline.Sub(timeNow())
		if remaining > 0 {
			timeoutMs = remaining.Milliseconds()
		} else {
			timeoutMs = 0
		}
	}
	return a.c.Stop(timeoutMs)
}
