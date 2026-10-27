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

// Package buildanalysis accumulates issue observations during and after builds.
// Build status is read from storage; status polling remains separate.
package buildanalysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildissue"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// PollDelayMs is the delay in milliseconds between successful polling passes.
// Two minutes limits provider traffic during long-running builds.
const PollDelayMs int64 = 120000

// Controller accumulates build issues while builds run and after completion.
// Terminal analysis must finish persistence and publication before its delivery completes.
type Controller struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	stores          storage.Factory
	detectorFactory buildissue.DetectorFactory
	registry        consumer.TopicRegistry
	topicKey        consumer.TopicKey
	consumerGroup   string
}

func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	detectorFactory buildissue.DetectorFactory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:          logger.Named("buildanalysis_controller"),
		metricsScope:    scope.SubScope("buildanalysis_controller"),
		stores:          stores,
		detectorFactory: detectorFactory,
		registry:        registry,
		topicKey:        topicKey,
		consumerGroup:   consumerGroup,
	}
}

// Process persists before publication and holds while further analysis is needed.
// Errors use normal queue retry or DLQ handling; partial writes are safe to retry.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()
	analysis := &stovepipemq.BuildAnalysis{}
	if err := stovepipemq.Unmarshal(msg.Payload, analysis); err != nil {
		return fmt.Errorf("failed to deserialize build analysis: %w", err)
	}

	if err := entityqueue.ValidatePayloadQueue(msg, analysis.GetQueueName()); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}

	if analysis.GetId() == "" {
		return fmt.Errorf("build analysis requires a build ID")
	}
	if msg.PartitionKey != analysis.GetId() {
		return fmt.Errorf("build analysis must be partitioned by build %q", analysis.GetId())
	}

	store, err := c.stores.For(storage.Config{QueueName: analysis.GetQueueName()})
	if err != nil {
		return fmt.Errorf("failed to resolve storage for queue %q: %w", analysis.GetQueueName(), err)
	}

	build, err := loader.ByID(ctx, analysis.GetId(), store.GetBuildStore().Get, "build")
	if err != nil {
		return err
	}

	request, err := loader.ByID(ctx, build.RequestID, store.GetRequestStore().Get, "request")
	if err != nil {
		return err
	}

	if request.Queue != analysis.GetQueueName() {
		return fmt.Errorf("request queue %q does not match analysis queue %q", request.Queue, analysis.GetQueueName())
	}
	if request.TerminalBuildID != "" && request.TerminalBuildID != build.ID {
		return nil
	}

	// The Build may finish between the initial Build read and the Request read.
	// Recheck its status so stopping here does not skip final analysis.
	if !build.Status.IsTerminal() && request.State.IsTerminal() {
		build, err = loader.ByID(ctx, analysis.GetId(), store.GetBuildStore().Get, "build")
		if err != nil {
			return err
		}

		if !build.Status.IsTerminal() {
			c.logger.Warnw("stopping analysis for a terminal request with nonterminal build",
				"queue", request.Queue, "build_id", build.ID, "request_id", request.ID, "request_state", string(request.State))
			return nil
		}
	}

	detector, err := c.detectorFactory.For(buildissue.DetectorConfig{QueueName: analysis.GetQueueName()})
	if err != nil {
		return fmt.Errorf("failed to resolve issue detector for queue %q: %w", analysis.GetQueueName(), err)
	}

	issues, err := detector.Detect(ctx, entity.BuildID{ID: build.ID})
	if err != nil {
		return fmt.Errorf("failed to detect issues for build %q: %w", build.ID, err)
	}

	issueStore := store.GetBuildIssueStore()
	for _, issue := range issues {
		if err := issueStore.Create(ctx, build.ID, issue); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to persist build issue build=%q task=%q unit=%q: %w", build.ID, issue.TaskID, issue.UnitID, err)
		}
	}

	hold := !build.Status.IsTerminal()
	finalize := false
	if build.Status.IsTerminal() {
		// Build status can become terminal before buildsignal records the winning build.
		request, err = loader.ByID(ctx, build.RequestID, store.GetRequestStore().Get, "request")
		if err != nil {
			return err
		}
		if request.Queue != analysis.GetQueueName() {
			return fmt.Errorf("request queue %q does not match analysis queue %q", request.Queue, analysis.GetQueueName())
		}
		if request.TerminalBuildID != "" && request.TerminalBuildID != build.ID {
			return nil
		}
		finalize = request.TerminalBuildID == build.ID
		hold = !finalize && !request.State.IsTerminal()
	}

	if err := c.publishIssueTracking(ctx, request.Queue, build.ID, finalize); err != nil {
		return err
	}

	if hold {
		delivery.Hold(PollDelayMs)
	}

	c.logger.Debugw("build analysis pass complete",
		"queue", analysis.GetQueueName(),
		"build_id", build.ID,
		"status", string(build.Status),
		"observed_issue_count", len(issues),
	)

	return nil
}

func (c *Controller) publishIssueTracking(ctx context.Context, queue, buildID string, finalize bool) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.IssueTrack{
		BuildId: buildID, QueueName: queue, Finalize: finalize,
	})
	if err != nil {
		return fmt.Errorf("serialize issue tracking notification: %w", err)
	}

	// Polls reuse one delivery, so no stable occurrence ID distinguishes refreshes.
	messageID := publish.UniqueID(buildID)
	if finalize {
		messageID = publish.IntentID(buildID, "finished")
	}
	if err := publish.Message(ctx, c.registry, stovepipemq.TopicKeyIssueTrack, publish.MessageParams{
		Tenant: queue, ID: messageID, Payload: payload, PartitionKey: queue,
	}); err != nil {
		return fmt.Errorf("publish issue tracking notification: %w", err)
	}
	return nil
}

func (c *Controller) Name() string { return "buildanalysis" }

func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }

func (c *Controller) ConsumerGroup() string { return c.consumerGroup }

var _ consumer.Controller = (*Controller)(nil)
