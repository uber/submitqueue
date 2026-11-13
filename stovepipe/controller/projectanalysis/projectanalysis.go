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

// Package projectanalysis records project statuses and issue attribution after build analysis.
package projectanalysis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/stovepipe/core/hookevent"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	projectissuescore "github.com/uber/submitqueue/stovepipe/core/projectissues"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/core/validationfact"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectissues"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

type Controller struct {
	logger                   *zap.SugaredLogger
	scope                    tally.Scope
	stores                   storage.Factory
	requestLogMaterializer   requestlog.Materializer
	projectIssueMaterializer projectissuescore.Materializer
	resolvers                projectstatus.ResolverFactory
	attributers              projectissues.AttributerFactory
	registry                 consumer.TopicRegistry
	topicKey                 consumer.TopicKey
	consumerGroup            string
}

func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	requestLogMaterializer requestlog.Materializer,
	resolvers projectstatus.ResolverFactory,
	attributers projectissues.AttributerFactory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:                   logger.Named("projectanalysis_controller"),
		scope:                    scope.SubScope("projectanalysis_controller"),
		stores:                   stores,
		requestLogMaterializer:   requestLogMaterializer,
		projectIssueMaterializer: projectissuescore.NewMaterializer(scope),
		resolvers:                resolvers,
		attributers:              attributers,
		registry:                 registry,
		topicKey:                 topicKey,
		consumerGroup:            consumerGroup,
	}
}

func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.scope, "process", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()
	analysis := &stovepipemq.ProjectAnalysis{}
	if err := stovepipemq.Unmarshal(msg.Payload, analysis); err != nil {
		return fmt.Errorf("deserialize project analysis: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, analysis.GetQueueName()); err != nil {
		return fmt.Errorf("invalid project analysis identity: %w", err)
	}
	if err := id.Validate(analysis.GetId()); err != nil {
		return fmt.Errorf("invalid project analysis request ID: %w", err)
	}
	if msg.PartitionKey != analysis.GetId() {
		return fmt.Errorf("project analysis must be partitioned by request ID")
	}
	store, err := c.stores.For(storage.Config{QueueName: analysis.GetQueueName()})
	if err != nil {
		return fmt.Errorf("resolve project analysis storage: %w", err)
	}
	request, err := loader.ByID(ctx, analysis.GetId(), store.GetRequestStore().Get, "project analysis request")
	if err != nil {
		return err
	}
	if request.Queue != analysis.GetQueueName() {
		return fmt.Errorf("project analysis request belongs to another queue")
	}
	switch request.State {
	case entity.RequestStateSucceeded, entity.RequestStateFailed:
	case entity.RequestStateCancelled:
		return publishRecord(ctx, c.registry, request)
	case entity.RequestStateSuperseded:
		return nil
	default:
		return fmt.Errorf("project analysis requires a terminal request")
	}
	history := store.GetRequestLogStore()
	projectAnalysisAbandonedLog := requestlog.NewRequestEventLog(request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
	retained, err := history.Get(ctx, request.ID, projectAnalysisAbandonedLog.ID)
	if err == nil {
		return c.requestLogMaterializer.PersistLog(ctx, store, retained)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("read project analysis abandonment: %w", err)
	}
	if request.TerminalBuildID == "" {
		return fmt.Errorf("project analysis requires the request's terminal build")
	}

	projectAnalysisCompletedLog := requestlog.NewRequestEventLog(request, entity.RequestEventProjectAnalysisCompleted, request.TerminalBuildID, nil)
	retained, err = history.Get(ctx, request.ID, projectAnalysisCompletedLog.ID)
	if err == nil {
		if err := c.requestLogMaterializer.PersistLog(ctx, store, retained); err != nil {
			return err
		}
		return c.publishCompletion(ctx, request, retained)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("read project analysis completion: %w", err)
	}
	buildAnalysisCompletedLog := requestlog.NewRequestEventLog(request, entity.RequestEventBuildAnalysisCompleted, request.TerminalBuildID, nil)
	if _, err := history.Get(ctx, request.ID, buildAnalysisCompletedLog.ID); err != nil {
		return fmt.Errorf("require completed terminal build analysis: %w", err)
	}

	if err := c.recordProjectStatuses(ctx, store, request); err != nil {
		return err
	}
	if err := c.recordIssueAttribution(ctx, store, request); err != nil {
		return err
	}
	if err := c.requestLogMaterializer.PersistLog(ctx, store, projectAnalysisCompletedLog); err != nil {
		return fmt.Errorf("record project analysis completion: %w", err)
	}
	retained, err = history.Get(ctx, request.ID, projectAnalysisCompletedLog.ID)
	if err != nil {
		return fmt.Errorf("read retained project analysis completion: %w", err)
	}
	return c.publishCompletion(ctx, request, retained)
}

func (c *Controller) publishCompletion(ctx context.Context, request entity.Request, log entity.RequestLog) error {
	event := hookevent.NewProjectAnalysisCompleted(log)
	if err := hook.Publish(ctx, c.registry, request.Queue, event, request.ID); err != nil {
		return fmt.Errorf("publish project analysis completion: %w", err)
	}
	return publishRecord(ctx, c.registry, request)
}

func (c *Controller) recordProjectStatuses(ctx context.Context, store storage.Storage, request entity.Request) (retErr error) {
	op := metrics.Begin(c.scope, "record_project_statuses", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	resolver, err := c.resolvers.For(projectstatus.ResolverConfig{QueueName: request.Queue})
	if err != nil {
		return fmt.Errorf("resolve project status resolver for queue %q: %w", request.Queue, err)
	}
	projectStatuses, err := resolver.Resolve(ctx, request)
	if err != nil {
		return fmt.Errorf("resolve project statuses for request %q: %w", request.ID, err)
	}

	if err := validateProjectStatuses(request, projectStatuses); err != nil {
		return err
	}
	metrics.NamedHistogram(c.scope, "record_project_statuses", "projects", metrics.CountBuckets,
		metrics.TagsFromContext(ctx)...,
	).RecordValue(float64(len(projectStatuses)))

	brokenCount := 0
	for _, status := range projectStatuses {
		fact, _, err := validationfact.Persist(ctx, c.logger, c.scope, store.GetValidationFactStore(), entity.ValidationFact{
			URI:         request.URI,
			Project:     status.ProjectInfo.Name,
			ProjectType: status.ProjectInfo.ProjectType,
			Degree:      status.Degree,
			RequestID:   request.ID,
			CreatedAt:   time.Now().UnixMilli(),
		})
		if err != nil {
			return err
		}
		if !fact.IsGreen() {
			brokenCount++
		}

		entry, err := entity.NewProjectFactByDegree(fact)
		if err != nil {
			return err
		}

		if err := store.GetProjectFactByDegreeStore().Create(ctx, entry); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("failed to index project fact for request %q and project %q: %w", request.ID, fact.Project, err)
		}
	}
	projectFactsRecordedLog := requestlog.NewRequestEventLog(
		request,
		entity.RequestEventProjectFactsRecorded,
		"all",
		map[string]string{
			requestlog.MetadataKeyProjectFactCount:       strconv.Itoa(len(projectStatuses)),
			requestlog.MetadataKeyBrokenProjectFactCount: strconv.Itoa(brokenCount),
		},
	)
	if err := c.requestLogMaterializer.PersistLog(ctx, store, projectFactsRecordedLog); err != nil {
		return fmt.Errorf("failed to record project facts for request %s: %w", request.ID, err)
	}
	return nil
}

func (c *Controller) recordIssueAttribution(ctx context.Context, store storage.Storage, request entity.Request) error {
	attributer, err := c.attributers.For(projectissues.AttributerConfig{QueueName: request.Queue})
	if err != nil {
		return fmt.Errorf("resolve project issue attributer: %w", err)
	}
	attributions, err := attributer.Attribute(ctx, request)
	if err != nil {
		return fmt.Errorf("attribute issues for request %q: %w", request.ID, err)
	}
	for _, attribution := range attributions {
		if attribution.TaskID == "" {
			return fmt.Errorf("project issue attribution requires a task ID")
		}
		if attribution.UnitID == "" {
			return fmt.Errorf("project issue attribution requires a unit-level issue")
		}
		for _, project := range attribution.Projects {
			entry := entity.ProjectIssueAttribution{TaskID: attribution.TaskID, UnitID: attribution.UnitID, Project: project}
			if err := c.projectIssueMaterializer.PersistAttribution(ctx, store, request.ID, entry); err != nil {
				return err
			}
		}
	}
	projectIssuesRecordedLog := requestlog.NewRequestEventLog(request, entity.RequestEventProjectIssuesRecorded, request.TerminalBuildID, nil)
	if err := c.requestLogMaterializer.PersistLog(ctx, store, projectIssuesRecordedLog); err != nil {
		return fmt.Errorf("record project issue completion: %w", err)
	}
	return nil
}

func validateProjectStatuses(request entity.Request, projectStatuses []projectstatus.Result) error {
	seen := make(map[string]struct{}, len(projectStatuses))
	for _, status := range projectStatuses {
		if status.ProjectInfo.Name == "" {
			return fmt.Errorf("project status for request %q has an empty project", request.ID)
		}
		if _, ok := seen[status.ProjectInfo.Name]; ok {
			return fmt.Errorf("project status for request %q contains duplicate project %q", request.ID, status.ProjectInfo.Name)
		}
		seen[status.ProjectInfo.Name] = struct{}{}
		if math.IsNaN(status.Degree) || status.Degree < entity.DegreeGreen || status.Degree > entity.DegreeBroken {
			return fmt.Errorf("project status for request %q and project %q has invalid degree %v", request.ID, status.ProjectInfo.Name, status.Degree)
		}
	}
	return nil
}

func publishRecord(ctx context.Context, registry consumer.TopicRegistry, request entity.Request) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: request.ID, QueueName: request.Queue})
	if err != nil {
		return fmt.Errorf("serialize repository recording request: %w", err)
	}
	return publish.Message(ctx, registry, stovepipemq.TopicKeyRecord, publish.MessageParams{
		Tenant:       request.Queue,
		ID:           publish.IntentID(request.ID),
		Payload:      payload,
		PartitionKey: request.ID,
	})
}

func (c *Controller) Name() string                { return "projectanalysis" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*Controller)(nil)
