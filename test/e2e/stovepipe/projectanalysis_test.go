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

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	queuemysql "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/stovepipe/controller/projectanalysis"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	projectissuesnoop "github.com/uber/submitqueue/stovepipe/extension/projectissues/noop"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
	projectstatusmock "github.com/uber/submitqueue/stovepipe/extension/projectstatus/mock"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcefake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	sourcemock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	projectAnalysisQueue    = "e2e-stovepipe/project-analysis"
	projectAnalysisDLQQueue = "e2e-stovepipe/project-analysis-dlq"
)

func (s *StovepipeE2ESuite) TestProjectAnalysisBatchRetryRetainsStoredResults() {
	t := s.T()
	ctrl := gomock.NewController(t)
	request := entity.Request{
		ID: "902", Queue: "e2e-stovepipe/project-analysis-batches", URI: "git://repository/batch/commit",
		State: entity.RequestStateFailed, TerminalBuildID: "build/902", Version: 1,
	}
	stores, err := s.appStorage.For(request.Queue)
	require.NoError(t, err)
	require.NoError(t, stores.GetRequestStore().Create(s.ctx, request))
	require.NoError(t, stores.GetRequestStore().Update(s.ctx, request, request.Version, request.Version+1))
	request.Version++
	history := requestlog.NewMaterializer(tally.NoopScope)
	require.NoError(t, history.PersistLog(s.ctx, stores, requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonBuildFailed)))
	require.NoError(t, history.PersistLog(s.ctx, stores, requestlog.NewRequestEventLog(request, entity.RequestEventBuildAnalysisCompleted, request.TerminalBuildID, nil)))

	statuses := make([]projectstatus.Result, 501)
	for i := range statuses {
		statuses[i] = projectstatus.Result{ProjectInfo: projectstatus.ProjectInfo{Name: fmt.Sprintf("project-%03d", i), ProjectType: "service"}, Degree: 1}
	}
	original := entity.ValidationFact{URI: request.URI, Project: "Project-000", ProjectType: "original", Degree: 0, RequestID: request.ID, CreatedAt: 10}
	require.NoError(t, stores.GetValidationFactStore().Create(s.ctx, original))
	require.NoError(t, stores.GetValidationFactStore().Create(s.ctx, entity.ValidationFact{
		URI: request.URI, Project: "unrelated", Degree: 1, RequestID: "another-request", CreatedAt: 5,
	}))

	aggregate := storagemock.NewMockStorage(ctrl)
	aggregate.EXPECT().GetRequestStore().Return(stores.GetRequestStore()).AnyTimes()
	aggregate.EXPECT().GetRequestLogStore().Return(stores.GetRequestLogStore()).AnyTimes()
	aggregate.EXPECT().GetRequestSummaryStore().Return(stores.GetRequestSummaryStore()).AnyTimes()
	aggregate.EXPECT().GetRequestAcceptanceStore().Return(stores.GetRequestAcceptanceStore()).AnyTimes()
	aggregate.EXPECT().GetValidationFactStore().Return(stores.GetValidationFactStore()).AnyTimes()
	aggregate.EXPECT().GetGreenProjectStore().Return(stores.GetGreenProjectStore()).AnyTimes()
	index := storagemock.NewMockProjectFactByDegreeStore(ctrl)
	aggregate.EXPECT().GetProjectFactByDegreeStore().Return(index).AnyTimes()
	indexCalls := 0
	writeErr := errors.New("batch index write failed")
	index.EXPECT().CreateBatch(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, entries []entity.ProjectFactByDegree) error {
		indexCalls++
		if indexCalls == 1 {
			require.NoError(t, stores.GetProjectFactByDegreeStore().CreateBatch(ctx, entries[:200]))
			return writeErr
		}
		return stores.GetProjectFactByDegreeStore().CreateBatch(ctx, entries)
	}).AnyTimes()
	factory := storagemock.NewMockFactory(ctrl)
	factory.EXPECT().For(storage.Config{QueueName: request.Queue}).Return(aggregate, nil).AnyTimes()
	resolvers := projectstatusmock.NewMockResolverFactory(ctrl)
	resolver := projectstatusmock.NewMockResolver(ctrl)
	resolvers.EXPECT().For(projectstatus.ResolverConfig{QueueName: request.Queue}).Return(resolver, nil).AnyTimes()
	resolverCalls := 0
	resolver.EXPECT().Resolve(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, entity.Request) ([]projectstatus.Result, error) {
		resolverCalls++
		return statuses, nil
	}).AnyTimes()

	publisher := queuemock.NewMockPublisher(ctrl)
	publications := 0
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, string, entityqueue.Message) error {
		publications++
		return nil
	}).AnyTimes()
	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyRecord, Name: "record", Queue: queue},
		{Key: basehook.TopicKeyHook, Name: "hook", Queue: queue},
	})
	require.NoError(t, err)
	trunkConfig := sourcecontrol.Config{QueueName: request.Queue}
	trunk := sourcefake.New(trunkConfig, []string{request.URI})
	sourceFactory := sourcemock.NewMockFactory(ctrl)
	sourceFactory.EXPECT().For(trunkConfig).Return(trunk, nil)
	controller := projectanalysis.NewController(zap.NewNop().Sugar(), tally.NoopScope, factory, history, resolvers, projectissuesnoop.New(), sourceFactory, registry, stovepipemq.TopicKeyProjectAnalysis, "batch-test")
	payload, err := stovepipemq.Marshal(&stovepipemq.ProjectAnalysis{Id: request.ID, QueueName: request.Queue})
	require.NoError(t, err)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(entityqueue.Message{ID: request.ID, Tenant: request.Queue, PartitionKey: request.ID, Payload: payload}).AnyTimes()
	require.ErrorIs(t, controller.Process(s.ctx, delivery), writeErr)
	require.Zero(t, publications)
	factsRecorded := requestlog.NewRequestEventLog(request, entity.RequestEventProjectFactsRecorded, "all", nil)
	completed := requestlog.NewRequestEventLog(request, entity.RequestEventProjectAnalysisCompleted, request.TerminalBuildID, nil)
	for _, event := range []entity.RequestLog{factsRecorded, completed} {
		_, err := stores.GetRequestLogStore().Get(s.ctx, request.ID, event.ID)
		require.ErrorIs(t, err, storage.ErrNotFound)
	}
	partial, err := stores.GetProjectFactByDegreeStore().List(s.ctx, request.URI, entity.ProjectFactByDegree{}, 1000)
	require.NoError(t, err)
	require.Len(t, partial, 200)
	for i := range statuses {
		statuses[i].Degree = 0
	}
	require.NoError(t, controller.Process(s.ctx, delivery))
	require.Equal(t, 2, publications)
	retained, err := stores.GetValidationFactStore().Get(s.ctx, request.URI, original.Project)
	require.NoError(t, err)
	require.Equal(t, original, retained)
	position, err := trunk.TrunkPosition(s.ctx, request.URI)
	require.NoError(t, err)
	greenHead, err := stores.GetGreenProjectStore().FindAtOrAfter(s.ctx, position.Lineage, original.Project, position.Position)
	require.NoError(t, err)
	require.Equal(t, request.URI, greenHead)
	greenHead, err = stores.GetGreenProjectStore().FindAtOrAfter(s.ctx, position.Lineage, statuses[1].ProjectInfo.Name, position.Position)
	require.NoError(t, err)
	require.Empty(t, greenHead)
	entries, err := stores.GetProjectFactByDegreeStore().List(s.ctx, request.URI, entity.ProjectFactByDegree{}, 1000)
	require.NoError(t, err)
	require.Len(t, entries, len(statuses))
	expectedEntry, err := entity.NewProjectFactByDegree(original)
	require.NoError(t, err)
	require.Contains(t, entries, expectedEntry)
	summary, err := stores.GetRequestSummaryStore().Get(s.ctx, request.ID)
	require.NoError(t, err)
	require.EqualValues(t, 501, summary.ProjectCount)
	require.EqualValues(t, 500, summary.BrokenProjectCount)
	require.Positive(t, summary.ProjectResultsCompletedAtMs)
	require.NoError(t, controller.Process(s.ctx, delivery))
	require.Equal(t, 2, resolverCalls)
	require.Equal(t, 2, indexCalls)
}

func (s *StovepipeE2ESuite) TestProjectAnalysisRecordsBeforeRepository() {
	t := s.T()
	id := s.ingest(projectAnalysisQueue)
	stores, err := s.appStorage.For(projectAnalysisQueue)
	require.NoError(t, err)
	request, err := stores.GetRequestStore().Get(s.ctx, id)
	require.NoError(t, err)
	s.awaitRepositoryFact(stores, request.URI)
	logs, err := stores.GetRequestLogStore().List(s.ctx, id)
	require.NoError(t, err)
	events := map[entity.RequestEvent]bool{}
	var projectAnalysisCompletedAtMs int64
	for _, log := range logs {
		events[log.Event] = true
		if log.Event == entity.RequestEventProjectAnalysisCompleted {
			projectAnalysisCompletedAtMs = log.TimestampMs
		}
	}
	for _, event := range []entity.RequestEvent{entity.RequestEventBuildAnalysisCompleted, entity.RequestEventProjectFactsRecorded, entity.RequestEventProjectIssuesRecorded, entity.RequestEventProjectAnalysisCompleted, entity.RequestEventValidationFactRecorded} {
		require.True(t, events[event], "missing event %s", event)
	}
	require.False(t, events[entity.RequestEventProjectAnalysisAbandoned])
	summary, err := stores.GetRequestSummaryStore().Get(s.ctx, id)
	require.NoError(t, err)
	require.Positive(t, summary.ProjectResultsCompletedAtMs)
	require.Equal(t, projectAnalysisCompletedAtMs, summary.ProjectResultsCompletedAtMs)
	result, err := s.client.ListRequestIssuesByProject(s.ctx, &pb.ListRequestIssuesByProjectRequest{Queue: projectAnalysisQueue, RequestId: id, Project: "project"})
	require.NoError(t, err)
	require.True(t, result.GetAttributionComplete())
	require.Empty(t, result.GetAttributions())
}

func (s *StovepipeE2ESuite) TestProjectAnalysisDLQDoesNotResumeRecording() {
	t := s.T()
	stores, err := s.appStorage.For(projectAnalysisDLQQueue)
	require.NoError(t, err)
	mq, err := queuemysql.NewQueue(queuemysql.Params{DB: s.queueDB, Logger: zap.NewNop(), MetricsScope: tally.NoopScope, Tenants: []string{projectAnalysisDLQQueue}})
	require.NoError(t, err)
	defer func() { require.NoError(t, mq.Close()) }()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: stovepipemq.TopicKeyProjectAnalysis, Name: "projectanalysis", Queue: mq}})
	require.NoError(t, err)
	for _, test := range []struct {
		id     string
		state  entity.RequestState
		reason entity.RequestOutcomeReason
	}{
		{"900", entity.RequestStateFailed, entity.RequestOutcomeReasonBuildFailed},
		{"901", entity.RequestStateSucceeded, entity.RequestOutcomeReasonBuildSucceeded},
	} {
		t.Run(string(test.state), func(t *testing.T) {
			request := entity.Request{ID: test.id, Queue: projectAnalysisDLQQueue, URI: "git://repository/project-analysis/" + test.id, State: test.state, Version: 1}
			require.NoError(t, stores.GetRequestStore().Create(s.ctx, request))
			request.TerminalBuildID = "build/" + test.id
			require.NoError(t, stores.GetRequestStore().Update(s.ctx, request, request.Version, request.Version+1))
			request.Version++
			storedRequest, err := stores.GetRequestStore().Get(s.ctx, request.ID)
			require.NoError(t, err)
			require.Equal(t, request, storedRequest)
			history := requestlog.NewMaterializer(tally.NoopScope)
			require.NoError(t, history.PersistLog(s.ctx, stores, requestlog.NewRequestStateLog(request, test.reason)))
			payload, err := stovepipemq.Marshal(&stovepipemq.ProjectAnalysis{Id: request.ID, QueueName: request.Queue})
			require.NoError(t, err)
			require.NoError(t, publish.Message(s.ctx, registry, stovepipemq.TopicKeyProjectAnalysis, publish.MessageParams{
				Tenant: request.Queue, ID: publish.IntentID(request.ID), Payload: payload, PartitionKey: request.ID,
			}))
			// Wait for recovery to acknowledge, not just retain the abandonment event.
			pollUntil(convergencePollInterval, func() bool {
				var offset int64
				err := s.queueDB.QueryRowContext(s.ctx, `
     SELECT offset_acked FROM queue_offsets
     WHERE tenant = ? AND topic = ? AND consumer_group = ? AND partition_key = ?`,
					request.Queue, "projectanalysis_dlq", "stovepipe-projectanalysis-dlq", request.ID).Scan(&offset)
				return err == nil && offset > 0
			})
			_, err = stores.GetValidationFactStore().Get(s.ctx, request.URI, "")
			require.ErrorIs(t, err, storage.ErrNotFound)
			var records int
			require.NoError(t, s.queueDB.QueryRowContext(s.ctx, `
    SELECT COUNT(*) FROM queue_messages WHERE tenant = ? AND topic = ? AND partition_key = ?`,
				request.Queue, "record", request.ID).Scan(&records))
			require.Zero(t, records)
			require.NoError(t, s.queueDB.QueryRowContext(s.ctx, `
    SELECT COUNT(*) FROM queue_offsets WHERE tenant = ? AND topic = ? AND partition_key = ?`,
				request.Queue, "record", request.ID).Scan(&records))
			require.Zero(t, records)
			retainedRequest, err := stores.GetRequestStore().Get(s.ctx, request.ID)
			require.NoError(t, err)
			require.Equal(t, request, retainedRequest)
			summary, err := stores.GetRequestSummaryStore().Get(s.ctx, request.ID)
			require.NoError(t, err)
			log := requestlog.NewRequestEventLog(request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
			retained, err := stores.GetRequestLogStore().Get(s.ctx, request.ID, log.ID)
			require.NoError(t, err)
			require.Positive(t, summary.ProjectResultsCompletedAtMs)
			require.Equal(t, retained.TimestampMs, summary.ProjectResultsCompletedAtMs)
			result, err := s.client.ListRequestIssuesByProject(s.ctx, &pb.ListRequestIssuesByProjectRequest{Queue: request.Queue, RequestId: request.ID, Project: "project"})
			require.NoError(t, err)
			require.False(t, result.GetAttributionComplete())
			require.Empty(t, result.GetAttributions())
		})
	}
}

func (s *StovepipeE2ESuite) awaitRepositoryFact(stores storage.Storage, uri string) entity.ValidationFact {
	s.T().Helper()
	var fact entity.ValidationFact
	pollUntil(convergencePollInterval, func() bool {
		var err error
		fact, err = stores.GetValidationFactStore().Get(s.ctx, uri, "")
		if storage.IsNotFound(err) {
			return false
		}
		require.NoError(s.T(), err)
		logs, err := stores.GetRequestLogStore().List(s.ctx, fact.RequestID)
		require.NoError(s.T(), err)
		for _, log := range logs {
			if log.Event == entity.RequestEventValidationFactRecorded {
				return true
			}
		}
		return false
	})
	return fact
}
