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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	queuemysql "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/publish"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const (
	projectAnalysisQueue    = "e2e-stovepipe/project-analysis"
	projectAnalysisDLQQueue = "e2e-stovepipe/project-analysis-dlq"
)

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
