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
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
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
	issueTrackingQueue    = "e2e-stovepipe/issue-tracking"
	issueTrackingDLQQueue = "e2e-stovepipe/issue-tracking-dlq"
)

func (s *StovepipeE2ESuite) awaitIssueTracking(queue string, phase entity.IssueTrackingPhase) entity.IssueTrackingState {
	s.T().Helper()
	store, err := s.appStorage.For(queue)
	require.NoError(s.T(), err)
	var progress entity.IssueTrackingState
	pollUntil(convergencePollInterval, func() bool {
		progress, err = store.GetIssueTrackingStore().Get(s.ctx)
		if storage.IsNotFound(err) {
			return false
		}
		require.NoError(s.T(), err)
		s.log.Logf("issue tracking queue=%s build=%s phase=%s (want %s)", queue, progress.BuildID, progress.Phase, phase)
		return progress.Phase == phase
	})
	return progress
}

func (s *StovepipeE2ESuite) TestIssueTracking_FinalizesAfterAnalysis() {
	t := s.T()
	requestID := s.ingest(issueTrackingQueue)
	progress := s.awaitIssueTracking(issueTrackingQueue, entity.IssueTrackingPhaseFinalized)
	require.Equal(t, requestID, progress.RequestID)
	require.True(t, progress.Finalize)

	store, err := s.appStorage.For(issueTrackingQueue)
	require.NoError(t, err)
	request, err := store.GetRequestStore().Get(s.ctx, requestID)
	require.NoError(t, err)
	require.Equal(t, entity.RequestStateSucceeded, request.State)
	require.Equal(t, request.TerminalBuildID, progress.BuildID)
	issues, err := store.GetIssueStateStore().List(s.ctx, storage.IssueStateRange{Limit: 1})
	require.NoError(t, err)
	require.Empty(t, issues)
}

func (s *StovepipeE2ESuite) TestIssueTracking_DLQAbandonsPrematureFinalNotification() {
	t := s.T()
	const queue = issueTrackingDLQQueue
	store, err := s.appStorage.For(queue)
	require.NoError(t, err)
	request := entity.Request{
		ID: "1", Queue: queue, URI: "git://" + queue + "/HEAD",
		State: entity.RequestStateProcessing, BuildStrategy: entity.BuildStrategyFull, Version: 1,
	}
	build := entity.Build{ID: "running-build", RequestID: request.ID, Status: entity.BuildStatusRunning, Version: 1}
	require.NoError(t, store.GetRequestStore().Create(s.ctx, request))
	require.NoError(t, store.GetBuildStore().Create(s.ctx, build))
	queueState, err := store.GetQueueStore().Get(s.ctx, queue)
	require.NoError(t, err)
	queueState.InFlightCount = 1
	require.NoError(t, store.GetQueueStore().Update(s.ctx, queueState, queueState.Version, queueState.Version+1))

	mq, err := queuemysql.NewQueue(queuemysql.Params{
		DB: s.queueDB, Logger: zap.NewNop(), MetricsScope: tally.NoopScope, Tenants: []string{queue},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mq.Close()) }()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyIssueTrack, Name: "issuetrack", Queue: mq},
	})
	require.NoError(t, err)
	payload, err := stovepipemq.Marshal(&stovepipemq.IssueTrack{QueueName: queue, BuildId: build.ID, Finalize: true})
	require.NoError(t, err)
	notificationID := publish.IntentID(build.ID, "premature-final")
	require.NoError(t, publish.Message(entityqueue.WithQueueName(s.ctx, queue), registry, stovepipemq.TopicKeyIssueTrack, publish.MessageParams{
		Tenant: queue, ID: notificationID, Payload: payload, PartitionKey: queue,
	}))

	progress := s.awaitIssueTracking(queue, entity.IssueTrackingPhaseAbandoned)
	require.Equal(t, notificationID, progress.NotificationID)
	require.Equal(t, build.ID, progress.BuildID)
	logs, err := store.GetRequestLogStore().List(s.ctx, request.ID)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, entity.RequestEventIssueTrackingAbandoned, logs[0].Event)
	require.Equal(t, build.ID, logs[0].Metadata[requestlog.MetadataKeyBuildID])
	retainedRequest, err := store.GetRequestStore().Get(s.ctx, request.ID)
	require.NoError(t, err)
	require.Equal(t, request, retainedRequest)
	retainedBuild, err := store.GetBuildStore().Get(s.ctx, build.ID)
	require.NoError(t, err)
	require.Equal(t, build, retainedBuild)
	require.Equal(t, int32(1), s.inFlightCount(queue))

	// The DLQ watermark survives message garbage collection and proves recovery acknowledged.
	pollUntil(convergencePollInterval, func() bool {
		var offset int64
		err := s.queueDB.QueryRowContext(s.ctx, `
			SELECT offset_acked FROM queue_offsets
			WHERE tenant = ? AND topic = ? AND consumer_group = ? AND partition_key = ?`,
			queue, "issuetrack_dlq", "stovepipe-issuetrack-dlq", queue).Scan(&offset)
		return err == nil && offset > 0
	})
}
