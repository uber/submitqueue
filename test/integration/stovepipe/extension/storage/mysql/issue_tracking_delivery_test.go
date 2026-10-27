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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	gatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queuemysql "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/stovepipe/controller/issuetrack"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap"
)

type trackingDeliveryResult struct {
	id       string
	held     bool
	err      error
	progress entity.IssueTrackingState
}

type observedTrackingDelivery struct {
	consumer.Delivery
	held bool
}

func (d *observedTrackingDelivery) Hold(delayMs int64) {
	d.held = true
	d.Delivery.Hold(delayMs)
}

type trackingObserver struct {
	consumer.Controller
	progress storage.IssueTrackingStore
	results  chan trackingDeliveryResult
}

func (o *trackingObserver) Process(ctx context.Context, delivery consumer.Delivery) error {
	observed := &observedTrackingDelivery{Delivery: delivery}
	err := o.Controller.Process(ctx, observed)
	progress, readErr := o.progress.Get(ctx)
	resultErr := err
	if resultErr == nil {
		resultErr = readErr
	}
	o.results <- trackingDeliveryResult{delivery.Message().ID, observed.held, resultErr, progress}
	return err
}

func testIssueTrackingDelivery(t *testing.T, ctx context.Context, db *sql.DB, factory storage.Factory) {
	t.Helper()
	const queueName = "tracking/delivery"
	const buildID = "build/delivery"
	bound, err := factory.For(storage.Config{QueueName: queueName})
	require.NoError(t, err)
	request := entity.Request{ID: "10", Queue: queueName, URI: "git://repo/tracking/delivery", State: entity.RequestStateProcessing, Version: 1}
	require.NoError(t, bound.GetRequestStore().Create(ctx, request))
	request.State, request.TerminalBuildID = entity.RequestStateFailed, buildID
	require.NoError(t, bound.GetRequestStore().Update(ctx, request, 1, 2))
	require.NoError(t, bound.GetBuildStore().Create(ctx, entity.Build{ID: buildID, RequestID: request.ID, Status: entity.BuildStatusFailed, Version: 1}))
	for i := 0; i < 103; i++ {
		require.NoError(t, bound.GetBuildIssueStore().Create(ctx, buildID, entity.BuildIssue{TaskID: "task", UnitID: fmt.Sprintf("unit/%03d", i)}))
	}
	require.NoError(t, bound.GetIssueStateStore().Create(ctx, entity.IssueState{TaskID: "z", FirstSeenBuildID: "old", Status: entity.IssueStatusOpen, Version: 1}))

	testutil.ApplySchema(t, testutil.NewTestLogger(t), db, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))
	queue, err := queuemysql.NewQueue(queuemysql.Params{DB: db, Logger: zap.NewNop(), MetricsScope: tally.NoopScope, Tenants: []string{queueName}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, queue.Close()) })
	config := extqueue.DefaultSubscriptionConfig("issue-tracking-test", "issue-tracking-test")
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: mq.TopicKeyIssueTrack, Name: "issue_tracking_test", Queue: queue, Subscription: config}})
	require.NoError(t, err)
	results := make(chan trackingDeliveryResult, 64)
	controller := issuetrack.NewController(zap.NewNop().Sugar(), tally.NoopScope, factory, mq.TopicKeyIssueTrack, config.ConsumerGroup)
	observer := &trackingObserver{Controller: controller, progress: bound.GetIssueTrackingStore(), results: results}
	runtime := consumer.New(zap.NewNop().Sugar(), tally.NoopScope, registry, errs.NewClassifierProcessor(), gatenoop.New())
	require.NoError(t, runtime.Register(observer))
	t.Cleanup(func() { require.NoError(t, runtime.Stop(30000)) })

	for _, notification := range []struct {
		id    string
		final bool
	}{{"final", true}, {"late-refresh", false}} {
		payload, err := mq.Marshal(&mq.IssueTrack{QueueName: queueName, BuildId: buildID, Finalize: notification.final})
		require.NoError(t, err)
		require.NoError(t, publish.Message(ctx, registry, mq.TopicKeyIssueTrack, publish.MessageParams{Tenant: queueName, ID: notification.id, PartitionKey: queueName, Payload: payload}))
	}
	require.NoError(t, runtime.Start(ctx))

	completed := map[string]bool{}
	held := false
	for len(completed) != 2 {
		result := <-results
		require.NoError(t, result.err)
		held = held || result.held
		if !result.held {
			require.Equal(t, entity.IssueTrackingPhaseFinalized, result.progress.Phase)
			completed[result.id] = true
		}
	}
	require.True(t, held)
	issues, err := bound.GetIssueStateStore().List(ctx, storage.IssueStateRange{Limit: 200})
	require.NoError(t, err)
	require.Len(t, issues, 104)
	open := 0
	for _, issue := range issues {
		if issue.Status == entity.IssueStatusOpen {
			open++
		}
	}
	require.Equal(t, 103, open)
	closed, err := bound.GetIssueStateStore().Get(ctx, "z", "")
	require.NoError(t, err)
	require.Equal(t, entity.IssueStatusClosed, closed.Status)
	require.Equal(t, int32(2), closed.Version)
	_, err = bound.GetIssueLogStore().Get(ctx, "z", "", buildID, entity.IssueEventResolved)
	require.NoError(t, err)
	checkpoint, err := bound.GetIssueTrackingStore().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, "final", checkpoint.NotificationID)
}

type pausedCreateFactory struct {
	storage.Factory
	paused chan struct{}
	resume <-chan struct{}
}

func (f pausedCreateFactory) For(config storage.Config) (storage.Storage, error) {
	bound, err := f.Factory.For(config)
	if err != nil {
		return nil, err
	}
	return pausedCreateStorage{bound, pausedCreateIssueStore{bound.GetIssueStateStore(), f.paused, f.resume}}, nil
}

type pausedCreateStorage struct {
	storage.Storage
	issues storage.IssueStateStore
}

func (s pausedCreateStorage) GetIssueStateStore() storage.IssueStateStore { return s.issues }

type pausedCreateIssueStore struct {
	storage.IssueStateStore
	paused chan struct{}
	resume <-chan struct{}
}

func (s pausedCreateIssueStore) Create(ctx context.Context, issue entity.IssueState) error {
	s.paused <- struct{}{}
	<-s.resume
	return s.IssueStateStore.Create(ctx, issue)
}

func testIssueTrackingLeaseTakeover(t *testing.T, ctx context.Context, db *sql.DB, factory storage.Factory) {
	const queueName = "tracking/takeover"
	const topic = "tracking_takeover"
	const group = "tracking-takeover"
	bound, err := factory.For(storage.Config{QueueName: queueName})
	require.NoError(t, err)
	for _, source := range []struct {
		id     string
		status entity.BuildStatus
	}{{"100", entity.BuildStatusRunning}, {"101", entity.BuildStatusSucceeded}} {
		request := entity.Request{ID: source.id, Queue: queueName, URI: "git://tracking/" + source.id, State: entity.RequestStateProcessing, Version: 1}
		require.NoError(t, bound.GetRequestStore().Create(ctx, request))
		if source.status.IsTerminal() {
			request.State, request.TerminalBuildID = entity.RequestStateSucceeded, "build/"+source.id
			require.NoError(t, bound.GetRequestStore().Update(ctx, request, 1, 2))
		}
		require.NoError(t, bound.GetBuildStore().Create(ctx, entity.Build{ID: "build/" + source.id, RequestID: source.id, Status: source.status, Version: 1}))
	}
	require.NoError(t, bound.GetBuildIssueStore().Create(ctx, "build/100", entity.BuildIssue{TaskID: "task", UnitID: "unit"}))
	testutil.ApplySchema(t, testutil.NewTestLogger(t), db, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))
	paused := make(chan struct{}, 1)
	resume := make(chan struct{})
	aFactory := pausedCreateFactory{factory, paused, resume}
	newRuntime := func(name string, stores storage.Factory) (consumer.Consumer, consumer.TopicRegistry, <-chan trackingDeliveryResult) {
		q, err := queuemysql.NewQueue(queuemysql.Params{DB: db, Logger: zap.NewNop(), MetricsScope: tally.NoopScope, Tenants: []string{queueName}})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, q.Close()) })
		config := extqueue.DefaultSubscriptionConfig(name, group)
		config.PollIntervalMs = 20
		config.PartitionDiscoveryIntervalMs = 50
		config.LeaseRenewalIntervalMs = 50
		config.LeaseDurationMs = 1000
		config.VisibilityTimeoutMs = 200
		config.BatchSize = 1
		config.Retry.MaxAttempts = 0
		registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: mq.TopicKeyIssueTrack, Name: topic, Queue: q, Subscription: config}})
		require.NoError(t, err)
		results := make(chan trackingDeliveryResult, 64)
		controller := issuetrack.NewController(zap.NewNop().Sugar(), tally.NoopScope, stores, mq.TopicKeyIssueTrack, group)
		observer := &trackingObserver{Controller: controller, progress: bound.GetIssueTrackingStore(), results: results}
		runtime := consumer.New(zap.NewNop().Sugar(), tally.NoopScope, registry, errs.NewClassifierProcessor(), gatenoop.New())
		require.NoError(t, runtime.Register(observer))
		t.Cleanup(func() { require.NoError(t, runtime.Stop(30000)) })
		return runtime, registry, results
	}
	a, registry, resultsA := newRuntime("tracking-a", aFactory)
	b, _, resultsB := newRuntime("tracking-b", factory)
	publishNotice := func(buildID, id string, final bool) {
		payload, err := mq.Marshal(&mq.IssueTrack{QueueName: queueName, BuildId: buildID, Finalize: final})
		require.NoError(t, err)
		require.NoError(t, publish.Message(ctx, registry, mq.TopicKeyIssueTrack, publish.MessageParams{Tenant: queueName, ID: id, PartitionKey: queueName, Payload: payload}))
	}
	publishNotice("build/100", "old-refresh", false)
	require.NoError(t, a.Start(ctx))
	<-paused
	defer close(resume)
	require.NoError(t, b.Start(ctx))
	// Transfer the partition while A is blocked at its real SQL create.
	result, err := db.ExecContext(ctx, "UPDATE queue_partition_leases SET leased_by = ?, lease_renewed_at = ? WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ?", "tracking-b", time.Now().UnixMilli(), queueName, group, topic, queueName)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
	resultB := <-resultsB
	require.NoError(t, resultB.err)
	require.Equal(t, "build/100", resultB.progress.BuildID)
	publishNotice("build/101", "new-final", true)
	for {
		resultB = <-resultsB
		require.NoError(t, resultB.err)
		if resultB.id == "new-final" {
			require.Equal(t, entity.IssueTrackingPhaseFinalized, resultB.progress.Phase)
			break
		}
	}
	closed, err := bound.GetIssueStateStore().Get(ctx, "task", "unit")
	require.NoError(t, err)
	require.Equal(t, entity.IssueStatusClosed, closed.Status)
	resume <- struct{}{}
	resultA := <-resultsA
	require.Error(t, resultA.err)
	issue, err := bound.GetIssueStateStore().Get(ctx, "task", "unit")
	require.NoError(t, err)
	require.Equal(t, closed, issue)
	require.Equal(t, entity.IssueStatusClosed, issue.Status)
	require.Equal(t, int32(2), issue.Version)
	_, err = bound.GetIssueLogStore().Get(ctx, "task", "unit", "build/100", entity.IssueEventOpened)
	require.NoError(t, err)
	_, err = bound.GetIssueLogStore().Get(ctx, "task", "unit", "build/101", entity.IssueEventResolved)
	require.NoError(t, err)
}
