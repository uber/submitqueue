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

package projectanalysis

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectissues"
	projectissuesmock "github.com/uber/submitqueue/stovepipe/extension/projectissues/mock"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
	projectstatusmock "github.com/uber/submitqueue/stovepipe/extension/projectstatus/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const testQueue = "validation/main"

type fixture struct {
	controller      *Controller
	dlq             *DLQController
	ctrl            *gomock.Controller
	request         entity.Request
	projectStatuses []projectstatus.Result
	attributions    []projectissues.Attribution
	facts           map[string]entity.ValidationFact
	indexes         map[entity.ProjectFactByDegree]bool
	source          map[entity.ProjectIssueAttribution]bool
	reverse         map[entity.ProjectIssueAttribution]bool
	logs            map[string]entity.RequestLog
	failures        map[string]error
	records         []entityqueue.Message
	publishAttempts []entityqueue.Message
	hookAttempts    []entityqueue.Message
	resolverCalls   int
	attributerCalls int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &fixture{
		ctrl:    ctrl,
		request: entity.Request{ID: "7", Queue: testQueue, URI: "git://repository/main/commit", State: entity.RequestStateFailed, TerminalBuildID: "build/7"},
		projectStatuses: []projectstatus.Result{
			{ProjectInfo: projectstatus.ProjectInfo{Name: "a", ProjectType: "service"}, Degree: entity.DegreeBroken},
			{ProjectInfo: projectstatus.ProjectInfo{Name: "b", ProjectType: "service"}, Degree: entity.DegreeGreen},
		},
		attributions: []projectissues.Attribution{{TaskID: "test", UnitID: "unit", Projects: []string{"a", "b"}}},
		facts:        map[string]entity.ValidationFact{},
		indexes:      map[entity.ProjectFactByDegree]bool{},
		source:       map[entity.ProjectIssueAttribution]bool{},
		reverse:      map[entity.ProjectIssueAttribution]bool{},
		logs:         map[string]entity.RequestLog{},
		failures:     map[string]error{},
	}
	analysis := requestlog.NewRequestEventLog(f.request, entity.RequestEventBuildAnalysisCompleted, f.request.TerminalBuildID, nil)
	f.logs[analysis.ID] = analysis
	stores := storagemock.NewMockFactory(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	requests := storagemock.NewMockRequestStore(ctrl)
	history := storagemock.NewMockRequestLogStore(ctrl)
	facts := storagemock.NewMockValidationFactStore(ctrl)
	indexes := storagemock.NewMockProjectFactByDegreeStore(ctrl)
	source := storagemock.NewMockRequestProjectsByIssueStore(ctrl)
	reverse := storagemock.NewMockRequestIssuesByProjectStore(ctrl)
	stores.EXPECT().For(storage.Config{QueueName: testQueue}).DoAndReturn(func(storage.Config) (storage.Storage, error) {
		return store, f.fail("storage/for")
	}).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(history).AnyTimes()
	store.EXPECT().GetValidationFactStore().Return(facts).AnyTimes()
	store.EXPECT().GetProjectFactByDegreeStore().Return(indexes).AnyTimes()
	store.EXPECT().GetRequestProjectsByIssueStore().Return(source).AnyTimes()
	store.EXPECT().GetRequestIssuesByProjectStore().Return(reverse).AnyTimes()
	requests.EXPECT().Get(gomock.Any(), "7").DoAndReturn(func(context.Context, string) (entity.Request, error) {
		return f.request, f.fail("request/get")
	}).AnyTimes()
	history.EXPECT().Get(gomock.Any(), "7", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, id string) (entity.RequestLog, error) {
		projectAnalysisAbandonedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
		operation := "analysis/get"
		if id == f.completion().ID {
			operation = "completion/get"
			if _, exists := f.logs[id]; exists {
				operation = "retained-completion/get"
			}
		} else if id == projectAnalysisAbandonedLog.ID {
			operation = "abandonment/get"
		}
		if err := f.fail(operation); err != nil {
			return entity.RequestLog{}, err
		}
		if log, ok := f.logs[id]; ok {
			return log, nil
		}
		return entity.RequestLog{}, storage.ErrNotFound
	}).AnyTimes()
	facts.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, fact entity.ValidationFact) error {
		if err := f.fail("fact/create"); err != nil {
			return err
		}
		if _, exists := f.facts[fact.Project]; exists {
			return storage.ErrAlreadyExists
		}
		f.facts[fact.Project] = fact
		return nil
	}).AnyTimes()
	facts.EXPECT().Get(gomock.Any(), f.request.URI, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, project string) (entity.ValidationFact, error) {
		return f.facts[project], nil
	}).AnyTimes()
	indexes.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, entry entity.ProjectFactByDegree) error {
		if err := f.fail("index/create"); err != nil {
			return err
		}
		if f.indexes[entry] {
			return storage.ErrAlreadyExists
		}
		f.indexes[entry] = true
		return nil
	}).AnyTimes()
	source.EXPECT().Create(gomock.Any(), "7", gomock.Any()).DoAndReturn(f.retainAttribution(f.source, "source/create")).AnyTimes()
	reverse.EXPECT().Create(gomock.Any(), "7", gomock.Any()).DoAndReturn(f.retainAttribution(f.reverse, "reverse/create")).AnyTimes()
	requestLogMaterializer := requestlogmock.NewMockMaterializer(ctrl)
	requestLogMaterializer.EXPECT().PersistLog(gomock.Any(), store, gomock.Any()).DoAndReturn(func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
		if log.Event != entity.RequestEventProjectAnalysisAbandoned {
			require.Len(t, f.facts, len(f.projectStatuses))
			require.Len(t, f.indexes, len(f.projectStatuses))
		}
		if log.Event == entity.RequestEventProjectIssuesRecorded {
			require.Equal(t, f.source, f.reverse)
			factsLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectFactsRecorded, "all", nil)
			require.Contains(t, f.logs, factsLog.ID)
		}
		if log.Event == entity.RequestEventProjectAnalysisCompleted {
			projectIssuesRecordedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectIssuesRecorded, f.request.TerminalBuildID, nil)
			require.Contains(t, f.logs, projectIssuesRecordedLog.ID)
		}
		if err := f.fail("log/" + string(log.Event)); err != nil {
			return err
		}
		if _, exists := f.logs[log.ID]; !exists {
			if log.Event == entity.RequestEventProjectAnalysisCompleted && log.TimestampMs == 0 {
				log.TimestampMs = 1234
			}
			f.logs[log.ID] = log
		}
		return nil
	}).AnyTimes()
	resolvers := projectstatusmock.NewMockResolverFactory(ctrl)
	resolver := projectstatusmock.NewMockResolver(ctrl)
	resolvers.EXPECT().For(projectstatus.ResolverConfig{QueueName: testQueue}).DoAndReturn(func(projectstatus.ResolverConfig) (projectstatus.Resolver, error) {
		return resolver, f.fail("resolver/for")
	}).AnyTimes()
	resolver.EXPECT().Resolve(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request entity.Request) ([]projectstatus.Result, error) {
		require.Equal(t, f.request, request)
		f.resolverCalls++
		return f.projectStatuses, f.fail("resolve")
	}).AnyTimes()
	attributers := projectissuesmock.NewMockAttributerFactory(ctrl)
	attributer := projectissuesmock.NewMockAttributer(ctrl)
	attributers.EXPECT().For(projectissues.AttributerConfig{QueueName: testQueue}).DoAndReturn(func(projectissues.AttributerConfig) (projectissues.Attributer, error) {
		return attributer, f.fail("attributer/for")
	}).AnyTimes()
	attributer.EXPECT().Attribute(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request entity.Request) ([]projectissues.Attribution, error) {
		require.Equal(t, f.request, request)
		require.Len(t, f.facts, len(f.projectStatuses))
		require.Len(t, f.indexes, len(f.projectStatuses))
		projectFactsRecordedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectFactsRecorded, "all", nil)
		require.Contains(t, f.logs, projectFactsRecordedLog.ID)
		f.attributerCalls++
		return f.attributions, f.fail("attribute")
	}).AnyTimes()
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "hook", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
		require.Contains(t, f.logs, f.completion().ID)
		require.Equal(t, testQueue, msg.Tenant)
		require.Equal(t, f.request.ID, msg.PartitionKey)
		f.hookAttempts = append(f.hookAttempts, msg)
		return f.fail("hook/publish")
	}).AnyTimes()
	publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
		if f.request.State != entity.RequestStateCancelled {
			projectAnalysisAbandonedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
			_, complete := f.logs[f.completion().ID]
			require.True(t, complete)
			require.NotContains(t, f.logs, projectAnalysisAbandonedLog.ID)
			require.NotEmpty(t, f.hookAttempts)
		}
		f.publishAttempts = append(f.publishAttempts, msg)
		if err := f.fail("record/publish"); err != nil {
			return err
		}
		f.records = append(f.records, msg)
		return nil
	}).AnyTimes()
	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyRecord, Name: "record", Queue: queue},
		{Key: basehook.TopicKeyHook, Name: "hook", Queue: queue},
	})
	require.NoError(t, err)
	f.controller = NewController(zap.NewNop().Sugar(), tally.NoopScope, stores, requestLogMaterializer, resolvers, attributers, registry, stovepipemq.TopicKeyProjectAnalysis, "stovepipe-projectanalysis")
	f.dlq = NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, stores, requestLogMaterializer, "projectanalysis_dlq", "stovepipe-projectanalysis-dlq")
	return f
}

func (f *fixture) completion() entity.RequestLog {
	log := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisCompleted, f.request.TerminalBuildID, nil)
	log.TimestampMs = 1234
	return log
}

func (f *fixture) fail(operation string) error {
	err := f.failures[operation]
	delete(f.failures, operation)
	return err
}

func (f *fixture) retainAttribution(rows map[entity.ProjectIssueAttribution]bool, operation string) func(context.Context, string, entity.ProjectIssueAttribution) error {
	return func(_ context.Context, _ string, entry entity.ProjectIssueAttribution) error {
		if err := f.fail(operation); err != nil {
			return err
		}
		if rows[entry] {
			return storage.ErrAlreadyExists
		}
		rows[entry] = true
		return nil
	}
}

func (f *fixture) message(t *testing.T) entityqueue.Message {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.ProjectAnalysis{Id: "7", QueueName: testQueue})
	require.NoError(t, err)
	return entityqueue.Message{ID: "7", Tenant: testQueue, PartitionKey: "7", Payload: payload}
}

func (f *fixture) deliver(ctx context.Context, msg entityqueue.Message) error {
	delivery := consumermock.NewMockDelivery(f.ctrl)
	delivery.EXPECT().Message().Return(msg)
	return f.controller.Process(ctx, delivery)
}

func TestProcessRecordsProjectStatuses(t *testing.T) {
	for _, tt := range []struct {
		name       string
		empty      bool
		unaffected bool
		duplicates bool
		state      entity.RequestState
	}{
		{name: "failed build", state: entity.RequestStateFailed},
		{name: "successful build", state: entity.RequestStateSucceeded},
		{name: "empty project statuses", empty: true, state: entity.RequestStateFailed},
		{name: "no affected projects", unaffected: true, state: entity.RequestStateFailed},
		{name: "duplicate associations", duplicates: true, state: entity.RequestStateFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.request.State = tt.state
			if tt.empty {
				f.projectStatuses, f.attributions = nil, nil
			}
			if tt.unaffected {
				f.attributions[0].Projects = nil
			}
			if tt.duplicates {
				f.attributions[0].Projects = []string{"a", "b", "a"}
			}
			wantPairs := map[entity.ProjectIssueAttribution]bool{}
			for _, attribution := range f.attributions {
				for _, project := range attribution.Projects {
					wantPairs[entity.ProjectIssueAttribution{TaskID: attribution.TaskID, UnitID: attribution.UnitID, Project: project}] = true
				}
			}
			require.NoError(t, f.deliver(context.Background(), f.message(t)))
			require.Len(t, f.facts, len(f.projectStatuses))
			require.Len(t, f.indexes, len(f.projectStatuses))
			require.Equal(t, wantPairs, f.source)
			require.Equal(t, f.source, f.reverse)
			require.Equal(t, f.completion(), f.logs[f.completion().ID])
			factsLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectFactsRecorded, "all", nil)
			expectedCounts := map[string]string{requestlog.MetadataKeyProjectFactCount: "2", requestlog.MetadataKeyBrokenProjectFactCount: "1"}
			if tt.empty {
				expectedCounts = map[string]string{requestlog.MetadataKeyProjectFactCount: "0", requestlog.MetadataKeyBrokenProjectFactCount: "0"}
			}
			require.Equal(t, expectedCounts, f.logs[factsLog.ID].Metadata)
			require.Len(t, f.hookAttempts, 1)
			event := &basehook.HookEvent{}
			require.NoError(t, basehook.Unmarshal(f.hookAttempts[0].Payload, event))
			require.Equal(t, "project.analysis.completed", event.GetType())
			require.Equal(t, int64(1234), event.GetTimestampMs())
			require.Equal(t, map[string]any{"queue": testQueue, "request_id": f.request.ID}, event.GetPayload().AsMap())
			require.Len(t, f.records, 1)
			record := &stovepipemq.Record{}
			require.NoError(t, stovepipemq.Unmarshal(f.records[0].Payload, record))
			require.Equal(t, f.request.ID, record.GetId())
			require.Equal(t, testQueue, record.GetQueueName())
			require.Equal(t, testQueue, f.records[0].Tenant)
			require.Equal(t, "7", f.records[0].PartitionKey)
			for _, fact := range f.facts {
				require.Equal(t, f.request.URI, fact.URI)
				require.Equal(t, f.request.ID, fact.RequestID)
				require.Equal(t, "service", fact.ProjectType)
				require.Positive(t, fact.CreatedAt)
			}
		})
	}
}

func TestProcessRetriesPartialWork(t *testing.T) {
	for _, operation := range []string{
		"storage/for", "request/get", "abandonment/get", "completion/get", "analysis/get", "resolver/for", "resolve", "attributer/for", "attribute",
		"source/create", "reverse/create", "fact/create", "index/create", "log/project_facts_recorded", "log/project_issues_recorded", "log/project_analysis_completed", "retained-completion/get", "hook/publish", "record/publish",
	} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			f.failures[operation] = assert.AnError
			msg := f.message(t)
			require.ErrorIs(t, f.deliver(context.Background(), msg), assert.AnError)
			require.Empty(t, f.records)
			if operation != "record/publish" && operation != "hook/publish" && operation != "retained-completion/get" {
				require.Empty(t, f.hookAttempts)
				require.NotContains(t, f.logs, f.completion().ID)
			}
			switch operation {
			case "resolve":
				require.Empty(t, f.source)
				require.Empty(t, f.facts)
			case "fact/create", "index/create", "log/project_facts_recorded":
				require.Zero(t, f.attributerCalls)
				require.Empty(t, f.source)
				require.Empty(t, f.reverse)
			case "log/project_analysis_completed":
				projectIssuesRecordedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectIssuesRecorded, f.request.TerminalBuildID, nil)
				require.Contains(t, f.logs, projectIssuesRecordedLog.ID)
			case "attributer/for", "attribute", "source/create", "reverse/create":
				require.Len(t, f.facts, 2)
				require.Len(t, f.indexes, 2)
				projectFactsRecordedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectFactsRecorded, "all", nil)
				require.Contains(t, f.logs, projectFactsRecordedLog.ID)
			}
			retained := make(map[string]entity.ValidationFact, len(f.facts))
			for project, fact := range f.facts {
				retained[project] = fact
			}
			require.NoError(t, f.deliver(context.Background(), msg))
			require.Len(t, f.facts, 2)
			require.Len(t, f.indexes, 2)
			require.Len(t, f.source, 2)
			require.Equal(t, f.source, f.reverse)
			require.Contains(t, f.logs, f.completion().ID)
			require.Len(t, f.records, 1)
			for project, fact := range retained {
				require.Equal(t, fact, f.facts[project])
			}
			resolverCalls, attributerCalls := f.resolverCalls, f.attributerCalls
			require.NoError(t, f.deliver(context.Background(), msg))
			require.Equal(t, resolverCalls, f.resolverCalls)
			require.Equal(t, attributerCalls, f.attributerCalls)
			require.Len(t, f.records, 2)
			for _, attempt := range f.publishAttempts {
				require.Equal(t, f.records[0].ID, attempt.ID)
				require.Equal(t, f.records[0].Payload, attempt.Payload)
			}
			for _, attempt := range f.hookAttempts {
				require.Equal(t, f.hookAttempts[0].ID, attempt.ID)
				require.JSONEq(t, string(f.hookAttempts[0].Payload), string(attempt.Payload))
			}
			if operation == "record/publish" || operation == "hook/publish" || operation == "retained-completion/get" {
				require.Equal(t, 1, f.resolverCalls)
				require.Equal(t, 1, f.attributerCalls)
			}
		})
	}
}

func TestProcessRequiresFinalAnalysis(t *testing.T) {
	for _, abandoned := range []bool{false, true} {
		name := "pending analysis"
		if abandoned {
			name = "abandoned analysis"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.logs = map[string]entity.RequestLog{}
			if abandoned {
				log := requestlog.NewRequestEventLog(f.request, entity.RequestEventBuildAnalysisAbandoned, f.request.TerminalBuildID, nil)
				f.logs[log.ID] = log
			}
			require.ErrorIs(t, f.deliver(context.Background(), f.message(t)), storage.ErrNotFound)
			require.Zero(t, f.resolverCalls)
			require.Zero(t, f.attributerCalls)
			require.Empty(t, f.hookAttempts)
			require.Empty(t, f.records)
		})
	}
}

func TestProcessRejectsInvalidProviderOutput(t *testing.T) {
	for _, tt := range []struct {
		name          string
		invalidStatus bool
		attribution   projectissues.Attribution
		wantFacts     int
	}{
		{name: "invalid status", invalidStatus: true},
		{name: "missing task", attribution: projectissues.Attribution{UnitID: "unit", Projects: []string{"a"}}, wantFacts: 2},
		{name: "task wide issue", attribution: projectissues.Attribution{TaskID: "task", Projects: []string{"a"}}, wantFacts: 2},
		{name: "missing project", attribution: projectissues.Attribution{TaskID: "task", UnitID: "unit", Projects: []string{""}}, wantFacts: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			if tt.invalidStatus {
				f.projectStatuses = []projectstatus.Result{{Degree: entity.DegreeGreen}}
			} else {
				f.attributions = []projectissues.Attribution{tt.attribution}
			}
			require.Error(t, f.deliver(context.Background(), f.message(t)))
			require.Len(t, f.facts, tt.wantFacts)
			require.Len(t, f.indexes, tt.wantFacts)
			require.Empty(t, f.source)
			require.Empty(t, f.reverse)
			require.NotContains(t, f.logs, f.completion().ID)
			require.Empty(t, f.records)
		})
	}
}

func TestProcessHandlesNonAnalysisOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		state   entity.RequestState
		buildID string
		wantErr bool
		records int
	}{
		{name: "cancelled", state: entity.RequestStateCancelled, records: 1},
		{name: "superseded", state: entity.RequestStateSuperseded},
		{name: "not terminal", state: entity.RequestStateProcessing, wantErr: true},
		{name: "no terminal build", state: entity.RequestStateFailed, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.request.State, f.request.TerminalBuildID = tt.state, tt.buildID
			f.logs = map[string]entity.RequestLog{}
			err := f.deliver(context.Background(), f.message(t))
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, f.resolverCalls)
			require.Zero(t, f.attributerCalls)
			require.Empty(t, f.facts)
			require.Empty(t, f.source)
			require.Len(t, f.records, tt.records)
			require.Empty(t, f.hookAttempts)
		})
	}
}

func TestProcessRejectsInvalidIdentity(t *testing.T) {
	for _, scenario := range []string{"malformed", "invalid request ID", "tenant mismatch", "partition mismatch", "stored queue mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			msg := f.message(t)
			switch scenario {
			case "malformed":
				msg.Payload = []byte("not json")
			case "invalid request ID":
				var err error
				msg.Payload, err = stovepipemq.Marshal(&stovepipemq.ProjectAnalysis{Id: "invalid", QueueName: testQueue})
				require.NoError(t, err)
			case "tenant mismatch":
				msg.Tenant = "another/queue"
			case "partition mismatch":
				msg.PartitionKey = "another-request"
			case "stored queue mismatch":
				f.request.Queue = "another/queue"
			}
			require.Error(t, f.deliver(context.Background(), msg))
			require.Zero(t, f.resolverCalls)
			require.Zero(t, f.attributerCalls)
			require.Empty(t, f.hookAttempts)
			require.Empty(t, f.records)
		})
	}
}

func TestCompletedAnalysisRepairsProjection(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "normal delivery"
		if recovery {
			name = "dead-letter recovery"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			msg := f.message(t)
			f.failures["record/publish"] = assert.AnError
			require.ErrorIs(t, f.deliver(context.Background(), msg), assert.AnError)
			f.failures["log/project_analysis_completed"] = assert.AnError
			deliver := f.deliver
			if recovery {
				deliver = f.recover
			}
			require.ErrorIs(t, deliver(context.Background(), msg), assert.AnError)
			require.Empty(t, f.records)
			require.Equal(t, 1, f.resolverCalls)
			require.Equal(t, 1, f.attributerCalls)
			require.NoError(t, deliver(context.Background(), msg))
			if recovery {
				require.Empty(t, f.records)
				require.Len(t, f.publishAttempts, 1)
				require.Len(t, f.hookAttempts, 1)
			} else {
				require.Len(t, f.records, 1)
			}
		})
	}
}

func TestAbandonedAnalysisRepairsProjectionWithoutResuming(t *testing.T) {
	f := newFixture(t)
	msg := f.message(t)
	require.NoError(t, f.recover(context.Background(), msg))
	f.failures["log/project_analysis_abandoned"] = assert.AnError
	require.ErrorIs(t, f.deliver(context.Background(), msg), assert.AnError)
	require.Empty(t, f.records)
	require.Zero(t, f.resolverCalls)
	require.Zero(t, f.attributerCalls)
	require.NoError(t, f.deliver(context.Background(), msg))
	require.Empty(t, f.records)
	require.Empty(t, f.publishAttempts)
	require.Empty(t, f.hookAttempts)
}

const projectStatusTestQueue = "monorepo/main"

var projectStatusTestChangeTime = time.Unix(1_700_000_000, 0).UTC()

type projectStatusFixture struct {
	store        *storagemock.MockStorage
	factStore    *storagemock.MockValidationFactStore
	indexStore   *storagemock.MockProjectFactByDegreeStore
	metricsScope tally.TestScope
}

func newProjectStatusFixture(ctrl *gomock.Controller) projectStatusFixture {
	f := projectStatusFixture{
		store:        storagemock.NewMockStorage(ctrl),
		factStore:    storagemock.NewMockValidationFactStore(ctrl),
		indexStore:   storagemock.NewMockProjectFactByDegreeStore(ctrl),
		metricsScope: tally.NewTestScope("", nil),
	}
	f.store.EXPECT().GetValidationFactStore().Return(f.factStore).AnyTimes()
	f.store.EXPECT().GetProjectFactByDegreeStore().Return(f.indexStore).AnyTimes()
	return f
}

func projectStatusContext() context.Context {
	ctx := entityqueue.WithQueueName(context.Background(), projectStatusTestQueue)
	return metrics.WithContextTags(ctx, metrics.NewTag("queue", projectStatusTestQueue))
}

func projectStatusTestRequest() entity.Request {
	return entity.Request{
		ID: "7", Queue: projectStatusTestQueue, URI: "git://remote/monorepo/main/head-sha",
		State: entity.RequestStateFailed, Version: 2, TerminalBuildID: "bk-1",
		BaseURI: "git://remote/monorepo/main/base-sha", BuildStrategy: entity.BuildStrategyIncrementalSinceGreen,
	}
}

func totalSamples(buckets map[time.Duration]int64) int64 {
	var sum int64
	for _, count := range buckets {
		sum += count
	}
	return sum
}

func TestRecordProjectStatusesPassesRequestBuildContext(t *testing.T) {
	for _, tt := range []struct {
		name            string
		buildID         string
		terminalBuildID string
	}{
		{name: "terminal outcome", buildID: "triggered-build", terminalBuildID: "terminal-build"},
		{name: "no terminal outcome", buildID: "triggered-build"},
		{name: "no build"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := newProjectStatusFixture(ctrl)
			request := projectStatusTestRequest()
			request.BuildID = tt.buildID
			request.TerminalBuildID = tt.terminalBuildID
			factory := projectstatusmock.NewMockResolverFactory(ctrl)
			resolver := projectstatusmock.NewMockResolver(ctrl)
			factory.EXPECT().For(projectstatus.ResolverConfig{QueueName: projectStatusTestQueue}).Return(resolver, nil)
			resolver.EXPECT().Resolve(gomock.Any(), request).Return(nil, nil)

			requestLogMaterializer := requestlogmock.NewMockMaterializer(ctrl)
			requestLogMaterializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil)
			c := &Controller{
				logger:                 zap.NewNop().Sugar(),
				scope:                  m.metricsScope.SubScope("projectanalysis_controller"),
				resolvers:              factory,
				requestLogMaterializer: requestLogMaterializer,
			}
			err := c.recordProjectStatuses(projectStatusContext(), m.store, request)

			require.NoError(t, err)
		})
	}
}

func TestRecordProjectStatusesResolverMetrics(t *testing.T) {
	validProjectStatuses := []projectstatus.Result{
		{ProjectInfo: projectstatus.ProjectInfo{Name: "project-a"}, Degree: entity.DegreeGreen},
		{ProjectInfo: projectstatus.ProjectInfo{Name: "project-b"}, Degree: entity.DegreeBroken},
	}
	for _, tt := range []struct {
		name            string
		projectStatuses []projectstatus.Result
		factoryErr      error
		resolveErr      error
		invalid         bool
		wantResult      string
	}{
		{name: "projects", projectStatuses: validProjectStatuses, wantResult: "success"},
		{name: "nil project statuses", wantResult: "success"},
		{name: "empty project statuses", projectStatuses: []projectstatus.Result{}, wantResult: "success"},
		{name: "factory error", factoryErr: assert.AnError, wantResult: "error"},
		{name: "factory cancellation", factoryErr: context.Canceled, wantResult: "cancel"},
		{name: "resolver error with partial project statuses", projectStatuses: validProjectStatuses, resolveErr: assert.AnError, wantResult: "error"},
		{name: "resolver cancellation", resolveErr: context.Canceled, wantResult: "cancel"},
		{name: "resolver deadline", resolveErr: context.DeadlineExceeded, wantResult: "error"},
		{name: "invalid project", projectStatuses: []projectstatus.Result{{Degree: entity.DegreeGreen}}, invalid: true, wantResult: "error"},
		{name: "invalid degree", projectStatuses: []projectstatus.Result{{ProjectInfo: projectstatus.ProjectInfo{Name: "project-a"}, Degree: math.NaN()}}, invalid: true, wantResult: "error"},
		{name: "duplicate projects", projectStatuses: []projectstatus.Result{validProjectStatuses[0], validProjectStatuses[0]}, invalid: true, wantResult: "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := newProjectStatusFixture(ctrl)
			request := projectStatusTestRequest()
			factory := projectstatusmock.NewMockResolverFactory(ctrl)
			resolver := projectstatusmock.NewMockResolver(ctrl)
			factory.EXPECT().For(projectstatus.ResolverConfig{QueueName: projectStatusTestQueue}).Return(resolver, tt.factoryErr)
			if tt.factoryErr == nil {
				resolver.EXPECT().Resolve(gomock.Any(), request).Return(tt.projectStatuses, tt.resolveErr)
			}

			requestLogMaterializer := requestlogmock.NewMockMaterializer(ctrl)
			if tt.factoryErr == nil && tt.resolveErr == nil && !tt.invalid {
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(len(tt.projectStatuses))
				m.indexStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(len(tt.projectStatuses))
				requestLogMaterializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil)
			}
			c := &Controller{
				logger:                 zap.NewNop().Sugar(),
				scope:                  m.metricsScope.SubScope("projectanalysis_controller"),
				resolvers:              factory,
				requestLogMaterializer: requestLogMaterializer,
			}
			err := c.recordProjectStatuses(projectStatusContext(), m.store, request)

			switch {
			case tt.factoryErr != nil:
				require.ErrorIs(t, err, tt.factoryErr)
			case tt.resolveErr != nil:
				require.ErrorIs(t, err, tt.resolveErr)
			case tt.invalid:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			assertProjectStatusOperationMetrics(t, m.metricsScope, "record_project_statuses", tt.wantResult)
			histograms := m.metricsScope.Snapshot().Histograms()
			const projectsKey = "projectanalysis_controller.record_project_statuses.projects+queue=" + projectStatusTestQueue
			if err != nil {
				assert.NotContains(t, histograms, projectsKey)
				return
			}
			projects, ok := histograms[projectsKey]
			require.True(t, ok)
			assert.EqualValues(t, 1, projects.Values()[float64(len(tt.projectStatuses))])
			var samples int64
			for _, count := range projects.Values() {
				samples += count
			}
			assert.EqualValues(t, 1, samples)
		})
	}
}

func TestRecordProjectStatusesPersistenceMetrics(t *testing.T) {
	for _, tt := range []struct {
		name          string
		empty         bool
		existing      bool
		factErr       error
		indexErr      error
		completionErr error
		wantResult    string
	}{
		{name: "projects", wantResult: "success"},
		{name: "empty project statuses", empty: true, wantResult: "success"},
		{name: "existing facts and indexes", existing: true, wantResult: "success"},
		{name: "fact write error", factErr: assert.AnError, wantResult: "error"},
		{name: "fact write cancellation", factErr: context.Canceled, wantResult: "cancel"},
		{name: "index write error", indexErr: assert.AnError, wantResult: "error"},
		{name: "index write cancellation", indexErr: context.Canceled, wantResult: "cancel"},
		{name: "completion error", completionErr: assert.AnError, wantResult: "error"},
		{name: "completion cancellation", completionErr: context.Canceled, wantResult: "cancel"},
		{name: "empty completion error", empty: true, completionErr: assert.AnError, wantResult: "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := newProjectStatusFixture(ctrl)
			request := projectStatusTestRequest()
			var projectStatuses []projectstatus.Result
			if !tt.empty {
				projectStatuses = []projectstatus.Result{
					{ProjectInfo: projectstatus.ProjectInfo{Name: "project-a"}, Degree: entity.DegreeGreen},
					{ProjectInfo: projectstatus.ProjectInfo{Name: "project-b"}, Degree: entity.DegreeBroken},
				}
			}
			factory := projectstatusmock.NewMockResolverFactory(ctrl)
			resolver := projectstatusmock.NewMockResolver(ctrl)
			factory.EXPECT().For(projectstatus.ResolverConfig{QueueName: projectStatusTestQueue}).Return(resolver, nil)
			resolver.EXPECT().Resolve(gomock.Any(), request).Return(projectStatuses, nil)
			requestLogMaterializer := requestlogmock.NewMockMaterializer(ctrl)
			c := &Controller{
				logger:                 zap.NewNop().Sugar(),
				scope:                  m.metricsScope.SubScope("projectanalysis_controller"),
				requestLogMaterializer: requestLogMaterializer,
				resolvers:              factory,
			}
			for _, status := range projectStatuses {
				fact := entity.ValidationFact{URI: request.URI, Project: status.ProjectInfo.Name, Degree: status.Degree, RequestID: request.ID, CreatedAt: projectStatusTestChangeTime.UnixMilli()}
				createErr := tt.factErr
				if tt.existing {
					createErr = storage.ErrAlreadyExists
				}
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(createErr)
				if tt.factErr != nil {
					break
				}
				indexErr := tt.indexErr
				if tt.existing {
					m.factStore.EXPECT().Get(gomock.Any(), request.URI, status.ProjectInfo.Name).Return(fact, nil)
					indexErr = storage.ErrAlreadyExists
				}
				entry, err := entity.NewProjectFactByDegree(fact)
				require.NoError(t, err)
				m.indexStore.EXPECT().Create(gomock.Any(), entry).Return(indexErr)
				if tt.indexErr != nil {
					break
				}
			}
			if tt.factErr == nil && tt.indexErr == nil {
				requestLogMaterializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).DoAndReturn(
					func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
						assert.Equal(t, entity.RequestEventProjectFactsRecorded, log.Event)
						return tt.completionErr
					})
			}

			err := c.recordProjectStatuses(projectStatusContext(), m.store, request)

			switch {
			case tt.factErr != nil:
				require.ErrorIs(t, err, tt.factErr)
			case tt.indexErr != nil:
				require.ErrorIs(t, err, tt.indexErr)
			case tt.completionErr != nil:
				require.ErrorIs(t, err, tt.completionErr)
			default:
				require.NoError(t, err)
			}
			assertProjectStatusOperationMetrics(t, m.metricsScope, "record_project_statuses", tt.wantResult)
		})
	}
}

func TestRecordProjectStatusesRepairsIndexOnRetry(t *testing.T) {
	for _, retryErr := range []error{nil, storage.ErrAlreadyExists} {
		name := "index repaired"
		if retryErr != nil {
			name = "index already repaired"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := newProjectStatusFixture(ctrl)
			request := projectStatusTestRequest()
			stored := entity.ValidationFact{URI: request.URI, Project: "p", Degree: 0.25, RequestID: request.ID, CreatedAt: 10}
			entry, err := entity.NewProjectFactByDegree(stored)
			require.NoError(t, err)
			writeErr := errors.New("index write unavailable")
			factory := projectstatusmock.NewMockResolverFactory(ctrl)
			resolver := projectstatusmock.NewMockResolver(ctrl)
			factory.EXPECT().For(projectstatus.ResolverConfig{QueueName: projectStatusTestQueue}).Return(resolver, nil).Times(2)
			requestLogMaterializer := requestlogmock.NewMockMaterializer(ctrl)
			c := &Controller{
				logger:                 zap.NewNop().Sugar(),
				scope:                  m.metricsScope.SubScope("projectanalysis_controller"),
				requestLogMaterializer: requestLogMaterializer,
				resolvers:              factory,
			}

			gomock.InOrder(
				resolver.EXPECT().Resolve(gomock.Any(), request).Return([]projectstatus.Result{{ProjectInfo: projectstatus.ProjectInfo{Name: "p"}, Degree: 0.25}}, nil),
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil),
				m.indexStore.EXPECT().Create(gomock.Any(), entry).Return(writeErr),
				resolver.EXPECT().Resolve(gomock.Any(), request).Return([]projectstatus.Result{{ProjectInfo: projectstatus.ProjectInfo{Name: "p"}, Degree: 0.75}}, nil),
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists),
				m.factStore.EXPECT().Get(gomock.Any(), request.URI, "p").Return(stored, nil),
				m.indexStore.EXPECT().Create(gomock.Any(), entry).Return(retryErr),
				requestLogMaterializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).DoAndReturn(func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
					assert.Equal(t, entity.RequestEventProjectFactsRecorded, log.Event)
					return nil
				}),
			)
			require.ErrorIs(t, c.recordProjectStatuses(projectStatusContext(), m.store, request), writeErr)
			require.NoError(t, c.recordProjectStatuses(projectStatusContext(), m.store, request))
			snapshot := m.metricsScope.Snapshot()
			assert.EqualValues(t, 2, snapshot.Counters()["projectanalysis_controller.record_project_statuses.start+queue="+projectStatusTestQueue].Value())
			for _, status := range []string{"success", "error"} {
				finish, ok := snapshot.Histograms()["projectanalysis_controller.record_project_statuses.finish+queue="+projectStatusTestQueue+",result="+status]
				require.True(t, ok)
				assert.EqualValues(t, 1, totalSamples(finish.Durations()))
			}
		})
	}
}

func TestRecordProjectStatusesCountsPersistedOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name                         string
		storedDegree, resolvedDegree float64
		broken                       string
	}{
		{name: "new green", storedDegree: entity.DegreeGreen, resolvedDegree: entity.DegreeGreen, broken: "0"},
		{name: "new partially broken", storedDegree: 0.25, resolvedDegree: 0.25, broken: "1"},
		{name: "new fully broken", storedDegree: entity.DegreeBroken, resolvedDegree: entity.DegreeBroken, broken: "1"},
		{name: "adopts existing green", storedDegree: entity.DegreeGreen, resolvedDegree: entity.DegreeBroken, broken: "0"},
		{name: "adopts existing broken", storedDegree: entity.DegreeBroken, resolvedDegree: entity.DegreeGreen, broken: "1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := newProjectStatusFixture(ctrl)
			request := projectStatusTestRequest()
			stored := entity.ValidationFact{URI: request.URI, Project: "p", Degree: tt.storedDegree, RequestID: request.ID, CreatedAt: 10}
			if tt.storedDegree != tt.resolvedDegree {
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
				m.factStore.EXPECT().Get(gomock.Any(), request.URI, "p").Return(stored, nil)
			} else {
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			}
			m.indexStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			factory := projectstatusmock.NewMockResolverFactory(ctrl)
			resolver := projectstatusmock.NewMockResolver(ctrl)
			factory.EXPECT().For(projectstatus.ResolverConfig{QueueName: projectStatusTestQueue}).Return(resolver, nil)
			resolver.EXPECT().Resolve(gomock.Any(), request).Return([]projectstatus.Result{{ProjectInfo: projectstatus.ProjectInfo{Name: "p"}, Degree: tt.resolvedDegree}}, nil)
			requestLogMaterializer := requestlogmock.NewMockMaterializer(ctrl)
			c := &Controller{
				logger:                 zap.NewNop().Sugar(),
				scope:                  m.metricsScope.SubScope("projectanalysis_controller"),
				requestLogMaterializer: requestLogMaterializer,
				resolvers:              factory,
			}
			requestLogMaterializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).DoAndReturn(func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
				assert.Equal(t, map[string]string{
					requestlog.MetadataKeyProjectFactCount:       "1",
					requestlog.MetadataKeyBrokenProjectFactCount: tt.broken,
				}, log.Metadata)
				return nil
			})
			require.NoError(t, c.recordProjectStatuses(projectStatusContext(), m.store, request))
		})
	}
}

func assertProjectStatusOperationMetrics(t *testing.T, scope tally.TestScope, operation, outcome string) {
	t.Helper()
	snapshot := scope.Snapshot()
	start, ok := snapshot.Counters()["projectanalysis_controller."+operation+".start+queue="+projectStatusTestQueue]
	require.True(t, ok)
	assert.EqualValues(t, 1, start.Value())
	finish, ok := snapshot.Histograms()["projectanalysis_controller."+operation+".finish+queue="+projectStatusTestQueue+",result="+outcome]
	require.True(t, ok)
	assert.EqualValues(t, 1, totalSamples(finish.Durations()))
}
