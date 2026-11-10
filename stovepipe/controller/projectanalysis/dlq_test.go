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
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func (f *fixture) recover(ctx context.Context, msg entityqueue.Message) error {
	delivery := consumermock.NewMockDelivery(f.ctrl)
	delivery.EXPECT().Message().Return(msg)
	delivery.EXPECT().Failure().Return(failure.Failure{Message: "project analysis failed"}, true).AnyTimes()
	return f.dlq.Process(ctx, delivery)
}

func TestDLQRetainsPartialResults(t *testing.T) {
	for _, operation := range []string{
		"resolve", "attribute", "reverse/create", "fact/create", "index/create",
		"log/project_facts_recorded", "log/project_issues_recorded",
	} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			msg := f.message(t)
			request := f.request
			f.failures[operation] = assert.AnError
			require.ErrorIs(t, f.deliver(context.Background(), msg), assert.AnError)
			facts, indexes := maps.Clone(f.facts), maps.Clone(f.indexes)
			source, reverse := maps.Clone(f.source), maps.Clone(f.reverse)
			resolverCalls, attributerCalls := f.resolverCalls, f.attributerCalls

			require.NoError(t, f.recover(context.Background(), msg))
			projectAnalysisAbandonedLog := requestlog.NewRequestEventLog(request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
			require.Equal(t, projectAnalysisAbandonedLog, f.logs[projectAnalysisAbandonedLog.ID])
			require.Empty(t, f.logs[projectAnalysisAbandonedLog.ID].Metadata)
			require.NotContains(t, f.logs, f.completion().ID)
			require.Equal(t, request, f.request)
			require.Equal(t, facts, f.facts)
			require.Equal(t, indexes, f.indexes)
			require.Equal(t, source, f.source)
			require.Equal(t, reverse, f.reverse)
			require.Equal(t, resolverCalls, f.resolverCalls)
			require.Equal(t, attributerCalls, f.attributerCalls)
			require.Len(t, f.records, 1)

			logCount := len(f.logs)
			require.NoError(t, f.recover(context.Background(), msg))
			require.NoError(t, f.deliver(context.Background(), msg))
			require.Len(t, f.logs, logCount)
			require.NotContains(t, f.logs, f.completion().ID)
			require.Equal(t, facts, f.facts)
			require.Equal(t, source, f.source)
			require.Equal(t, reverse, f.reverse)
			require.Equal(t, resolverCalls, f.resolverCalls)
			require.Equal(t, attributerCalls, f.attributerCalls)
			require.Len(t, f.records, 3)
			for _, record := range f.records {
				require.Equal(t, f.records[0].ID, record.ID)
				require.Equal(t, f.records[0].Payload, record.Payload)
			}
		})
	}
}

func TestDLQPreservesCompletedAnalysis(t *testing.T) {
	f := newFixture(t)
	msg := f.message(t)
	f.failures["record/publish"] = assert.AnError
	require.ErrorIs(t, f.deliver(context.Background(), msg), assert.AnError)
	facts, source, reverse := maps.Clone(f.facts), maps.Clone(f.source), maps.Clone(f.reverse)
	require.Contains(t, f.logs, f.completion().ID)

	require.NoError(t, f.recover(context.Background(), msg))
	require.NoError(t, f.recover(context.Background(), msg))
	require.NotContains(t, f.logs, requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil).ID)
	require.Equal(t, facts, f.facts)
	require.Equal(t, source, f.source)
	require.Equal(t, reverse, f.reverse)
	require.Equal(t, 1, f.resolverCalls)
	require.Equal(t, 1, f.attributerCalls)
	require.Len(t, f.records, 2)
	require.Equal(t, f.publishAttempts[0].ID, f.records[0].ID)
}

func TestDLQRetriesRecoveryFailures(t *testing.T) {
	for _, tt := range []struct {
		operation string
		err       error
	}{
		{operation: "request/get", err: context.Canceled},
		{operation: "completion/get", err: assert.AnError},
		{operation: "log/project_analysis_abandoned", err: assert.AnError},
		{operation: "record/publish", err: assert.AnError},
	} {
		t.Run(tt.operation, func(t *testing.T) {
			f := newFixture(t)
			msg := f.message(t)
			f.failures[tt.operation] = tt.err
			require.ErrorIs(t, f.recover(context.Background(), msg), tt.err)
			require.Empty(t, f.records)
			require.Zero(t, f.resolverCalls)
			require.Zero(t, f.attributerCalls)
			if tt.operation == "record/publish" {
				require.Contains(t, f.logs, requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil).ID)
			}

			require.NoError(t, f.recover(context.Background(), msg))
			require.Contains(t, f.logs, requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil).ID)
			require.Len(t, f.records, 1)
			require.Empty(t, f.facts)
			require.Empty(t, f.source)
			require.NotContains(t, f.logs, f.completion().ID)
		})
	}
}

func TestDLQHandlesTerminalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name        string
		state       entity.RequestState
		build       string
		abandoned   bool
		recordCount int
	}{
		{name: "failed", state: entity.RequestStateFailed, build: "build/7", abandoned: true, recordCount: 1},
		{name: "succeeded", state: entity.RequestStateSucceeded, build: "build/7", abandoned: true, recordCount: 1},
		{name: "failed without a build outcome", state: entity.RequestStateFailed, abandoned: true, recordCount: 1},
		{name: "cancelled", state: entity.RequestStateCancelled, recordCount: 1},
		{name: "superseded", state: entity.RequestStateSuperseded},
		{name: "nonterminal", state: entity.RequestStateProcessing},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.request.State, f.request.TerminalBuildID = tt.state, tt.build
			f.logs = map[string]entity.RequestLog{}
			msg := f.message(t)
			require.NoError(t, f.recover(context.Background(), msg))
			require.Len(t, f.records, tt.recordCount)
			if tt.abandoned {
				projectAnalysisAbandonedLog := requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
				require.Equal(t, projectAnalysisAbandonedLog, f.logs[projectAnalysisAbandonedLog.ID])
				require.NoError(t, f.deliver(context.Background(), msg))
				require.Len(t, f.records, tt.recordCount+1)
			} else {
				require.Empty(t, f.logs)
			}
			require.Zero(t, f.resolverCalls)
			require.Zero(t, f.attributerCalls)
			require.Empty(t, f.facts)
			require.Empty(t, f.source)
		})
	}
}

func TestDLQDiscardsUnusableMessages(t *testing.T) {
	for _, scenario := range []string{
		"malformed", "invalid request ID", "tenant mismatch", "partition mismatch",
		"stored queue mismatch", "missing request", "unknown queue",
	} {
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
			case "missing request":
				f.failures["request/get"] = storage.ErrNotFound
			case "unknown queue":
				f.failures["storage/for"] = assert.AnError
			}
			require.NoError(t, f.recover(context.Background(), msg))
			require.Empty(t, f.records)
			require.Zero(t, f.resolverCalls)
			require.Zero(t, f.attributerCalls)
			require.NotContains(t, f.logs, requestlog.NewRequestEventLog(f.request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil).ID)
		})
	}
}

func TestDLQReportsOriginalFailure(t *testing.T) {
	f := newFixture(t)
	core, logs := observer.New(zap.ErrorLevel)
	f.dlq.logger = zap.New(core).Sugar()
	require.NoError(t, f.recover(context.Background(), f.message(t)))
	require.Len(t, logs.All(), 1)
	fields := logs.All()[0].ContextMap()
	require.Equal(t, "project analysis failed", fields["failure"])
	require.Equal(t, f.request.Queue, fields["queue"])
	require.Equal(t, f.request.ID, fields["request_id"])
	require.Equal(t, f.request.TerminalBuildID, fields["build_id"])
}
