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

package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	orchmock "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestParseOptions(t *testing.T) {
	for _, tt := range []struct {
		name  string
		args  []string
		valid bool
		apply bool
	}{
		{name: "dry run default", args: []string{"--queue", "q"}, valid: true},
		{name: "explicit dry run", args: []string{"--queue", "q", "--dry-run"}, valid: true},
		{name: "apply", args: []string{"--queue", "q", "--apply", "--recovery-id", "operator-run"}, valid: true, apply: true},
		{name: "no queues"},
		{name: "empty queue", args: []string{"--queue", ""}},
		{name: "whitespace queue", args: []string{"--queue", " q"}},
		{name: "duplicate queue", args: []string{"--queue", "q", "--queue", "q"}},
		{name: "apply requires identity", args: []string{"--queue", "q", "--apply"}},
		{name: "conflicting modes", args: []string{"--queue", "q", "--apply", "--dry-run", "--recovery-id", "run"}},
		{name: "positional argument", args: []string{"--queue", "q", "extra"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseOptions(tt.args, &bytes.Buffer{})
			if !tt.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.apply, opts.apply)
		})
	}
}

func recoveryStore(ctrl *gomock.Controller, batches []entity.Batch, sets map[string]entity.SpeculationPathSet) *orchmock.MockStorage {
	store := orchmock.NewMockStorage(ctrl)
	states := storagemock.NewMockQueueBatchStateStore(ctrl)
	batchStore := storagemock.NewMockBatchStore(ctrl)
	paths := storagemock.NewMockSpeculationPathSetStore(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(states).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationPathSetStore().Return(paths).AnyTimes()
	var records []entity.QueueBatchState
	for _, batch := range batches {
		records = append(records, entity.QueueBatchState{BatchID: batch.ID})
		batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	}
	states.EXPECT().List(gomock.Any(), entity.BatchStateSpeculating).Return(records, nil).AnyTimes()
	paths.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.SpeculationPathSet, error) {
		set, ok := sets[id]
		if !ok {
			return entity.SpeculationPathSet{}, storage.ErrNotFound
		}
		return set, nil
	}).AnyTimes()
	return store
}

func TestCancelledPathRecovery(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(map[bool]string{false: "dry run", true: "apply partial failure and repeat"}[apply], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cancelled := entity.SpeculationPathSet{Paths: []entity.SpeculationPathEntry{{Status: entity.SpeculationPathStatusCancelled}}}
			store := recoveryStore(ctrl, []entity.Batch{
				{ID: "10", State: entity.BatchStateSpeculating},
				{ID: "2", State: entity.BatchStateSpeculating},
				{ID: "3", State: entity.BatchStateSpeculating},
				{ID: "1", State: entity.BatchStateLanding},
			}, map[string]entity.SpeculationPathSet{"10": cancelled, "2": cancelled, "1": cancelled})
			publisher := queuemock.NewMockPublisher(ctrl)
			queue := queuemock.NewMockQueue(ctrl)
			queue.EXPECT().Publisher().Return(publisher).AnyTimes()
			registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: sqmq.TopicKeySpeculate, Name: "speculate", Queue: queue}})
			require.NoError(t, err)
			var messages []entityqueue.Message
			if apply {
				publisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
					messages = append(messages, msg)
					if len(messages) == 1 {
						return assert.AnError
					}
					return nil
				}).Times(3)
			}
			storeFor := func(string) (orchstorage.Storage, error) { return store, nil }
			opts := options{queues: []string{"q", "other"}, recoveryID: "operator-run", apply: apply}
			var output bytes.Buffer
			err = publishCancelledPathRecovery(context.Background(), opts, storeFor, registry, &output)
			if apply {
				require.ErrorIs(t, err, assert.AnError)
				opts.queues = []string{"q"}
				require.NoError(t, publishCancelledPathRecovery(context.Background(), opts, storeFor, registry, &output))
				require.Len(t, messages, 3)
				assert.Equal(t, messages[0].ID, messages[2].ID)
				assert.Equal(t, "2/cancelled-path-recovery/operator-run", messages[0].ID)
				for _, msg := range messages {
					id, queue, err := sqmq.UnmarshalID(sqmq.TopicKeySpeculate, msg.Payload)
					require.NoError(t, err)
					assert.Equal(t, "2", id)
					assert.Equal(t, queue, msg.Tenant)
					assert.Equal(t, queue, msg.PartitionKey)
				}
				assert.Contains(t, output.String(), "queued-head=2")
			} else {
				require.NoError(t, err)
				assert.Empty(t, messages)
				assert.NotContains(t, output.String(), "queued-head")
			}
			assert.Contains(t, output.String(), "affected-head=2")
		})
	}
}

func TestRecoveryInspection(t *testing.T) {
	for _, tt := range []struct {
		name    string
		state   entity.BatchState
		set     entity.SpeculationPathSet
		missing bool
		want    string
	}{
		{name: "no cancelled paths", state: entity.BatchStateSpeculating},
		{name: "missing path set", state: entity.BatchStateSpeculating, missing: true},
		{name: "cancelled live head", state: entity.BatchStateSpeculating, set: entity.SpeculationPathSet{Paths: []entity.SpeculationPathEntry{{Status: entity.SpeculationPathStatusCancelled}}}, want: "2"},
		{name: "terminal head is stale membership", state: entity.BatchStateCancelled, set: entity.SpeculationPathSet{Paths: []entity.SpeculationPathEntry{{Status: entity.SpeculationPathStatusCancelled}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			sets := map[string]entity.SpeculationPathSet{}
			if !tt.missing {
				sets["2"] = tt.set
			}
			store := recoveryStore(ctrl, []entity.Batch{{ID: "2", State: tt.state}}, sets)
			id, err := affectedHead(context.Background(), store)
			require.NoError(t, err)
			assert.Equal(t, tt.want, id)
		})
	}
	t.Run("unavailable storage", func(t *testing.T) {
		for _, failure := range []error{nil, assert.AnError} {
			err := publishCancelledPathRecovery(context.Background(), options{queues: []string{"q"}}, func(string) (orchstorage.Storage, error) { return nil, failure }, consumer.TopicRegistry{}, &bytes.Buffer{})
			require.Error(t, err)
		}
	})
	t.Run("path read fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := orchmock.NewMockStorage(ctrl)
		states := storagemock.NewMockQueueBatchStateStore(ctrl)
		batches := storagemock.NewMockBatchStore(ctrl)
		paths := storagemock.NewMockSpeculationPathSetStore(ctrl)
		store.EXPECT().GetQueueBatchStateStore().Return(states)
		store.EXPECT().GetBatchStore().Return(batches)
		store.EXPECT().GetSpeculationPathSetStore().Return(paths)
		states.EXPECT().List(gomock.Any(), entity.BatchStateSpeculating).Return([]entity.QueueBatchState{{BatchID: "2"}}, nil)
		batches.EXPECT().Get(gomock.Any(), "2").Return(entity.Batch{ID: "2", State: entity.BatchStateSpeculating}, nil)
		paths.EXPECT().Get(gomock.Any(), "2").Return(entity.SpeculationPathSet{}, assert.AnError)
		_, err := affectedHead(context.Background(), store)
		require.ErrorIs(t, err, assert.AnError)
	})
	t.Run("missing publisher", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := recoveryStore(ctrl, []entity.Batch{{ID: "2", State: entity.BatchStateSpeculating}}, map[string]entity.SpeculationPathSet{
			"2": {Paths: []entity.SpeculationPathEntry{{Status: entity.SpeculationPathStatusCancelled}}},
		})
		err := publishCancelledPathRecovery(context.Background(), options{queues: []string{"q"}, apply: true, recoveryID: "run"}, func(string) (orchstorage.Storage, error) { return store, nil }, consumer.TopicRegistry{}, &bytes.Buffer{})
		require.Error(t, err)
	})
}
