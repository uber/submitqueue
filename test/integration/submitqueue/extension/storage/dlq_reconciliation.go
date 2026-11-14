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

package storage

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func (s *StorageContractSuite) TestStorage_DLQReconciliationSnapshots() {
	t := s.T()
	ctx := s.ctx
	store := s.forQueue("dlq-snapshots").GetDLQReconciliationStore()
	source := entity.DLQReconciliationSource{Topic: "speculate.final", PartitionKey: "queue", MessageID: "intent"}
	_, err := store.Get(ctx, source)
	require.ErrorIs(t, err, storage.ErrNotFound)

	initial := entity.DLQReconciliation{ID: "1", Queue: "dlq-snapshots", Source: source, BatchIDs: []string{"10"}, Version: 1}
	require.NoError(t, store.Create(ctx, initial))
	contender := initial
	contender.ID = "2"
	require.ErrorIs(t, store.Create(ctx, contender), storage.ErrAlreadyExists)
	winner, err := store.Get(ctx, source)
	require.NoError(t, err)
	assert.Equal(t, initial, winner)

	completed := initial
	completed.BatchIDs = append(slices.Clone(initial.BatchIDs), "11")
	completed.Completed = true
	completed.Version = 99
	require.NoError(t, store.Update(ctx, completed, 1, 2))
	assert.Equal(t, int32(99), completed.Version)
	assert.Equal(t, []string{"10"}, initial.BatchIDs)
	require.ErrorIs(t, store.Update(ctx, initial, 1, 2), storage.ErrVersionMismatch)
	require.ErrorIs(t, store.Update(ctx, contender, 2, 3), storage.ErrVersionMismatch)

	reloadedStore := s.forQueue(initial.Queue).GetDLQReconciliationStore()
	persisted, err := reloadedStore.Get(ctx, source)
	require.NoError(t, err)
	completed.Version = 2
	assert.Equal(t, completed, persisted)
}

func (s *StorageContractSuite) TestStorage_DLQReconciliationSourceIdentity() {
	t := s.T()
	base := entity.DLQReconciliation{
		Queue: "dlq-identity", Source: entity.DLQReconciliationSource{Topic: "speculate.final", PartitionKey: "partition", MessageID: "message"},
		BatchIDs: []string{}, Version: 1,
	}
	tests := []struct {
		name   string
		change func(*entity.DLQReconciliation)
	}{
		{name: "base", change: func(*entity.DLQReconciliation) {}},
		{name: "other queue", change: func(r *entity.DLQReconciliation) { r.Queue += "-other" }},
		{name: "queue case", change: func(r *entity.DLQReconciliation) { r.Queue = strings.ToUpper(r.Queue) }},
		{name: "queue trailing space", change: func(r *entity.DLQReconciliation) { r.Queue += " " }},
		{name: "other topic", change: func(r *entity.DLQReconciliation) { r.Source.Topic = "build.final" }},
		{name: "topic case", change: func(r *entity.DLQReconciliation) { r.Source.Topic = strings.ToUpper(r.Source.Topic) }},
		{name: "topic trailing space", change: func(r *entity.DLQReconciliation) { r.Source.Topic += " " }},
		{name: "partition case", change: func(r *entity.DLQReconciliation) { r.Source.PartitionKey = "Partition" }},
		{name: "partition trailing space", change: func(r *entity.DLQReconciliation) { r.Source.PartitionKey += " " }},
		{name: "message case", change: func(r *entity.DLQReconciliation) { r.Source.MessageID = "Message" }},
		{name: "message trailing space", change: func(r *entity.DLQReconciliation) { r.Source.MessageID += " " }},
		{name: "maximum key width", change: func(r *entity.DLQReconciliation) {
			r.Queue = strings.Repeat("q", 255)
			r.Source.Topic = strings.Repeat("t", 255)
			r.Source.PartitionKey = strings.Repeat("🌳", 255)
			r.Source.MessageID = strings.Repeat("🚀", 255)
		}},
	}
	records := make([]entity.DLQReconciliation, 0, len(tests))
	for i, tt := range tests {
		record := base
		record.ID = fmt.Sprint(i + 1)
		tt.change(&record)
		s.Run(tt.name, func() {
			require.NoError(s.T(), s.forQueue(record.Queue).GetDLQReconciliationStore().Create(s.ctx, record))
		})
		records = append(records, record)
	}
	for _, record := range records {
		got, err := s.forQueue(record.Queue).GetDLQReconciliationStore().Get(s.ctx, record.Source)
		require.NoError(t, err)
		assert.Equal(t, record, got)
	}
	otherQueue := s.forQueue("dlq-unrelated").GetDLQReconciliationStore()
	_, err := otherQueue.Get(s.ctx, base.Source)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Error(t, otherQueue.Create(s.ctx, records[0]))
	require.Error(t, otherQueue.Update(s.ctx, records[0], 1, 2))
}
