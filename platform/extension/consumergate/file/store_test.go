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

package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/extension/consumergate"
)

func TestIsGated(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		close     []consumergate.Key
		group     string
		partition string
		want      bool
	}{
		{
			name:      "no gates",
			group:     "orchestrator-batch",
			partition: "queue-a",
			want:      false,
		},
		{
			name:      "all-partitions gate matches any partition",
			close:     []consumergate.Key{{ConsumerGroup: "orchestrator-batch"}},
			group:     "orchestrator-batch",
			partition: "queue-a",
			want:      true,
		},
		{
			name:      "all-partitions gate matches empty partition",
			close:     []consumergate.Key{{ConsumerGroup: "orchestrator-batch"}},
			group:     "orchestrator-batch",
			partition: "",
			want:      true,
		},
		{
			name:      "partition gate matches its partition",
			close:     []consumergate.Key{{ConsumerGroup: "orchestrator-batch", PartitionKey: "queue-a"}},
			group:     "orchestrator-batch",
			partition: "queue-a",
			want:      true,
		},
		{
			name:      "partition gate leaves other partitions open",
			close:     []consumergate.Key{{ConsumerGroup: "orchestrator-batch", PartitionKey: "queue-a"}},
			group:     "orchestrator-batch",
			partition: "queue-b",
			want:      false,
		},
		{
			name:      "gate on one group leaves other groups open",
			close:     []consumergate.Key{{ConsumerGroup: "orchestrator-batch"}},
			group:     "runway-merge",
			partition: "queue-a",
			want:      false,
		},
		{
			name:      "partition key with slash is encoded and matched",
			close:     []consumergate.Key{{ConsumerGroup: "orchestrator-batch", PartitionKey: "queue/1"}},
			group:     "orchestrator-batch",
			partition: "queue/1",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := New(t.TempDir())
			for _, key := range tt.close {
				require.NoError(t, store.Close(ctx, key, consumergate.Metadata{Reason: "test", CreatedBy: "unit", CreatedAtMs: 1}))
			}
			got, err := store.isGated(tt.group, tt.partition)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOpenClosesGate(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir())
	key := consumergate.Key{ConsumerGroup: "orchestrator-batch", PartitionKey: "queue-a"}

	require.NoError(t, store.Close(ctx, key, consumergate.Metadata{Reason: "pause", CreatedBy: "unit", CreatedAtMs: 1}))
	gated, err := store.isGated(key.ConsumerGroup, key.PartitionKey)
	require.NoError(t, err)
	require.True(t, gated)

	require.NoError(t, store.Open(ctx, key))
	gated, err = store.isGated(key.ConsumerGroup, key.PartitionKey)
	require.NoError(t, err)
	assert.False(t, gated)

	// Opening an already-open gate is a no-op.
	require.NoError(t, store.Open(ctx, key))
}

func TestCloseRequiresConsumerGroup(t *testing.T) {
	store := New(t.TempDir())
	err := store.Close(context.Background(), consumergate.Key{}, consumergate.Metadata{})
	require.Error(t, err)
}

func TestParkedRecordLifecycle(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir())

	parked := consumergate.Parked{
		ConsumerGroup: "runway-mergeconflictcheck",
		Topic:         "merge-conflict-check",
		MessageID:     "e2e-queue/42",
		PartitionKey:  "e2e-queue",
		Payload:       []byte(`{"id":"e2e-queue/42"}`),
		Attempt:       1,
		ParkedAtMs:    1111,
	}
	require.NoError(t, store.recordParked(parked))

	records, err := store.ListParked(ctx, parked.ConsumerGroup)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, parked, records[0])

	// Re-recording the same delivery (redelivery) overwrites, not duplicates.
	parked.Attempt = 2
	require.NoError(t, store.recordParked(parked))
	records, err = store.ListParked(ctx, parked.ConsumerGroup)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, 2, records[0].Attempt)

	require.NoError(t, store.removeParked(parked.ConsumerGroup, parked.Topic, parked.MessageID))
	records, err = store.ListParked(ctx, parked.ConsumerGroup)
	require.NoError(t, err)
	assert.Empty(t, records)

	// Removing an already-absent record is a no-op.
	require.NoError(t, store.removeParked(parked.ConsumerGroup, parked.Topic, parked.MessageID))
}

func TestListParkedEmpty(t *testing.T) {
	store := New(t.TempDir())
	records, err := store.ListParked(context.Background(), "no-such-group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestListParkedSkipsTempFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := New(dir)

	parked := consumergate.Parked{
		ConsumerGroup: "group",
		Topic:         "topic",
		MessageID:     "id",
		PartitionKey:  "part",
		ParkedAtMs:    1,
	}
	require.NoError(t, store.recordParked(parked))

	// Simulate an in-flight temp file awaiting rename alongside the record.
	tmpPath := filepath.Join(dir, "parked", "group", "topic", "id.json.tmp123")
	require.NoError(t, os.WriteFile(tmpPath, []byte("partial"), 0o644))

	records, err := store.ListParked(ctx, "group")
	require.NoError(t, err)
	assert.Len(t, records, 1)
}

func TestMissingDirIsNotGated(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "does-not-exist"))
	gated, err := store.isGated("group", "part")
	require.NoError(t, err)
	assert.False(t, gated)
}

func TestEnter_OpenGateUnblocked(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir())

	descriptor := consumergate.DeliveryDescriptor{
		Topic:     "topic",
		MessageID: "msg-1",
		Payload:   []byte("hello"),
		Attempt:   1,
	}

	entry, err := store.Enter(ctx, consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	assert.False(t, entry.Blocked())

	// Unparking a never-parked delivery is a no-op on the admit path.
	require.NoError(t, entry.Unpark(ctx, descriptor))

	records, err := store.ListParked(ctx, "group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestEnter_ClosedGateParkAndRelease(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir())
	key := consumergate.Key{ConsumerGroup: "group"}

	require.NoError(t, store.Close(ctx, key, consumergate.Metadata{Reason: "test", CreatedBy: "unit", CreatedAtMs: 1}))

	descriptor := consumergate.DeliveryDescriptor{
		Topic:     "topic",
		MessageID: "msg-1",
		Payload:   []byte("hello"),
		Attempt:   1,
	}

	entry, err := store.Enter(ctx, consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	require.True(t, entry.Blocked())

	// Park records the delivery; the caller supplies only the delivery
	// content, the store stamps the entered identity and ParkedAtMs.
	require.NoError(t, entry.Park(ctx, descriptor))

	records, err := store.ListParked(ctx, "group")
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "group", records[0].ConsumerGroup)
	assert.Equal(t, "part", records[0].PartitionKey)
	assert.Equal(t, "msg-1", records[0].MessageID)
	assert.Equal(t, "topic", records[0].Topic)
	assert.Equal(t, []byte("hello"), records[0].Payload)
	assert.Equal(t, 1, records[0].Attempt)
	assert.NotZero(t, records[0].ParkedAtMs)

	// Re-parking on a re-check overwrites the record, not duplicates it.
	descriptor.Attempt = 1 // postponed redeliveries restart at attempt 1
	require.NoError(t, entry.Park(ctx, descriptor))
	records, err = store.ListParked(ctx, "group")
	require.NoError(t, err)
	require.Len(t, records, 1)

	// Open the gate; the next Enter is unblocked and Unpark removes the record.
	require.NoError(t, store.Open(ctx, key))
	entry, err = store.Enter(ctx, consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	require.False(t, entry.Blocked())
	require.NoError(t, entry.Unpark(ctx, descriptor))

	records, err = store.ListParked(ctx, "group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestEnter_MediumError(t *testing.T) {
	// Make the store root a regular file so stat fails with ENOTDIR.
	dir := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(dir, []byte("x"), 0o644))

	store := New(dir)
	_, err := store.Enter(context.Background(), consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.Error(t, err)
}
