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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/extension/consumergate"
)

// testCfg keeps Wait tests fast: 5ms poll interval.
var testCfg = consumergate.Config{PollIntervalMs: 5}

// awaitParked indefinitely waits for a parked record to appear in the store, returning the records.
// It will wait up until the test times out.
func awaitParked(t *testing.T, store *Store, ctx context.Context, consumerGroup string) []consumergate.Parked {
	t.Helper()
	ticker := time.NewTicker(time.Duration(testCfg.PollIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		records, err := store.ListParked(ctx, consumerGroup)
		require.NoError(t, err)
		if len(records) > 0 {
			return records
		}
		<-ticker.C
	}
}

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
			store := New(t.TempDir(), testCfg)
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
	store := New(t.TempDir(), testCfg)
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
	store := New(t.TempDir(), testCfg)
	err := store.Close(context.Background(), consumergate.Key{}, consumergate.Metadata{})
	require.Error(t, err)
}

func TestParkedRecordLifecycle(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir(), testCfg)

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
	store := New(t.TempDir(), testCfg)
	records, err := store.ListParked(context.Background(), "no-such-group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestListParkedSkipsTempFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := New(dir, testCfg)

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
	store := New(filepath.Join(t.TempDir(), "does-not-exist"), testCfg)
	gated, err := store.isGated("group", "part")
	require.NoError(t, err)
	assert.False(t, gated)
}

func TestEnter_OpenGateUnblocked(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir(), testCfg)

	entry, err := store.Enter(ctx, consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	assert.False(t, entry.Blocked())
	require.NoError(t, consumergate.Wait(ctx, entry, consumergate.DeliveryDescriptor{
		Topic:     "topic",
		MessageID: "msg-1",
		Payload:   []byte("hello"),
		Attempt:   1,
	}))

	// No parked record should exist — the gate was open.
	records, err := store.ListParked(ctx, "group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestEnter_ClosedGateParksThenReleases(t *testing.T) {
	ctx := context.Background()
	store := New(t.TempDir(), testCfg)
	key := consumergate.Key{ConsumerGroup: "group"}

	require.NoError(t, store.Close(ctx, key, consumergate.Metadata{Reason: "test", CreatedBy: "unit", CreatedAtMs: 1}))

	entry, err := store.Enter(ctx, consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	require.True(t, entry.Blocked())

	// Wait records the parked delivery before blocking; the caller supplies
	// only the delivery content, the store stamps the entered identity.
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- consumergate.Wait(ctx, entry, consumergate.DeliveryDescriptor{
			Topic:     "topic",
			MessageID: "msg-1",
			Payload:   []byte("hello"),
			Attempt:   1,
		})
	}()

	records := awaitParked(t, store, ctx, "group")
	require.Len(t, records, 1)
	assert.Equal(t, "group", records[0].ConsumerGroup)
	assert.Equal(t, "part", records[0].PartitionKey)
	assert.Equal(t, "msg-1", records[0].MessageID)
	assert.Equal(t, "topic", records[0].Topic)
	assert.Equal(t, []byte("hello"), records[0].Payload)
	assert.Equal(t, 1, records[0].Attempt)
	assert.NotZero(t, records[0].ParkedAtMs)

	// Assert Wait has not returned yet.
	select {
	case <-waitDone:
		t.Fatal("Wait returned before the gate was opened")
	default:
	}

	// Open the gate — Wait should return nil and remove the active parked
	// record before returning.
	require.NoError(t, store.Open(ctx, key))
	require.NoError(t, <-waitDone)

	records, err = store.ListParked(ctx, "group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestEnter_ClosedGateCtxCancel(t *testing.T) {
	store := New(t.TempDir(), testCfg)
	key := consumergate.Key{ConsumerGroup: "group"}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, store.Close(ctx, key, consumergate.Metadata{Reason: "test", CreatedBy: "unit", CreatedAtMs: 1}))

	entry, err := store.Enter(ctx, consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	require.True(t, entry.Blocked())

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- consumergate.Wait(ctx, entry, consumergate.DeliveryDescriptor{
			Topic:     "topic",
			MessageID: "msg-1",
			Payload:   []byte("hello"),
			Attempt:   1,
		})
	}()

	// Wait until the parked record appears, then cancel.
	awaitParked(t, store, ctx, "group")

	cancel()
	require.ErrorIs(t, <-waitDone, context.Canceled)

	// Cancellation ends the active wait, so its parked record is removed.
	records, err := store.ListParked(context.Background(), "group")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestEnter_MediumError(t *testing.T) {
	// Make the store root a regular file so stat fails with ENOTDIR.
	dir := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(dir, []byte("x"), 0o644))

	store := New(dir, testCfg)
	_, err := store.Enter(context.Background(), consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.Error(t, err)
}
