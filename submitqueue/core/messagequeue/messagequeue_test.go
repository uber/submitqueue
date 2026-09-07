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

package messagequeue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestStartRoundTrip(t *testing.T) {
	req := StartFromLandRequest(entity.LandRequest{
		ID:           "q/1",
		Queue:        "q",
		Change:       change.Change{URIs: []string{"github://github.example.com/org/repo/pull/1/0123456789abcdef0123456789abcdef01234567"}},
		LandStrategy: mergestrategy.MergeStrategySquashRebase,
	})

	data, err := Marshal(req)
	require.NoError(t, err)

	got := &Start{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(req, got))
	assert.Equal(t, mergestrategy.MergeStrategySquashRebase, LandRequestFromStart(got).LandStrategy)
}

func TestCancelRoundTrip(t *testing.T) {
	msg := CancelFromEntity(entity.CancelRequest{ID: "q/7", Queue: "q", Reason: "user"})
	data, err := Marshal(msg)
	require.NoError(t, err)
	got := &Cancel{}
	require.NoError(t, Unmarshal(data, got))
	assert.Equal(t, entity.CancelRequest{ID: "q/7", Queue: "q", Reason: "user"}, CancelToEntity(got))
}

func TestIDMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  proto.Message
		into proto.Message
	}{
		{name: "validate", msg: &Validate{Id: "q/1", Queue: "q"}, into: &Validate{}},
		{name: "batch", msg: &Batch{Id: "q/1", Queue: "q"}, into: &Batch{}},
		{name: "dependency-analysis", msg: &DependencyAnalysis{Id: "q/batch/1", Queue: "q"}, into: &DependencyAnalysis{}},
		{name: "speculate", msg: &Speculate{Id: "q/batch/1", Queue: "q"}, into: &Speculate{}},
		{name: "build", msg: &Build{Id: "q/batch/1", Queue: "q"}, into: &Build{}},
		{name: "buildsignal", msg: &BuildSignal{Id: "build-1", Queue: "q"}, into: &BuildSignal{}},
		{name: "merge", msg: &Merge{Id: "q/batch/1", Queue: "q"}, into: &Merge{}},
		{name: "conclude", msg: &Conclude{Id: "q/batch/1", Queue: "q"}, into: &Conclude{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Marshal(tt.msg)
			require.NoError(t, err)
			require.NoError(t, Unmarshal(data, tt.into))
			assert.True(t, proto.Equal(tt.msg, tt.into))
		})
	}
}

func TestLogRoundTrip(t *testing.T) {
	entry := entity.NewRequestStatusLog("q", "q/1", entity.RequestStatusStarted, 1, "boom", map[string]string{"k": "v"})
	entry.TimestampMs = 1700000000000

	data, err := Marshal(LogFromEntity(entry))
	require.NoError(t, err)
	got := &Log{}
	require.NoError(t, Unmarshal(data, got))
	assert.Equal(t, entry, LogToEntity(got))
}

func TestLogToEntityDefaults(t *testing.T) {
	got := LogToEntity(&Log{RequestId: "q/1", Queue: "q"})
	assert.Equal(t, entity.RequestLogTypeStatus, got.Type)
	assert.NotNil(t, got.Metadata)
	assert.Empty(t, got.Metadata)
}

func TestLogEventRoundTrip(t *testing.T) {
	entry := entity.NewRequestEventLog("q", "q/1", entity.RequestEventBuilding, map[string]string{"build_id": "b/7"})
	entry.TimestampMs = 1700000000000

	data, err := Marshal(LogFromEntity(entry))
	require.NoError(t, err)
	got, err := UnmarshalRequestLog(data)
	require.NoError(t, err)
	assert.Equal(t, entry, got)
	assert.Equal(t, entity.RequestLogTypeEvent, got.Type)
	assert.Equal(t, entity.RequestStatusUnknown, got.Status)
	assert.Equal(t, int32(0), got.RequestVersion)
}

func TestLandStrategyMapping(t *testing.T) {
	tests := []struct {
		name string
		in   mergestrategy.MergeStrategy
		want mergestrategy.MergeStrategy
	}{
		{name: "rebase", in: mergestrategy.MergeStrategyRebase, want: mergestrategy.MergeStrategyRebase},
		{name: "squash", in: mergestrategy.MergeStrategySquashRebase, want: mergestrategy.MergeStrategySquashRebase},
		{name: "merge", in: mergestrategy.MergeStrategyMerge, want: mergestrategy.MergeStrategyMerge},
		{name: "promote", in: mergestrategy.MergeStrategyPromote, want: mergestrategy.MergeStrategyPromote},
		{name: "unknown_stays_unknown", in: mergestrategy.MergeStrategyUnknown, want: mergestrategy.MergeStrategyUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LandRequestFromStart(StartFromLandRequest(entity.LandRequest{LandStrategy: tt.in}))
			assert.Equal(t, tt.want, got.LandStrategy)
		})
	}
}

func TestStartNilChange(t *testing.T) {
	got := LandRequestFromStart(&Start{Id: "q/1", Queue: "q"})
	assert.Nil(t, got.Change.URIs)
	assert.Equal(t, mergestrategy.MergeStrategyUnknown, got.LandStrategy)
}

// TestWireFormat locks protojson encoding: snake_case names, UPPER_SNAKE enums,
// and int64 as a JSON string.
func TestWireFormat(t *testing.T) {
	start, err := Marshal(StartFromLandRequest(entity.LandRequest{
		ID:           "q/1",
		Queue:        "q",
		Change:       change.Change{URIs: []string{"u"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
	}))
	require.NoError(t, err)
	assert.Contains(t, string(start), `"id"`)
	assert.Contains(t, string(start), `"queue"`)
	assert.Contains(t, string(start), `"land_strategy"`)
	assert.Contains(t, string(start), `"REBASE"`)

	logBytes, err := Marshal(LogFromEntity(entity.RequestLog{RequestID: "q/1", TimestampMs: 42}))
	require.NoError(t, err)
	assert.Contains(t, string(logBytes), `"timestamp_ms":"42"`)
}

func TestMarshalIDRoundTripPerTopic(t *testing.T) {
	keys := []TopicKey{
		TopicKeyValidate,
		TopicKeyBatch,
		TopicKeyDependencyAnalysis,
		TopicKeySpeculate,
		TopicKeyBuild,
		TopicKeyBuildSignal,
		TopicKeyLand,
		TopicKeyConclude,
	}
	for _, key := range keys {
		t.Run(key.String(), func(t *testing.T) {
			data, err := MarshalID(key, "id-1", "q")
			require.NoError(t, err)
			id, queue, err := UnmarshalID(key, data)
			require.NoError(t, err)
			assert.Equal(t, "id-1", id)
			assert.Equal(t, "q", queue)
		})
	}

	rid, err := UnmarshalRequestID(TopicKeyValidate, []byte(`{"id":"r","queue":"q"}`))
	require.NoError(t, err)
	assert.Equal(t, entity.RequestID{ID: "r", Queue: "q"}, rid)

	build, err := UnmarshalBuildID(TopicKeyBuildSignal, []byte(`{"id":"b","queue":"q"}`))
	require.NoError(t, err)
	assert.Equal(t, entity.BuildID{ID: "b", Queue: "q"}, build)

	_, err = UnmarshalRequestID(TopicKeyValidate, []byte(`{`))
	require.Error(t, err)
	_, err = UnmarshalBatchID(TopicKeySpeculate, []byte(`{`))
	require.Error(t, err)
	_, err = UnmarshalBuildID(TopicKeyBuildSignal, []byte(`{`))
	require.Error(t, err)
	_, err = UnmarshalLandRequest([]byte(`{`))
	require.Error(t, err)
	_, err = UnmarshalCancelRequest([]byte(`{`))
	require.Error(t, err)
	_, err = UnmarshalRequestLog([]byte(`{`))
	require.Error(t, err)

	land, err := UnmarshalLandRequest([]byte(`{"id":"q/1","queue":"q"}`))
	require.NoError(t, err)
	assert.Equal(t, "q/1", land.ID)
	cancel, err := UnmarshalCancelRequest([]byte(`{"id":"q/7","queue":"q","reason":"x"}`))
	require.NoError(t, err)
	assert.Equal(t, "x", cancel.Reason)
	logEntry, err := UnmarshalRequestLog([]byte(`{"request_id":"q/1","queue":"q"}`))
	require.NoError(t, err)
	assert.Equal(t, entity.RequestLogTypeStatus, logEntry.Type)
}

func TestMarshalIDRejectsUnknownTopic(t *testing.T) {
	_, err := MarshalID(TopicKeyStart, "q/1", "q")
	require.Error(t, err)
}

func TestUnmarshalIDAndTypedIDs(t *testing.T) {
	data, err := MarshalID(TopicKeyBuild, "q/batch/1", "q")
	require.NoError(t, err)

	id, queue, err := UnmarshalID(TopicKeyBuild, data)
	require.NoError(t, err)
	assert.Equal(t, "q/batch/1", id)
	assert.Equal(t, "q", queue)

	bid, err := UnmarshalBatchID(TopicKeyBuild, data)
	require.NoError(t, err)
	assert.Equal(t, entity.BatchID{ID: "q/batch/1", Queue: "q"}, bid)

	_, _, err = UnmarshalID(TopicKeyBuild, []byte(`{`))
	require.Error(t, err)
}

func TestUnmarshalIDRejectsNonIDTopic(t *testing.T) {
	_, _, err := UnmarshalID(TopicKeyStart, []byte(`{"id":"q/1","queue":"q"}`))
	require.Error(t, err)
	_, err = UnmarshalRequestID(TopicKeySpeculate, []byte(`{"id":"q/1","queue":"q"}`))
	require.Error(t, err)
	_, err = UnmarshalBatchID(TopicKeyValidate, []byte(`{"id":"q/1","queue":"q"}`))
	require.Error(t, err)
	_, err = UnmarshalBuildID(TopicKeyBuild, []byte(`{"id":"b","queue":"q"}`))
	require.Error(t, err)
}

func TestTopicKeysBindEveryTopicKey(t *testing.T) {
	bound := map[string]int{}
	for _, m := range []proto.Message{
		&Start{}, &Cancel{}, &Validate{}, &Batch{}, &DependencyAnalysis{},
		&Speculate{}, &Build{}, &BuildSignal{}, &Merge{}, &Conclude{}, &Log{},
	} {
		keys := TopicKeys(m)
		require.NotEmpty(t, keys, "message must declare a non-empty topic_keys option")
		for _, key := range keys {
			bound[key]++
		}
	}

	keys := []TopicKey{
		TopicKeyStart,
		TopicKeyCancel,
		TopicKeyValidate,
		TopicKeyBatch,
		TopicKeyDependencyAnalysis,
		TopicKeySpeculate,
		TopicKeyBuild,
		TopicKeyBuildSignal,
		TopicKeyLand,
		TopicKeyConclude,
		TopicKeyLog,
	}

	valid := map[string]bool{}
	for _, k := range keys {
		valid[k.String()] = true
		assert.Equalf(t, 1, bound[k.String()], "topic key %q must be bound to exactly one message via the topic_keys option", k)
	}
	for key := range bound {
		assert.Truef(t, valid[key], "topic_keys option names unknown key %q", key)
	}
}
