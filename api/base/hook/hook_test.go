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

package hook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func mustStruct(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(fields)
	require.NoError(t, err)
	return s
}

func TestHookEventRoundTrip(t *testing.T) {
	cases := map[string]*HookEvent{
		"versioned with nested payload": {
			Id:          "submitqueue/batch.failed/batch-778/4",
			Source:      "submitqueue",
			Type:        "batch.failed",
			TimestampMs: 1722800012345,
			Version:     4,
			Payload: mustStruct(t, map[string]any{
				"batch_id":       "batch-778",
				"queue":          "go-monorepo",
				"error":          "merge conflict",
				"failed_step":    "sq-12346",
				"conflict_paths": []any{"foo/bar.go"},
			}),
		},
		"unversioned": {
			Id:          "runway/land.completed/queue-a-42/msg-9/0",
			Source:      "runway",
			Type:        "land.completed",
			TimestampMs: 1722800012345,
			Payload:     mustStruct(t, map[string]any{"request_id": "queue-a/42"}),
		},
		"envelope only": {
			Id:          "stovepipe/commit.green/git-abc/1",
			Source:      "stovepipe",
			Type:        "commit.green",
			TimestampMs: 1722800012345,
			Version:     1,
		},
	}

	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			data, err := Marshal(event)
			require.NoError(t, err)

			got := &HookEvent{}
			require.NoError(t, Unmarshal(data, got))
			assert.True(t, proto.Equal(event, got), "round-tripped HookEvent should equal the original")
		})
	}
}

func TestWireFormat(t *testing.T) {
	data, err := Marshal(&HookEvent{
		Id:          "submitqueue/batch.failed/batch-778/4",
		Source:      "submitqueue",
		Type:        "batch.failed",
		TimestampMs: 1722800012345,
		Version:     4,
	})
	require.NoError(t, err)

	assert.Contains(t, string(data), `"timestamp_ms"`, "fields must serialize as snake_case")
	assert.Contains(t, string(data), `"1722800012345"`, "int64 must serialize as a JSON string")
}

func TestUnmarshalDiscardsUnknownFields(t *testing.T) {
	data := []byte(`{"id":"a/b/c/1","source":"a","type":"b","field_from_the_future":7}`)

	got := &HookEvent{}
	require.NoError(t, Unmarshal(data, got))
	assert.Equal(t, "a/b/c/1", got.GetId())
}

func TestTopicKeysBindEveryTopicKey(t *testing.T) {
	bound := map[string]int{}
	for _, m := range []proto.Message{&HookEvent{}} {
		keys := TopicKeys(m)
		require.NotEmpty(t, keys, "message must declare a non-empty topic_keys option")
		for _, key := range keys {
			bound[key]++
		}
	}

	keys := []TopicKey{TopicKeyHook}

	valid := map[string]bool{}
	for _, k := range keys {
		valid[k.String()] = true
		assert.Equalf(t, 1, bound[k.String()], "topic key %q must be bound to exactly one message via the topic_keys option", k)
	}
	for key := range bound {
		assert.Truef(t, valid[key], "topic_keys option names unknown key %q", key)
	}
}

func TestEventIDIsDerived(t *testing.T) {
	t.Run("same transition mints the same id", func(t *testing.T) {
		assert.Equal(t,
			NewEventID("submitqueue", "batch.failed", "batch-778", 4),
			NewEventID("submitqueue", "batch.failed", "batch-778", 4),
		)
	})

	t.Run("distinct transitions mint distinct ids", func(t *testing.T) {
		ids := map[string]string{
			"baseline":        NewEventID("submitqueue", "batch.failed", "batch-778", 4),
			"later version":   NewEventID("submitqueue", "batch.failed", "batch-778", 5),
			"other subject":   NewEventID("submitqueue", "batch.failed", "batch-779", 4),
			"other type":      NewEventID("submitqueue", "batch.succeeded", "batch-778", 4),
			"other source":    NewEventID("stovepipe", "batch.failed", "batch-778", 4),
			"unversioned":     NewUnversionedEventID("submitqueue", "batch.failed", "batch-778", "msg-1", 0),
			"second ordinal":  NewUnversionedEventID("submitqueue", "batch.failed", "batch-778", "msg-1", 1),
			"other cause":     NewUnversionedEventID("submitqueue", "batch.failed", "batch-778", "msg-2", 0),
			"slashed subject": NewEventID("stovepipe", "commit.green", "request/monorepo/main/42", 4),
		}

		seen := map[string]string{}
		for name, id := range ids {
			if other, dup := seen[id]; dup {
				t.Errorf("%q and %q both mint id %q", name, other, id)
			}
			seen[id] = name
		}
	})
}

func TestValidate(t *testing.T) {
	valid := func() *HookEvent {
		return &HookEvent{Id: "submitqueue/batch.failed/batch-778/4", Source: "submitqueue", Type: "batch.failed"}
	}

	t.Run("well-formed event", func(t *testing.T) {
		require.NoError(t, Validate(valid()))
	})

	t.Run("unversioned event is well-formed", func(t *testing.T) {
		event := valid()
		event.Version = 0
		require.NoError(t, Validate(event))
	})

	t.Run("event with no payload is well-formed", func(t *testing.T) {
		event := valid()
		event.Payload = nil
		require.NoError(t, Validate(event))
	})

	malformed := map[string]*HookEvent{
		"nil":       nil,
		"no id":     {Source: "submitqueue", Type: "batch.failed"},
		"no source": {Id: "submitqueue/batch.failed/batch-778/4", Type: "batch.failed"},
		"no type":   {Id: "submitqueue/batch.failed/batch-778/4", Source: "submitqueue"},
	}
	for name, event := range malformed {
		t.Run(name, func(t *testing.T) {
			require.Error(t, Validate(event))
		})
	}
}
