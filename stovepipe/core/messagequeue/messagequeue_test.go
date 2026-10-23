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
)

func TestProcessRequestRoundTrip(t *testing.T) {
	req := &ProcessRequest{Id: "42"}

	data, err := Marshal(req)
	require.NoError(t, err)

	got := &ProcessRequest{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(req, got), "round-tripped ProcessRequest should equal the original")
}

func TestBuildRequestRoundTrip(t *testing.T) {
	req := &BuildRequest{Id: "42"}

	data, err := Marshal(req)
	require.NoError(t, err)

	got := &BuildRequest{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(req, got), "round-tripped BuildRequest should equal the original")
}

func TestBuildSignalRoundTrip(t *testing.T) {
	sig := &BuildSignal{Id: "bk-1001"}

	data, err := Marshal(sig)
	require.NoError(t, err)

	got := &BuildSignal{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(sig, got), "round-tripped BuildSignal should equal the original")
}

func TestRecordRoundTrip(t *testing.T) {
	rec := &Record{Id: "bk-1001"}

	data, err := Marshal(rec)
	require.NoError(t, err)

	got := &Record{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(rec, got), "round-tripped Record should equal the original")
}

// TestWireFormat locks the protojson encoding decision the contract relies on:
// snake_case field names (UseProtoNames).
func TestWireFormat(t *testing.T) {
	data, err := Marshal(&ProcessRequest{Id: "42"})
	require.NoError(t, err)

	assert.Contains(t, string(data), `"id"`, "fields must serialize as snake_case")
}

// TestTopicKeysBindEveryTopicKey is the topic-binding drift guard: every
// Stovepipe topic key is carried by exactly one message's topic_keys option, and
// no topic_keys option names an unknown key.
func TestTopicKeysBindEveryTopicKey(t *testing.T) {
	bound := map[string]int{}
	for _, m := range []proto.Message{&ProcessRequest{}, &BuildRequest{}, &BuildSignal{}, &BuildAnalysis{}, &IssueTrack{}, &FixRequest{}, &Record{}} {
		keys := TopicKeys(m)
		require.NotEmpty(t, keys, "message must declare a non-empty topic_keys option")
		for _, key := range keys {
			bound[key]++
		}
	}

	keys := []TopicKey{
		TopicKeyProcess,
		TopicKeyBuild,
		TopicKeyBuildSignal,
		TopicKeyBuildAnalysis,
		TopicKeyIssueTrack,
		TopicKeyFix,
		TopicKeyRecord,
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

func TestBuildAnalysisRoundTrip(t *testing.T) {
	analysis := &BuildAnalysis{Id: "build/42", QueueName: "validation/main"}
	data, err := Marshal(analysis)
	require.NoError(t, err)

	got := &BuildAnalysis{}
	require.NoError(t, Unmarshal(data, got))

	assert.True(t, proto.Equal(analysis, got))
	assert.JSONEq(t, `{"id":"build/42","queue_name":"validation/main"}`, string(data))
}

func TestIssueTrackRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name     string
		finalize bool
		wire     string
	}{
		{name: "observations", wire: `{"build_id":"build/42","queue_name":"validation/main"}`},
		{name: "final analysis", finalize: true, wire: `{"build_id":"build/42","queue_name":"validation/main","finalize":true}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			notification := &IssueTrack{BuildId: "build/42", QueueName: "validation/main", Finalize: tt.finalize}
			data, err := Marshal(notification)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wire, string(data))

			got := &IssueTrack{}
			require.NoError(t, Unmarshal(data, got))
			assert.True(t, proto.Equal(notification, got))
		})
	}
}

func TestIssueTrackToleratesUnknownFields(t *testing.T) {
	got := &IssueTrack{}
	require.NoError(t, Unmarshal([]byte(`{"build_id":"build/42","queue_name":"validation/main","finalize":true,"future_field":42}`), got))
	assert.True(t, proto.Equal(&IssueTrack{BuildId: "build/42", QueueName: "validation/main", Finalize: true}, got))
}

func TestFixRequestRoundTrip(t *testing.T) {
	request := &FixRequest{Id: "Build/42 ", QueueName: "validation/main"}
	data, err := Marshal(request)
	require.NoError(t, err)

	got := &FixRequest{}
	require.NoError(t, Unmarshal(data, got))

	assert.True(t, proto.Equal(request, got))
	assert.JSONEq(t, `{"id":"Build/42 ","queue_name":"validation/main"}`, string(data))
}
