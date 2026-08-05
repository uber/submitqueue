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

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	"github.com/uber/submitqueue/api/runway/messagequeue/protopb"
)

func TestMergeRequestRoundTrip(t *testing.T) {
	req := &MergeRequest{
		Id:        "queue-a/42",
		QueueName: "queue-a",
		Steps: []*MergeStep{
			{
				StepId:   "queue-a/1",
				Change:   &changepb.Change{Uris: []string{"github://github.example.com/uber/repo/pull/1/0123456789abcdef0123456789abcdef01234567"}},
				Strategy: strategypb.Strategy_REBASE,
			},
			{
				StepId:   "queue-a/2",
				Change:   &changepb.Change{Uris: []string{"github://github.example.com/uber/repo/pull/2/89abcdef0123456789abcdef0123456789abcdef"}},
				Strategy: strategypb.Strategy_MERGE,
			},
		},
	}

	data, err := Marshal(req)
	require.NoError(t, err)

	got := &MergeRequest{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(req, got), "round-tripped MergeRequest should equal the original")
}

func TestMergeResultRoundTrip(t *testing.T) {
	// A committing merge reports the revisions each step produced on the target;
	// a dry-run check leaves outputs empty and reports a per-step reason on
	// failure. Both shapes share the one MergeResult contract.
	cases := map[string]*MergeResult{
		"merged with produced revisions": {
			Id:      "queue-a/42",
			Outcome: protopb.Outcome_SUCCEEDED,
			Steps: []*StepResult{
				{StepId: "queue-a/1", Outputs: []*StepOutput{{Id: "0123456789abcdef0123456789abcdef01234567"}}},
			},
		},
		"failed with per-step reason": {
			Id:      "queue-a/42",
			Outcome: protopb.Outcome_FAILED,
			Reason:  "conflict in foo.go",
			Steps:   []*StepResult{{StepId: "queue-a/2", Reason: "conflict in foo.go"}},
		},
		"minimal": {
			Id:      "queue-a/42",
			Outcome: protopb.Outcome_SUCCEEDED,
		},
	}

	for name, res := range cases {
		t.Run(name, func(t *testing.T) {
			data, err := Marshal(res)
			require.NoError(t, err)

			got := &MergeResult{}
			require.NoError(t, Unmarshal(data, got))
			assert.True(t, proto.Equal(res, got), "round-tripped MergeResult should equal the original")
		})
	}
}

// TestWireFormat locks the two protojson encoding decisions the contract relies
// on: snake_case field names (UseProtoNames) and proto-conventional UPPER_SNAKE
// enum values on the wire.
func TestWireFormat(t *testing.T) {
	data, err := Marshal(&MergeRequest{
		Id:        "queue-a/42",
		QueueName: "queue-a",
		Steps:     []*MergeStep{{StepId: "queue-a/1", Strategy: strategypb.Strategy_SQUASH_REBASE}},
	})
	require.NoError(t, err)

	assert.Contains(t, string(data), `"queue_name"`, "fields must serialize as snake_case")
	assert.Contains(t, string(data), `"SQUASH_REBASE"`, "enums must serialize as their UPPER_SNAKE value name")
}

// TestTopicKeysBindEveryTopicKey is the topic-binding drift guard: every Runway
// topic key is carried by exactly one message's topic_keys option, and no
// topic_keys option names an unknown key.
func TestTopicKeysBindEveryTopicKey(t *testing.T) {
	bound := map[string]int{}
	for _, m := range []proto.Message{&MergeRequest{}, &MergeResult{}} {
		keys := TopicKeys(m)
		require.NotEmpty(t, keys, "message must declare a non-empty topic_keys option")
		for _, key := range keys {
			bound[key]++
		}
	}

	keys := []TopicKey{
		TopicKeyMergeConflictCheck,
		TopicKeyMergeConflictCheckSignal,
		TopicKeyMerge,
		TopicKeyMergeSignal,
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
