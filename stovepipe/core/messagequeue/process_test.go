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
	req := &ProcessRequest{Id: "request/monorepo/main/42"}

	data, err := Marshal(req)
	require.NoError(t, err)

	got := &ProcessRequest{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(req, got), "round-tripped ProcessRequest should equal the original")
}

func TestBuildRequestRoundTrip(t *testing.T) {
	req := &BuildRequest{Id: "request/monorepo/main/42"}

	data, err := Marshal(req)
	require.NoError(t, err)

	got := &BuildRequest{}
	require.NoError(t, Unmarshal(data, got))
	assert.True(t, proto.Equal(req, got), "round-tripped BuildRequest should equal the original")
}

// TestWireFormat locks the protojson encoding decision the contract relies on:
// snake_case field names (UseProtoNames).
func TestWireFormat(t *testing.T) {
	data, err := Marshal(&ProcessRequest{Id: "request/monorepo/main/42"})
	require.NoError(t, err)

	assert.Contains(t, string(data), `"id"`, "fields must serialize as snake_case")
}

// TestTopicKeysBindEveryTopicKey is the topic-binding drift guard: every
// Stovepipe topic key is carried by exactly one message's topic_keys option, and
// no topic_keys option names an unknown key.
func TestTopicKeysBindEveryTopicKey(t *testing.T) {
	bound := map[string]int{}
	for _, m := range []proto.Message{&ProcessRequest{}, &BuildRequest{}} {
		keys := TopicKeys(m)
		require.NotEmpty(t, keys, "message must declare a non-empty topic_keys option")
		for _, key := range keys {
			bound[key]++
		}
	}

	keys := []TopicKey{
		TopicKeyProcess,
		TopicKeyBuild,
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
