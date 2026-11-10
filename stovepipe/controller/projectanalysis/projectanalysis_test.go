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
	"testing"

	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
)

func TestDecodeProjectAnalysis(t *testing.T) {
	for _, scenario := range []string{"valid", "malformed", "invalid request ID", "tenant mismatch", "partition mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			payload, err := stovepipemq.Marshal(&stovepipemq.ProjectAnalysis{Id: "10", QueueName: "monorepo/main"})
			require.NoError(t, err)
			msg := entityqueue.Message{Tenant: "monorepo/main", ID: "10", PartitionKey: "10", Payload: payload}
			switch scenario {
			case "malformed":
				msg.Payload = []byte("not json")
			case "invalid request ID":
				msg.Payload, err = stovepipemq.Marshal(&stovepipemq.ProjectAnalysis{Id: "invalid", QueueName: "monorepo/main"})
				require.NoError(t, err)
			case "tenant mismatch":
				msg.Tenant = "another/queue"
			case "partition mismatch":
				msg.PartitionKey = "another-request"
			}
			got, err := decodeProjectAnalysis(msg)
			if scenario != "valid" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "10", got.GetId())
			require.Equal(t, "monorepo/main", got.GetQueueName())
		})
	}
}
