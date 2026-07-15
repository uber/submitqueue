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

package noop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/extension/consumergate"
)

func TestGate_EnterNeverBlocks(t *testing.T) {
	g := New()
	entry, err := g.Enter(context.Background(), consumergate.Key{ConsumerGroup: "group", PartitionKey: "part"})
	require.NoError(t, err)
	assert.False(t, entry.Blocked())
	require.NoError(t, consumergate.Wait(context.Background(), entry, consumergate.DeliveryDescriptor{
		Topic:     "topic",
		MessageID: "msg-1",
		Payload:   []byte("hello"),
		Attempt:   1,
	}))
}
