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

package dlq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQLogController_InterfaceAndAccessors(t *testing.T) {
	c := NewDLQLogController(zaptest.NewLogger(t).Sugar(), testScope(), TopicKey(topickey.TopicKeyLog), "orchestrator-log-dlq")

	assert.Equal(t, "log_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("log_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-log-dlq", c.ConsumerGroup())
}

func TestDLQLogController_Process_AcksUnconditionally(t *testing.T) {
	ctrl := gomock.NewController(t)

	c := NewDLQLogController(zaptest.NewLogger(t).Sugar(), testScope(), TopicKey(topickey.TopicKeyLog), "orchestrator-log-dlq")

	delivery := newMockDelivery(ctrl, []byte("anything goes"))
	require.NoError(t, c.Process(context.Background(), delivery))
}
