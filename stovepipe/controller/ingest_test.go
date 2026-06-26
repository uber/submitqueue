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

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newIngestController(t *testing.T, c *countermock.MockCounter) *IngestController {
	t.Helper()
	return NewIngestController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), c)
}

func TestIngestController_Ingest(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCounter := countermock.NewMockCounter(ctrl)
	mockCounter.EXPECT().Next(gomock.Any(), "request/monorepo/main").Return(int64(7), nil)

	c := newIngestController(t, mockCounter)

	resp, err := c.Ingest(context.Background(), &pb.IngestRequest{Queue: "monorepo/main"})
	require.NoError(t, err)
	assert.Equal(t, "request/monorepo/main/7", resp.Id)
}

func TestIngestController_Ingest_EmptyQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCounter := countermock.NewMockCounter(ctrl)
	// Counter must not be consulted when the queue is missing.

	c := newIngestController(t, mockCounter)

	_, err := c.Ingest(context.Background(), &pb.IngestRequest{Queue: ""})
	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestIngestController_Ingest_CounterError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCounter := countermock.NewMockCounter(ctrl)
	mockCounter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), errors.New("counter unavailable"))

	c := newIngestController(t, mockCounter)

	_, err := c.Ingest(context.Background(), &pb.IngestRequest{Queue: "monorepo/main"})
	require.Error(t, err)
	assert.False(t, IsInvalidRequest(err))
}
