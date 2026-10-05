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
	"github.com/uber/submitqueue/submitqueue/entity"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestListQueues(t *testing.T) {
	configErr := errors.New("queue config unavailable")
	tests := []struct {
		name   string
		queues []entity.QueueConfig
		err    error
		want   []entity.QueueConfig
	}{
		{
			name:   "configured queues sorted by name",
			queues: []entity.QueueConfig{{Name: "release"}, {Name: "main"}, {Name: "demo"}},
			want:   []entity.QueueConfig{{Name: "demo"}, {Name: "main"}, {Name: "release"}},
		},
		{
			name:   "single queue",
			queues: []entity.QueueConfig{{Name: "main"}},
			want:   []entity.QueueConfig{{Name: "main"}},
		},
		{
			name: "no configured queues",
			want: []entity.QueueConfig{},
		},
		{
			name:   "store failure returns no partial result",
			queues: []entity.QueueConfig{{Name: "main"}},
			err:    configErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueConfigs := qcmock.NewMockStore(gomock.NewController(t))
			ctx := context.Background()
			queueConfigs.EXPECT().List(ctx).Return(tt.queues, tt.err)
			original := append([]entity.QueueConfig(nil), tt.queues...)
			c := NewListQueuesController(zap.NewNop().Sugar(), tally.NoopScope, queueConfigs)

			got, err := c.ListQueues(ctx)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, original, tt.queues)
			if len(got) > 0 {
				got[0].Name = "mutated"
				assert.Equal(t, original, tt.queues)
			}
		})
	}
}
