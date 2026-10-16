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

package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestGet(t *testing.T) {
	frozen := entity.QueuePause{FreezeProcessing: true, Reason: "incident"}
	tests := []struct {
		name  string
		setup func(*Store)
		queue string
		want  entity.QueuePause
	}{
		{name: "queue never set is not paused", setup: func(*Store) {}, queue: "q", want: entity.QueuePause{}},
		{name: "queue set is returned", setup: func(s *Store) { s.Set("q", frozen) }, queue: "q", want: frozen},
		{name: "other queue is unaffected", setup: func(s *Store) { s.Set("q", frozen) }, queue: "other", want: entity.QueuePause{}},
		{name: "setting the zero state unpauses", setup: func(s *Store) { s.Set("q", frozen).Set("q", entity.QueuePause{}) }, queue: "q", want: entity.QueuePause{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			tt.setup(s)

			got, err := s.Get(context.Background(), tt.queue)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFailWith(t *testing.T) {
	boom := errors.New("backend down")
	s := New().Set("q", entity.QueuePause{RejectLands: true}).FailWith(boom)

	got, err := s.Get(context.Background(), "q")
	require.ErrorIs(t, err, boom)
	assert.Equal(t, entity.QueuePause{}, got)

	got, err = s.FailWith(nil).Get(context.Background(), "q")
	require.NoError(t, err)
	assert.Equal(t, entity.QueuePause{RejectLands: true}, got)
}
