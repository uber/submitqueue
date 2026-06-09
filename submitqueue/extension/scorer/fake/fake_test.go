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

package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/heuristic"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ scorer.Scorer = New(nil)
}

// delegate returns a heuristic scorer that scores every batch at want.
func delegate(t *testing.T, want float64) scorer.Scorer {
	t.Helper()
	return heuristic.New(
		[]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: want}},
		func(_ context.Context, c entity.BatchChanges) (int, error) { return len(c.Changes), nil },
		tally.NoopScope,
	)
}

func batch(uris ...string) entity.BatchChanges {
	changes := make([]entity.ChangeInfo, 0, len(uris))
	for _, u := range uris {
		changes = append(changes, entity.ChangeInfo{URI: u})
	}
	return entity.BatchChanges{BatchID: "q/batch/1", Queue: "q", Changes: changes}
}

func TestScore_DelegatesWhenUnmarked(t *testing.T) {
	s := New(delegate(t, 0.7))
	got, err := s.Score(context.Background(), batch("github://o/r/pull/1/a"))
	require.NoError(t, err)
	assert.Equal(t, 0.7, got)
}

func TestScore_ErrorMarker(t *testing.T) {
	s := New(delegate(t, 0.7))
	_, err := s.Score(context.Background(), batch("github://o/r/pull/1/a?sq-fake=score-error"))
	require.Error(t, err)
}
