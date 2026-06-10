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
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/scorer/heuristic"
)

const batchID = "q/batch/1"

func TestNew_ImplementsInterface(t *testing.T) {
	var _ scorer.Scorer = New(nil, nil)
}

// resolverFor returns a changeset resolver seeded so that batchID's detailed
// changes carry the given URIs.
func resolverFor(uris ...string) changeset.Resolver {
	changes := make([]entity.ChangeInfo, 0, len(uris))
	for _, u := range uris {
		changes = append(changes, entity.ChangeInfo{URI: u})
	}
	return changesetfake.New().SetDetailed(batchID, entity.BatchChanges{BatchID: batchID, Queue: "q", Changes: changes})
}

// delegate returns a heuristic scorer (backed by resolver) that scores every batch at want.
func delegate(resolver changeset.Resolver, want float64) scorer.Scorer {
	return heuristic.New(
		resolver,
		[]heuristic.Bucket{{Min: 0, Max: 1<<31 - 1, Score: want}},
		func(_ context.Context, c entity.BatchChanges) (int, error) { return len(c.Changes), nil },
		tally.NoopScope,
	)
}

func TestScore_DelegatesWhenUnmarked(t *testing.T) {
	r := resolverFor("github://o/r/pull/1/a")
	s := New(r, delegate(r, 0.7))
	got, err := s.Score(context.Background(), entity.Batch{ID: batchID})
	require.NoError(t, err)
	assert.Equal(t, 0.7, got)
}

func TestScore_ErrorMarker(t *testing.T) {
	r := resolverFor("github://o/r/pull/1/a?sq-fake=score-error")
	s := New(r, delegate(r, 0.7))
	_, err := s.Score(context.Background(), entity.Batch{ID: batchID})
	require.Error(t, err)
}
