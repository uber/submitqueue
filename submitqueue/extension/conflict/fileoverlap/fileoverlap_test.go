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

package fileoverlap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// detailed builds a BatchChanges whose single change touches the given files.
func detailed(batchID string, files ...string) entity.BatchChanges {
	changed := make([]entity.ChangedFile, 0, len(files))
	for _, f := range files {
		changed = append(changed, entity.ChangedFile{Path: f})
	}
	return entity.BatchChanges{
		BatchID: batchID,
		Changes: []entity.ChangeInfo{{Details: entity.ChangeDetails{ChangedFiles: changed}}},
	}
}

func TestAnalyze(t *testing.T) {
	tests := []struct {
		name        string
		candidate   entity.BatchChanges
		inFlight    map[string]entity.BatchChanges
		inFlightIDs []string
		wantBatches []string
	}{
		{
			name:      "overlap on a shared file conflicts",
			candidate: detailed("cand", "a.go", "b.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "b.go", "c.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: []string{"x"},
		},
		{
			name:      "disjoint files do not conflict",
			candidate: detailed("cand", "a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "z.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: nil,
		},
		{
			name:      "only overlapping in-flight batches are reported, in order",
			candidate: detailed("cand", "a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "a.go"),
				"y": detailed("y", "q.go"),
				"z": detailed("z", "a.go"),
			},
			inFlightIDs: []string{"x", "y", "z"},
			wantBatches: []string{"x", "z"},
		},
		{
			name:      "candidate with no targets conflicts with nothing",
			candidate: detailed("cand"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "a.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := changesetfake.New().SetDetailed("cand", tt.candidate)
			inFlight := make([]entity.Batch, 0, len(tt.inFlightIDs))
			for _, id := range tt.inFlightIDs {
				resolver.SetDetailed(id, tt.inFlight[id])
				inFlight = append(inFlight, entity.Batch{ID: id})
			}

			got, err := New(resolver).Analyze(context.Background(), entity.Batch{ID: "cand"}, inFlight)
			require.NoError(t, err)

			var ids []string
			for _, c := range got {
				assert.Equal(t, entity.ConflictTypeTargetOverlap, c.Type)
				ids = append(ids, c.BatchID)
			}
			assert.Equal(t, tt.wantBatches, ids)
		})
	}
}

func TestAnalyze_EmptyInFlight(t *testing.T) {
	got, err := New(changesetfake.New()).Analyze(context.Background(), entity.Batch{ID: "cand"}, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestAnalyze_ResolverError(t *testing.T) {
	sentinel := errors.New("resolve failed")
	resolver := changesetfake.New().FailWith(sentinel)

	_, err := New(resolver).Analyze(context.Background(), entity.Batch{ID: "cand"}, []entity.Batch{{ID: "x"}})
	require.ErrorIs(t, err, sentinel)
}
