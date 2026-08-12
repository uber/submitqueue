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

package pathoverlap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
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

func TestPathKey(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		wantFile      string
		wantDirectory string
	}{
		{
			name:          "nested path",
			path:          "src/pkg/a.go",
			wantFile:      "src/pkg/a.go",
			wantDirectory: "src/pkg",
		},
		{
			name:          "repository root file keys on the root directory",
			path:          "README.md",
			wantFile:      "README.md",
			wantDirectory: ".",
		},
		{
			name:          "unclean path is normalized",
			path:          "./src/pkg/../pkg/a.go",
			wantFile:      "src/pkg/a.go",
			wantDirectory: "src/pkg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantFile, ByFile(tt.path))
			assert.Equal(t, tt.wantDirectory, ByDirectory(tt.path))
		})
	}
}

func TestAnalyze(t *testing.T) {
	tests := []struct {
		name        string
		key         PathKey
		candidate   entity.BatchChanges
		inFlight    map[string]entity.BatchChanges
		inFlightIDs []string
		wantBatches []string
	}{
		{
			name:      "overlap on a shared file conflicts",
			key:       ByFile,
			candidate: detailed("cand", "a.go", "b.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "b.go", "c.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: []string{"x"},
		},
		{
			name:      "disjoint files do not conflict",
			key:       ByFile,
			candidate: detailed("cand", "a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "z.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: nil,
		},
		{
			name:      "only overlapping in-flight batches are reported, in order",
			key:       ByFile,
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
			key:       ByFile,
			candidate: detailed("cand"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "a.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: nil,
		},
		{
			name:      "sibling files in one directory do not conflict by file",
			key:       ByFile,
			candidate: detailed("cand", "src/pkg/a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "src/pkg/b.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: nil,
		},
		{
			name:      "sibling files in one directory conflict by directory",
			key:       ByDirectory,
			candidate: detailed("cand", "src/pkg/a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "src/pkg/b.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: []string{"x"},
		},
		{
			name:      "files in sibling directories do not conflict by directory",
			key:       ByDirectory,
			candidate: detailed("cand", "src/pkg/a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "src/other/a.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: nil,
		},
		{
			name:      "the same file still conflicts by directory",
			key:       ByDirectory,
			candidate: detailed("cand", "src/pkg/a.go"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "src/pkg/a.go"),
			},
			inFlightIDs: []string{"x"},
			wantBatches: []string{"x"},
		},
		{
			name:      "repository root files conflict with each other by directory",
			key:       ByDirectory,
			candidate: detailed("cand", "README.md"),
			inFlight: map[string]entity.BatchChanges{
				"x": detailed("x", "go.mod"),
				"y": detailed("y", "src/pkg/a.go"),
			},
			inFlightIDs: []string{"x", "y"},
			wantBatches: []string{"x"},
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

			got, err := New(conflict.Config{QueueName: "test-queue"}, resolver, tt.key).Analyze(context.Background(), entity.Batch{ID: "cand"}, inFlight)
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
	got, err := New(conflict.Config{QueueName: "test-queue"}, changesetfake.New(), ByFile).Analyze(context.Background(), entity.Batch{ID: "cand"}, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestAnalyze_ResolverError(t *testing.T) {
	sentinel := errors.New("resolve failed")
	resolver := changesetfake.New().FailWith(sentinel)

	_, err := New(conflict.Config{QueueName: "test-queue"}, resolver, ByFile).Analyze(context.Background(), entity.Batch{ID: "cand"}, []entity.Batch{{ID: "x"}})
	require.ErrorIs(t, err, sentinel)
}

func TestNew_NilKeyPanics(t *testing.T) {
	assert.Panics(t, func() {
		New(conflict.Config{QueueName: "test-queue"}, changesetfake.New(), nil)
	})
}
