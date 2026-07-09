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
	"github.com/uber/submitqueue/platform/base/change"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

const headBatchID = "head-batch"

// newFake returns a fake runner whose head batch resolves to a single change
// carrying the given URIs.
func newFake(uris ...string) buildrunner.BuildRunner {
	return New(changesetfake.New().Set(headBatchID, change.Change{URIs: uris}))
}

func TestNew_ImplementsInterface(t *testing.T) {
	var _ buildrunner.BuildRunner = New(changesetfake.New())
}

func TestRunner_Trigger_UniqueIDs(t *testing.T) {
	ctx := context.Background()

	id1, err := newFake("github://github.example.com/o/r/pull/1/a").Trigger(ctx, nil, entity.Batch{ID: headBatchID}, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, id1.ID)

	// Same runner instance, different trigger (empty head — no marker).
	r := New(changesetfake.New())
	id2, err := r.Trigger(ctx, nil, entity.Batch{ID: "x"}, nil)
	require.NoError(t, err)
	id3, err := r.Trigger(ctx, nil, entity.Batch{ID: "x"}, nil)
	require.NoError(t, err)
	assert.NotEqual(t, id2, id3)

	// Distinct runner instances must not collide: IDs are globally unique, not
	// per-instance counters.
	assert.NotEqual(t, id1, id2)
}

func TestRunner_TriggerError(t *testing.T) {
	r := newFake("github://github.example.com/o/r/pull/1/a?sq-fake=trigger-error")
	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: headBatchID}, nil)
	require.Error(t, err)
}

func TestRunner_Status(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		headURIs   []string
		wantStatus entity.BuildStatus
		wantErr    bool
	}{
		{
			name:       "no marker succeeds",
			headURIs:   []string{"github://github.example.com/o/r/pull/1/a"},
			wantStatus: entity.BuildStatusSucceeded,
		},
		{
			name:       "build-fail marker fails",
			headURIs:   []string{"github://github.example.com/o/r/pull/1/a?sq-fake=build-fail"},
			wantStatus: entity.BuildStatusFailed,
		},
		{
			name:       "build-fail marker among other query params",
			headURIs:   []string{"github://github.example.com/o/r/pull/1/a?ref=main&sq-fake=build-fail&attempt=2"},
			wantStatus: entity.BuildStatusFailed,
		},
		{
			name:     "build-error marker errors",
			headURIs: []string{"github://github.example.com/o/r/pull/1/a?sq-fake=build-error"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newFake(tt.headURIs...)
			id, err := r.Trigger(ctx, nil, entity.Batch{ID: headBatchID}, nil)
			require.NoError(t, err)

			status, _, err := r.Status(ctx, id)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestRunner_Status_UnknownBuildSucceeds(t *testing.T) {
	r := New(changesetfake.New())
	status, _, err := r.Status(context.Background(), entity.BuildID{ID: "never-triggered"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}

// TestStatus_StatelessAcrossInstances proves the outcome is carried by the
// BuildID, not by per-instance state: a build triggered by one runner is read
// back correctly by a different runner instance.
func TestStatus_StatelessAcrossInstances(t *testing.T) {
	ctx := context.Background()
	id, err := newFake("github://github.example.com/o/r/pull/1/a?sq-fake=build-fail").Trigger(ctx, nil, entity.Batch{ID: headBatchID}, nil)
	require.NoError(t, err)

	status, _, err := New(changesetfake.New()).Status(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusFailed, status)
}

func TestRunner_Cancel(t *testing.T) {
	r := New(changesetfake.New())
	assert.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "any"}))
}
