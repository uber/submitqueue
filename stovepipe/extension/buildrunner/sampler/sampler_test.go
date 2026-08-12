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

package sampler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
	buildrunnermock "github.com/uber/submitqueue/stovepipe/extension/buildrunner/mock"
)

// fixedDraw returns a sampling function that always draws value, pinning which
// delegate a Trigger routes to.
func fixedDraw(value int) func(int) int {
	return func(int) int { return value }
}

// newTestRunner builds a sampler over two mock delegates with a pinned draw, so
// routing is asserted without depending on the random source.
func newTestRunner(t *testing.T, candidatePercent, draw int) (*runner, *buildrunnermock.MockBuildRunner, *buildrunnermock.MockBuildRunner) {
	t.Helper()
	ctrl := gomock.NewController(t)
	baseline := buildrunnermock.NewMockBuildRunner(ctrl)
	candidate := buildrunnermock.NewMockBuildRunner(ctrl)
	return &runner{
		baseline:         baseline,
		candidate:        candidate,
		candidatePercent: candidatePercent,
		logger:           zap.NewNop().Sugar(),
		intn:             fixedDraw(draw),
	}, baseline, candidate
}

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	delegate := buildrunnermock.NewMockBuildRunner(ctrl)

	tests := []struct {
		name    string
		params  Params
		wantErr bool
	}{
		{
			name:   "valid params",
			params: Params{Baseline: delegate, Candidate: delegate, CandidatePercent: 10, Logger: zap.NewNop().Sugar()},
		},
		{
			name:   "zero percent is valid",
			params: Params{Baseline: delegate, Candidate: delegate, Logger: zap.NewNop().Sugar()},
		},
		{
			name:   "full percent is valid",
			params: Params{Baseline: delegate, Candidate: delegate, CandidatePercent: 100, Logger: zap.NewNop().Sugar()},
		},
		{
			name:    "missing baseline",
			params:  Params{Candidate: delegate, Logger: zap.NewNop().Sugar()},
			wantErr: true,
		},
		{
			name:    "missing candidate",
			params:  Params{Baseline: delegate, Logger: zap.NewNop().Sugar()},
			wantErr: true,
		},
		{
			name:    "negative percent",
			params:  Params{Baseline: delegate, Candidate: delegate, CandidatePercent: -1, Logger: zap.NewNop().Sugar()},
			wantErr: true,
		},
		{
			name:    "percent above 100",
			params:  Params{Baseline: delegate, Candidate: delegate, CandidatePercent: 101, Logger: zap.NewNop().Sugar()},
			wantErr: true,
		},
		{
			name:    "missing logger",
			params:  Params{Baseline: delegate, Candidate: delegate},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.params)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			var _ buildrunner.BuildRunner = got
		})
	}
}

func TestTrigger_RoutesByDraw(t *testing.T) {
	tests := []struct {
		name             string
		candidatePercent int
		draw             int
		wantSlot         string
	}{
		{name: "zero percent never samples", candidatePercent: 0, draw: 0, wantSlot: _slotBaseline},
		{name: "full percent always samples", candidatePercent: 100, draw: 99, wantSlot: _slotCandidate},
		{name: "draw below percent samples", candidatePercent: 25, draw: 24, wantSlot: _slotCandidate},
		{name: "draw at percent does not sample", candidatePercent: 25, draw: 25, wantSlot: _slotBaseline},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, baseline, candidate := newTestRunner(t, tt.candidatePercent, tt.draw)
			routed := baseline
			if tt.wantSlot == _slotCandidate {
				routed = candidate
			}
			routed.EXPECT().
				Trigger(gomock.Any(), "base", "head", entity.BuildMetadata{"k": "v"}).
				Return(entity.BuildID{ID: "delegate-id"}, nil)

			got, err := r.Trigger(context.Background(), "base", "head", entity.BuildMetadata{"k": "v"})
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("sampler-%s-delegate-id", tt.wantSlot), got.ID)
		})
	}
}

// TestTrigger_SplitsTrafficWithRealDraws covers the actual promise of the sampler
// — that a middling percentage reaches both delegates — against the random source
// the wiring layer uses, which the pinned-draw tests deliberately bypass. The
// MinTimes(1) expectations on both delegates are the assertion.
func TestTrigger_SplitsTrafficWithRealDraws(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseline := buildrunnermock.NewMockBuildRunner(ctrl)
	candidate := buildrunnermock.NewMockBuildRunner(ctrl)
	baseline.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{ID: "baseline-id"}, nil).MinTimes(1)
	candidate.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{ID: "candidate-id"}, nil).MinTimes(1)

	r, err := New(Params{
		Baseline:         baseline,
		Candidate:        candidate,
		CandidatePercent: 50,
		Logger:           zap.NewNop().Sugar(),
	})
	require.NoError(t, err)

	for range 100 {
		_, err := r.Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
		require.NoError(t, err)
	}
}

// TestTrigger_DelegateErrorIsNotRetriedElsewhere pins the deliberate absence of a
// fallback: the sampled delegate's failure surfaces instead of being covered by
// the other delegate, which is what makes the split useful as a signal.
func TestTrigger_DelegateErrorIsNotRetriedElsewhere(t *testing.T) {
	r, _, candidate := newTestRunner(t, 100, 0)
	candidate.EXPECT().
		Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{}, fmt.Errorf("candidate unavailable"))

	got, err := r.Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
	require.Error(t, err)
	assert.Empty(t, got.ID)
}

func TestStatus_RoutesToMintingDelegate(t *testing.T) {
	tests := []struct {
		name           string
		buildID        string
		wantSlot       string
		wantDelegateID string
	}{
		{
			name:           "baseline tag",
			buildID:        "sampler-baseline-fake-ok-0-a1b2c3d4",
			wantSlot:       _slotBaseline,
			wantDelegateID: "fake-ok-0-a1b2c3d4",
		},
		{
			name:           "candidate tag",
			buildID:        "sampler-candidate-12345",
			wantSlot:       _slotCandidate,
			wantDelegateID: "12345",
		},
		{
			// A delegate that is itself a sampler keeps its own tag intact, so
			// samplers nest without either layer misrouting.
			name:           "nested sampler tag",
			buildID:        "sampler-candidate-sampler-baseline-fake-ok-0-a1b2c3d4",
			wantSlot:       _slotCandidate,
			wantDelegateID: "sampler-baseline-fake-ok-0-a1b2c3d4",
		},
		{
			// Ids minted before the sampler was wired carry no tag and belong to
			// the runner that was serving traffic then.
			name:           "untagged id falls back to baseline",
			buildID:        "fake-ok-0-a1b2c3d4",
			wantSlot:       _slotBaseline,
			wantDelegateID: "fake-ok-0-a1b2c3d4",
		},
		{
			name:           "unknown slot falls back to baseline",
			buildID:        "sampler-mystery-a1b2c3d4",
			wantSlot:       _slotBaseline,
			wantDelegateID: "sampler-mystery-a1b2c3d4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, baseline, candidate := newTestRunner(t, 50, 0)
			routed := baseline
			if tt.wantSlot == _slotCandidate {
				routed = candidate
			}
			want := entity.BuildID{ID: tt.wantDelegateID}
			routed.EXPECT().
				Status(gomock.Any(), want).
				Return(entity.BuildStatusRunning, entity.BuildMetadata{"url": "http://build"}, nil)

			status, metadata, err := r.Status(context.Background(), entity.BuildID{ID: tt.buildID})
			require.NoError(t, err)
			assert.Equal(t, entity.BuildStatusRunning, status)
			assert.Equal(t, entity.BuildMetadata{"url": "http://build"}, metadata)
		})
	}
}

func TestStatus_DelegateError(t *testing.T) {
	r, baseline, _ := newTestRunner(t, 0, 0)
	baseline.EXPECT().
		Status(gomock.Any(), entity.BuildID{ID: "delegate-id"}).
		Return(entity.BuildStatusUnknown, nil, fmt.Errorf("backend down"))

	status, metadata, err := r.Status(context.Background(), entity.BuildID{ID: "sampler-baseline-delegate-id"})
	require.Error(t, err)
	assert.Equal(t, entity.BuildStatusUnknown, status)
	assert.Nil(t, metadata)
}

func TestCancel_RoutesToMintingDelegate(t *testing.T) {
	r, _, candidate := newTestRunner(t, 0, 0)
	candidate.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: "12345"}).Return(nil)

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "sampler-candidate-12345"}))
}

func TestCancel_DelegateError(t *testing.T) {
	r, baseline, _ := newTestRunner(t, 0, 0)
	baseline.EXPECT().Cancel(gomock.Any(), gomock.Any()).Return(fmt.Errorf("backend down"))

	require.Error(t, r.Cancel(context.Background(), entity.BuildID{ID: "sampler-baseline-12345"}))
}

// TestTriggerThenStatus_RoundTrip covers the whole point of tagging: a build's
// status reaches the delegate that minted it, with the delegate's own id restored.
func TestTriggerThenStatus_RoundTrip(t *testing.T) {
	r, _, candidate := newTestRunner(t, 100, 0)
	candidate.EXPECT().
		Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{ID: "fake-ok-0-a1b2c3d4"}, nil)
	candidate.EXPECT().
		Status(gomock.Any(), entity.BuildID{ID: "fake-ok-0-a1b2c3d4"}).
		Return(entity.BuildStatusSucceeded, nil, nil)

	buildID, err := r.Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
	require.NoError(t, err)

	status, _, err := r.Status(context.Background(), buildID)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}
