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

package tango

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/submitqueue/platform/base/change"
	changesetmock "github.com/uber/submitqueue/submitqueue/core/changeset/mock"
	"github.com/uber/submitqueue/submitqueue/entity"
	baserevisionmock "github.com/uber/submitqueue/submitqueue/extension/baserevision/mock"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

const analyzerTestQueue = "monorepo"

var (
	analyzerTestBaseRevision = "9f2c1d0e3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d"
	analyzerTestNew          = entity.Batch{ID: "10", Queue: analyzerTestQueue}
	analyzerTestFailure      = errors.New("dependency down")
)

type analyzerHarness struct {
	signatures *MockSignatureStore
	client     *MockChangedTargetsClient
	baseErr    error
	changes    *changesetmock.MockResolver
	analyzer   conflict.Analyzer
	scope      tally.TestScope
}

func newAnalyzerHarness(t *testing.T) *analyzerHarness {
	t.Helper()
	ctrl := gomock.NewController(t)
	baseRevisions := baserevisionmock.NewMockResolver(ctrl)
	h := &analyzerHarness{
		signatures: NewMockSignatureStore(ctrl),
		client:     NewMockChangedTargetsClient(ctrl),
		changes:    changesetmock.NewMockResolver(ctrl),
		scope:      tally.NewTestScope("", nil),
	}
	baseRevisions.EXPECT().Resolve(gomock.Any(), analyzerTestQueue).DoAndReturn(
		func(context.Context, string) (string, error) { return analyzerTestBaseRevision, h.baseErr },
	).AnyTimes()
	h.analyzer = New(Params{
		Signatures:    h.signatures,
		Cache:         NewSignatureCache(1 << 20),
		Client:        h.client,
		BaseRevisions: baseRevisions,
		Changes:       h.changes,
		MetricsScope:  h.scope,
	})
	return h
}

var analyzerTestAffected = []LabelDistance{{Label: "//app:lib", Distance: 0}, {Label: "//app:bin", Distance: 1}}

func analyzerTestNewSignature() TargetSignature {
	targets, _ := toSignatureTargets(analyzerTestAffected)
	return TargetSignature{
		Queue:        analyzerTestQueue,
		BatchID:      analyzerTestNew.ID,
		BaseRevision: analyzerTestBaseRevision,
		Targets:      targets,
	}
}

func analyzerTestSignature(batchID string, ids ...uint64) TargetSignature {
	targets := make([]SignatureTarget, len(ids))
	for i, id := range ids {
		targets[i] = SignatureTarget{ID: id}
	}
	return TargetSignature{Queue: analyzerTestQueue, BatchID: batchID, Targets: targets}
}

func analyzerTestBatch(id string) entity.Batch {
	return entity.Batch{ID: id, Queue: analyzerTestQueue}
}

func (h *analyzerHarness) expectNewBatchNotRecorded() {
	h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(TargetSignature{}, false, nil)
}

func (h *analyzerHarness) expectSigningNewBatch(createErr error) {
	h.expectNewBatchNotRecorded()
	h.changes.EXPECT().ChangesForBatch(gomock.Any(), analyzerTestNew).Return([]change.Change{{URIs: []string{"u1", "u2"}}}, nil)
	h.client.EXPECT().ChangedTargets(gomock.Any(), ChangedTargetsRequest{
		BaseRevision: analyzerTestBaseRevision,
		ChangeURIs:   []string{"u1", "u2"},
	}).Return(analyzerTestAffected, nil)
	h.signatures.EXPECT().Create(gomock.Any(), gomock.Cond(func(got TargetSignature) bool {
		stamped := got.CreatedAt != 0
		got.CreatedAt = 0
		return stamped && assert.ObjectsAreEqual(analyzerTestNewSignature(), got)
	})).Return(createErr)
}

func TestAnalyzer_Analyze(t *testing.T) {
	mine := analyzerTestNewSignature().Targets
	shared := mine[0].ID

	tests := []struct {
		name          string
		inFlight      []entity.Batch
		setup         func(h *analyzerHarness)
		wantConflicts []entity.Conflict
		wantErr       error
	}{
		{
			name: "signs the new batch even when nothing is in flight",
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
			},
		},
		{
			name:     "reports overlapping in-flight batches in in-flight order",
			inFlight: []entity.Batch{analyzerTestBatch("11"), analyzerTestBatch("12"), analyzerTestBatch("13")},
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", 1, shared), true, nil)
				h.signatures.EXPECT().Get(gomock.Any(), "12").Return(analyzerTestSignature("12", 1, 2), true, nil)
				h.signatures.EXPECT().Get(gomock.Any(), "13").Return(analyzerTestSignature("13", shared), true, nil)
			},
			wantConflicts: []entity.Conflict{
				{BatchID: "11", Type: entity.ConflictTypeTargetOverlap},
				{BatchID: "13", Type: entity.ConflictTypeTargetOverlap},
			},
		},
		{
			name:     "reports nothing when no in-flight batch overlaps",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", 1, 2), true, nil)
			},
		},
		{
			name:     "uses its own record without calling Tango",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			setup: func(h *analyzerHarness) {
				h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(analyzerTestNewSignature(), true, nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", shared), true, nil)
			},
			wantConflicts: []entity.Conflict{{BatchID: "11", Type: entity.ConflictTypeTargetOverlap}},
		},
		{
			name:     "uses the stored signature when another attempt created it first",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(storage.ErrAlreadyExists)
				winner := analyzerTestSignature(analyzerTestNew.ID, 7)
				h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(winner, true, nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", 7), true, nil)
			},
			wantConflicts: []entity.Conflict{{BatchID: "11", Type: entity.ConflictTypeTargetOverlap}},
		},
		{
			name:     "an in-flight batch with no record fails closed without asking Tango for it",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(TargetSignature{}, false, nil)
			},
			wantErr: ErrNoSignature,
		},
		{
			name:     "a store error reading an in-flight batch fails the analysis",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(TargetSignature{}, false, analyzerTestFailure)
			},
			wantErr: analyzerTestFailure,
		},
		{
			name: "a store error reading the new batch fails the analysis",
			setup: func(h *analyzerHarness) {
				h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(TargetSignature{}, false, analyzerTestFailure)
			},
			wantErr: analyzerTestFailure,
		},
		{
			name: "a Tango error fails the analysis and creates nothing",
			setup: func(h *analyzerHarness) {
				h.expectNewBatchNotRecorded()
				h.changes.EXPECT().ChangesForBatch(gomock.Any(), analyzerTestNew).Return(nil, nil)
				h.client.EXPECT().ChangedTargets(gomock.Any(), gomock.Any()).Return(nil, analyzerTestFailure)
			},
			wantErr: analyzerTestFailure,
		},
		{
			name: "a base resolver error fails the analysis",
			setup: func(h *analyzerHarness) {
				h.expectNewBatchNotRecorded()
				h.changes.EXPECT().ChangesForBatch(gomock.Any(), analyzerTestNew).Return(nil, nil)
				h.baseErr = analyzerTestFailure
			},
			wantErr: analyzerTestFailure,
		},
		{
			name: "a changes resolver error fails the analysis",
			setup: func(h *analyzerHarness) {
				h.expectNewBatchNotRecorded()
				h.changes.EXPECT().ChangesForBatch(gomock.Any(), analyzerTestNew).Return(nil, analyzerTestFailure)
			},
			wantErr: analyzerTestFailure,
		},
		{
			name: "a create error other than already-exists fails the analysis",
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(analyzerTestFailure)
			},
			wantErr: analyzerTestFailure,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newAnalyzerHarness(t)
			tt.setup(h)

			got, err := h.analyzer.Analyze(context.Background(), analyzerTestNew, tt.inFlight)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantConflicts, got)
		})
	}
}

func TestAnalyzer_Analyze_NoSignatureIsDistinctFromStoreError(t *testing.T) {
	h := newAnalyzerHarness(t)
	h.expectSigningNewBatch(nil)
	h.signatures.EXPECT().Get(gomock.Any(), "11").Return(TargetSignature{}, false, analyzerTestFailure)

	_, err := h.analyzer.Analyze(context.Background(), analyzerTestNew, []entity.Batch{analyzerTestBatch("11")})

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNoSignature)
}

func TestAnalyzer_Analyze_ReadsEachSignatureOnce(t *testing.T) {
	h := newAnalyzerHarness(t)
	h.expectSigningNewBatch(nil)
	h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", 1), true, nil).Times(1)
	inFlight := []entity.Batch{analyzerTestBatch("11")}

	_, err := h.analyzer.Analyze(context.Background(), analyzerTestNew, inFlight)
	require.NoError(t, err)
	_, err = h.analyzer.Analyze(context.Background(), analyzerTestNew, inFlight)
	require.NoError(t, err)
}

func TestAnalyzer_Analyze_ExistingSignatureVanishes(t *testing.T) {
	h := newAnalyzerHarness(t)
	h.expectSigningNewBatch(storage.ErrAlreadyExists)
	h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(TargetSignature{}, false, nil)

	_, err := h.analyzer.Analyze(context.Background(), analyzerTestNew, nil)

	require.Error(t, err)
}

func TestAnalyzer_Analyze_Metrics(t *testing.T) {
	shared := analyzerTestNewSignature().Targets[0].ID
	const queue = "queue=monorepo"

	tests := []struct {
		name         string
		inFlight     []entity.Batch
		analyses     int
		setup        func(h *analyzerHarness)
		wantErr      bool
		wantCounters map[string]int64
		wantFinishes []string
		wantNoCounts []string
	}{
		{
			name:     "a created signature and an overlapping in-flight batch",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			analyses: 1,
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", shared), true, nil)
			},
			wantCounters: map[string]int64{
				"tango_conflict.analyze.start+" + queue:                            1,
				"tango_conflict.base_revision.start+" + queue:                      1,
				"tango_conflict.signature.outcomes+outcome=created," + queue:       1,
				"tango_conflict.cache.miss+" + queue:                               1,
				"tango_conflict.conflicts.total+" + queue + ",type=target_overlap": 1,
			},
			wantFinishes: []string{
				"tango_conflict.analyze.finish+" + queue + ",result=success",
				"tango_conflict.base_revision.finish+" + queue + ",result=success",
			},
			wantNoCounts: []string{"tango_conflict.no_signature.total+" + queue},
		},
		{
			name:     "a reused signature never resolves the base revision",
			analyses: 1,
			setup: func(h *analyzerHarness) {
				h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(analyzerTestNewSignature(), true, nil)
			},
			wantCounters: map[string]int64{
				"tango_conflict.signature.outcomes+outcome=reused," + queue: 1,
			},
			wantNoCounts: []string{"tango_conflict.base_revision.start+" + queue},
		},
		{
			name:     "a lost create race",
			analyses: 1,
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(storage.ErrAlreadyExists)
				h.signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(analyzerTestSignature(analyzerTestNew.ID, 7), true, nil)
			},
			wantCounters: map[string]int64{
				"tango_conflict.signature.outcomes+outcome=race_lost," + queue: 1,
			},
			wantNoCounts: []string{"tango_conflict.signature.outcomes+outcome=created," + queue},
		},
		{
			name:     "a second analysis hits the cache for the in-flight signature",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			analyses: 2,
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", 1), true, nil).Times(1)
			},
			wantCounters: map[string]int64{
				"tango_conflict.cache.miss+" + queue: 1,
				"tango_conflict.cache.hit+" + queue:  1,
			},
		},
		{
			name:     "an in-flight batch with no record is counted",
			inFlight: []entity.Batch{analyzerTestBatch("11")},
			analyses: 1,
			setup: func(h *analyzerHarness) {
				h.expectSigningNewBatch(nil)
				h.signatures.EXPECT().Get(gomock.Any(), "11").Return(TargetSignature{}, false, nil)
			},
			wantErr: true,
			wantCounters: map[string]int64{
				"tango_conflict.no_signature.total+" + queue: 1,
			},
			wantFinishes: []string{"tango_conflict.analyze.finish+" + queue + ",result=error"},
		},
		{
			name:     "a base resolver error is visible on the base revision operation",
			analyses: 1,
			setup: func(h *analyzerHarness) {
				h.expectNewBatchNotRecorded()
				h.changes.EXPECT().ChangesForBatch(gomock.Any(), analyzerTestNew).Return(nil, nil)
				h.baseErr = analyzerTestFailure
			},
			wantErr: true,
			wantFinishes: []string{
				"tango_conflict.base_revision.finish+" + queue + ",result=error",
				"tango_conflict.analyze.finish+" + queue + ",result=error",
			},
			wantNoCounts: []string{"tango_conflict.signature.outcomes+outcome=created," + queue},
		},
		{
			name:     "a Tango error fails the analysis and records no signature outcome",
			analyses: 1,
			setup: func(h *analyzerHarness) {
				h.expectNewBatchNotRecorded()
				h.changes.EXPECT().ChangesForBatch(gomock.Any(), analyzerTestNew).Return(nil, nil)
				h.client.EXPECT().ChangedTargets(gomock.Any(), gomock.Any()).Return(nil, analyzerTestFailure)
			},
			wantErr:      true,
			wantFinishes: []string{"tango_conflict.analyze.finish+" + queue + ",result=error"},
			wantNoCounts: []string{"tango_conflict.signature.outcomes+outcome=created," + queue},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newAnalyzerHarness(t)
			tt.setup(h)

			for range tt.analyses {
				_, err := h.analyzer.Analyze(context.Background(), analyzerTestNew, tt.inFlight)
				assert.Equal(t, tt.wantErr, err != nil)
			}

			snapshot := h.scope.Snapshot()
			for key, want := range tt.wantCounters {
				require.Contains(t, snapshot.Counters(), key)
				assert.Equal(t, want, snapshot.Counters()[key].Value(), key)
			}
			for _, key := range tt.wantNoCounts {
				assert.NotContains(t, snapshot.Counters(), key)
			}
			for _, key := range tt.wantFinishes {
				require.Contains(t, snapshot.Histograms(), key)
			}
		})
	}
}

func TestAnalyzer_Analyze_Metrics_RecordSizes(t *testing.T) {
	h := newAnalyzerHarness(t)
	h.expectSigningNewBatch(nil)
	h.signatures.EXPECT().Get(gomock.Any(), "11").Return(analyzerTestSignature("11", 1), true, nil)

	_, err := h.analyzer.Analyze(context.Background(), analyzerTestNew, []entity.Batch{analyzerTestBatch("11")})
	require.NoError(t, err)

	histograms := h.scope.Snapshot().Histograms()
	targets := histograms["tango_conflict.signature.targets+queue=monorepo"]
	require.NotNil(t, targets)
	assert.Equal(t, int64(1), targets.Values()[10], "two targets fall in the bucket bounded by 10")
	inFlight := histograms["tango_conflict.analyze.in_flight+queue=monorepo"]
	require.NotNil(t, inFlight)
	assert.Equal(t, int64(1), inFlight.Values()[1])
}

func TestAnalyzer_Analyze_NilMetricsScope(t *testing.T) {
	ctrl := gomock.NewController(t)
	signatures := NewMockSignatureStore(ctrl)
	signatures.EXPECT().Get(gomock.Any(), analyzerTestNew.ID).Return(analyzerTestNewSignature(), true, nil)
	analyzer := New(Params{Signatures: signatures, Cache: NewSignatureCache(1 << 20)})

	conflicts, err := analyzer.Analyze(context.Background(), analyzerTestNew, nil)

	require.NoError(t, err)
	assert.Empty(t, conflicts)
}
