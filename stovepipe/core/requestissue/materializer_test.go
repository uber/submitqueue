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

package requestissue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func sourceObservation() entity.RequestIssueSummary {
	return entity.RequestIssueSummary{RequestID: "7", BuildID: "build/1", TaskID: "test", UnitID: "unit", Version: 1}
}

func TestObservationRepairAndClassificationConvergeInAnyOrder(t *testing.T) {
	for _, order := range [][3]string{
		{"observation", "repair", "classification"}, {"observation", "classification", "repair"},
		{"repair", "observation", "classification"}, {"repair", "classification", "observation"},
		{"classification", "observation", "repair"}, {"classification", "repair", "observation"},
	} {
		t.Run(order[0]+"/"+order[1], func(t *testing.T) {
			summaries := storagemock.NewMockRequestIssueSummaryStore(gomock.NewController(t))
			observation := sourceObservation()
			repair := observation
			repair.FixID = "fix/1"
			classified := observation
			classified.Novelty = entity.IssueNoveltyNew
			inputs := map[string]entity.RequestIssueSummary{"observation": observation, "repair": repair, "classification": classified}
			var retained entity.RequestIssueSummary
			summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").DoAndReturn(func(context.Context, string, string, string, string) (entity.RequestIssueSummary, error) {
				if retained.Version == 0 {
					return retained, storage.ErrNotFound
				}
				return retained, nil
			}).AnyTimes()
			summaries.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, value entity.RequestIssueSummary) error { retained = value; return nil })
			summaries.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, value entity.RequestIssueSummary, _, newVersion int32) error {
				value.Version = newVersion
				retained = value
				return nil
			}).AnyTimes()
			for range 2 {
				for _, source := range order {
					require.NoError(t, MaterializeObservation(context.Background(), summaries, inputs[source]))
				}
			}
			require.Equal(t, "fix/1", retained.FixID)
			require.Equal(t, entity.IssueNoveltyNew, retained.Novelty)
		})
	}
}

func TestClassificationCASRetryPreservesConcurrentRepair(t *testing.T) {
	summaries := storagemock.NewMockRequestIssueSummaryStore(gomock.NewController(t))
	observation := sourceObservation()
	classified := observation
	classified.Novelty = entity.IssueNoveltyExisting
	repaired := observation
	repaired.FixID, repaired.Version = "fix/1", 2
	merged := repaired
	merged.Novelty = classified.Novelty
	gomock.InOrder(
		summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(observation, nil),
		summaries.EXPECT().Update(gomock.Any(), classified, int32(1), int32(2)).Return(storage.ErrVersionMismatch),
		summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(repaired, nil),
		summaries.EXPECT().Update(gomock.Any(), merged, int32(2), int32(3)).Return(nil),
	)
	require.NoError(t, MaterializeObservation(context.Background(), summaries, classified))
}

func TestClassificationReplayPreservesKnownFactsAndRejectsConflicts(t *testing.T) {
	for _, novelty := range []entity.IssueNovelty{entity.IssueNoveltyUnknown, entity.IssueNoveltyNew, entity.IssueNoveltyExisting} {
		t.Run(string(novelty), func(t *testing.T) {
			summaries := storagemock.NewMockRequestIssueSummaryStore(gomock.NewController(t))
			retained := sourceObservation()
			retained.FixID, retained.Novelty = "fix/1", entity.IssueNoveltyNew
			summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(retained, nil)
			incoming := sourceObservation()
			incoming.Novelty = novelty
			err := MaterializeObservation(context.Background(), summaries, incoming)
			if novelty == entity.IssueNoveltyExisting {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProjectionCreateAndUpdateRacesPreserveWinningRepair(t *testing.T) {
	for _, createRace := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement race", true: "creation race"}[createRace], func(t *testing.T) {
			summaries := storagemock.NewMockRequestIssueSummaryStore(gomock.NewController(t))
			incoming := sourceObservation()
			repaired := incoming
			repaired.FixID, repaired.Version = "fix/1", 2
			if createRace {
				gomock.InOrder(
					summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(entity.RequestIssueSummary{}, storage.ErrNotFound),
					summaries.EXPECT().Create(gomock.Any(), incoming).Return(storage.ErrAlreadyExists),
					summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(repaired, nil),
				)
			} else {
				replacement := incoming
				replacement.FixID = "fix/1"
				gomock.InOrder(
					summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(incoming, nil),
					summaries.EXPECT().Update(gomock.Any(), replacement, int32(1), int32(2)).Return(storage.ErrVersionMismatch),
					summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(repaired, nil),
				)
				incoming.FixID = "fix/1"
			}
			original := incoming
			require.NoError(t, MaterializeObservation(context.Background(), summaries, incoming))
			require.Equal(t, original, incoming)
		})
	}
}

func TestProjectionConflictingRepairsAndWriteFailures(t *testing.T) {
	for _, failure := range []string{"conflict", "read", "create", "update"} {
		t.Run(failure, func(t *testing.T) {
			summaries := storagemock.NewMockRequestIssueSummaryStore(gomock.NewController(t))
			incoming := sourceObservation()
			incoming.FixID = "fix/1"
			current := sourceObservation()
			var getErr error
			switch failure {
			case "conflict":
				current.FixID = "fix/other"
			case "read":
				getErr = assert.AnError
			case "create":
				getErr = storage.ErrNotFound
			}
			summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(current, getErr)
			if failure == "create" {
				summaries.EXPECT().Create(gomock.Any(), gomock.Any()).Return(assert.AnError)
			}
			if failure == "update" {
				summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(assert.AnError)
			}
			err := MaterializeObservation(context.Background(), summaries, incoming)
			require.Error(t, err)
			if failure != "conflict" {
				require.ErrorIs(t, err, assert.AnError)
			}
		})
	}
}

func TestBuildProjectionUsesPersistedMembershipWithoutOperationalRepairFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	stores := storagemock.NewMockStorage(ctrl)
	observations := storagemock.NewMockBuildIssueStore(ctrl)
	summaries := storagemock.NewMockRequestIssueSummaryStore(ctrl)
	stores.EXPECT().GetBuildIssueStore().Return(observations)
	stores.EXPECT().GetRequestIssueSummaryStore().Return(summaries)
	observations.EXPECT().List(gomock.Any(), "build/1", storage.BuildIssueRange{Limit: observationPageSize}).Return([]entity.BuildIssue{{TaskID: "test", UnitID: "unit", FixID: "operational-only", Version: 99}}, nil)
	summaries.EXPECT().Get(gomock.Any(), "7", "build/1", "test", "unit").Return(entity.RequestIssueSummary{}, storage.ErrNotFound)
	summaries.EXPECT().Create(gomock.Any(), sourceObservation()).Return(nil)
	require.NoError(t, MaterializeBuild(context.Background(), stores, "7", "build/1"))
}
