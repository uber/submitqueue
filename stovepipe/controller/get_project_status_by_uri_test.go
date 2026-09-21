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

package controller

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	projectStatusQueue = "monorepo/main"
	projectStatusURI   = "git://monorepo/main/abc"
	projectStatusID    = "request/monorepo/main/7"
)

func TestGetProjectStatusByURI(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, State: entity.RequestStateProcessing, RequestVersion: 1, StateTimestampMs: 1}
	tests := []struct {
		name            string
		request         entity.GetProjectStatusByURIRequest
		uriErr          error
		summaryErr      error
		fact            entity.ValidationFact
		factErr         error
		wantFact        bool
		wantNotFound    bool
		wantRetryable   bool
		wantInvalid     bool
		noProjectLookup bool
		wantConsistency bool
	}{
		{name: "in progress without fact", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, factErr: storage.ErrNotFound},
		{name: "recorded green fact", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, fact: entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Degree: entity.DegreeGreen}, wantFact: true},
		{name: "missing uri", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, uriErr: storage.ErrNotFound, wantNotFound: true},
		{name: "request summary visibility gap", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, summaryErr: storage.ErrNotFound, wantRetryable: true},
		{name: "empty queue", request: entity.GetProjectStatusByURIRequest{ChangeURI: projectStatusURI}, wantInvalid: true},
		{name: "empty projects", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI}, wantInvalid: true},
		{name: "empty project", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{""}}, wantInvalid: true},
		{name: "duplicate projects", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a", "project-a"}}, wantInvalid: true},
		{name: "invalid page size", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, PageSize: maxProjectStatusPageSize + 1}, wantInvalid: true},
		{name: "malformed page token", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}, PageToken: "token"}, factErr: storage.ErrNotFound, wantInvalid: true, noProjectLookup: true},
		{name: "invalid fact degree", request: entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, fact: entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Degree: math.NaN()}, wantConsistency: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mockCtrl)
			store := storagemock.NewMockStorage(mockCtrl)
			uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
			summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
			factStore := storagemock.NewMockValidationFactStore(mockCtrl)
			if !(tt.wantInvalid && tt.request.PageToken == "") {
				factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
				store.EXPECT().GetRequestURIStore().Return(uriStore)
				uriStore.EXPECT().GetIDByURI(gomock.Any(), projectStatusURI).Return(projectStatusID, tt.uriErr)
				if tt.uriErr == nil {
					store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
					summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(requestSummary, tt.summaryErr)
					if tt.summaryErr == nil {
						store.EXPECT().GetValidationFactStore().Return(factStore)
						factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(tt.fact, tt.factErr)
						if !tt.noProjectLookup && !(tt.factErr == nil && tt.wantConsistency) {
							for _, project := range tt.request.Projects {
								factStore.EXPECT().Get(gomock.Any(), projectStatusURI, project).Return(entity.ValidationFact{}, storage.ErrNotFound)
							}
						}
					}
				}
			}

			controller := NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, factory)
			got, err := controller.GetProjectStatusByURI(context.Background(), tt.request)

			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			assert.Equal(t, tt.wantNotFound, IsProjectStatusNotFound(err))
			assert.Equal(t, tt.wantRetryable, errs.IsRetryable(err))
			assert.Equal(t, tt.wantConsistency, IsProjectStatusConsistency(err))
			if tt.wantInvalid || tt.wantNotFound || tt.wantRetryable || tt.wantConsistency {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, requestSummary, got.RequestSummary)
			assert.Equal(t, tt.wantFact, got.HasRepositoryValidationFact)
			assert.False(t, got.ProjectResultsComplete)
			if tt.wantFact {
				assert.Equal(t, tt.fact, got.RepositoryValidationFact)
			}
		})
	}
}

func TestGetProjectStatusByURIUsesRequestSummaryTimestamp(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, State: entity.RequestStateProcessing, RequestVersion: 2, StateTimestampMs: 10}
	mockCtrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mockCtrl)
	store := storagemock.NewMockStorage(mockCtrl)
	uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
	factStore := storagemock.NewMockValidationFactStore(mockCtrl)
	factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
	store.EXPECT().GetRequestURIStore().Return(uriStore)
	uriStore.EXPECT().GetIDByURI(gomock.Any(), projectStatusURI).Return(projectStatusID, nil)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(requestSummary, nil)
	store.EXPECT().GetValidationFactStore().Return(factStore)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, CreatedAt: 20}, nil)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-a").Return(entity.ValidationFact{}, storage.ErrNotFound)

	controller := NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	got, err := controller.GetProjectStatusByURI(context.Background(), entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}})
	require.NoError(t, err)
	assert.Equal(t, int64(20), got.UpdatedAtMs)
}

func TestGetProjectStatusByURIReturnsProjectFactsInRequestedOrder(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, StateTimestampMs: 10}
	mockCtrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mockCtrl)
	store := storagemock.NewMockStorage(mockCtrl)
	uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
	factStore := storagemock.NewMockValidationFactStore(mockCtrl)
	factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
	store.EXPECT().GetRequestURIStore().Return(uriStore)
	uriStore.EXPECT().GetIDByURI(gomock.Any(), projectStatusURI).Return(projectStatusID, nil)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(requestSummary, nil)
	store.EXPECT().GetValidationFactStore().Return(factStore)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(entity.ValidationFact{}, storage.ErrNotFound)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-b").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-b", Degree: entity.DegreeBroken, CreatedAt: 20}, nil)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "missing").Return(entity.ValidationFact{}, storage.ErrNotFound)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-a").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: entity.DegreeGreen, CreatedAt: 30}, nil)

	controller := NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	got, err := controller.GetProjectStatusByURI(context.Background(), entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-b", "missing", "project-a"}})
	require.NoError(t, err)
	require.Len(t, got.ProjectValidationFacts, 2)
	assert.Equal(t, []string{"project-b", "project-a"}, []string{got.ProjectValidationFacts[0].Project, got.ProjectValidationFacts[1].Project})
	assert.Equal(t, int64(30), got.UpdatedAtMs)
	assert.Empty(t, got.NextPageToken)
	assert.False(t, got.ProjectResultsComplete)
}

func TestGetProjectStatusByURIPaginatesProjectFacts(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, StateTimestampMs: 10}
	projects := []string{"project-a", "project-b"}

	firstPage := func(t *testing.T) string {
		mockCtrl := gomock.NewController(t)
		factory := storagemock.NewMockFactory(mockCtrl)
		store := storagemock.NewMockStorage(mockCtrl)
		uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
		summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
		factStore := storagemock.NewMockValidationFactStore(mockCtrl)
		factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
		store.EXPECT().GetRequestURIStore().Return(uriStore)
		uriStore.EXPECT().GetIDByURI(gomock.Any(), projectStatusURI).Return(projectStatusID, nil)
		store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
		summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(requestSummary, nil)
		store.EXPECT().GetValidationFactStore().Return(factStore)
		factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(entity.ValidationFact{}, storage.ErrNotFound)
		factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-a").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: entity.DegreeGreen}, nil)

		got, err := NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetProjectStatusByURI(context.Background(), entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: projects, PageSize: 1})
		require.NoError(t, err)
		require.Len(t, got.ProjectValidationFacts, 1)
		assert.Equal(t, "project-a", got.ProjectValidationFacts[0].Project)
		require.NotEmpty(t, got.NextPageToken)
		return got.NextPageToken
	}(t)

	mockCtrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mockCtrl)
	store := storagemock.NewMockStorage(mockCtrl)
	uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
	factStore := storagemock.NewMockValidationFactStore(mockCtrl)
	factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
	store.EXPECT().GetRequestURIStore().Return(uriStore)
	uriStore.EXPECT().GetIDByURI(gomock.Any(), projectStatusURI).Return(projectStatusID, nil)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(requestSummary, nil)
	store.EXPECT().GetValidationFactStore().Return(factStore)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(entity.ValidationFact{}, storage.ErrNotFound)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-b").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-b", Degree: entity.DegreeBroken}, nil)
	got, err := NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetProjectStatusByURI(context.Background(), entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: projects, PageSize: 1, PageToken: firstPage})
	require.NoError(t, err)
	require.Len(t, got.ProjectValidationFacts, 1)
	assert.Equal(t, "project-b", got.ProjectValidationFacts[0].Project)
	assert.Empty(t, got.NextPageToken)
}

func TestSelectProjectStatusPageRejectsMismatchedToken(t *testing.T) {
	token, err := encodeProjectStatusPageToken(projectStatusPageToken{RequestID: projectStatusID, Projects: []string{"project-a", "project-b"}, NextIndex: 1})
	require.NoError(t, err)
	_, err = selectProjectStatusPage(entity.GetProjectStatusByURIRequest{Projects: []string{"project-b", "project-a"}, PageToken: token}, projectStatusID)
	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestValidateProjectFact(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, URI: projectStatusURI}
	assert.NoError(t, validateProjectFact(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: entity.DegreeGreen}, requestSummary, "project-a"))
	assert.True(t, IsProjectStatusConsistency(validateProjectFact(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-b", Degree: entity.DegreeGreen}, requestSummary, "project-a")))
	assert.True(t, IsProjectStatusConsistency(validateProjectFact(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: math.NaN()}, requestSummary, "project-a")))
}

func TestGetProjectStatusByURIRejectsInconsistentRequest(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mockCtrl)
	store := storagemock.NewMockStorage(mockCtrl)
	uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
	factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
	store.EXPECT().GetRequestURIStore().Return(uriStore)
	uriStore.EXPECT().GetIDByURI(gomock.Any(), projectStatusURI).Return(projectStatusID, nil)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: "other"}, nil)

	controller := NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	_, err := controller.GetProjectStatusByURI(context.Background(), entity.GetProjectStatusByURIRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}})
	require.Error(t, err)
	assert.True(t, IsProjectStatusConsistency(err))
	assert.False(t, errors.Is(err, storage.ErrNotFound))
}
