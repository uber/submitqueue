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
	"strings"
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
	projectStatusID    = "7"
)

func TestGetProjectStatus(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, State: entity.RequestStateProcessing, RequestVersion: 1, StateTimestampMs: 1}
	projectFact := entity.ValidationFact{
		URI:       projectStatusURI,
		RequestID: projectStatusID,
		Project:   "project-a",
		Degree:    0.5,
		CreatedAt: 20,
	}
	greenFact := projectFact
	greenFact.Project = "project-green"
	greenFact.Degree = entity.DegreeGreen
	readErr := errors.New("project listing failed")

	tests := []struct {
		name            string
		request         entity.GetProjectStatusRequest
		uriErr          error
		summaryErr      error
		fact            entity.ValidationFact
		factErr         error
		projectFacts    []entity.ValidationFact
		wantProjects    []entity.ValidationFact
		wantErr         error
		wantFact        bool
		wantNotFound    bool
		wantRetryable   bool
		wantInvalid     bool
		noProjectLookup bool
		wantConsistency bool
	}{
		{name: "in progress without fact", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, factErr: storage.ErrNotFound},
		{name: "recorded green fact", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, fact: entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Degree: entity.DegreeGreen}, wantFact: true},
		{name: "missing uri", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, uriErr: storage.ErrNotFound, wantNotFound: true},
		{name: "request summary visibility gap", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, summaryErr: storage.ErrNotFound, wantRetryable: true},
		{name: "empty queue", request: entity.GetProjectStatusRequest{ChangeURI: projectStatusURI}, wantInvalid: true},
		{
			name:    "omitted projects returns stored results",
			request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI},
			factErr: storage.ErrNotFound, projectFacts: []entity.ValidationFact{projectFact, greenFact},
			wantProjects: []entity.ValidationFact{projectFact, greenFact},
		},
		{
			name:    "empty projects returns stored results",
			request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{}},
			factErr: storage.ErrNotFound, projectFacts: []entity.ValidationFact{projectFact},
			wantProjects: []entity.ValidationFact{projectFact},
		},
		{
			name:    "no recorded projects",
			request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI},
			factErr: storage.ErrNotFound,
		},
		{
			name:    "project listing failure",
			request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI},
			factErr: storage.ErrNotFound, wantErr: readErr,
		},
		{name: "empty project", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{""}}, wantInvalid: true},
		{name: "duplicate projects", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a", "project-a"}}, wantInvalid: true},
		{name: "invalid page size", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, PageSize: maxProjectStatusPageSize + 1}, wantInvalid: true},
		{name: "malformed page token", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}, PageToken: "token"}, factErr: storage.ErrNotFound, wantInvalid: true, noProjectLookup: true},
		{name: "invalid fact degree", request: entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}}, fact: entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Degree: math.NaN()}, wantConsistency: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mockCtrl)
			store := storagemock.NewMockStorage(mockCtrl)
			uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
			summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
			factStore := storagemock.NewMockValidationFactStore(mockCtrl)
			indexStore := storagemock.NewMockProjectFactByDegreeStore(mockCtrl)
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
							if len(tt.request.Projects) == 0 {
								store.EXPECT().GetProjectFactByDegreeStore().Return(indexStore)
								indexStore.EXPECT().List(gomock.Any(), projectStatusURI, entity.ProjectFactByDegree{}, defaultProjectStatusPageSize+1).
									Return(projectStatusIndexEntries(t, tt.projectFacts), tt.wantErr)
								if tt.wantErr == nil {
									for _, fact := range tt.projectFacts {
										factStore.EXPECT().Get(gomock.Any(), projectStatusURI, fact.Project).Return(fact, nil)
									}
								}
							}
							for _, project := range tt.request.Projects {
								factStore.EXPECT().Get(gomock.Any(), projectStatusURI, project).Return(entity.ValidationFact{}, storage.ErrNotFound)
							}
						}
					}
				}
			}

			controller := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory)
			got, err := controller.GetProjectStatus(context.Background(), tt.request)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}

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
			assert.Equal(t, tt.wantProjects, got.ProjectValidationFacts)
			if tt.wantFact {
				assert.Equal(t, tt.fact, got.RepositoryValidationFact)
			}
		})
	}
}

func TestGetProjectStatusByID(t *testing.T) {
	readErr := errors.New("storage read failed")
	summary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, BaseURI: "git://monorepo/main/base", State: entity.RequestStateSucceeded, StateTimestampMs: 10}
	tests := []struct {
		name            string
		summary         entity.RequestSummary
		summaryErr      error
		fact            entity.ValidationFact
		factErr         error
		wantNotFound    bool
		wantConsistency bool
		wantErr         error
	}{
		{name: "in progress without facts", summary: summary, factErr: storage.ErrNotFound},
		{name: "recorded green fact", summary: summary, fact: entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Degree: entity.DegreeGreen, CreatedAt: 20}},
		{name: "unknown ID", summaryErr: storage.ErrNotFound, wantNotFound: true},
		{name: "summary read failure", summaryErr: readErr, wantErr: readErr},
		{name: "wrong queue", summary: entity.RequestSummary{RequestID: projectStatusID, Queue: "other/main", URI: projectStatusURI}, wantConsistency: true},
		{name: "wrong ID", summary: entity.RequestSummary{RequestID: "8", Queue: projectStatusQueue, URI: projectStatusURI}, wantConsistency: true},
		{name: "missing URI", summary: entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue}, wantConsistency: true},
		{name: "fact belongs to another request", summary: summary, fact: entity.ValidationFact{URI: projectStatusURI, RequestID: "8"}, wantConsistency: true},
		{name: "fact read failure", summary: summary, factErr: readErr, wantErr: readErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mockCtrl)
			store := storagemock.NewMockStorage(mockCtrl)
			summaries := storagemock.NewMockRequestSummaryStore(mockCtrl)
			facts := storagemock.NewMockValidationFactStore(mockCtrl)
			factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
			store.EXPECT().GetRequestSummaryStore().Return(summaries)
			summaries.EXPECT().Get(gomock.Any(), projectStatusID).Return(tt.summary, tt.summaryErr)
			if tt.summaryErr == nil && tt.summary == summary {
				store.EXPECT().GetValidationFactStore().Return(facts)
				facts.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(tt.fact, tt.factErr)
				if !tt.wantConsistency && tt.wantErr == nil {
					facts.EXPECT().Get(gomock.Any(), projectStatusURI, "project-a").Return(entity.ValidationFact{}, storage.ErrNotFound)
				}
			}
			got, err := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetProjectStatus(context.Background(), entity.GetProjectStatusRequest{
				Queue: projectStatusQueue, RequestID: projectStatusID, Projects: []string{"project-a"},
			})
			if tt.wantNotFound || tt.wantConsistency || tt.wantErr != nil {
				require.Error(t, err)
				assert.Equal(t, tt.wantNotFound, IsProjectStatusNotFound(err))
				assert.Equal(t, tt.wantConsistency, IsProjectStatusConsistency(err))
				assert.False(t, errs.IsRetryable(err))
				if tt.wantNotFound {
					assert.True(t, errs.IsUserError(err))
					var notFound *ProjectStatusNotFoundError
					require.ErrorAs(t, err, &notFound)
					assert.Equal(t, projectStatusID, notFound.RequestID)
					assert.Equal(t, projectStatusQueue, notFound.Queue)
				}
				if tt.wantErr != nil {
					assert.ErrorIs(t, err, tt.wantErr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, summary, got.RequestSummary)
			assert.Equal(t, tt.factErr == nil, got.HasRepositoryValidationFact)
			assert.Equal(t, max(summary.StateTimestampMs, tt.fact.CreatedAt), got.UpdatedAtMs)
		})
	}
}

func TestGetProjectStatusRejectsInvalidIDSelectors(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*entity.GetProjectStatusRequest)
	}{
		{name: "missing selector", modify: func(req *entity.GetProjectStatusRequest) { req.RequestID = "" }},
		{name: "both selectors", modify: func(req *entity.GetProjectStatusRequest) { req.ChangeURI = projectStatusURI }},
		{name: "empty queue", modify: func(req *entity.GetProjectStatusRequest) { req.Queue = "" }},
		{name: "long queue", modify: func(req *entity.GetProjectStatusRequest) { req.Queue = strings.Repeat("q", 256) }},
		{name: "noncanonical ID", modify: func(req *entity.GetProjectStatusRequest) { req.RequestID = "07" }},
		{name: "nonnumeric ID", modify: func(req *entity.GetProjectStatusRequest) { req.RequestID = "request/7" }},
		{name: "zero ID", modify: func(req *entity.GetProjectStatusRequest) { req.RequestID = "0" }},
		{name: "negative ID", modify: func(req *entity.GetProjectStatusRequest) { req.RequestID = "-7" }},
		{name: "overflow ID", modify: func(req *entity.GetProjectStatusRequest) { req.RequestID = "9223372036854775808" }},
		{name: "empty project", modify: func(req *entity.GetProjectStatusRequest) { req.Projects = []string{""} }},
		{name: "duplicate project", modify: func(req *entity.GetProjectStatusRequest) { req.Projects = []string{"p", "p"} }},
		{name: "negative page size", modify: func(req *entity.GetProjectStatusRequest) { req.PageSize = -1 }},
		{name: "large page size", modify: func(req *entity.GetProjectStatusRequest) { req.PageSize = maxProjectStatusPageSize + 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := entity.GetProjectStatusRequest{Queue: projectStatusQueue, RequestID: projectStatusID, Projects: []string{"project-a"}}
			tt.modify(&req)
			factory := storagemock.NewMockFactory(gomock.NewController(t))
			_, err := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetProjectStatus(context.Background(), req)
			require.Error(t, err)
			assert.True(t, IsInvalidRequest(err))
		})
	}
}

func TestGetProjectStatusUsesRequestSummaryTimestamp(t *testing.T) {
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

	controller := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	got, err := controller.GetProjectStatus(context.Background(), entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}})
	require.NoError(t, err)
	assert.Equal(t, int64(20), got.UpdatedAtMs)
}

func TestGetProjectStatusReturnsProjectFactsInRequestedOrder(t *testing.T) {
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
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-b").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-b", Degree: entity.DegreeGreen, CreatedAt: 20}, nil)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "missing").Return(entity.ValidationFact{}, storage.ErrNotFound)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-a").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: entity.DegreeBroken, CreatedAt: 30}, nil)

	controller := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	got, err := controller.GetProjectStatus(context.Background(), entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-b", "missing", "project-a"}})
	require.NoError(t, err)
	require.Len(t, got.ProjectValidationFacts, 2)
	assert.Equal(t, []string{"project-b", "project-a"}, []string{got.ProjectValidationFacts[0].Project, got.ProjectValidationFacts[1].Project})
	assert.Equal(t, int64(30), got.UpdatedAtMs)
	assert.Empty(t, got.NextPageToken)
	assert.False(t, got.ProjectResultsComplete)
}

func TestGetProjectStatusPaginatesProjectFacts(t *testing.T) {
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

		got, err := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetProjectStatus(context.Background(), entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: projects, PageSize: 1})
		require.NoError(t, err)
		require.Len(t, got.ProjectValidationFacts, 1)
		assert.Equal(t, "project-a", got.ProjectValidationFacts[0].Project)
		require.NotEmpty(t, got.NextPageToken)
		return got.NextPageToken
	}(t)

	mockCtrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mockCtrl)
	store := storagemock.NewMockStorage(mockCtrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
	factStore := storagemock.NewMockValidationFactStore(mockCtrl)
	factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), projectStatusID).Return(requestSummary, nil)
	store.EXPECT().GetValidationFactStore().Return(factStore)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(entity.ValidationFact{}, storage.ErrNotFound)
	factStore.EXPECT().Get(gomock.Any(), projectStatusURI, "project-b").Return(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-b", Degree: entity.DegreeBroken}, nil)
	got, err := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetProjectStatus(context.Background(), entity.GetProjectStatusRequest{Queue: projectStatusQueue, RequestID: projectStatusID, Projects: projects, PageSize: 1, PageToken: firstPage})
	require.NoError(t, err)
	require.Len(t, got.ProjectValidationFacts, 1)
	assert.Equal(t, "project-b", got.ProjectValidationFacts[0].Project)
	assert.Empty(t, got.NextPageToken)
}

func TestParseProjectStatusPageTokenRejectsMismatches(t *testing.T) {
	modes := []struct {
		name     string
		projects []string
	}{
		{name: "listed projects"},
		{
			name:     "requested projects",
			projects: []string{"a", "b"},
		},
	}

	tests := []struct {
		name   string
		modify func(*projectStatusPageToken)
	}{
		{
			name:   "other request",
			modify: func(token *projectStatusPageToken) { token.RequestID = "8" },
		},
		{
			name:   "other queue",
			modify: func(token *projectStatusPageToken) { token.Queue = "other/main" },
		},
		{
			name:   "missing queue",
			modify: func(token *projectStatusPageToken) { token.Queue = "" },
		},
		{
			name:   "different selection",
			modify: func(token *projectStatusPageToken) { token.Projects = []string{"b", "a"} },
		},
		{
			name: "mixed cursors",
			modify: func(token *projectStatusPageToken) {
				token.AfterProject = "a"
				token.NextIndex = 1
			},
		},
		{
			name: "missing cursor",
			modify: func(token *projectStatusPageToken) {
				token.AfterProject = ""
				token.NextIndex = 0
			},
		},
		{
			name: "invalid cursor",
			modify: func(token *projectStatusPageToken) {
				if len(token.Projects) == 0 {
					token.AfterProject = strings.Repeat("p", 256)
				} else {
					token.NextIndex = len(token.Projects)
				}
			},
		},
		{
			name:   "negative index",
			modify: func(token *projectStatusPageToken) { token.NextIndex = -1 },
		},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					token := projectStatusPageToken{Queue: projectStatusQueue, RequestID: projectStatusID, Projects: mode.projects}
					if len(mode.projects) == 0 {
						token.AfterProject = "a"
					} else {
						token.NextIndex = 1
					}

					tt.modify(&token)
					encoded, err := encodeProjectStatusPageToken(token)
					require.NoError(t, err)

					_, err = parseProjectStatusPageToken(
						entity.GetProjectStatusRequest{Projects: mode.projects, PageToken: encoded},
						entity.RequestSummary{Queue: projectStatusQueue, RequestID: projectStatusID},
					)
					require.Error(t, err)
					assert.True(t, IsInvalidRequest(err))
				})
			}
		})
	}
}

func TestGetProjectStatusPaginatesStoredProjects(t *testing.T) {
	summary := entity.RequestSummary{RequestID: projectStatusID, Queue: projectStatusQueue, URI: projectStatusURI, StateTimestampMs: 10}
	stored := []entity.ValidationFact{
		{URI: projectStatusURI, RequestID: projectStatusID, Project: "c", Degree: 1, CreatedAt: 60},
		{URI: projectStatusURI, RequestID: projectStatusID, Project: "a", Degree: 0.5, CreatedAt: 20},
		{URI: projectStatusURI, RequestID: projectStatusID, Project: "b", Degree: 0, CreatedAt: 50},
	}

	ctrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	summaries := storagemock.NewMockRequestSummaryStore(ctrl)
	facts := storagemock.NewMockValidationFactStore(ctrl)
	indexStore := storagemock.NewMockProjectFactByDegreeStore(ctrl)
	entries := projectStatusIndexEntries(t, stored)

	factory.EXPECT().For(storage.Config{QueueName: projectStatusQueue}).Return(store, nil).Times(2)
	store.EXPECT().GetRequestSummaryStore().Return(summaries).Times(2)
	summaries.EXPECT().Get(gomock.Any(), projectStatusID).Return(summary, nil).Times(2)
	store.EXPECT().GetValidationFactStore().Return(facts).Times(2)
	store.EXPECT().GetProjectFactByDegreeStore().Return(indexStore).Times(2)
	facts.EXPECT().Get(gomock.Any(), projectStatusURI, "").Return(entity.ValidationFact{}, storage.ErrNotFound).Times(2)
	indexStore.EXPECT().List(gomock.Any(), projectStatusURI, entity.ProjectFactByDegree{}, 3).Return(entries, nil)
	indexStore.EXPECT().List(gomock.Any(), projectStatusURI, entries[1], defaultProjectStatusPageSize+1).Return(entries[2:], nil)
	facts.EXPECT().Get(gomock.Any(), projectStatusURI, "c").Return(stored[0], nil)
	facts.EXPECT().Get(gomock.Any(), projectStatusURI, "a").Return(stored[1], nil).Times(2)
	facts.EXPECT().Get(gomock.Any(), projectStatusURI, "b").Return(stored[2], nil)

	controller := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	req := entity.GetProjectStatusRequest{Queue: projectStatusQueue, RequestID: projectStatusID, PageSize: 2}

	first, err := controller.GetProjectStatus(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, stored[:2], first.ProjectValidationFacts)
	assert.Equal(t, int64(60), first.UpdatedAtMs)
	require.NotEmpty(t, first.NextPageToken)

	req.PageToken = first.NextPageToken
	req.Projects = []string{}
	req.PageSize = 0

	second, err := controller.GetProjectStatus(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, stored[2:], second.ProjectValidationFacts)
	assert.Equal(t, int64(50), second.UpdatedAtMs)
	assert.Empty(t, second.NextPageToken)
}

func projectStatusIndexEntries(t *testing.T, facts []entity.ValidationFact) []entity.ProjectFactByDegree {
	t.Helper()
	var entries []entity.ProjectFactByDegree
	for _, fact := range facts {
		entry, err := entity.NewProjectFactByDegree(fact)
		require.NoError(t, err)
		entries = append(entries, entry)
	}
	return entries
}

func TestListProjectStatusPageRejectsMissingCursor(t *testing.T) {
	for _, readErr := range []error{storage.ErrNotFound, errors.New("database unavailable")} {
		t.Run(readErr.Error(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			facts := storagemock.NewMockValidationFactStore(ctrl)
			index := storagemock.NewMockProjectFactByDegreeStore(ctrl)
			facts.EXPECT().Get(gomock.Any(), projectStatusURI, "cursor").Return(entity.ValidationFact{}, readErr)
			_, err := listProjectStatusPage(context.Background(), entity.RequestSummary{URI: projectStatusURI}, facts, index, projectStatusPageToken{AfterProject: "cursor"}, 2)
			require.ErrorIs(t, err, readErr)
		})
	}
}

func TestListProjectStatusPageRejectsInconsistentIndex(t *testing.T) {
	fact := entity.ValidationFact{URI: projectStatusURI, Project: "p", Degree: 0.5, RequestID: projectStatusID}
	entry, err := entity.NewProjectFactByDegree(fact)
	require.NoError(t, err)
	for _, tt := range []struct {
		name    string
		modify  func(*entity.ProjectFactByDegree, *entity.ValidationFact)
		factErr error
	}{
		{name: "missing fact", factErr: storage.ErrNotFound, modify: func(*entity.ProjectFactByDegree, *entity.ValidationFact) {}},
		{name: "read failure", factErr: errors.New("database unavailable"), modify: func(*entity.ProjectFactByDegree, *entity.ValidationFact) {}},
		{name: "wrong degree", modify: func(e *entity.ProjectFactByDegree, _ *entity.ValidationFact) { e.Degree = 0.75 }},
		{name: "wrong URI", modify: func(e *entity.ProjectFactByDegree, _ *entity.ValidationFact) { e.URI = "other" }},
		{name: "wrong request", modify: func(_ *entity.ProjectFactByDegree, f *entity.ValidationFact) { f.RequestID = "other" }},
		{name: "repository fact", modify: func(e *entity.ProjectFactByDegree, f *entity.ValidationFact) { e.Project = ""; f.Project = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			facts := storagemock.NewMockValidationFactStore(ctrl)
			index := storagemock.NewMockProjectFactByDegreeStore(ctrl)
			e, f := entry, fact
			tt.modify(&e, &f)
			index.EXPECT().List(gomock.Any(), projectStatusURI, entity.ProjectFactByDegree{}, 3).Return([]entity.ProjectFactByDegree{e}, nil)
			facts.EXPECT().Get(gomock.Any(), projectStatusURI, e.Project).Return(f, tt.factErr)
			_, err := listProjectStatusPage(context.Background(), entity.RequestSummary{URI: projectStatusURI, RequestID: projectStatusID}, facts, index, projectStatusPageToken{}, 2)
			require.Error(t, err)
			if tt.factErr != nil {
				assert.ErrorIs(t, err, tt.factErr)
			} else {
				assert.True(t, IsProjectStatusConsistency(err))
			}
		})
	}
}

func TestValidateProjectFact(t *testing.T) {
	requestSummary := entity.RequestSummary{RequestID: projectStatusID, URI: projectStatusURI}
	assert.NoError(t, validateProjectFact(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: entity.DegreeGreen}, requestSummary, "project-a"))
	assert.True(t, IsProjectStatusConsistency(validateProjectFact(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-b", Degree: entity.DegreeGreen}, requestSummary, "project-a")))
	assert.True(t, IsProjectStatusConsistency(validateProjectFact(entity.ValidationFact{URI: projectStatusURI, RequestID: projectStatusID, Project: "project-a", Degree: math.NaN()}, requestSummary, "project-a")))
}

func TestGetProjectStatusRejectsInconsistentRequest(t *testing.T) {
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

	controller := NewProjectStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory)
	_, err := controller.GetProjectStatus(context.Background(), entity.GetProjectStatusRequest{Queue: projectStatusQueue, ChangeURI: projectStatusURI, Projects: []string{"project-a"}})
	require.Error(t, err)
	assert.True(t, IsProjectStatusConsistency(err))
	assert.False(t, errors.Is(err, storage.ErrNotFound))
}
