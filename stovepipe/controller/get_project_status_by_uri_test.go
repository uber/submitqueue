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

const testProjectStatusRequestID = "request/monorepo/main/7"

type projectStatusMocks struct {
	factory   *storagemock.MockFactory
	store     *storagemock.MockStorage
	uriStore  *storagemock.MockRequestURIStore
	reqStore  *storagemock.MockRequestStore
	factStore *storagemock.MockValidationFactStore
}

func newProjectStatusController(t *testing.T) (*GetProjectStatusByURIController, projectStatusMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := projectStatusMocks{
		factory:   storagemock.NewMockFactory(ctrl),
		store:     storagemock.NewMockStorage(ctrl),
		uriStore:  storagemock.NewMockRequestURIStore(ctrl),
		reqStore:  storagemock.NewMockRequestStore(ctrl),
		factStore: storagemock.NewMockValidationFactStore(ctrl),
	}
	m.store.EXPECT().GetRequestURIStore().Return(m.uriStore).AnyTimes()
	m.store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	m.store.EXPECT().GetValidationFactStore().Return(m.factStore).AnyTimes()
	return NewGetProjectStatusByURIController(zap.NewNop().Sugar(), tally.NoopScope, m.factory), m
}

func validProjectStatusRequest() entity.GetProjectStatusByURIRequest {
	return entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI}
}

func validStoredProjectStatusRequest() entity.Request {
	return entity.Request{
		ID:      testProjectStatusRequestID,
		Queue:   testQueue,
		URI:     testURI,
		BaseURI: "git://repo/monorepo/main/base",
		State:   entity.RequestStateProcessing,
	}
}

func expectProjectStatusRequestLoaded(m projectStatusMocks, request entity.Request) {
	m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return(testProjectStatusRequestID, nil)
	m.reqStore.EXPECT().Get(gomock.Any(), testProjectStatusRequestID).Return(request, nil)
}

func TestGetProjectStatusByURIController_GetProjectStatusByURI(t *testing.T) {
	t.Run("returns request and recorded green repository fact", func(t *testing.T) {
		controller, m := newProjectStatusController(t)
		request := validStoredProjectStatusRequest()
		expectProjectStatusRequestLoaded(m, request)
		m.factStore.EXPECT().Get(gomock.Any(), testURI, "").Return(entity.ValidationFact{
			URI: testURI, RequestID: testProjectStatusRequestID, Degree: entity.DegreeGreen,
		}, nil)

		result, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

		require.NoError(t, err)
		assert.Equal(t, request, result.Request)
		assert.True(t, result.HasRepositoryValidationFact)
		assert.Equal(t, entity.DegreeGreen, result.RepositoryValidationFact.Degree)
	})

	t.Run("leaves repository degree absent when fact is missing", func(t *testing.T) {
		controller, m := newProjectStatusController(t)
		expectProjectStatusRequestLoaded(m, validStoredProjectStatusRequest())
		m.factStore.EXPECT().Get(gomock.Any(), testURI, "").Return(entity.ValidationFact{}, storage.ErrNotFound)

		result, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

		require.NoError(t, err)
		assert.False(t, result.HasRepositoryValidationFact)
		assert.Equal(t, entity.ValidationFact{}, result.RepositoryValidationFact)
	})

	t.Run("reports missing URI mapping as not found", func(t *testing.T) {
		controller, m := newProjectStatusController(t)
		m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
		m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)

		_, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

		require.Error(t, err)
		assert.True(t, IsProjectStatusNotFound(err))
	})

	t.Run("reports mapping without request as retryable", func(t *testing.T) {
		controller, m := newProjectStatusController(t)
		m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
		m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return(testProjectStatusRequestID, nil)
		m.reqStore.EXPECT().Get(gomock.Any(), testProjectStatusRequestID).Return(entity.Request{}, storage.ErrNotFound)

		_, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

		require.Error(t, err)
		assert.True(t, errs.IsRetryable(err))
	})

	t.Run("reports unsupported exact project as not found", func(t *testing.T) {
		controller, m := newProjectStatusController(t)
		expectProjectStatusRequestLoaded(m, validStoredProjectStatusRequest())
		req := validProjectStatusRequest()
		req.HasProject = true
		req.Project = "//project"

		_, err := controller.GetProjectStatusByURI(context.Background(), req)

		require.Error(t, err)
		assert.True(t, IsProjectStatusNotFound(err))
	})
}

func TestGetProjectStatusByURIController_RejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name    string
		request entity.GetProjectStatusByURIRequest
	}{
		{name: "empty queue", request: entity.GetProjectStatusByURIRequest{ChangeURI: testURI}},
		{name: "oversized queue", request: entity.GetProjectStatusByURIRequest{Queue: strings.Repeat("q", 256), ChangeURI: testURI}},
		{name: "empty change URI", request: entity.GetProjectStatusByURIRequest{Queue: testQueue}},
		{name: "oversized change URI", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: strings.Repeat("u", 256)}},
		{name: "explicit empty project", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, HasProject: true}},
		{name: "oversized project", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, HasProject: true, Project: strings.Repeat("p", 256)}},
		{name: "project with page size", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, HasProject: true, Project: "//project", PageSize: 1}},
		{name: "project with page token", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, HasProject: true, Project: "//project", PageToken: "token"}},
		{name: "negative page size", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, PageSize: -1}},
		{name: "page size above maximum", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, PageSize: 201}},
		{name: "page token before project list", request: entity.GetProjectStatusByURIRequest{Queue: testQueue, ChangeURI: testURI, PageToken: "token"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, _ := newProjectStatusController(t)

			_, err := controller.GetProjectStatusByURI(context.Background(), tt.request)

			require.Error(t, err)
			assert.True(t, IsInvalidRequest(err))
		})
	}
}

func TestGetProjectStatusByURIController_RejectsInconsistentRecords(t *testing.T) {
	requestMismatchTests := []struct {
		name    string
		request entity.Request
	}{
		{name: "request ID", request: func() entity.Request { r := validStoredProjectStatusRequest(); r.ID = "other"; return r }()},
		{name: "request queue", request: func() entity.Request { r := validStoredProjectStatusRequest(); r.Queue = "other"; return r }()},
		{name: "request URI", request: func() entity.Request { r := validStoredProjectStatusRequest(); r.URI = "other"; return r }()},
	}
	for _, tt := range requestMismatchTests {
		t.Run(tt.name, func(t *testing.T) {
			controller, m := newProjectStatusController(t)
			expectProjectStatusRequestLoaded(m, tt.request)

			_, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

			require.Error(t, err)
			assert.True(t, IsProjectStatusConsistency(err))
		})
	}

	factMismatchTests := []struct {
		name string
		fact entity.ValidationFact
	}{
		{name: "fact URI", fact: entity.ValidationFact{URI: "other", RequestID: testProjectStatusRequestID}},
		{name: "fact project", fact: entity.ValidationFact{URI: testURI, Project: "//project", RequestID: testProjectStatusRequestID}},
		{name: "fact request ID", fact: entity.ValidationFact{URI: testURI, RequestID: "other"}},
		{name: "fact degree below range", fact: entity.ValidationFact{URI: testURI, RequestID: testProjectStatusRequestID, Degree: -0.1}},
		{name: "fact degree above range", fact: entity.ValidationFact{URI: testURI, RequestID: testProjectStatusRequestID, Degree: 1.1}},
		{name: "fact degree NaN", fact: entity.ValidationFact{URI: testURI, RequestID: testProjectStatusRequestID, Degree: math.NaN()}},
	}
	for _, tt := range factMismatchTests {
		t.Run(tt.name, func(t *testing.T) {
			controller, m := newProjectStatusController(t)
			expectProjectStatusRequestLoaded(m, validStoredProjectStatusRequest())
			m.factStore.EXPECT().Get(gomock.Any(), testURI, "").Return(tt.fact, nil)

			_, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

			require.Error(t, err)
			assert.True(t, IsProjectStatusConsistency(err))
		})
	}
}

func TestGetProjectStatusByURIController_PropagatesInfrastructureErrors(t *testing.T) {
	infrastructureErr := errors.New("storage unavailable")
	tests := []struct {
		name  string
		setup func(projectStatusMocks)
	}{
		{
			name: "storage factory",
			setup: func(m projectStatusMocks) {
				m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, infrastructureErr)
			},
		},
		{
			name: "URI mapping",
			setup: func(m projectStatusMocks) {
				m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", infrastructureErr)
			},
		},
		{
			name: "request",
			setup: func(m projectStatusMocks) {
				m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return(testProjectStatusRequestID, nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testProjectStatusRequestID).Return(entity.Request{}, infrastructureErr)
			},
		},
		{
			name: "repository fact",
			setup: func(m projectStatusMocks) {
				expectProjectStatusRequestLoaded(m, validStoredProjectStatusRequest())
				m.factStore.EXPECT().Get(gomock.Any(), testURI, "").Return(entity.ValidationFact{}, infrastructureErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, m := newProjectStatusController(t)
			tt.setup(m)

			_, err := controller.GetProjectStatusByURI(context.Background(), validProjectStatusRequest())

			require.Error(t, err)
			assert.ErrorIs(t, err, infrastructureErr)
			assert.False(t, IsInvalidRequest(err))
			assert.False(t, IsProjectStatusNotFound(err))
			assert.False(t, IsProjectStatusConsistency(err))
		})
	}
}
