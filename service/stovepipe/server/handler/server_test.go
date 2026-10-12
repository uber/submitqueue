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

package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	controllermock "github.com/uber/submitqueue/stovepipe/controller/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/mock/gomock"
)

func TestGetRequestHistoryByID(t *testing.T) {
	controllerErr := errors.New("controller failed")
	logs := []entity.RequestLog{
		{ID: "occurrence/1", State: entity.RequestStateAccepted, TimestampMs: 1000},
		{ID: "occurrence/2", Event: entity.RequestEventBuildTriggered, TimestampMs: 2000},
	}
	tests := []struct {
		name     string
		logs     []entity.RequestLog
		err      error
		wantLogs int
	}{
		{name: "maps successful result", logs: logs, wantLogs: 2},
		{name: "maps empty result", logs: nil, wantLogs: 0},
		{name: "returns controller error unchanged", err: controllerErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := entity.RequestHistory{
				RequestID: "1", Events: tt.logs,
				RequestSummary: entity.RequestSummary{RequestID: "1", Queue: "monorepo/main", URI: "uri", State: entity.RequestStateSucceeded},
			}
			historyController := controllermock.NewMockRequestHistoryController(gomock.NewController(t))
			historyController.EXPECT().GetRequestHistoryByID(gomock.Any(), entity.GetRequestHistoryByIDRequest{Queue: "monorepo/main", ID: "1"}).Return(history, tt.err)
			srv := &StovepipeServer{requestHistoryController: historyController}

			resp, err := srv.GetRequestHistoryByID(context.Background(), &pb.GetRequestHistoryByIDRequest{
				Queue: "monorepo/main", RequestId: "1",
			})

			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.Events, tt.wantLogs)
			require.NotNil(t, resp.RequestSummary)
			assert.Equal(t, "succeeded", resp.RequestSummary.GetRequestState())
			if tt.wantLogs > 0 {
				assert.Equal(t, "accepted", resp.Events[0].GetRequestState())
				assert.Equal(t, "build_triggered", resp.Events[1].GetEvent())
			}
		})
	}
}

func TestGetRequestHistoryByURI(t *testing.T) {
	controllerErr := errors.New("controller failed")
	histories := []entity.RequestHistory{{
		RequestID:      "1",
		Events:         []entity.RequestLog{{ID: "occurrence/1", State: entity.RequestStateAccepted}},
		RequestSummary: entity.RequestSummary{RequestID: "1", Queue: "monorepo/main", URI: "git://monorepo/abc", State: entity.RequestStateProcessing},
	}}
	tests := []struct {
		name          string
		histories     []entity.RequestHistory
		err           error
		wantHistories int
	}{
		{name: "maps successful result", histories: histories, wantHistories: 1},
		{name: "maps empty result", histories: nil, wantHistories: 0},
		{name: "returns controller error unchanged", err: controllerErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			historyController := controllermock.NewMockRequestHistoryController(gomock.NewController(t))
			historyController.EXPECT().GetRequestHistoryByURI(gomock.Any(), entity.GetRequestHistoryByURIRequest{Queue: "monorepo/main", URI: "git://monorepo/abc"}).Return(tt.histories, tt.err)
			srv := &StovepipeServer{requestHistoryController: historyController}

			resp, err := srv.GetRequestHistoryByURI(context.Background(), &pb.GetRequestHistoryByURIRequest{
				Queue: "monorepo/main", Uri: "git://monorepo/abc",
			})

			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.Histories, tt.wantHistories)
			if tt.wantHistories > 0 {
				require.NotNil(t, resp.Histories[0].RequestSummary)
				assert.Equal(t, "processing", resp.Histories[0].RequestSummary.GetRequestState())
				assert.Equal(t, "1", resp.Histories[0].RequestId)
				require.Len(t, resp.Histories[0].Events, 1)
				assert.Equal(t, "accepted", resp.Histories[0].Events[0].GetRequestState())
			}
		})
	}
}

type fakeProjectStatusController struct {
	getStatus func(context.Context, entity.GetProjectStatusRequest) (entity.GetProjectStatusResult, error)
}

func (f *fakeProjectStatusController) GetProjectStatus(ctx context.Context, req entity.GetProjectStatusRequest) (entity.GetProjectStatusResult, error) {
	return f.getStatus(ctx, req)
}

func TestGetProjectStatus(t *testing.T) {
	controllerErr := errors.New("controller failed")
	for _, selector := range []string{"ID", "URI"} {
		for _, callErr := range []error{nil, controllerErr} {
			name := selector + "/success"
			if callErr != nil {
				name = selector + "/controller error"
			}
			t.Run(name, func(t *testing.T) {
				wantReq := entity.GetProjectStatusRequest{Queue: "queue", Projects: []string{"project-a"}, PageSize: 25, PageToken: "token"}
				if selector == "ID" {
					wantReq.RequestID = "7"
				} else {
					wantReq.ChangeURI = "uri"
				}
				fake := &fakeProjectStatusController{getStatus: func(_ context.Context, req entity.GetProjectStatusRequest) (entity.GetProjectStatusResult, error) {
					assert.Equal(t, wantReq, req)
					return entity.GetProjectStatusResult{
						RequestSummary: entity.RequestSummary{RequestID: "7", Queue: "queue", URI: "uri", State: entity.RequestStateSucceeded},
					}, callErr
				}}
				srv := &StovepipeServer{projectStatusController: fake}
				if selector == "ID" {
					response, err := srv.GetProjectStatusByID(context.Background(), &pb.GetProjectStatusByIDRequest{
						Queue: "queue", RequestId: "7", Projects: []string{"project-a"}, PageSize: 25, PageToken: "token",
					})
					if callErr != nil {
						require.ErrorIs(t, err, callErr)
						assert.Nil(t, response)
						return
					}
					require.NoError(t, err)
					assert.Equal(t, "7", response.GetRequestId())
					assert.Equal(t, "succeeded", response.GetRequestState())
				} else {
					response, err := srv.GetProjectStatusByURI(context.Background(), &pb.GetProjectStatusByURIRequest{
						Queue: "queue", ChangeUri: "uri", Projects: []string{"project-a"}, PageSize: 25, PageToken: "token",
					})
					if callErr != nil {
						require.ErrorIs(t, err, callErr)
						assert.Nil(t, response)
						return
					}
					require.NoError(t, err)
					assert.Equal(t, "7", response.GetRequestId())
					assert.Equal(t, "succeeded", response.GetRequestState())
				}
			})
		}
	}
}
