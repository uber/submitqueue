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
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestGetIssuesByRequestIDPagination(t *testing.T) {
	mocks := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mocks)
	store := storagemock.NewMockStorage(mocks)
	history := storagemock.NewMockRequestLogStore(mocks)
	issues := storagemock.NewMockRequestIssueSummaryStore(mocks)
	c := NewGetIssuesByRequestIDController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"})
	factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(store, nil).Times(2)
	store.EXPECT().GetRequestLogStore().Return(history).Times(2)
	history.EXPECT().List(gomock.Any(), "12").Return([]entity.RequestLog{{Event: entity.RequestEventIssueTrackingStarted}, {Event: entity.RequestEventIssuesReconciled}}, nil).Times(2)
	store.EXPECT().GetRequestIssueSummaryStore().Return(issues).Times(2)
	rows := []entity.RequestIssueSummary{
		{RequestID: "12", TaskID: "a", Kind: entity.RequestIssueKindNew},
		{RequestID: "12", TaskID: "a", UnitID: "unit", Kind: entity.RequestIssueKindExisting},
		{RequestID: "12", TaskID: "b", UnitID: "unit", Kind: entity.RequestIssueKindClosed},
	}
	issues.EXPECT().List(gomock.Any(), "12", storage.RequestIssueSummaryRange{Limit: 3}).Return(rows, nil)
	first, err := c.GetIssuesByRequestID(context.Background(), entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12", PageSize: 2})
	require.NoError(t, err)
	require.Equal(t, entity.RequestIssuesStatusComplete, first.Status)
	require.Equal(t, rows[:2], first.Issues)
	require.NotEmpty(t, first.NextPageToken)
	issues.EXPECT().List(gomock.Any(), "12", storage.RequestIssueSummaryRange{After: storage.RequestIssueSummaryCursor{TaskID: "a", UnitID: "unit"}, Limit: 3}).Return(rows[2:], nil)
	last, err := c.GetIssuesByRequestID(context.Background(), entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12", PageSize: 2, PageToken: first.NextPageToken})
	require.NoError(t, err)
	require.Equal(t, rows[2:], last.Issues)
	require.Empty(t, last.NextPageToken)
}

func TestRequestIssuesStatus(t *testing.T) {
	for _, tt := range []struct {
		name   string
		events []entity.RequestEvent
		want   entity.RequestIssuesStatus
	}{
		{"not started", nil, entity.RequestIssuesStatusUnavailable},
		{"historical completion without coverage", []entity.RequestEvent{entity.RequestEventIssuesReconciled}, entity.RequestIssuesStatusUnavailable},
		{"in progress", []entity.RequestEvent{entity.RequestEventIssueTrackingStarted}, entity.RequestIssuesStatusInProgress},
		{"complete", []entity.RequestEvent{entity.RequestEventIssueTrackingStarted, entity.RequestEventIssuesReconciled}, entity.RequestIssuesStatusComplete},
		{"abandoned", []entity.RequestEvent{entity.RequestEventIssueTrackingStarted, entity.RequestEventIssueTrackingAbandoned}, entity.RequestIssuesStatusIncomplete},
		{"abandoned before starting", []entity.RequestEvent{entity.RequestEventIssueTrackingAbandoned}, entity.RequestIssuesStatusIncomplete},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var history []entity.RequestLog
			for _, event := range tt.events {
				history = append(history, entity.RequestLog{Event: event})
			}
			require.Equal(t, tt.want, requestIssuesStatus(history))
		})
	}
}

func TestRequestIssuePageBounds(t *testing.T) {
	valid := requestIssuesPageToken{Version: requestIssuesPageTokenVersion, Queue: "queue", RequestID: "12", LastTaskID: "task"}
	for _, tt := range []struct {
		name   string
		change func(*requestIssuesPageToken)
	}{
		{"other queue", func(t *requestIssuesPageToken) { t.Queue = "other" }},
		{"other request", func(t *requestIssuesPageToken) { t.RequestID = "13" }},
		{"old version", func(t *requestIssuesPageToken) { t.Version = 0 }},
		{"missing task", func(t *requestIssuesPageToken) { t.LastTaskID = "" }},
		{"long task", func(t *requestIssuesPageToken) { t.LastTaskID = strings.Repeat("t", 256) }},
		{"long unit", func(t *requestIssuesPageToken) { t.LastUnitID = strings.Repeat("u", 1025) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token := valid
			tt.change(&token)
			data, err := json.Marshal(token)
			require.NoError(t, err)
			_, err = resolveRequestIssueSummaryRange(entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12", PageToken: base64.RawURLEncoding.EncodeToString(data)})
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
	for _, pageSize := range []int32{-1, 201} {
		_, err := resolveRequestIssueSummaryRange(entity.GetIssuesByRequestIDRequest{PageSize: pageSize})
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
	for _, token := range []string{"bad!", base64.RawURLEncoding.EncodeToString([]byte("not-json"))} {
		_, err := resolveRequestIssueSummaryRange(entity.GetIssuesByRequestIDRequest{PageToken: token})
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
	bounds, err := resolveRequestIssueSummaryRange(entity.GetIssuesByRequestIDRequest{})
	require.NoError(t, err)
	require.Equal(t, 51, bounds.Limit)
}

func TestGetIssuesByRequestIDRejectsInvalidIdentity(t *testing.T) {
	factory := storagemock.NewMockFactory(gomock.NewController(t))
	c := NewGetIssuesByRequestIDController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"})
	for _, req := range []entity.GetIssuesByRequestIDRequest{
		{}, {Queue: "other", RequestID: "12"}, {Queue: "queue"}, {Queue: "queue", RequestID: "012"},
	} {
		_, err := c.GetIssuesByRequestID(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
}

func TestGetIssuesByRequestIDReadFailures(t *testing.T) {
	for _, step := range []string{"factory", "history", "issues"} {
		t.Run(step, func(t *testing.T) {
			mocks := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mocks)
			store := storagemock.NewMockStorage(mocks)
			history := storagemock.NewMockRequestLogStore(mocks)
			issues := storagemock.NewMockRequestIssueSummaryStore(mocks)
			c := NewGetIssuesByRequestIDController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"})

			failAt := func(name string) error {
				if step == name {
					return assert.AnError
				}
				return nil
			}
			factory.EXPECT().For(gomock.Any()).Return(store, failAt("factory"))
			if step != "factory" {
				store.EXPECT().GetRequestLogStore().Return(history)
				history.EXPECT().List(gomock.Any(), "12").Return(nil, failAt("history"))
			}
			if step == "issues" {
				store.EXPECT().GetRequestIssueSummaryStore().Return(issues)
				issues.EXPECT().List(gomock.Any(), "12", gomock.Any()).Return(nil, assert.AnError)
			}
			_, err := c.GetIssuesByRequestID(context.Background(), entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12"})
			require.ErrorIs(t, err, assert.AnError)
		})
	}
}

func TestGetIssuesByRequestIDMissingRequest(t *testing.T) {
	mocks := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mocks)
	store := storagemock.NewMockStorage(mocks)
	history := storagemock.NewMockRequestLogStore(mocks)
	c := NewGetIssuesByRequestIDController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"})
	factory.EXPECT().For(gomock.Any()).Return(store, nil)
	store.EXPECT().GetRequestLogStore().Return(history)
	history.EXPECT().List(gomock.Any(), "12").Return(nil, storage.ErrNotFound)
	_, err := c.GetIssuesByRequestID(context.Background(), entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12"})
	require.ErrorIs(t, err, storage.ErrNotFound)
}
