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
)

func TestIssueHistoryPagination(t *testing.T) {
	mocks := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mocks)
	store := storagemock.NewMockStorage(mocks)
	logs := storagemock.NewMockIssueLogStore(mocks)
	builds := storagemock.NewMockBuildStore(mocks)
	c := NewListIssueHistoryController(tally.NoopScope, factory, []string{"queue"})
	factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(store, nil).Times(2)
	store.EXPECT().GetIssueLogStore().Return(logs).Times(2)
	store.EXPECT().GetBuildStore().Return(builds).Times(4)
	rows := []entity.IssueLog{
		{TaskID: "task", BuildID: "z", Event: entity.IssueEventOpened, TimestampMs: 100, FirstSeenBuildID: "z"},
		{TaskID: "task", BuildID: "a", Event: entity.IssueEventReported, TimestampMs: 200, FirstSeenBuildID: "z"},
		{TaskID: "task", BuildID: "a", Event: entity.IssueEventClosed, TimestampMs: 300, FirstSeenBuildID: "z"},
		{TaskID: "task", BuildID: "b", Event: entity.IssueEventOpened, TimestampMs: 400, FirstSeenBuildID: "b"},
	}
	logs.EXPECT().List(gomock.Any(), "task", "", storage.IssueLogRange{Limit: 3}).Return(rows[:3], nil)
	builds.EXPECT().Get(gomock.Any(), "z").Return(entity.Build{ID: "z", RequestID: "1"}, nil)
	builds.EXPECT().Get(gomock.Any(), "a").Return(entity.Build{ID: "a", RequestID: "2"}, nil).Times(2)
	builds.EXPECT().Get(gomock.Any(), "b").Return(entity.Build{}, storage.ErrNotFound)
	req := entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", PageSize: 2}
	first, err := c.ListIssueHistory(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{{Log: rows[0], RequestID: "1"}, {Log: rows[1], RequestID: "2"}}, first.Events)
	require.NotEmpty(t, first.NextPageToken)
	logs.EXPECT().List(gomock.Any(), "task", "", storage.IssueLogRange{Limit: 3, After: storage.IssueLogCursor{TimestampMs: 200, BuildID: "a", Event: entity.IssueEventReported}}).Return(rows[2:], nil)
	req.PageToken = first.NextPageToken
	last, err := c.ListIssueHistory(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{{Log: rows[2], RequestID: "2"}, {Log: rows[3]}}, last.Events)
	require.Empty(t, last.NextPageToken)
}

func TestIssueHistoryInvalidRequests(t *testing.T) {
	valid := issueHistoryPageToken{Version: 1, Queue: "queue", TaskID: "task", UnitID: "unit", After: storage.IssueLogCursor{TimestampMs: 1, BuildID: "b", Event: entity.IssueEventOpened}}
	for _, tt := range []struct {
		name   string
		change func(*issueHistoryPageToken)
	}{
		{"queue", func(t *issueHistoryPageToken) { t.Queue = "other" }},
		{"task", func(t *issueHistoryPageToken) { t.TaskID = "other" }},
		{"unit", func(t *issueHistoryPageToken) { t.UnitID = "" }},
		{"version", func(t *issueHistoryPageToken) { t.Version = 0 }},
		{"timestamp", func(t *issueHistoryPageToken) { t.After.TimestampMs = 0 }},
		{"build", func(t *issueHistoryPageToken) { t.After.BuildID = "" }},
		{"oversized build", func(t *issueHistoryPageToken) { t.After.BuildID = strings.Repeat("b", 1021) }},
		{"event", func(t *issueHistoryPageToken) { t.After.Event = "future" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token := valid
			tt.change(&token)
			data, err := json.Marshal(token)
			require.NoError(t, err)
			_, err = resolveIssueHistoryRange(entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", UnitID: "unit", PageToken: base64.RawURLEncoding.EncodeToString(data)})
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
	factory := storagemock.NewMockFactory(gomock.NewController(t))
	c := NewListIssueHistoryController(tally.NoopScope, factory, []string{"queue"})
	for _, req := range []entity.ListIssueHistoryRequest{
		{}, {Queue: "other", TaskID: "task"}, {Queue: "queue"},
		{Queue: "queue", TaskID: strings.Repeat("t", 256)},
		{Queue: "queue", TaskID: "task", UnitID: strings.Repeat("u", 1025)},
		{Queue: "queue", TaskID: "task", PageSize: -1}, {Queue: "queue", TaskID: "task", PageSize: 201},
		{Queue: "queue", TaskID: "task", PageToken: "bad!"},
		{Queue: "queue", TaskID: "task", PageToken: base64.RawURLEncoding.EncodeToString([]byte("not json"))},
		{Queue: "queue", TaskID: "task", PageToken: strings.Repeat("a", maxIssueHistoryPageTokenLength+1)},
	} {
		_, err := c.ListIssueHistory(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
}

func TestIssueHistoryReads(t *testing.T) {
	for _, step := range []string{"empty", "factory", "logs", "build"} {
		t.Run(step, func(t *testing.T) {
			mocks := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mocks)
			store := storagemock.NewMockStorage(mocks)
			logs := storagemock.NewMockIssueLogStore(mocks)
			builds := storagemock.NewMockBuildStore(mocks)
			c := NewListIssueHistoryController(tally.NoopScope, factory, []string{"queue"})
			failAt := func(s string) error {
				if s == step {
					return assert.AnError
				}
				return nil
			}
			factory.EXPECT().For(gomock.Any()).Return(store, failAt("factory"))
			if step != "factory" {
				store.EXPECT().GetIssueLogStore().Return(logs)
				var rows []entity.IssueLog
				if step == "build" {
					rows = []entity.IssueLog{{BuildID: "build"}}
				}
				logs.EXPECT().List(gomock.Any(), "task", "unit", storage.IssueLogRange{Limit: 51}).Return(rows, failAt("logs"))
			}
			if step == "build" {
				store.EXPECT().GetBuildStore().Return(builds)
				builds.EXPECT().Get(gomock.Any(), "build").Return(entity.Build{}, assert.AnError)
			}
			got, err := c.ListIssueHistory(context.Background(), entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", UnitID: "unit"})
			if step == "empty" {
				require.NoError(t, err)
				require.Empty(t, got.Events)
				require.Empty(t, got.NextPageToken)
			} else {
				require.ErrorIs(t, err, assert.AnError)
				require.Empty(t, got.Events)
			}
		})
	}
}

func TestIssueHistoryEscapedIdentityContinuation(t *testing.T) {
	token := issueHistoryPageToken{Version: 1, Queue: strings.Repeat("\x00", 255), TaskID: strings.Repeat("\x00", 255), UnitID: strings.Repeat("\x00", 1024),
		After: storage.IssueLogCursor{TimestampMs: 1, BuildID: strings.Repeat("\x00", 1020), Event: entity.IssueEventOpened}}
	data, err := json.Marshal(token)
	require.NoError(t, err)
	bounds, err := resolveIssueHistoryRange(entity.ListIssueHistoryRequest{Queue: token.Queue, TaskID: token.TaskID, UnitID: token.UnitID, PageToken: base64.RawURLEncoding.EncodeToString(data)})
	require.NoError(t, err)
	require.Equal(t, token.After, bounds.After)
}
