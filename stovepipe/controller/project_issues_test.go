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
	"fmt"
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

var attributionScopes = []projectIssueScope{
	{Queue: "queue", RequestID: "7", TaskID: "test", UnitID: "unit"},
	{Queue: "queue", RequestID: "7", Project: "app"},
}

type projectIssuesFixture struct {
	controller ProjectIssuesController
	factory    *storagemock.MockFactory
	stores     *storagemock.MockStorage
	history    *storagemock.MockRequestLogStore
	source     *storagemock.MockRequestProjectsByIssueStore
	reverse    *storagemock.MockRequestIssuesByProjectStore
}

func newProjectIssuesFixture(t *testing.T) projectIssuesFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := projectIssuesFixture{
		factory: storagemock.NewMockFactory(ctrl), stores: storagemock.NewMockStorage(ctrl),
		history: storagemock.NewMockRequestLogStore(ctrl), source: storagemock.NewMockRequestProjectsByIssueStore(ctrl),
		reverse: storagemock.NewMockRequestIssuesByProjectStore(ctrl),
	}
	f.controller = NewProjectIssuesController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, []string{"queue", "other"})
	f.stores.EXPECT().GetRequestLogStore().Return(f.history).AnyTimes()
	f.stores.EXPECT().GetRequestProjectsByIssueStore().Return(f.source).AnyTimes()
	f.stores.EXPECT().GetRequestIssuesByProjectStore().Return(f.reverse).AnyTimes()
	return f
}

func (f projectIssuesFixture) list(scope projectIssueScope, size int32, token string) (entity.ListRequestProjectIssuesResult, error) {
	if scope.Project != "" {
		return f.controller.ListRequestIssuesByProject(context.Background(), entity.ListRequestIssuesByProjectRequest{
			Queue: scope.Queue, RequestID: scope.RequestID, Project: scope.Project, PageSize: size, PageToken: token,
		})
	}
	return f.controller.ListRequestProjectsByIssue(context.Background(), entity.ListRequestProjectsByIssueRequest{
		Queue: scope.Queue, RequestID: scope.RequestID, TaskID: scope.TaskID, UnitID: scope.UnitID, PageSize: size, PageToken: token,
	})
}

func (f projectIssuesFixture) expectHistory(scope projectIssueScope, history []entity.RequestLog) {
	f.factory.EXPECT().For(storage.Config{QueueName: scope.Queue}).Return(f.stores, nil)
	f.history.EXPECT().List(gomock.Any(), scope.RequestID).Return(history, nil)
}

func (f projectIssuesFixture) expectPage(scope projectIssueScope, after entity.ProjectIssueAttribution, limit int, rows []entity.ProjectIssueAttribution, err error) {
	f.expectHistory(scope, []entity.RequestLog{{Event: entity.RequestEventProjectIssuesRecorded}})
	if scope.Project != "" {
		f.reverse.EXPECT().List(gomock.Any(), scope.RequestID, scope.Project, after, limit).Return(rows, err)
	} else {
		f.source.EXPECT().ListByIssue(gomock.Any(), scope.RequestID, scope.TaskID, scope.UnitID, after.Project, limit).Return(rows, err)
	}
}

func TestProjectIssuesPages(t *testing.T) {
	for _, scope := range attributionScopes {
		for count := 0; count <= 3; count++ {
			t.Run(fmt.Sprintf("%s/%s/%d", scope.TaskID, scope.Project, count), func(t *testing.T) {
				f := newProjectIssuesFixture(t)
				rows := make([]entity.ProjectIssueAttribution, 3)
				for i := range rows {
					rows[i] = entity.ProjectIssueAttribution{TaskID: scope.TaskID, UnitID: scope.UnitID, Project: string(rune('a' + i))}
					if scope.Project != "" {
						rows[i] = entity.ProjectIssueAttribution{TaskID: string(rune('a' + i)), UnitID: "unit", Project: scope.Project}
					}
				}
				f.expectPage(scope, entity.ProjectIssueAttribution{}, 3, rows[:count], nil)
				page, err := f.list(scope, 2, "")
				require.NoError(t, err)
				require.True(t, page.AttributionComplete)
				require.Equal(t, rows[:min(count, 2)], page.Attributions)
				if count <= 2 {
					require.Empty(t, page.NextPageToken)
					return
				}
				require.NotEmpty(t, page.NextPageToken)
				f.expectPage(scope, rows[1], 2, rows[2:], nil)
				last, err := f.list(scope, 1, page.NextPageToken)
				require.NoError(t, err)
				require.Equal(t, rows[2:], last.Attributions)
				require.Empty(t, last.NextPageToken)
				require.True(t, last.AttributionComplete)
			})
		}
	}
}

func TestProjectIssuesWaitsForCompletion(t *testing.T) {
	for _, scope := range attributionScopes {
		for _, event := range []entity.RequestEvent{
			entity.RequestEventUnknown, entity.RequestEventBuildAnalysisCompleted,
			entity.RequestEventProjectFactsRecorded, entity.RequestEventRecordAbandoned,
		} {
			t.Run(fmt.Sprintf("%s/%s/%s", scope.TaskID, scope.Project, event), func(t *testing.T) {
				f := newProjectIssuesFixture(t)
				f.expectHistory(scope, []entity.RequestLog{{Event: event}})
				got, err := f.list(scope, 1, "")
				require.NoError(t, err)
				require.Equal(t, entity.ListRequestProjectIssuesResult{}, got)
			})
		}
	}
}

func TestProjectIssuesDefaultPageSize(t *testing.T) {
	for _, scope := range attributionScopes {
		f := newProjectIssuesFixture(t)
		f.expectPage(scope, entity.ProjectIssueAttribution{}, 51, nil, nil)
		got, err := f.list(scope, 0, "")
		require.NoError(t, err)
		require.True(t, got.AttributionComplete)
		require.Empty(t, got.NextPageToken)
	}
}

func TestProjectIssuesFailures(t *testing.T) {
	for _, scope := range attributionScopes {
		for _, step := range []string{"factory", "history", "page", "missing request", "cancelled"} {
			t.Run(fmt.Sprintf("%s/%s/%s", scope.TaskID, scope.Project, step), func(t *testing.T) {
				f := newProjectIssuesFixture(t)
				want := assert.AnError
				switch step {
				case "factory":
					f.factory.EXPECT().For(storage.Config{QueueName: scope.Queue}).Return(nil, want)
				case "page":
					f.expectPage(scope, entity.ProjectIssueAttribution{}, 3, nil, want)
				default:
					if step == "missing request" {
						want = storage.ErrNotFound
					}
					if step == "cancelled" {
						want = context.Canceled
					}
					f.factory.EXPECT().For(storage.Config{QueueName: scope.Queue}).Return(f.stores, nil)
					f.history.EXPECT().List(gomock.Any(), scope.RequestID).Return(nil, want)
				}
				got, err := f.list(scope, 2, "")
				require.ErrorIs(t, err, want)
				require.Empty(t, got)
			})
		}
	}
}

func TestProjectIssuesInvalidRequests(t *testing.T) {
	for _, scope := range attributionScopes {
		for _, tt := range []struct {
			name   string
			change func(*projectIssueScope)
			size   int32
		}{
			{name: "empty queue", change: func(s *projectIssueScope) { s.Queue = "" }},
			{name: "unknown queue", change: func(s *projectIssueScope) { s.Queue = "unknown" }},
			{name: "long queue", change: func(s *projectIssueScope) { s.Queue = strings.Repeat("q", 256) }},
			{name: "empty request", change: func(s *projectIssueScope) { s.RequestID = "" }},
			{name: "noncanonical request", change: func(s *projectIssueScope) { s.RequestID = "07" }},
			{name: "invalid request", change: func(s *projectIssueScope) { s.RequestID = "request" }},
			{name: "negative page", size: -1}, {name: "oversize page", size: 201},
		} {
			t.Run(fmt.Sprintf("%s/%s/%s", scope.TaskID, scope.Project, tt.name), func(t *testing.T) {
				f := newProjectIssuesFixture(t)
				query := scope
				if tt.change != nil {
					tt.change(&query)
				}
				_, err := f.list(query, tt.size, "")
				require.ErrorIs(t, err, ErrInvalidRequest)
			})
		}
	}
	for _, req := range []entity.ListRequestProjectsByIssueRequest{
		{Queue: "queue", RequestID: "7", UnitID: "unit"},
		{Queue: "queue", RequestID: "7", TaskID: "test"},
		{Queue: "queue", RequestID: "7", TaskID: strings.Repeat("t", 256), UnitID: "unit"},
		{Queue: "queue", RequestID: "7", TaskID: "test", UnitID: strings.Repeat("u", 1025)},
	} {
		f := newProjectIssuesFixture(t)
		_, err := f.controller.ListRequestProjectsByIssue(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
	for _, project := range []string{"", strings.Repeat("p", 1021)} {
		f := newProjectIssuesFixture(t)
		_, err := f.controller.ListRequestIssuesByProject(context.Background(), entity.ListRequestIssuesByProjectRequest{Queue: "queue", RequestID: "7", Project: project})
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
}

func TestProjectIssuesRejectsInvalidTokens(t *testing.T) {
	for _, scope := range attributionScopes {
		after := entity.ProjectIssueAttribution{TaskID: scope.TaskID, UnitID: scope.UnitID, Project: "a"}
		if scope.Project != "" {
			after = entity.ProjectIssueAttribution{TaskID: "a", UnitID: "unit", Project: scope.Project}
		}
		for _, tt := range []struct {
			name   string
			change func(*projectIssuesPageToken)
		}{
			{"version", func(p *projectIssuesPageToken) { p.Version++ }},
			{"queue", func(p *projectIssuesPageToken) { p.Scope.Queue = "other" }},
			{"request", func(p *projectIssuesPageToken) { p.Scope.RequestID = "8" }},
			{"task", func(p *projectIssuesPageToken) { p.Scope.TaskID += "other" }},
			{"unit", func(p *projectIssuesPageToken) { p.Scope.UnitID += "other" }},
			{"project", func(p *projectIssuesPageToken) { p.Scope.Project += "other" }},
			{"direction", func(p *projectIssuesPageToken) {
				p.Scope = attributionScopes[0]
				if scope.Project == "" {
					p.Scope = attributionScopes[1]
				}
			}},
			{"empty cursor", func(p *projectIssuesPageToken) { p.After = entity.ProjectIssueAttribution{} }},
			{"long cursor", func(p *projectIssuesPageToken) { p.After.Project = strings.Repeat("p", 1021) }},
			{"cursor scope", func(p *projectIssuesPageToken) {
				if scope.Project != "" {
					p.After.Project = "other"
				} else {
					p.After.UnitID = "other"
				}
			}},
		} {
			t.Run(fmt.Sprintf("%s/%s/%s", scope.TaskID, scope.Project, tt.name), func(t *testing.T) {
				token := projectIssuesPageToken{Version: 1, Scope: scope, After: after}
				tt.change(&token)
				contents, err := json.Marshal(token)
				require.NoError(t, err)
				f := newProjectIssuesFixture(t)
				_, err = f.list(scope, 1, base64.RawURLEncoding.EncodeToString(contents))
				require.ErrorIs(t, err, ErrInvalidRequest)
			})
		}
		for _, token := range []string{"not base64!", base64.RawURLEncoding.EncodeToString([]byte("not json"))} {
			f := newProjectIssuesFixture(t)
			_, err := f.list(scope, 1, token)
			require.ErrorIs(t, err, ErrInvalidRequest)
		}
	}
}
