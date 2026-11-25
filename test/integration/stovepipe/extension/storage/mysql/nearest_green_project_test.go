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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/controller"
	"github.com/uber/submitqueue/stovepipe/core/greenproject"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	sourcemock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
)

func testNearestGreenProject(t *testing.T, ctx context.Context, factory mysqlFactory) {
	t.Helper()
	const queue = "monorepo/main"
	config := sourcecontrol.Config{QueueName: queue}
	trunk := fake.New(config, []string{"C", "B", "A"})
	active := trunk
	sources := sourcemock.NewMockFactory(gomock.NewController(t))
	sources.EXPECT().For(config).DoAndReturn(func(sourcecontrol.Config) (sourcecontrol.SourceControl, error) { return active, nil }).AnyTimes()
	lookup := controller.NewNearestGreenCommitController(tally.NoopScope, factory, sources)
	bound, err := factory.For(storage.Config{QueueName: queue})
	require.NoError(t, err)
	for _, fact := range []entity.ValidationFact{
		{URI: "B", Project: "payments", Degree: entity.DegreeGreen, RequestID: "1", CreatedAt: 1},
		{URI: "B", Project: "search", Degree: entity.DegreeBroken, RequestID: "1", CreatedAt: 1},
		{URI: "B", Degree: entity.DegreePartial, RequestID: "1", CreatedAt: 1},
		{URI: "C", Project: "payments", Degree: entity.DegreeGreen, RequestID: "2", CreatedAt: 1},
		{URI: "C", Project: "search", Degree: entity.DegreeGreen, RequestID: "2", CreatedAt: 1},
	} {
		require.NoError(t, bound.GetValidationFactStore().Create(ctx, fact))
	}
	find := func(desired, project string) (entity.NearestGreenCommitResult, error) {
		return lookup.NearestGreenCommitThatIncludes(ctx, entity.NearestGreenCommitRequest{Queue: queue, DesiredCommitURI: desired, Project: project})
	}
	// Historical facts alone don't make the forward-only lookup return a match.
	result, err := find("A", "payments")
	require.NoError(t, err)
	require.Empty(t, result)
	// Index later heads first to exercise trunk order rather than write order.
	require.NoError(t, greenproject.IndexGreenProjectsForCommit(ctx, sources, bound, queue, "C", []string{"payments", "search"}))
	require.NoError(t, greenproject.IndexGreenProjectsForCommit(ctx, sources, bound, queue, "B", []string{"payments"}))
	require.NoError(t, greenproject.IndexGreenProjectsForCommit(ctx, sources, bound, queue, "B", []string{"payments"}))

	for _, tc := range []struct{ desired, project, head, requestID string }{
		{"A", "payments", "B", "1"},
		{"A", "search", "C", "2"},
		{"C", "payments", "C", "2"},
		{"B", "payments", "B", "1"},
		{"A", "billing", "", ""},
	} {
		result, err := find(tc.desired, tc.project)
		require.NoError(t, err)
		require.Equal(t, entity.NearestGreenCommitResult{CommitURI: tc.head, RequestID: tc.requestID}, result, "from %s for %s", tc.desired, tc.project)
	}
	position, err := trunk.TrunkPosition(ctx, "A")
	require.NoError(t, err)
	other, err := factory.For(storage.Config{QueueName: "another/main"})
	require.NoError(t, err)
	otherProjection := other.GetGreenProjectStore()
	uri, err := otherProjection.FindAtOrAfter(ctx, position.Lineage, "payments", position.Position)
	require.NoError(t, err)
	require.Empty(t, uri)

	// A rewrite starts an empty index, even for commits in the retained prefix.
	active = fake.New(config, []string{"D", "B", "A"})
	for _, project := range []string{"payments", "search"} {
		result, err := find("A", project)
		require.NoError(t, err)
		require.Empty(t, result)
	}
	_, err = find("C", "search")
	require.ErrorIs(t, err, sourcecontrol.ErrNotFound)

	// A new validation on the rewritten trunk becomes queryable immediately.
	require.NoError(t, bound.GetValidationFactStore().Create(ctx, entity.ValidationFact{
		URI: "D", Project: "search", Degree: entity.DegreeGreen, RequestID: "3", CreatedAt: 2,
	}))
	require.NoError(t, greenproject.IndexGreenProjectsForCommit(ctx, sources, bound, queue, "D", []string{"search"}))
	result, err = find("A", "search")
	require.NoError(t, err)
	require.Equal(t, entity.NearestGreenCommitResult{CommitURI: "D", RequestID: "3"}, result)
}
