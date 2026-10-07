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

// Package github exercises the GitHub merger extension on its own against a
// real repository on github.com. It opens throwaway branches, pull requests and
// stacks in a test repository, lands them through the merger, and checks
// what GitHub recorded — the one place the merger's assumptions about the
// stacks and async merge APIs meet the real service rather than a fake.
//
// It skips unless a test repository is configured; see test/testutil/githubtestrepo.
package github

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/runway/extension/merger"
	githubmerger "github.com/uber/submitqueue/runway/extension/merger/github"
	"github.com/uber/submitqueue/test/testutil/githubtestrepo"
)

func newMerger(t *testing.T, repo *githubtestrepo.TestRepo, strategy mergestrategypb.Strategy) merger.Merger {
	t.Helper()
	m, err := githubmerger.New(githubmerger.Params{
		HTTPClient:      repo.HTTPClient(),
		Host:            githubtestrepo.Host,
		Owner:           repo.Owner(),
		Repo:            repo.Repo(),
		Target:          repo.Trunk(),
		DefaultStrategy: strategy,
		PollInterval:    time.Second,
		MaxPollDuration: 3 * time.Minute,
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
	})
	require.NoError(t, err)
	return m
}

func mergeRequest(repo *githubtestrepo.TestRepo, strategy mergestrategypb.Strategy, pulls ...githubtestrepo.Pull) *runwaymq.MergeRequest {
	return &runwaymq.MergeRequest{
		Id:        "sq-it-" + repo.RunID(),
		QueueName: "sq-it",
		Steps: []*runwaymq.MergeStep{{
			StepId:   "step-1",
			Change:   &changepb.Change{Uris: repo.URIs(pulls...)},
			Strategy: strategy,
		}},
	}
}

func outputIDs(result *runwaymq.MergeResult) []string {
	var ids []string
	for _, step := range result.GetSteps() {
		for _, o := range step.GetOutputs() {
			ids = append(ids, o.GetId())
		}
	}
	return ids
}

func TestGitHubMergerLive(t *testing.T) {
	ctx := context.Background()

	t.Run("stack lands in one merge and redelivery converges", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		m := newMerger(t, repo, mergestrategypb.Strategy_SQUASH_REBASE)
		pulls := repo.OpenStack("stack", 2)
		req := mergeRequest(repo, mergestrategypb.Strategy_DEFAULT, pulls...)

		check, err := m.CheckMergeability(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, runwaypb.Outcome_SUCCEEDED, check.GetOutcome())

		result, err := m.Merge(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.GetOutcome())

		var want []string
		for _, p := range pulls {
			require.True(t, repo.PullState(p.Number).Merged, "pull request #%d", p.Number)
			want = append(want, repo.MergeCommit(p.Number))
		}
		assert.Equal(t, want, outputIDs(result))

		redelivered, err := m.Merge(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, want, outputIDs(redelivered), "a redelivery reports the same merge commits")
	})

	t.Run("bottom of a stack lands and leaves the rest open", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		m := newMerger(t, repo, mergestrategypb.Strategy_REBASE)
		pulls := repo.OpenStack("partial", 3)
		req := mergeRequest(repo, mergestrategypb.Strategy_REBASE, pulls[0], pulls[1])

		_, err := m.CheckMergeability(ctx, req)
		require.NoError(t, err)
		result, err := m.Merge(ctx, req)
		require.NoError(t, err)
		assert.Len(t, outputIDs(result), 2)

		assert.True(t, repo.PullState(pulls[0].Number).Merged)
		assert.True(t, repo.PullState(pulls[1].Number).Merged)
		top := repo.PullState(pulls[2].Number)
		assert.False(t, top.Merged)
		assert.Equal(t, "open", top.State)
	})

	t.Run("pull requests that are not a stack are rejected", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		m := newMerger(t, repo, mergestrategypb.Strategy_SQUASH_REBASE)
		independent := []githubtestrepo.Pull{repo.OpenPull("independent-1", repo.Trunk()), repo.OpenPull("independent-2", repo.Trunk())}
		chained := repo.OpenChain("chain", 2)

		for name, pulls := range map[string][]githubtestrepo.Pull{"independent on trunk": independent, "chained without a stack": chained} {
			req := mergeRequest(repo, mergestrategypb.Strategy_DEFAULT, pulls...)
			_, err := m.CheckMergeability(ctx, req)
			require.ErrorIs(t, err, merger.ErrInvalidRequest, name)
			_, err = m.Merge(ctx, req)
			require.ErrorIs(t, err, merger.ErrInvalidRequest, name)
			for _, p := range pulls {
				assert.False(t, repo.PullState(p.Number).Merged, "%s: #%d must not merge", name, p.Number)
			}
		}
	})

	t.Run("pull request whose head moved is rejected", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		m := newMerger(t, repo, mergestrategypb.Strategy_SQUASH_REBASE)
		p := repo.OpenPull("stale", repo.Trunk())
		req := mergeRequest(repo, mergestrategypb.Strategy_DEFAULT, p)
		repo.AwaitPullHead(p.Number, repo.CommitFile(p.Branch, repo.FilePath("stale-followup"), "followup"))

		_, err := m.CheckMergeability(ctx, req)
		require.ErrorIs(t, err, merger.ErrInvalidRequest)
		_, err = m.Merge(ctx, req)
		require.ErrorIs(t, err, merger.ErrInvalidRequest)
		assert.False(t, repo.PullState(p.Number).Merged)
	})
}
