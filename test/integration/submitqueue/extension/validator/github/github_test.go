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

// Package github exercises the GitHub validator against a real repository on
// github.com, the one place its GraphQL query and its reading of the response
// meet the real API rather than a fake. The test repository is the one the
// merger lands pull requests in without approvals or required checks, so a
// fresh pull request there must pass.
//
// It skips unless a test repository is configured; see test/testutil/githubtestrepo.
package github

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
	githubvalidator "github.com/uber/submitqueue/submitqueue/extension/validator/github"
	"github.com/uber/submitqueue/test/testutil/githubtestrepo"
)

func newValidator(t *testing.T, repo *githubtestrepo.TestRepo) validator.Validator {
	t.Helper()
	v, err := githubvalidator.New(githubvalidator.Params{
		Config:       validator.Config{QueueName: "sq-it"},
		HTTPClient:   repo.HTTPClient(),
		Host:         githubtestrepo.Host,
		Logger:       zaptest.NewLogger(t).Sugar(),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)
	return v
}

func landRequest(repo *githubtestrepo.TestRepo, uris ...string) entity.Request {
	return entity.Request{ID: "sq-it/" + repo.RunID(), Queue: "sq-it", Change: change.Change{URIs: uris}}
}

func TestGitHubValidator(t *testing.T) {
	ctx := context.Background()

	t.Run("open pull request passes", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		v := newValidator(t, repo)
		p := repo.OpenPull("pass", repo.Trunk())

		assert.NoError(t, v.Validate(ctx, landRequest(repo, repo.URI(p))))
	})

	t.Run("stack passes", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		v := newValidator(t, repo)
		pulls := repo.OpenStack("stack", 2)

		assert.NoError(t, v.Validate(ctx, landRequest(repo, repo.URIs(pulls...)...)))
	})

	t.Run("pull request whose head moved is rejected", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		v := newValidator(t, repo)
		p := repo.OpenPull("stale", repo.Trunk())
		req := landRequest(repo, repo.URI(p))
		// Passing first pins the failure below on the moved head: the query
		// reads this pull request fine, and nothing else changes in between.
		require.NoError(t, v.Validate(ctx, req))
		repo.AwaitPullHead(p.Number, repo.CommitFile(p.Branch, repo.FilePath("stale-followup"), "followup"))

		require.Error(t, v.Validate(ctx, req))
	})

	t.Run("pull request that does not exist fails", func(t *testing.T) {
		repo := githubtestrepo.New(t)
		v := newValidator(t, repo)
		p := repo.OpenPull("missing", repo.Trunk())
		missing := fmt.Sprintf("github://%s/%s/%s/pull/%d/%s", githubtestrepo.Host, repo.Owner(), repo.Repo(), 1<<30, p.Head)

		require.Error(t, v.Validate(ctx, landRequest(repo, missing)))
	})
}
