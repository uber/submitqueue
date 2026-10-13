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

// End-to-end coverage of landing real pull requests on github.com.
//
// The git suite (git_suite_test.go) proves the merge machinery against a bare
// repository and deliberately leaves out the half that is specific to a change
// provider. This suite covers that half: the orchestrator reads change metadata
// from GitHub, and Runway lands each change through the GitHub merger, so a
// request reaching `landed` here means GitHub itself merged the pull requests.
// Builds stay fake — what is under test is the land path, not CI.
//
// It needs a test repository and a token with write access to it, and skips
// without them; see test/testutil/githubtestrepo.
package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/test/testutil"
	"github.com/uber/submitqueue/test/testutil/githubtestrepo"
)

// githubQueue is the queue the generated provider configuration wires to
// GitHub; it is also declared in the gateway's queues.yaml.
const githubQueue = "e2e-github-queue"

type GitHubLandSuite struct {
	suite.Suite
	ctx           context.Context
	log           *testutil.TestLogger
	githubRepo    *githubtestrepo.TestRepo
	stack         *testutil.ComposeStack
	gatewayClient gatewaypb.SubmitQueueGatewayClient
}

func TestGitHubLandE2E(t *testing.T) {
	suite.Run(t, new(GitHubLandSuite))
}

func (s *GitHubLandSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)
	// Skips the suite before any container starts when no test repository is configured.
	s.githubRepo = githubtestrepo.New(t)

	t.Setenv("SQ_CONTAINER_USER", dockerContainerUser(t))
	t.Setenv("SQ_CONSUMER_GATE_DIR", t.TempDir())
	t.Setenv("SQ_PROVIDER_CONFIG_DIR", s.writeProviderConfig())
	t.Setenv("GITHUB_TOKEN", s.githubRepo.Token())

	composeFile := testutil.Runfile("service/submitqueue/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-submitqueue-github",
		testutil.WithOverlay(testutil.Runfile("service/submitqueue/docker-compose.provider.yml")),
		testutil.WithBuildContext(map[string]string{
			".docker-bin/gateway":                                "service/submitqueue/gateway/server/gateway_linux",
			".docker-bin/orchestrator":                           "service/submitqueue/orchestrator/server/orchestrator_linux",
			".docker-bin/runway":                                 "service/runway/server/runway_linux",
			"service/submitqueue/gateway/server/Dockerfile":      "service/submitqueue/gateway/server/Dockerfile",
			"service/submitqueue/gateway/server/queues.yaml":     "service/submitqueue/gateway/server/queues.yaml",
			"service/submitqueue/orchestrator/server/Dockerfile": "service/submitqueue/orchestrator/server/Dockerfile",
			"service/runway/server/Dockerfile":                   "service/runway/server/Dockerfile",
		}))
	require.NoError(t, s.stack.Up(), "failed to start compose stack")

	db, err := s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	queueDB, err := s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err)
	t.Cleanup(func() { queueDB.Close() })
	testutil.ApplySubmitQueueStorageSchema(t, s.log, db)
	testutil.ApplySchema(t, s.log, db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))
	testutil.ApplySchema(t, s.log, queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	conn, err := s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err)
	s.gatewayClient = gatewaypb.NewSubmitQueueGatewayClient(conn)
	s.log.Logf("github E2E suite ready (repository %s/%s)", s.githubRepo.Owner(), s.githubRepo.Repo())
}

func (s *GitHubLandSuite) TestLand_SinglePullRequest_IsMergedOnGitHub() {
	p := s.githubRepo.OpenPull("e2e-single", s.githubRepo.Trunk())

	s.requireStatus(s.land(p), entity.RequestStatusLanded)

	s.True(s.githubRepo.PullState(p.Number).Merged, "pull request #%d must be merged", p.Number)
	s.True(s.githubRepo.FileOnTrunk(s.githubRepo.FilePath("e2e-single")), "its change must be on the trunk")
}

func (s *GitHubLandSuite) TestLand_Stack_IsMergedOnGitHub() {
	pulls := s.githubRepo.OpenStack("e2e-stack", 2)

	s.requireStatus(s.land(pulls...), entity.RequestStatusLanded)

	for i, p := range pulls {
		s.True(s.githubRepo.PullState(p.Number).Merged, "pull request #%d must be merged", p.Number)
		s.True(s.githubRepo.FileOnTrunk(s.githubRepo.FilePath(fmt.Sprintf("e2e-stack-%d", i+1))))
	}
}

func (s *GitHubLandSuite) TestLand_PullRequestsThatAreNotAStack_AreRejected() {
	pulls := []githubtestrepo.Pull{
		s.githubRepo.OpenPull("e2e-independent-1", s.githubRepo.Trunk()),
		s.githubRepo.OpenPull("e2e-independent-2", s.githubRepo.Trunk()),
	}

	s.requireStatus(s.land(pulls...), entity.RequestStatusError)

	for _, p := range pulls {
		s.False(s.githubRepo.PullState(p.Number).Merged, "pull request #%d must not merge", p.Number)
	}
}

// land submits the pull requests as one change, bottom of the stack first, and
// returns the request's sqid.
func (s *GitHubLandSuite) land(pulls ...githubtestrepo.Pull) string {
	resp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue:    githubQueue,
		Change:   &changepb.Change{Uris: s.githubRepo.URIs(pulls...)},
		Strategy: mergestrategypb.Strategy_SQUASH_REBASE,
	})
	s.Require().NoError(err, "Land failed")
	s.Require().NotEmpty(resp.Sqid)
	return resp.Sqid
}

// requireStatus waits for the request to reach a terminal status and asserts
// which one. Bazel's test timeout is the only deadline.
func (s *GitHubLandSuite) requireStatus(sqid string, want entity.RequestStatus) {
	var got entity.RequestStatus
	pollUntil(persistPollInterval, func() bool {
		resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: sqid, Queue: githubQueue})
		if err != nil || resp.Request == nil {
			return false
		}
		got = entity.RequestStatus(resp.Request.Status)
		s.log.Logf("request %s status=%q (awaiting terminal)", sqid, got)
		return isTerminalStatus(got)
	})
	s.Require().Equal(want, got, "request %s reached the wrong terminal status", sqid)
}

// writeProviderConfig writes the orchestrator profiles and Runway merge targets
// for githubQueue, pointed at the configured test repository, and returns their
// directory. Generated rather than committed because the test repository is
// itself configuration.
func (s *GitHubLandSuite) writeProviderConfig() string {
	t := s.T()
	dir := t.TempDir()
	profiles := fmt.Sprintf(`defaults:
  changeProvider: {type: fake}
  buildRunner: {type: fake}
  analyzer: {type: all}
queues:
  - name: %s
    changeProvider: {type: github}
    analyzer: {type: none}
`, githubQueue)
	merge := fmt.Sprintf(`defaults:
  merger: {type: noop}
queues:
  - name: %s
    merger:
      type: github
      owner: %s
      repo: %s
      target: %s
      defaultStrategy: SQUASH_REBASE
      tokenEnv: GITHUB_TOKEN
      pollInterval: 1s
`, githubQueue, s.githubRepo.Owner(), s.githubRepo.Repo(), s.githubRepo.Trunk())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "profiles.yaml"), []byte(profiles), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "merge.yaml"), []byte(merge), 0o644))
	return dir
}
