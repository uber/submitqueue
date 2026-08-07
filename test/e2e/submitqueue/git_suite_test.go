// Copyright (c) 2025 Uber Technologies, Inc.
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

// Hermetic end-to-end coverage of a *real* merge.
//
// The tier-1 suite (suite_test.go) runs Runway on the noop merger, so "landed"
// there proves the pipeline's choreography and nothing about git. This suite
// points Runway at a bare repository on a shared volume and asserts against the
// repository itself: what reached the target branch, in what order, and in how
// many ref updates.
//
// It needs no credential, no network, and no account anywhere, because none of
// that is what the merge machinery depends on — which is what lets these
// assertions gate a pull request. What it deliberately cannot cover is the half
// that is specific to a change provider: reading change metadata, that
// provider's CI, and a real change being marked merged. That is the manual
// tier; see doc/howto/PROVIDER-E2E.md.
package e2e_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

// gitQueue is the queue wired to the git merger in
// service/submitqueue/demo/provider/local/merge.yaml.
const gitQueue = "e2e-git-queue"

// sandboxRemote is the host name used in git:// change URIs. The merger reads
// the commit and ref out of the URI and reaches the repository through its own
// configured remote, so this identifies the change rather than routing to it.
const sandboxRemote = "git.example.com"

type GitMergeSuite struct {
	suite.Suite
	ctx           context.Context
	log           *testutil.TestLogger
	stack         *testutil.ComposeStack
	gatewayClient gatewaypb.SubmitQueueGatewayClient
	db            *sql.DB
	queueDB       *sql.DB

	// git is the pinned git the test drives the bare repository with — the same
	// build the merger uses inside the container.
	git string
	// bare is the host path of the repository Runway merges into.
	bare string
	// work is a clone the test authors changes in, standing in for a developer.
	work string
}

func TestGitMergeE2E(t *testing.T) {
	suite.Run(t, new(GitMergeSuite))
}

func (s *GitMergeSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.git = pinnedGit(t)
	containerUser := dockerContainerUser(t)
	t.Setenv("SQ_CONTAINER_USER", containerUser)
	t.Setenv("SQ_CONSUMER_GATE_DIR", t.TempDir())

	// The bare repository lives in a host directory bind-mounted into Runway, so
	// the test can seed it and read back exactly what the merger pushed.
	sandbox := t.TempDir()
	s.bare = filepath.Join(sandbox, "sandbox.git")
	s.work = filepath.Join(t.TempDir(), "work")
	t.Setenv("SQ_GIT_SANDBOX_DIR", sandbox)
	// Runway provisions its working trees here. Mounted rather than left to the
	// image because the service runs as the host user (so its bind mounts stay
	// readable from the host), which cannot create directories under /var.
	t.Setenv("SQ_RUNWAY_CHECKOUT_DIR", t.TempDir())
	s.seedRepository()

	// The committed example configuration is what selects the git merger, so
	// the suite runs the real file rather than generating one. Runfiles are
	// symlinks into the execroot, which a container cannot follow through a
	// bind mount, so the files are staged as real copies first.
	t.Setenv("SQ_PROVIDER_CONFIG_DIR", s.stageProviderConfig())

	// The base stack plus an overlay naming only what differs: Runway's merge
	// target. Sharing the base file means a service added there reaches this
	// suite too, which a copied compose file would silently miss.
	composeFile := testutil.Runfile("service/submitqueue/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-submitqueue-git",
		testutil.WithOverlay(testutil.Runfile("service/submitqueue/docker-compose.git.yml")),
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

	var err error
	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err)
	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err)

	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err)
	s.gatewayClient = gatewaypb.NewSubmitQueueGatewayClient(conn)

	s.log.Logf("git merge E2E suite ready (bare repo at %s)", s.bare)
}

func (s *GitMergeSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
	if s.queueDB != nil {
		s.queueDB.Close()
	}
}

// --- assertions against the repository itself ---

func (s *GitMergeSuite) TestLand_SingleChange_ReachesTheTargetBranch() {
	before := s.mainSHA()
	head := s.pushChange("feature/single", map[string]string{"single.txt": "single\n"}, "add single")

	sqid := s.land(gitQueue, s.uri("feature/single", head))
	s.requireStatus(sqid, entity.RequestStatusLanded)

	// The commit on the target is a *new* object: REBASE replays the change
	// rather than moving the target to it.
	s.NotEqual(before, s.mainSHA())
	s.NotEqual(head, s.mainSHA())
	s.Equal([]string{"add single"}, s.subjectsSince(before))
	s.Equal("single\n", s.fileOnMain("single.txt"))
}

func (s *GitMergeSuite) TestLand_Stack_LandsInOrderInOneRefUpdate() {
	// The property that distinguishes a submit queue from merging changes one
	// at a time: a stack reaches the target as a single atomic ref update, so
	// no reader ever observes it half-landed.
	before := s.mainSHA()
	updatesBefore := s.mainRefUpdateCount()

	first := s.pushChange("feature/stack-1", map[string]string{"one.txt": "one\n"}, "add one")
	second := s.pushChangeOnto(first, "feature/stack-2", map[string]string{"two.txt": "two\n"}, "add two")
	third := s.pushChangeOnto(second, "feature/stack-3", map[string]string{"three.txt": "three\n"}, "add three")

	sqid := s.land(gitQueue,
		s.uri("feature/stack-1", first),
		s.uri("feature/stack-2", second),
		s.uri("feature/stack-3", third),
	)
	s.requireStatus(sqid, entity.RequestStatusLanded)

	s.Equal([]string{"add one", "add two", "add three"}, s.subjectsSince(before),
		"the stack must land in the order it was submitted")
	s.Equal(1, s.mainRefUpdateCount()-updatesBefore,
		"the whole stack must reach the target in exactly one ref update")
}

func (s *GitMergeSuite) TestLand_MovesEachChangeHeadBranchToItsLandedCommit() {
	// What makes a provider mark a rebased change merged: its head branch is moved
	// to the commit the change became, so the head is reachable from the target.
	before := s.mainSHA()
	first := s.pushChange("feature/head-1", map[string]string{"h1.txt": "h1\n"}, "add h1")
	second := s.pushChange("feature/head-2", map[string]string{"h2.txt": "h2\n"}, "add h2")

	sqid := s.land(gitQueue, s.uri("feature/head-1", first), s.uri("feature/head-2", second))
	s.requireStatus(sqid, entity.RequestStatusLanded)

	landed := s.shasSince(before)
	s.Require().Len(landed, 2)

	// Each change gets its own landed commit, not the final tip.
	s.Equal(landed[0], s.branchSHA("feature/head-1"))
	s.Equal(landed[1], s.branchSHA("feature/head-2"))
	s.NotEqual(s.branchSHA("feature/head-1"), s.branchSHA("feature/head-2"))

	// Reachability is the property a provider actually reads.
	s.True(s.isAncestorOfMain(s.branchSHA("feature/head-1")))
	s.True(s.isAncestorOfMain(s.branchSHA("feature/head-2")))
}

func (s *GitMergeSuite) TestLand_Conflict_FailsAndLeavesTheTargetUntouched() {
	// Two changes editing the same line from the same base: the first lands,
	// the second cannot be replayed onto it.
	base := s.mainSHA()
	winner := s.pushChangeOnto(base, "feature/conflict-a", map[string]string{"contested.txt": "alpha\n"}, "contest alpha")
	loser := s.pushChangeOnto(base, "feature/conflict-b", map[string]string{"contested.txt": "beta\n"}, "contest beta")

	s.requireStatus(s.land(gitQueue, s.uri("feature/conflict-a", winner)), entity.RequestStatusLanded)
	afterWinner := s.mainSHA()

	s.requireStatus(s.land(gitQueue, s.uri("feature/conflict-b", loser)), entity.RequestStatusError)
	s.Equal(afterWinner, s.mainSHA(), "a conflicting change must not move the target")
	s.Equal("alpha\n", s.fileOnMain("contested.txt"))

	// The change is left exactly as its author pushed it.
	s.Equal(loser, s.branchSHA("feature/conflict-b"))
}

func (s *GitMergeSuite) TestLand_ResubmittedAfterLanding_IsRejectedAsStale() {
	// Landing a change moves its head branch to the commit it became, so the
	// URI that was submitted no longer describes where that branch points. The
	// staleness check catches exactly that, which is what stops a change from
	// being replayed onto the target a second time.
	head := s.pushChange("feature/already", map[string]string{"already.txt": "already\n"}, "add already")
	s.requireStatus(s.land(gitQueue, s.uri("feature/already", head)), entity.RequestStatusLanded)

	settled := s.mainSHA()
	updates := s.mainRefUpdateCount()
	landedAs := s.branchSHA("feature/already")
	s.NotEqual(head, landedAs, "the head branch moved to the landed commit")

	s.requireStatus(s.land(gitQueue, s.uri("feature/already", head)), entity.RequestStatusError)
	s.Equal(settled, s.mainSHA(), "a stale resubmission must not move the target")
	s.Equal(updates, s.mainRefUpdateCount(), "and must not push at all")
	s.Equal(landedAs, s.branchSHA("feature/already"), "nor disturb the change's branch")
}

// --- gateway helpers ---

// land submits a request and returns its sqid. Repeated URIs are the stack, in
// the order they must be applied.
func (s *GitMergeSuite) land(queue string, uris ...string) string {
	resp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue:    queue,
		Change:   &changepb.Change{Uris: uris},
		Strategy: mergestrategypb.Strategy_REBASE,
	})
	s.Require().NoError(err, "Land failed for queue %s", queue)
	s.Require().NotEmpty(resp.Sqid)
	return resp.Sqid
}

// requireStatus waits for the request to reach a terminal status and asserts
// which one. Bazel's test timeout is the only deadline.
func (s *GitMergeSuite) requireStatus(sqid string, want entity.RequestStatus) {
	var got entity.RequestStatus
	pollUntil(persistPollInterval, func() bool {
		resp, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: sqid, Queue: gitQueue})
		if err != nil || resp.Request == nil {
			return false
		}
		got = entity.RequestStatus(resp.Request.Status)
		s.log.Logf("request %s status=%q (awaiting terminal)", sqid, got)
		return isTerminalStatus(got)
	})
	s.Require().Equal(want, got, "request %s reached the wrong terminal status", sqid)
}

// uri builds the git:// change URI for a branch pinned at a commit. The ref is
// percent-encoded so a branch name containing slashes stays one path segment.
func (s *GitMergeSuite) uri(branch, sha string) string {
	ref := "refs/heads/" + branch
	return fmt.Sprintf("git://%s/sandbox/%s/%s", sandboxRemote, url.PathEscape(ref), sha)
}

// --- repository helpers ---

// stageProviderConfig copies the committed example configuration into a directory
// the containers can bind-mount, and returns its path.
func (s *GitMergeSuite) stageProviderConfig() string {
	t := s.T()
	staged := t.TempDir()
	for _, name := range []string{"merge.yaml", "profiles.yaml"} {
		contents, err := os.ReadFile(testutil.Runfile("service/submitqueue/demo/provider/local/" + name))
		require.NoError(t, err, "reading example config %s", name)
		require.NoError(t, os.WriteFile(filepath.Join(staged, name), contents, 0o644))
	}
	return staged
}

// pinnedGit resolves the git build the test target supplies, rather than
// whatever git the host has. The assertions here depend on repository
// mechanics, and an ambient git brings ambient configuration — hooks, signing,
// templates — with it.
//
// rules_go expands $(location) to an execroot-relative path, which for an
// external output has to be re-rooted under the runfiles directory the test
// runs from.
func pinnedGit(t *testing.T) string {
	t.Helper()
	path := os.Getenv("SUBMITQUEUE_TEST_GIT")
	require.NotEmpty(t, path, "SUBMITQUEUE_TEST_GIT must be set by the test target")

	if absolute, err := filepath.Abs(path); err == nil {
		if _, err := os.Stat(absolute); err == nil {
			return absolute
		}
	}

	slashed := filepath.ToSlash(path)
	external := ""
	if strings.HasPrefix(slashed, "external/") {
		external = strings.TrimPrefix(slashed, "external/")
	} else if i := strings.Index(slashed, "/external/"); i >= 0 {
		external = slashed[i+len("/external/"):]
	}
	require.NotEmpty(t, external, "SUBMITQUEUE_TEST_GIT=%q is not a runfile", path)

	root := os.Getenv("TEST_SRCDIR")
	require.NotEmpty(t, root)
	candidate := filepath.Join(root, filepath.FromSlash(external))
	_, err := os.Stat(candidate)
	require.NoError(t, err)
	return candidate
}

// seedRepository creates the bare repository Runway merges into, plus a working
// clone the test authors changes in.
func (s *GitMergeSuite) seedRepository() {
	t := s.T()
	s.runGit(filepath.Dir(s.bare), "init", "--bare", "-b", "main", s.bare)
	// Bare repositories do not log ref updates by default, and the reflog is
	// how this suite counts pushes to prove a stack lands atomically.
	s.runGit(s.bare, "config", "core.logAllRefUpdates", "true")

	s.runGit(filepath.Dir(s.work), "clone", s.bare, s.work)
	s.configureWorkClone()

	require.NoError(t, os.WriteFile(filepath.Join(s.work, "seed.txt"), []byte("seed\n"), 0o644))
	s.runGit(s.work, "add", ".")
	s.runGit(s.work, "commit", "-m", "seed")
	s.runGit(s.work, "push", "origin", "main")
}

func (s *GitMergeSuite) configureWorkClone() {
	for _, kv := range [][2]string{
		{"user.name", "E2E Author"},
		{"user.email", "author@example.com"},
		{"commit.gpgsign", "false"},
	} {
		s.runGit(s.work, "config", kv[0], kv[1])
	}
	// Neutralize any system-installed hooks that would reject test commits.
	hooks := filepath.Join(s.work, ".no-hooks")
	require.NoError(s.T(), os.MkdirAll(hooks, 0o755))
	s.runGit(s.work, "config", "core.hooksPath", hooks)
}

// pushChange authors a change branched off the current target tip and pushes
// it, returning its head SHA — all a change URI ever carries.
func (s *GitMergeSuite) pushChange(branch string, files map[string]string, message string) string {
	return s.pushChangeOnto("origin/main", branch, files, message)
}

// pushChangeOnto is pushChange based at an explicit start point, for building a
// change that stacks on another rather than on the target.
func (s *GitMergeSuite) pushChangeOnto(base, branch string, files map[string]string, message string) string {
	s.runGit(s.work, "fetch", "origin")
	s.runGit(s.work, "checkout", "-B", branch, base)
	for path, contents := range files {
		require.NoError(s.T(), os.WriteFile(filepath.Join(s.work, path), []byte(contents), 0o644))
	}
	s.runGit(s.work, "add", ".")
	s.runGit(s.work, "commit", "-m", message)
	s.runGit(s.work, "push", "-f", "origin", branch)
	return s.runGit(s.work, "rev-parse", "HEAD")
}

// mainSHA is the current tip of the target branch on the bare repository.
func (s *GitMergeSuite) mainSHA() string {
	return s.runGit(s.bare, "rev-parse", "refs/heads/main")
}

// branchSHA is the current tip of a change's head branch.
func (s *GitMergeSuite) branchSHA(branch string) string {
	return s.runGit(s.bare, "rev-parse", "refs/heads/"+branch)
}

// shasSince lists the commits added to the target since a known point, oldest
// first.
func (s *GitMergeSuite) shasSince(since string) []string {
	out := s.runGit(s.bare, "rev-list", "--reverse", since+"..refs/heads/main")
	return strings.Fields(out)
}

// subjectsSince lists the messages of the commits added to the target since a
// known point, oldest first — the readable form of what landed and in what
// order.
func (s *GitMergeSuite) subjectsSince(since string) []string {
	out := s.runGit(s.bare, "log", "--reverse", "--format=%s", since+"..refs/heads/main")
	var subjects []string
	for _, line := range strings.Split(out, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			subjects = append(subjects, trimmed)
		}
	}
	return subjects
}

// fileOnMain reads a file's contents at the target tip.
func (s *GitMergeSuite) fileOnMain(path string) string {
	return s.runGit(s.bare, "show", "refs/heads/main:"+path) + "\n"
}

// isAncestorOfMain reports whether a commit is reachable from the target — the
// property a provider reads to decide a change has merged.
func (s *GitMergeSuite) isAncestorOfMain(sha string) bool {
	cmd := exec.Command(s.git, "merge-base", "--is-ancestor", sha, "refs/heads/main")
	cmd.Dir = s.bare
	return cmd.Run() == nil
}

// mainRefUpdateCount is how many times the target branch has been updated,
// read from the bare repository's reflog. One land must cost exactly one,
// however many changes it carried.
func (s *GitMergeSuite) mainRefUpdateCount() int {
	out := s.runGit(s.bare, "reflog", "show", "--format=%H", "refs/heads/main")
	count := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

// runGit runs the pinned git and returns its trimmed stdout, failing the test
// on a non-zero exit.
func (s *GitMergeSuite) runGit(dir string, args ...string) string {
	s.T().Helper()
	cmd := exec.Command(s.git, args...)
	cmd.Dir = dir
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(s.T(), err, "git %s in %s: %s", strings.Join(args, " "), dir, stderr.String())
	return strings.TrimSpace(string(out))
}
