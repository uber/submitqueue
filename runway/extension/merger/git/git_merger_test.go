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

package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	gitexectest "github.com/uber/submitqueue/platform/git/exectest"
	"github.com/uber/submitqueue/runway/extension/merger"
)

const pinnedGitVersion = "2.55.0"

// fakeSHA is a syntactically valid 40-char hex SHA used by request-validation
// tests that never reach git (the request is rejected before any command runs).
const fakeSHA = "0123456789abcdef0123456789abcdef01234567"

// gitFixture provides a bare "remote" repository plus a working checkout that
// the Merger operates on. Tests run real `git` commands so they exercise the
// same code path as production.
//
// The fixture also exposes helpers for pushing additional commits to side
// branches on the remote, so each test can build the SHAs it needs the Merger
// to apply.
type gitFixture struct {
	root        string
	remoteDir   string
	checkoutDir string
	authorDir   string // a separate working clone used to author "PR" commits
}

func setupGitFixture(t *testing.T) gitFixture {
	t.Helper()

	root := t.TempDir()
	remoteDir := filepath.Join(root, "remote.git")
	checkoutDir := filepath.Join(root, "checkout")
	authorDir := filepath.Join(root, "author")

	mustGit(t, root, "init", "--bare", "-b", "main", remoteDir)

	// Seed main with one initial commit so the Merger's reset/fetch flow has
	// something to land on.
	mustGit(t, root, "init", "-b", "main", authorDir)
	configRepo(t, authorDir, "author", "author@example.com")
	require.NoError(t, writeFile(filepath.Join(authorDir, "hello.txt"), "hello\nworld\n"))
	mustGit(t, authorDir, "add", ".")
	mustGit(t, authorDir, "commit", "-m", "seed")
	mustGit(t, authorDir, "remote", "add", "origin", remoteDir)
	mustGit(t, authorDir, "push", "origin", "main")

	mustGit(t, root, "clone", remoteDir, checkoutDir)
	configRepo(t, checkoutDir, "checkout", "checkout@example.com")

	return gitFixture{
		root:        root,
		remoteDir:   remoteDir,
		checkoutDir: checkoutDir,
		authorDir:   authorDir,
	}
}

func TestGitRuntimeValidate(t *testing.T) {
	abs, err := filepath.Abs("git")
	require.NoError(t, err)
	tests := []struct {
		name    string
		runtime GitRuntime
		wantErr bool
	}{
		{
			name: "valid",
			runtime: GitRuntime{
				Executable:  abs,
				ExecPath:    abs,
				TemplateDir: abs,
			},
		},
		{
			name: "missing executable",
			runtime: GitRuntime{
				ExecPath:    abs,
				TemplateDir: abs,
			},
			wantErr: true,
		},
		{
			name: "relative exec path",
			runtime: GitRuntime{
				Executable:  abs,
				ExecPath:    "git-core",
				TemplateDir: abs,
			},
			wantErr: true,
		},
		{
			name: "relative template dir",
			runtime: GitRuntime{
				Executable:  abs,
				ExecPath:    abs,
				TemplateDir: "templates",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.runtime.validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNewGitCommandDoesNotInheritEnvironment(t *testing.T) {
	t.Setenv("SUBMITQUEUE_GIT_AMBIENT", "ambient")
	runtime := testGitRuntime(t)

	cmd := newGitCommand(context.Background(), runtime, t.TempDir(), "--version")

	assert.Equal(t, runtime.Executable, cmd.Path)
	assert.NotContains(t, cmd.Env, "SUBMITQUEUE_GIT_AMBIENT=ambient")
	assert.Contains(t, cmd.Env, "GIT_CONFIG_NOSYSTEM=1")
	assert.Contains(t, cmd.Env, "GIT_EXEC_PATH="+runtime.ExecPath)
}

func TestNewGitCommandPreservesAuthEnvironment(t *testing.T) {
	// Scrubbing denies git ambient configuration; it must not also deny it the
	// means to reach the remote. Without the agent socket an SSH remote cannot
	// authenticate, and without PATH git cannot even exec ssh.
	t.Setenv("SSH_AUTH_SOCK", "/tmp/agent.sock")
	t.Setenv("PATH", "/usr/bin:/bin")
	t.Setenv("HTTPS_PROXY", "http://proxy.example.com:3128")
	t.Setenv("SUBMITQUEUE_GIT_AMBIENT", "ambient")

	cmd := newGitCommand(context.Background(), testGitRuntime(t), t.TempDir(), "--version")

	assert.Contains(t, cmd.Env, "SSH_AUTH_SOCK=/tmp/agent.sock")
	assert.Contains(t, cmd.Env, "PATH=/usr/bin:/bin")
	assert.Contains(t, cmd.Env, "HTTPS_PROXY=http://proxy.example.com:3128")
	assert.NotContains(t, cmd.Env, "SUBMITQUEUE_GIT_AMBIENT=ambient")
}

func TestNewGitCommandOmitsUnsetAuthEnvironment(t *testing.T) {
	// An unset variable must be omitted rather than exported empty: an empty
	// SSH_AUTH_SOCK tells ssh there is no agent instead of letting it look.
	t.Setenv("SSH_AUTH_SOCK", "")
	require.NoError(t, os.Unsetenv("SSH_AUTH_SOCK"))

	cmd := newGitCommand(context.Background(), testGitRuntime(t), t.TempDir(), "--version")

	for _, entry := range cmd.Env {
		assert.False(t, strings.HasPrefix(entry, "SSH_AUTH_SOCK="), "unset variable leaked as %q", entry)
	}
}

func TestNewGitCommandPassesThroughExtraEnvironment(t *testing.T) {
	t.Setenv("SUBMITQUEUE_CUSTOM_TRANSPORT", "value")
	runtime := testGitRuntime(t)
	runtime.PassthroughEnv = []string{"SUBMITQUEUE_CUSTOM_TRANSPORT"}

	cmd := newGitCommand(context.Background(), runtime, t.TempDir(), "--version")

	assert.Contains(t, cmd.Env, "SUBMITQUEUE_CUSTOM_TRANSPORT=value")
}

func TestCherryPickRange_NonConflictFailureIsRetryable(t *testing.T) {
	// A cherry-pick can exit non-zero for reasons that have nothing to do with
	// the change colliding — a bad revision here, standing in for a missing
	// object or an unreadable repository. Reporting those as ErrConflict would
	// permanently fail a request that a retry could complete.
	f := setupGitFixture(t)
	m := f.newMerger(t, mergestrategypb.Strategy_REBASE).(*gitMerger)

	err := m.cherryPickRange(context.Background(), "notarevision", "alsonotarevision")
	require.Error(t, err)
	assert.False(t, errors.Is(err, merger.ErrConflict), "a non-conflict failure must stay retryable")
	assert.False(t, errors.Is(err, merger.ErrInvalidRequest))
}

func TestCherryPickRange_RealConflictIsErrConflict(t *testing.T) {
	f := setupGitFixture(t)
	mainBefore, conflictingSHA := f.setupConflict(t)
	require.NotEmpty(t, mainBefore)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE).(*gitMerger)
	require.NoError(t, m.resetToRemote(context.Background()))
	require.NoError(t, m.ensureObjects(context.Background(), []changeRef{{SHA: conflictingSHA}}))

	err := m.cherryPickRange(context.Background(), conflictingSHA+"^", conflictingSHA)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrConflict))
	assert.True(t, f.checkoutClean(t), "the aborted pick must leave no conflicted index behind")
}

// --- REBASE ---

func TestMerge_Rebase_SingleChangeSingleURI(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)

	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(sha))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)

	out := res.GetSteps()[0].GetOutputs()[0]
	assert.Equal(t, []string{out.GetId()}, f.remoteCommitsSinceSeed(t))
	assert.Equal(t, "hello\nearth\n", f.remoteFile(t, "hello.txt"))
}

func TestMerge_Rebase_StackedURIsInOneChange(t *testing.T) {
	f := setupGitFixture(t)
	sha1, sha2 := f.pushStack(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(sha1), uri(sha2)),
	))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	outputs := res.GetSteps()[0].GetOutputs()
	require.Len(t, outputs, 2)

	assert.Equal(t, []string{outputs[0].GetId(), outputs[1].GetId()}, f.remoteCommitsSinceSeed(t))
	assert.Equal(t, "hello\nearth\ngoodbye\n", f.remoteFile(t, "hello.txt"))
}

func TestMerge_Rebase_AlreadyLandedChange(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")

	// Land the same content on main outside the Merger so the cherry-pick
	// finds nothing new to add.
	f.landOnMain(t, sha)
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(sha))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
	assert.Equal(t, mainBefore, f.remoteHEAD(t),
		"rebased-out merge should not advance the remote tip")
}

func TestMerge_Rebase_MixedChanges(t *testing.T) {
	t.Run("two steps", func(t *testing.T) {
		f := setupGitFixture(t)
		subsumedSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
		f.landOnMain(t, subsumedSHA)
		freshSHA := f.pushPRCommit(t, "feature/b", "extra.txt", "extra\n", "add extra")

		m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
		res, err := m.Merge(context.Background(), req("b",
			stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(subsumedSHA)),
			stepOf(mergestrategypb.Strategy_REBASE, "s2", uri(freshSHA)),
		))
		require.NoError(t, err)
		require.Len(t, res.GetSteps(), 2)
		assert.Empty(t, res.GetSteps()[0].GetOutputs())
		require.Len(t, res.GetSteps()[1].GetOutputs(), 1)
		assert.Equal(t, "extra\n", f.remoteFile(t, "extra.txt"))
	})

	t.Run("one change two URIs", func(t *testing.T) {
		f := setupGitFixture(t)
		subsumedSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
		f.landOnMain(t, subsumedSHA)
		freshSHA := f.pushPRCommit(t, "feature/b", "extra.txt", "extra\n", "add extra")

		m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
		res, err := m.Merge(context.Background(), req("b",
			stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(subsumedSHA), uri(freshSHA)),
		))
		require.NoError(t, err)
		require.Len(t, res.GetSteps(), 1)
		// Only the fresh URI produced a commit; the subsumed one is a no-op.
		require.Len(t, res.GetSteps()[0].GetOutputs(), 1)
		assert.Equal(t, "extra\n", f.remoteFile(t, "extra.txt"))
	})
}

func TestMerge_Rebase_Conflict(t *testing.T) {
	f := setupGitFixture(t)
	mainBefore, conflictingSHA := f.setupConflict(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(conflictingSHA))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, mainBefore, f.remoteHEAD(t), "on conflict the remote tip must not move")
}

func TestMerge_Rebase_ResetsBetweenCalls(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)

	// Dirty the checkout so that, without a reset, subsequent operations would
	// fail or include unrelated changes.
	require.NoError(t, writeFile(filepath.Join(f.checkoutDir, "stray.txt"), "leftover\n"))

	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(sha))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)

	landed := res.GetSteps()[0].GetOutputs()[0].GetId()
	out := mustGitOutput(t, f.remoteDir, "ls-tree", "--name-only", landed)
	assert.NotContains(t, string(out), "stray.txt", "unrelated file should not have landed")
	assert.Contains(t, string(out), "hello.txt")
}

func TestMerge_Rebase_RecoversAfterPriorConflict(t *testing.T) {
	f := setupGitFixture(t)
	_, conflictingSHA := f.setupConflict(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(conflictingSHA))))
	require.Error(t, err)

	// A subsequent, clean merge must succeed even though the prior call left a
	// cherry-pick in progress before its rollback.
	freshSHA := f.pushPRCommit(t, "feature/c", "extra.txt", "extra\n", "add extra")
	res, err := m.Merge(context.Background(), req("b2", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(freshSHA))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	assert.Equal(t, "extra\n", f.remoteFile(t, "extra.txt"))
}

func TestMerge_Rebase_RetriesWhenRemoteMovesUnderUs(t *testing.T) {
	f := setupGitFixture(t)
	// Pre-stage one race commit, install the hook, then build the feature
	// commit. Order matters: pushPRCommit also goes through the hook, so race +
	// feature must be on the remote before the hook is armed.
	raceSHA := f.pushPRCommit(t, "race", "race.txt", "race\n", "race commit")
	featureSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.installRaceHook(t, []string{raceSHA})

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(featureSHA))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)

	assert.Equal(t, 2, f.hookInvocations(t),
		"first attempt rejected by hook, second attempt allowed through")

	commits := f.remoteCommitsSinceSeed(t)
	require.Len(t, commits, 2)
	assert.Equal(t, raceSHA, commits[0], "race commit landed first via the hook")
	assert.Equal(t, res.GetSteps()[0].GetOutputs()[0].GetId(), commits[1],
		"our cherry-pick landed on top after the retry")
	assert.Equal(t, "hello\nearth\n", f.remoteFile(t, "hello.txt"))
}

func TestMerge_Rebase_GivesUpAfterMaxAttempts(t *testing.T) {
	f := setupGitFixture(t)
	raceSHAs := []string{
		f.pushPRCommit(t, "race1", "r1.txt", "1\n", "r1"),
		f.pushPRCommit(t, "race2", "r2.txt", "2\n", "r2"),
	}
	featureSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.installRaceHook(t, raceSHAs)

	m, err := NewMerger(Params{
		CheckoutPath:    f.checkoutDir,
		Remote:          "origin",
		Target:          "main",
		DefaultStrategy: mergestrategypb.Strategy_REBASE,
		Runtime:         testGitRuntime(t),
		MaxPushAttempts: 2,
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
		CommitterName:   "Test",
		CommitterEmail:  "test@example.com",
	})
	require.NoError(t, err)

	_, err = m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(featureSHA))))
	require.Error(t, err)
	assert.False(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, 2, f.hookInvocations(t), "both attempts hit the hook before the cap kicked in")
	// The remote ended up at race2 (the last hook injection), and our feature
	// commit never landed.
	assert.Equal(t, raceSHAs[1], f.remoteHEAD(t))
}

// --- SQUASH_REBASE ---

func TestMerge_SquashRebase_OneCommitPerChange(t *testing.T) {
	// Two URIs in one change are a stack of two pull requests. Each squashes on
	// its own — collapsing them together would erase the boundary the stack
	// exists to express.
	f := setupGitFixture(t)
	sha1, sha2 := f.pushStack(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_SQUASH_REBASE, "s1", uri(sha1), uri(sha2)),
	))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)

	outputs := res.GetSteps()[0].GetOutputs()
	require.Len(t, outputs, 2, "one squashed commit per change")
	commits := f.remoteCommitsSinceSeed(t)
	require.Len(t, commits, 2)
	assert.Equal(t, []string{outputs[0].GetId(), outputs[1].GetId()}, commits, "in application order")
	assert.Equal(t, "hello\nearth\ngoodbye\n", f.remoteFile(t, "hello.txt"))
}

func TestMerge_SquashRebase_AlreadyLanded(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.landOnMain(t, sha)
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_SQUASH_REBASE, "s1", uri(sha))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

// --- MERGE ---

func TestMerge_Merge_FreshChangeCreatesMergeCommit(t *testing.T) {
	f := setupGitFixture(t)
	freshSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(freshSHA))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)

	mergeSHA := res.GetSteps()[0].GetOutputs()[0].GetId()
	assert.Equal(t, mergeSHA, f.remoteHEAD(t))
	assert.Len(t, f.parents(t, mergeSHA), 2, "a --no-ff merge commit has two parents")
	assert.Contains(t, f.parents(t, mergeSHA), freshSHA,
		"the original head commit is preserved as the merge's second parent")
}

func TestMerge_Merge_AlreadyAncestor(t *testing.T) {
	f := setupGitFixture(t)
	freshSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.advanceMain(t, freshSHA) // fast-forward main directly to freshSHA
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(freshSHA))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

// --- PROMOTE ---

func TestMerge_Promote_MultiCommitChange(t *testing.T) {
	// PROMOTE moves the ref to the named commit, so a change of any size
	// arrives whole by construction — its ancestry comes along with it.
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/ff",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
		commitSpec{"c.txt", "c\n", "add c"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Equal(t, head, f.remoteHEAD(t), "promote fast-forwards to the exact named commit")
	assert.Equal(t, "a\n", f.remoteFile(t, "a.txt"))
	assert.Equal(t, "c\n", f.remoteFile(t, "c.txt"))
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)
	assert.Equal(t, head, res.GetSteps()[0].GetOutputs()[0].GetId())
}

func TestMerge_Promote_UnavailableCommitIsInvalidNotRetryable(t *testing.T) {
	// promote does not run through tryApply, so it needs its own availability
	// check; otherwise the containment queries fail with a plain error and the
	// consumer retries a request that can never succeed.
	f := setupGitFixture(t)
	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)

	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(fakeSHA))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrInvalidRequest))
	assert.False(t, errors.Is(err, merger.ErrConflict))
}

func TestMerge_Promote_FastForward(t *testing.T) {
	f := setupGitFixture(t)
	ffSHA := f.pushPRCommit(t, "feature/ff", "hello.txt", "hello\nearth\n", "ff")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(ffSHA))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)
	assert.Equal(t, ffSHA, res.GetSteps()[0].GetOutputs()[0].GetId(), "promote reports the exact named SHA")
	assert.Equal(t, ffSHA, f.remoteHEAD(t))
}

func TestMerge_Promote_AlreadyContained(t *testing.T) {
	f := setupGitFixture(t)
	seedSHA := f.remoteSHA(t, "main")
	advSHA := f.pushPRCommit(t, "feature/adv", "adv.txt", "adv\n", "adv")
	f.advanceMain(t, advSHA)
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(seedSHA))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	assert.Equal(t, mainBefore, f.remoteHEAD(t), "promoting an already-contained SHA does not move the tip")
}

func TestMerge_Promote_Divergent(t *testing.T) {
	f := setupGitFixture(t)
	seedSHA := f.remoteSHA(t, "main")
	divSHA := f.pushPRCommitFrom(t, seedSHA, "feature/div", "div.txt", "div\n", "div")
	otherSHA := f.pushPRCommitFrom(t, seedSHA, "feature/other", "other.txt", "other\n", "other")
	f.advanceMain(t, otherSHA)
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(divSHA))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

// --- DEFAULT ---

func TestMerge_Default_ResolvesToRebase(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_DEFAULT, "s1", uri(sha))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1, "DEFAULT resolves to REBASE: one output per fresh change")
	assert.Equal(t, []string{res.GetSteps()[0].GetOutputs()[0].GetId()}, f.remoteCommitsSinceSeed(t))
}

func TestClassifyMergeFailure(t *testing.T) {
	f := setupGitFixture(t)
	m := f.newMerger(t, mergestrategypb.Strategy_REBASE).(*gitMerger)
	ref := changeRef{SHA: fakeSHA, Label: "uber/repo#1"}

	tests := []struct {
		name         string
		out          string
		conflicted   bool
		wantConflict bool
		wantInvalid  bool
	}{
		{
			name:         "content conflict",
			out:          "CONFLICT (content): Merge conflict in a.txt",
			conflicted:   true,
			wantConflict: true,
		},
		{
			name:        "unrelated histories",
			out:         "fatal: refusing to merge unrelated histories",
			conflicted:  false,
			wantInvalid: true,
		},
		{
			name:       "infrastructure failure",
			out:        "fatal: unable to read tree",
			conflicted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := m.classifyMergeFailure(ref, []byte(tt.out), tt.conflicted)
			require.Error(t, err)
			assert.Equal(t, tt.wantConflict, errors.Is(err, merger.ErrConflict))
			assert.Equal(t, tt.wantInvalid, errors.Is(err, merger.ErrInvalidRequest))
		})
	}
}

// --- change provider consistency ---

// newUnrunnableMerger builds a Merger whose git executable does not exist, so
// any git invocation fails loudly. A request rejected by this Merger with
// ErrInvalidRequest was therefore rejected before it reached for git — a
// stronger claim than observing that the remote did not move.
func (f gitFixture) newUnrunnableMerger(t *testing.T) merger.Merger {
	t.Helper()
	return f.newMergerWith(t, func(p *Params) {
		p.Runtime.Executable = filepath.Join(t.TempDir(), "no-such-git")
	})
}

func TestMerge_RejectsInconsistentProvider(t *testing.T) {
	const otherSHA = "89abcdef0123456789abcdef0123456789abcdef"

	tests := []struct {
		name string
		req  *runwaymq.MergeRequest
	}{
		{
			name: "two steps using different providers",
			req: req("b",
				stepOf(mergestrategypb.Strategy_REBASE, "s1", "github://github.example.com/uber/one/pull/1/"+fakeSHA),
				stepOf(mergestrategypb.Strategy_REBASE, "s2", "git://git.example.com/uber/one/refs%2Fheads%2Fmain/"+otherSHA),
			),
		},
		{
			name: "one change spanning two providers",
			req: req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1",
				"github://github.example.com/uber/one/pull/1/"+fakeSHA,
				"git://git.example.com/uber/one/refs%2Fheads%2Fmain/"+otherSHA,
			)),
		},
		{
			name: "unsupported provider",
			req:  req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", "phab://phab.example.com/D123/456")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := setupGitFixture(t)
			m := f.newUnrunnableMerger(t)

			_, err := m.Merge(context.Background(), tt.req)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merger.ErrInvalidRequest),
				"want ErrInvalidRequest before any git runs, got %v", err)
			assert.False(t, errors.Is(err, merger.ErrConflict))
		})
	}
}

func TestMerge_AcceptsMultipleChangesFromOneProvider(t *testing.T) {
	// Guard against over-rejecting: several steps and several URIs are normal,
	// so long as they are all addressed through one provider.
	f := setupGitFixture(t)
	a := f.pushPRCommit(t, "feature/a", "a.txt", "a\n", "add a")
	b := f.pushPRCommit(t, "feature/b", "b.txt", "b\n", "add b")
	c := f.pushPRCommit(t, "feature/c", "c.txt", "c\n", "add c")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(a), uri(b)),
		stepOf(mergestrategypb.Strategy_REBASE, "s2", uri(c)),
	))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	assert.Len(t, f.remoteCommitsSinceSeed(t), 3)
}

// --- repo migration (unrelated histories) ---
//
// A repository migration reaches Runway as an ordinary change in the target
// repo whose branch carries the *source* repo's entire history. Being in the
// same repository is what makes the commits fetchable; it says nothing about
// ancestry, and the two graphs still share no common ancestor.

// pushImportedHistory builds a branch with its own root commit — an imported
// history, unrelated to the target's — and publishes it in the target repo as
// a change ref, the way such a migration is actually proposed. Returns the head
// SHA and every commit of the imported history, oldest first.
func (f gitFixture) pushImportedHistory(t *testing.T, prNumber int, commits ...commitSpec) (string, []string) {
	t.Helper()
	dir := filepath.Join(f.root, fmt.Sprintf("import-%d", prNumber))
	mustGit(t, f.root, "init", "-b", "main", dir)
	configRepo(t, dir, "importer", "importer@example.com")
	for _, c := range commits {
		require.NoError(t, writeFile(filepath.Join(dir, c.path), c.contents))
		mustGit(t, dir, "add", ".")
		mustGit(t, dir, "commit", "-m", c.message)
	}
	head := strings.TrimSpace(string(mustGitOutput(t, dir, "rev-parse", "HEAD")))
	mustGit(t, dir, "remote", "add", "origin", f.remoteDir)
	mustGit(t, dir, "push", "-f", "origin", fmt.Sprintf("HEAD:refs/pull/%d/head", prNumber))

	out := mustGitOutput(t, dir, "rev-list", "--reverse", "HEAD")
	return head, strings.Fields(string(out))
}

func TestMerge_Merge_ImportsUnrelatedHistory(t *testing.T) {
	f := setupGitFixture(t)
	head, imported := f.pushImportedHistory(t, 1,
		commitSpec{"src/lib.go", "v1\n", "micro: initial"},
		commitSpec{"src/lib.go", "v2\n", "micro: feature A"},
		commitSpec{"src/util.go", "u\n", "micro: feature B"},
	)
	require.Len(t, imported, 3)

	m := f.newMergerWith(t, func(p *Params) { p.AllowUnrelatedHistories = true })
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())

	// The target keeps its own file and gains the imported tree.
	assert.Equal(t, "hello\nworld\n", f.remoteFile(t, "hello.txt"))
	assert.Equal(t, "v2\n", f.remoteFile(t, "src/lib.go"))
	assert.Equal(t, "u\n", f.remoteFile(t, "src/util.go"))

	// The point of using MERGE for a migration: every imported commit survives
	// under its original hash rather than being rewritten.
	for _, sha := range imported {
		assert.True(t, f.remoteHasCommit(t, sha), "imported commit %s must be reachable unchanged", sha)
	}

	// The merge commit joins both graphs.
	require.Len(t, res.GetSteps(), 1)
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)
	assert.Len(t, f.remoteParents(t, res.GetSteps()[0].GetOutputs()[0].GetId()), 2)
}

func TestMerge_Merge_ImportedHistoryRedeliveryIsNoOp(t *testing.T) {
	f := setupGitFixture(t)
	head, _ := f.pushImportedHistory(t, 1,
		commitSpec{"src/lib.go", "v1\n", "micro: initial"},
		commitSpec{"src/lib.go", "v2\n", "micro: feature A"},
	)

	m := f.newMergerWith(t, func(p *Params) { p.AllowUnrelatedHistories = true })
	request := req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(head)))
	_, err := m.Merge(context.Background(), request)
	require.NoError(t, err)
	afterFirst := f.remoteHEAD(t)

	res, err := m.Merge(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, afterFirst, f.remoteHEAD(t), "redelivery must not merge the import twice")
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
}

func TestMerge_Merge_UnrelatedHistoryRejectedByDefault(t *testing.T) {
	// Without the option the merge is refused. It must not be reported as a
	// conflict: nothing collided, the merger simply declined to join two
	// unrelated graphs.
	f := setupGitFixture(t)
	head, _ := f.pushImportedHistory(t, 1, commitSpec{"src/lib.go", "v1\n", "micro: initial"})
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(head))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrInvalidRequest))
	assert.False(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
	assert.True(t, f.checkoutClean(t))
}

func TestMerge_Rebase_UnrelatedHistoryRejected(t *testing.T) {
	// A picking strategy cannot serve a migration at all: there is no range to
	// compute, and replaying the import commit-by-commit would rewrite every
	// hash in it — the opposite of preserving the history.
	f := setupGitFixture(t)
	head, _ := f.pushImportedHistory(t, 1,
		commitSpec{"src/lib.go", "v1\n", "micro: initial"},
		commitSpec{"src/lib.go", "v2\n", "micro: feature A"},
	)

	for _, strategy := range []mergestrategypb.Strategy{
		mergestrategypb.Strategy_REBASE,
		mergestrategypb.Strategy_SQUASH_REBASE,
	} {
		t.Run(strategy.String(), func(t *testing.T) {
			m := f.newMergerWith(t, func(p *Params) { p.AllowUnrelatedHistories = true })
			_, err := m.Merge(context.Background(), req("b", stepOf(strategy, "s1", uri(head))))
			require.Error(t, err)
			assert.True(t, errors.Is(err, merger.ErrInvalidRequest))
			assert.False(t, errors.Is(err, merger.ErrConflict))
		})
	}
}

// --- multi-commit changes under the remaining strategies ---

func TestMerge_SquashRebase_MultiCommitChangeSingleURI(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/multi",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
		commitSpec{"c.txt", "c\n", "add c"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_SQUASH_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 1, "a change of any length collapses into one commit")
	// All three commits' content is present despite the single output.
	assert.Equal(t, "a\n", f.remoteFile(t, "a.txt"))
	assert.Equal(t, "b\n", f.remoteFile(t, "b.txt"))
	assert.Equal(t, "c\n", f.remoteFile(t, "c.txt"))
	assert.Len(t, f.remoteCommitsSinceSeed(t), 1)
}

func TestMerge_Merge_MultiCommitChangeSingleURI(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/multi",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Equal(t, "a\n", f.remoteFile(t, "a.txt"))
	assert.Equal(t, "b\n", f.remoteFile(t, "b.txt"))
	require.Len(t, res.GetSteps(), 1)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 1, "one merge commit regardless of change size")
}

// --- multi-commit changes (one URI, many commits) ---
//
// A change URI pins a pull request to a single head SHA, so these are the tests
// that actually reflect the wire contract: however many commits the change
// contains, the request names one. Passing the intermediate SHAs as extra URIs
// would be testing a shape production cannot produce.

func TestMerge_Rebase_MultiCommitChangeSingleURI(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/multi",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
		commitSpec{"c.txt", "c\n", "add c"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())

	// Every commit of the change lands, not just the one the URI names.
	require.Len(t, res.GetSteps(), 1)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 3)
	assert.Equal(t, "a\n", f.remoteFile(t, "a.txt"))
	assert.Equal(t, "b\n", f.remoteFile(t, "b.txt"))
	assert.Equal(t, "c\n", f.remoteFile(t, "c.txt"))
}

func TestMerge_Rebase_MultiCommitChangeSharedFile(t *testing.T) {
	// Commits that build on each other in the same region: applying only the
	// head would conflict against context its predecessors establish.
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/shared",
		commitSpec{"hello.txt", "hello\nworld\nearth\n", "add earth"},
		commitSpec{"hello.txt", "hello\nworld\nearth\nmars\n", "add mars"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	assert.Equal(t, "hello\nworld\nearth\nmars\n", f.remoteFile(t, "hello.txt"))
}

func TestMerge_Rebase_PartiallyLandedChange(t *testing.T) {
	// The change's first commit is already on the target. The range still
	// covers it; git declines it as redundant and the rest is applied.
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/partial",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
	)
	first := strings.TrimSpace(string(mustGitOutput(t, f.authorDir, "rev-parse", head+"^")))
	f.landOnMain(t, first)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 1, "only the not-yet-landed commit produces output")
	assert.Equal(t, "a\n", f.remoteFile(t, "a.txt"))
	assert.Equal(t, "b\n", f.remoteFile(t, "b.txt"))
}

func TestMerge_Rebase_EmptyCommitInRange(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/empty",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{message: "an empty commit"},
		commitSpec{"b.txt", "b\n", "add b"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 2, "the empty commit is skipped, not landed")
	assert.Equal(t, "a\n", f.remoteFile(t, "a.txt"))
	assert.Equal(t, "b\n", f.remoteFile(t, "b.txt"))
}

func TestMerge_Rebase_ConflictInRangeLeavesCheckoutUsable(t *testing.T) {
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/conflicting",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"hello.txt", "hello\nmars\n", "diverge hello"},
	)
	// Land a conflicting edit to the same file outside the merger.
	other := f.pushPRCommit(t, "feature/other", "hello.txt", "hello\nvenus\n", "venus")
	f.landOnMain(t, other)
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, mainBefore, f.remoteHEAD(t), "a conflicting range must not advance the remote")
	assert.True(t, f.checkoutClean(t), "the aborted range must leave no sequencer state behind")

	// The checkout is still usable for the next request.
	clean := f.pushPRCommit(t, "feature/clean", "z.txt", "z\n", "add z")
	_, err = m.Merge(context.Background(), req("c", stepOf(mergestrategypb.Strategy_REBASE, "s2", uri(clean))))
	require.NoError(t, err)
}

func TestMerge_Rebase_StackedMultiCommitChanges(t *testing.T) {
	// Change B is authored on top of change A, and both carry several commits.
	// B's range reaches back over A's commits; they are redundant by the time B
	// applies, so they must be skipped rather than duplicated.
	f := setupGitFixture(t)
	aHead := f.pushMultiCommitPR(t, "feature/a",
		commitSpec{"a1.txt", "a1\n", "a1"},
		commitSpec{"a2.txt", "a2\n", "a2"},
	)
	bHead := f.pushMultiCommitPRFrom(t, aHead, "feature/b",
		commitSpec{"b1.txt", "b1\n", "b1"},
		commitSpec{"b2.txt", "b2\n", "b2"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(aHead)),
		stepOf(mergestrategypb.Strategy_REBASE, "s2", uri(bHead)),
	))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 2)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 2)
	assert.Len(t, res.GetSteps()[1].GetOutputs(), 2, "A's commits must not be replayed inside B's range")
	assert.Len(t, f.remoteCommitsSinceSeed(t), 4)
	for _, name := range []string{"a1.txt", "a2.txt", "b1.txt", "b2.txt"} {
		assert.NotEmpty(t, f.remoteFile(t, name), name)
	}
}

// --- authorship ---

func TestAuthorIdent(t *testing.T) {
	tests := []struct {
		name     string
		ident    authorIdent
		complete bool
		wantEnv  []string
	}{
		{
			name:     "both halves present",
			ident:    authorIdent{Name: "Jane Dev", Email: "jane@example.com"},
			complete: true,
			wantEnv:  []string{"GIT_AUTHOR_NAME=Jane Dev", "GIT_AUTHOR_EMAIL=jane@example.com"},
		},
		{
			// Passed through verbatim: the environment carries the two halves
			// separately, so nothing has to survive being parsed back apart.
			name:     "name containing angle brackets",
			ident:    authorIdent{Name: "Jane <the dev> Doe", Email: "jane@example.com"},
			complete: true,
			wantEnv:  []string{"GIT_AUTHOR_NAME=Jane <the dev> Doe", "GIT_AUTHOR_EMAIL=jane@example.com"},
		},
		{
			name:  "missing email falls back",
			ident: authorIdent{Name: "Jane Dev"},
		},
		{
			name:  "missing name falls back",
			ident: authorIdent{Email: "jane@example.com"},
		},
		{
			name:  "empty falls back",
			ident: authorIdent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.complete, tt.ident.complete())
			assert.Equal(t, tt.wantEnv, tt.ident.env())
		})
	}
}

func TestMerge_SquashRebase_CreditsChangeAuthor(t *testing.T) {
	// A squash mints a fresh commit, so without attribution it would be
	// authored by the merger. The person who wrote the change should be its
	// author; the merger stays the committer, since it is what applied it.
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_SQUASH_REBASE, "s1", uri(sha)),
	))
	require.NoError(t, err)
	require.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())

	author, committer := f.remoteIdent(t, f.remoteHEAD(t))
	assert.Equal(t, "author <author@example.com>", author)
	assert.Equal(t, "Test <test@example.com>", committer)
}

func TestMerge_Merge_CreditsChangeAuthorOnMergeCommit(t *testing.T) {
	// `git merge` has no --author flag, so the merge commit is the case that
	// only the environment can attribute.
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(sha)),
	))
	require.NoError(t, err)
	require.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())

	mergeSHA := f.remoteHEAD(t)
	require.Len(t, f.parents(t, mergeSHA), 2, "expected a merge commit")
	author, committer := f.remoteIdent(t, mergeSHA)
	assert.Equal(t, "author <author@example.com>", author)
	assert.Equal(t, "Test <test@example.com>", committer)
}

func TestMerge_SquashRebase_CreditsAuthorOfHeadCommit(t *testing.T) {
	// The URI pins a head SHA, and that commit's author is the one the request
	// names. A change whose commits have different authors is credited to the
	// head's, not to whoever happened to start it.
	f := setupGitFixture(t)
	head := f.pushMultiCommitPRAs(t, "feature/multi",
		authoredCommit{spec: commitSpec{path: "a.go", contents: "package a\n", message: "add a"},
			name: "First Author", email: "first@example.com"},
		authoredCommit{spec: commitSpec{path: "b.go", contents: "package b\n", message: "add b"},
			name: "Head Author", email: "head@example.com"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_SQUASH_REBASE, "s1", uri(head)),
	))
	require.NoError(t, err)
	require.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())

	author, committer := f.remoteIdent(t, f.remoteHEAD(t))
	assert.Equal(t, "Head Author <head@example.com>", author)
	assert.Equal(t, "Test <test@example.com>", committer)
	// Both commits' content still landed — attribution did not change what the
	// squash contains.
	assert.Equal(t, "package a\n", f.remoteFile(t, "a.go"))
	assert.Equal(t, "package b\n", f.remoteFile(t, "b.go"))
}

func TestMerge_Rebase_PreservesEachCommitsOwnAuthor(t *testing.T) {
	// REBASE cherry-picks, which carries the author across on its own. Nothing
	// collapses, so each commit keeps the person who wrote it rather than
	// everything being credited to the head's author.
	f := setupGitFixture(t)
	head := f.pushMultiCommitPRAs(t, "feature/multi",
		authoredCommit{spec: commitSpec{path: "a.go", contents: "package a\n", message: "add a"},
			name: "First Author", email: "first@example.com"},
		authoredCommit{spec: commitSpec{path: "b.go", contents: "package b\n", message: "add b"},
			name: "Head Author", email: "head@example.com"},
	)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b",
		stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head)),
	))
	require.NoError(t, err)
	require.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())

	landed := f.remoteCommitsSinceSeed(t)
	require.Len(t, landed, 2)
	firstAuthor, firstCommitter := f.remoteIdent(t, landed[0])
	headAuthor, headCommitter := f.remoteIdent(t, landed[1])
	assert.Equal(t, "First Author <first@example.com>", firstAuthor)
	assert.Equal(t, "Head Author <head@example.com>", headAuthor)
	assert.Equal(t, "Test <test@example.com>", firstCommitter)
	assert.Equal(t, "Test <test@example.com>", headCommitter)
}

// --- object availability and staleness ---

func TestMerge_UnavailableCommitIsInvalidRequestNotConflict(t *testing.T) {
	// A commit the remote cannot supply is a request we can never satisfy. It
	// must not be reported as a merge conflict: that would tell the client its
	// change conflicts when nothing was ever applied.
	f := setupGitFixture(t)
	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)

	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(fakeSHA))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrInvalidRequest))
	assert.False(t, errors.Is(err, merger.ErrConflict))
}

func TestMerge_ChangeReachableOnlyViaPRRef(t *testing.T) {
	// The head is published as a pull-request ref and never as a branch, the
	// normal shape for a fork PR. The default fetch refspec does not cover it.
	f := setupGitFixture(t)
	head := f.pushMultiCommitPR(t, "feature/forked",
		commitSpec{"a.txt", "a\n", "add a"},
		commitSpec{"b.txt", "b\n", "add b"},
	)
	f.publishPRRef(t, 1, head)
	mustGit(t, f.authorDir, "push", "origin", "--delete", "feature/forked")

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(head))))
	require.NoError(t, err)
	require.Len(t, res.GetSteps(), 1)
	assert.Len(t, res.GetSteps()[0].GetOutputs(), 2)
}

func TestMerge_StaleChangeRejected(t *testing.T) {
	f := setupGitFixture(t)
	stale := f.pushPRCommit(t, "feature/s", "s.txt", "v1\n", "v1")
	f.publishPRRef(t, 1, stale)
	// The author force-pushes: the PR head moves on, but the old commit is
	// still fetchable, so only an explicit check catches it.
	current := f.pushPRCommit(t, "feature/s2", "s.txt", "v2\n", "v2")
	f.publishPRRef(t, 1, current)

	m := f.newMergerWith(t, func(p *Params) { p.CheckStaleness = true })
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(stale))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrInvalidRequest))

	// The same request is accepted when the URI names the current head.
	_, err = m.Merge(context.Background(), req("c", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(current))))
	require.NoError(t, err)
}

func TestMerge_StalenessCheckOffByDefault(t *testing.T) {
	f := setupGitFixture(t)
	stale := f.pushPRCommit(t, "feature/s", "s.txt", "v1\n", "v1")
	f.publishPRRef(t, 1, stale)
	current := f.pushPRCommit(t, "feature/s2", "t.txt", "v2\n", "v2")
	f.publishPRRef(t, 1, current)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.Merge(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(stale))))
	require.NoError(t, err)
}

// --- change provider resolution ---

func TestResolveChange(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantProvider string
		wantSHA      string
		wantRef      string
		wantLabel    string
		wantErr      bool
	}{
		{
			name:         "github pull request",
			uri:          "github://github.example.com/uber/submitqueue/pull/42/" + fakeSHA,
			wantProvider: "github",
			wantSHA:      fakeSHA,
			wantRef:      "refs/pull/42/head",
			wantLabel:    "uber/submitqueue#42",
		},
		{
			name:         "git ref",
			uri:          "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/" + fakeSHA,
			wantProvider: "git",
			wantSHA:      fakeSHA,
			wantRef:      "refs/heads/main",
			wantLabel:    "uber/monorepo@refs/heads/main",
		},
		{name: "unsupported scheme", uri: "phab://phab.example.com/D123/456", wantErr: true},
		{name: "no scheme", uri: "not-a-uri", wantErr: true},
		{name: "malformed github", uri: "github://github.example.com/nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveChange(tt.uri)
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, merger.ErrInvalidRequest))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantProvider, got.Provider)
			assert.Equal(t, tt.wantSHA, got.SHA)
			assert.Equal(t, tt.wantRef, got.Ref)
			assert.Equal(t, tt.wantLabel, got.Label)
		})
	}
}

// --- invalid requests ---

func TestMerge_InvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		req  *runwaymq.MergeRequest
	}{
		{
			name: "no steps",
			req:  req("b"),
		},
		{
			name: "promote with two steps",
			req: req("b",
				stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(fakeSHA)),
				stepOf(mergestrategypb.Strategy_PROMOTE, "s2", uri(fakeSHA)),
			),
		},
		{
			name: "promote step with two URIs",
			req:  req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(fakeSHA), uri(fakeSHA))),
		},
		{
			name: "malformed URI",
			req:  req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", "not a uri")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := setupGitFixture(t)
			m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
			_, err := m.Merge(context.Background(), tt.req)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merger.ErrInvalidRequest))
		})
	}
}

// --- CheckMergeability (dry-run) ---

func TestCheckMergeability_RebaseMergeable(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.CheckMergeability(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(sha))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs(), "a dry run reports no outputs")
	assert.Equal(t, mainBefore, f.remoteHEAD(t), "a dry run does not advance the remote tip")
}

func TestCheckMergeability_MergeMergeable(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.CheckMergeability(context.Background(), req("b", stepOf(mergestrategypb.Strategy_MERGE, "s1", uri(sha))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

func TestCheckMergeability_Conflict(t *testing.T) {
	f := setupGitFixture(t)
	mainBefore, conflictingSHA := f.setupConflict(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.CheckMergeability(context.Background(), req("b", stepOf(mergestrategypb.Strategy_REBASE, "s1", uri(conflictingSHA))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

func TestCheckMergeability_PromoteFastForward(t *testing.T) {
	f := setupGitFixture(t)
	ffSHA := f.pushPRCommit(t, "feature/ff", "hello.txt", "hello\nearth\n", "ff")
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	res, err := m.CheckMergeability(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(ffSHA))))
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 1)
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

func TestCheckMergeability_PromoteDivergent(t *testing.T) {
	f := setupGitFixture(t)
	seedSHA := f.remoteSHA(t, "main")
	divSHA := f.pushPRCommitFrom(t, seedSHA, "feature/div", "div.txt", "div\n", "div")
	otherSHA := f.pushPRCommitFrom(t, seedSHA, "feature/other", "other.txt", "other\n", "other")
	f.advanceMain(t, otherSHA)
	mainBefore := f.remoteHEAD(t)

	m := f.newMerger(t, mergestrategypb.Strategy_REBASE)
	_, err := m.CheckMergeability(context.Background(), req("b", stepOf(mergestrategypb.Strategy_PROMOTE, "s1", uri(divSHA))))
	require.Error(t, err)
	assert.True(t, errors.Is(err, merger.ErrConflict))
	assert.Equal(t, mainBefore, f.remoteHEAD(t))
}

func TestPinnedGitVersion(t *testing.T) {
	out := mustGitOutput(t, t.TempDir(), "--version")
	assert.Equal(t, "git version "+pinnedGitVersion, strings.TrimSpace(string(out)))
}

// --- request builders ---

// req builds a MergeRequest with the given correlation id and ordered steps.
func req(id string, steps ...*runwaymq.MergeStep) *runwaymq.MergeRequest {
	return &runwaymq.MergeRequest{Id: id, Steps: steps}
}

// stepOf builds a step whose change carries the given URIs.
func stepOf(strategy mergestrategypb.Strategy, stepID string, uris ...string) *runwaymq.MergeStep {
	return &runwaymq.MergeStep{
		StepId:   stepID,
		Strategy: strategy,
		Change:   &changepb.Change{Uris: uris},
	}
}

// --- merger + fixture helpers ---

// remoteHasCommit reports whether sha is reachable in the remote repository.
func (f gitFixture) remoteHasCommit(t *testing.T, sha string) bool {
	t.Helper()
	mustGit(t, f.checkoutDir, "fetch", "origin")
	cmd := exec.Command("git", "cat-file", "-e", sha+"^{commit}")
	cmd.Dir = f.checkoutDir
	return cmd.Run() == nil
}

// remoteParents returns the parent SHAs of a commit on the remote.
func (f gitFixture) remoteParents(t *testing.T, sha string) []string {
	t.Helper()
	mustGit(t, f.checkoutDir, "fetch", "origin")
	out := mustGitOutput(t, f.checkoutDir, "rev-list", "--parents", "-n", "1", sha)
	fields := strings.Fields(string(out))
	return fields[1:]
}

// commitSpec describes one commit to build on a change branch.
type commitSpec struct {
	path     string
	contents string
	message  string
}

// pushMultiCommitPR builds a branch of several commits off the current
// origin/main and returns ONLY the head SHA — which is all a change URI ever
// carries, however many commits the change actually contains. Tests that pass
// the intermediate SHAs separately are not exercising the real contract.
func (f gitFixture) pushMultiCommitPR(t *testing.T, branch string, commits ...commitSpec) string {
	t.Helper()
	return f.pushMultiCommitPRFrom(t, "origin/main", branch, commits...)
}

// pushMultiCommitPRFrom is pushMultiCommitPR based at an explicit start point,
// for building a change that stacks on another change rather than on trunk.
func (f gitFixture) pushMultiCommitPRFrom(t *testing.T, base, branch string, commits ...commitSpec) string {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	mustGit(t, f.authorDir, "checkout", "-B", branch, base)
	for _, c := range commits {
		if c.path == "" {
			mustGit(t, f.authorDir, "commit", "--allow-empty", "-m", c.message)
			continue
		}
		require.NoError(t, writeFile(filepath.Join(f.authorDir, c.path), c.contents))
		mustGit(t, f.authorDir, "add", ".")
		mustGit(t, f.authorDir, "commit", "-m", c.message)
	}
	mustGit(t, f.authorDir, "push", "-f", "origin", branch)
	return strings.TrimSpace(string(mustGitOutput(t, f.authorDir, "rev-parse", "HEAD")))
}

// publishPRRef points refs/pull/<n>/head at sha on the remote, mirroring how a
// host publishes a pull request head. Needed by anything exercising the
// canonical-ref fallback or the staleness check.
func (f gitFixture) publishPRRef(t *testing.T, n int, sha string) {
	t.Helper()
	mustGit(t, f.authorDir, "push", "-f", "origin",
		fmt.Sprintf("%s:refs/pull/%d/head", sha, n))
}

// newMergerWith builds a Merger with non-default Params, for the options that
// only some tests exercise.
func (f gitFixture) newMergerWith(t *testing.T, mutate func(*Params)) merger.Merger {
	t.Helper()
	p := Params{
		CheckoutPath:    f.checkoutDir,
		Remote:          "origin",
		Target:          "main",
		DefaultStrategy: mergestrategypb.Strategy_REBASE,
		Runtime:         testGitRuntime(t),
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
		CommitterName:   "Test",
		CommitterEmail:  "test@example.com",
	}
	mutate(&p)
	m, err := NewMerger(p)
	require.NoError(t, err)
	return m
}

// checkoutClean reports whether the merger's checkout has no in-progress
// operation and no dirty state — what must hold after any failed apply.
func (f gitFixture) checkoutClean(t *testing.T) bool {
	t.Helper()
	out := mustGitOutput(t, f.checkoutDir, "status", "--porcelain")
	if strings.TrimSpace(string(out)) != "" {
		return false
	}
	for _, marker := range []string{"CHERRY_PICK_HEAD", "MERGE_HEAD", "sequencer"} {
		if _, err := os.Stat(filepath.Join(f.checkoutDir, ".git", marker)); err == nil {
			return false
		}
	}
	return true
}

func (f gitFixture) newMerger(t *testing.T, defaultStrategy mergestrategypb.Strategy) merger.Merger {
	t.Helper()
	m, err := NewMerger(Params{
		CheckoutPath:    f.checkoutDir,
		Remote:          "origin",
		Target:          "main",
		DefaultStrategy: defaultStrategy,
		Runtime:         testGitRuntime(t),
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
		CommitterName:   "Test",
		CommitterEmail:  "test@example.com",
	})
	require.NoError(t, err)
	return m
}

// configRepo applies the test-only config that lets git commit work in a
// sandbox without GPG signing, system identity, or system git hooks. The devpod
// environment installs hooks via the system git config (core.hooksPath =
// /etc/git-hooks) that interfere with test commits — point each test repo at an
// empty hooks dir to disarm them without resorting to --no-verify.
func configRepo(t *testing.T, dir, name, email string) {
	mustGit(t, dir, "config", "user.name", name)
	mustGit(t, dir, "config", "user.email", email)
	mustGit(t, dir, "config", "commit.gpgsign", "false")
	mustGit(t, dir, "config", "tag.gpgsign", "false")

	hooksDir := filepath.Join(dir, ".no-hooks")
	require.NoError(t, os.MkdirAll(hooksDir, 0o755))
	mustGit(t, dir, "config", "core.hooksPath", hooksDir)
}

// pushPRCommit creates a single commit on a feature branch on the remote
// branched off the *current* origin/main, returning the resulting SHA.
func (f gitFixture) pushPRCommit(t *testing.T, branch, path, contents, message string) string {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	return f.pushPRCommitFrom(t, "origin/main", branch, path, contents, message)
}

// pushPRCommitFrom creates a single commit on `branch` based on the given base
// ref, returning the resulting SHA. Use this to create branches that diverge
// from a specific ancestor (so they conflict when cherry-picked onto a newer
// main).
func (f gitFixture) pushPRCommitFrom(t *testing.T, base, branch, path, contents, message string) string {
	t.Helper()
	mustGit(t, f.authorDir, "checkout", "-B", branch, base)
	require.NoError(t, writeFile(filepath.Join(f.authorDir, path), contents))
	mustGit(t, f.authorDir, "add", ".")
	mustGit(t, f.authorDir, "commit", "-m", message)
	mustGit(t, f.authorDir, "push", "-f", "origin", branch)
	out := mustGitOutput(t, f.authorDir, "rev-parse", "HEAD")
	return strings.TrimSpace(string(out))
}

// pushStack builds a two-commit stack on a single branch (so the second SHA's
// parent is the first SHA) and returns both SHAs in order.
func (f gitFixture) pushStack(t *testing.T) (string, string) {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	mustGit(t, f.authorDir, "checkout", "-B", "feature/stack", "origin/main")

	require.NoError(t, writeFile(filepath.Join(f.authorDir, "hello.txt"), "hello\nearth\n"))
	mustGit(t, f.authorDir, "add", ".")
	mustGit(t, f.authorDir, "commit", "-m", "step 1")
	sha1 := strings.TrimSpace(string(mustGitOutput(t, f.authorDir, "rev-parse", "HEAD")))

	require.NoError(t, writeFile(filepath.Join(f.authorDir, "hello.txt"), "hello\nearth\ngoodbye\n"))
	mustGit(t, f.authorDir, "add", ".")
	mustGit(t, f.authorDir, "commit", "-m", "step 2")
	sha2 := strings.TrimSpace(string(mustGitOutput(t, f.authorDir, "rev-parse", "HEAD")))

	mustGit(t, f.authorDir, "push", "-f", "origin", "feature/stack")
	return sha1, sha2
}

// setupConflict lands "earth" on main and returns the pre-merge main SHA plus a
// divergent "mars" SHA that conflicts when applied onto the new main.
func (f gitFixture) setupConflict(t *testing.T) (mainBefore, conflictingSHA string) {
	t.Helper()
	seedSHA := f.remoteSHA(t, "main")
	earthSHA := f.pushPRCommitFrom(t, seedSHA, "feature/a", "hello.txt", "hello\nearth\n", "earth")
	f.landOnMain(t, earthSHA)
	mainBefore = f.remoteHEAD(t)
	conflictingSHA = f.pushPRCommitFrom(t, seedSHA, "feature/b", "hello.txt", "hello\nmars\n", "mars")
	return mainBefore, conflictingSHA
}

// remoteSHA returns the SHA at refs/heads/<branch> on the bare remote.
func (f gitFixture) remoteSHA(t *testing.T, branch string) string {
	t.Helper()
	out := mustGitOutput(t, f.remoteDir, "rev-parse", branch)
	return strings.TrimSpace(string(out))
}

// landOnMain cherry-picks an arbitrary SHA directly onto main on the remote,
// simulating "this content is already on the target branch" for rebased-out
// tests.
func (f gitFixture) landOnMain(t *testing.T, sha string) {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	mustGit(t, f.authorDir, "checkout", "-B", "land", "origin/main")
	mustGit(t, f.authorDir, "cherry-pick", sha)
	mustGit(t, f.authorDir, "push", "origin", "land:main")
}

// advanceMain fast-forwards main on the remote directly to sha (which must be a
// fast-forward of the current tip), used to pre-place a commit on the target.
func (f gitFixture) advanceMain(t *testing.T, sha string) {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	mustGit(t, f.authorDir, "push", "origin", sha+":main")
}

// uri builds a github-format URI ending in `sha` so the Merger's parser resolves
// it to that SHA.
func uri(sha string) string {
	return fmt.Sprintf("github://github.example.com/uber/submitqueue/pull/1/%s", sha)
}

// installRaceHook writes a pre-receive hook on the bare remote that simulates
// concurrent pushes. On its Nth invocation it reads the Nth line of race-shas,
// points refs/heads/main at that SHA via update-ref, and exits 1 (rejecting the
// current push). Once race-shas is exhausted it exits 0 and the push goes
// through.
//
// Combined with the gitMerger's contention-detection (refetch + compare base
// SHA), this lets a test drive the full retry loop using only real git
// mechanics — the second-attempt's reset picks up the moved tip and proceeds
// from there.
func (f gitFixture) installRaceHook(t *testing.T, raceSHAs []string) {
	t.Helper()
	hookDir := filepath.Join(f.remoteDir, "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0o755))
	// Override the system-wide core.hooksPath so the hook we just wrote actually
	// fires on the bare remote (the devpod sets a global /etc/git-hooks
	// directory that would otherwise win).
	mustGit(t, f.remoteDir, "config", "core.hooksPath", hookDir)
	require.NoError(t, writeFile(
		filepath.Join(hookDir, "race-shas"),
		strings.Join(raceSHAs, "\n")+"\n",
	))
	const script = `#!/bin/sh
# Contend only on the target branch. A merge that moves change head branches
# pushes those first, and they are not what this hook simulates a race for.
target_pushed=0
while read -r _old _new ref; do
  if [ "$ref" = "refs/heads/main" ]; then
    target_pushed=1
  fi
done
if [ "$target_pushed" -eq 0 ]; then
  exit 0
fi
counter_file="$GIT_DIR/hooks/race-counter"
race_sha_file="$GIT_DIR/hooks/race-shas"
count=$(cat "$counter_file" 2>/dev/null || echo 0)
count=$((count + 1))
echo "$count" > "$counter_file"
next_sha=$(sed -n "${count}p" "$race_sha_file")
if [ -z "$next_sha" ]; then
  exit 0
fi
# Pre-receive runs in git's quarantine env; unset its markers so update-ref is
# allowed to mutate the live ref store.
unset GIT_QUARANTINE_PATH GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES
"$GIT_EXEC_PATH/git" update-ref refs/heads/main "$next_sha"
echo "race hook moved main to $next_sha and rejected push" >&2
exit 1
`
	hookPath := filepath.Join(hookDir, "pre-receive")
	require.NoError(t, os.WriteFile(hookPath, []byte(script), 0o755))
}

// installRefRejectHook makes the bare remote refuse every push that touches the
// given fully-qualified ref, leaving all other refs alone. Used to fail a head
// branch update without disturbing the target.
func (f gitFixture) installRefRejectHook(t *testing.T, rejectRef string) {
	t.Helper()
	hookDir := filepath.Join(f.remoteDir, "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0o755))
	// Override the system-wide core.hooksPath so the hook we just wrote actually
	// fires on the bare remote.
	mustGit(t, f.remoteDir, "config", "core.hooksPath", hookDir)
	require.NoError(t, writeFile(filepath.Join(hookDir, "reject-ref"), rejectRef+"\n"))
	const script = `#!/bin/sh
reject_ref=$(cat "$GIT_DIR/hooks/reject-ref")
while read -r _old _new ref; do
  if [ "$ref" = "$reject_ref" ]; then
    echo "hook rejects $ref" >&2
    exit 1
  fi
done
exit 0
`
	hookPath := filepath.Join(hookDir, "pre-receive")
	require.NoError(t, os.WriteFile(hookPath, []byte(script), 0o755))
}

// hookInvocations returns the number of times the pre-receive race hook has
// fired. Used by retry tests to verify the loop ran the expected number of
// attempts.
func (f gitFixture) hookInvocations(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.remoteDir, "hooks", "race-counter"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	require.NoError(t, err)
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	return n
}

// remoteHEAD returns the SHA that origin/main currently points to.
func (f gitFixture) remoteHEAD(t *testing.T) string {
	out := mustGitOutput(t, f.remoteDir, "rev-parse", "main")
	return strings.TrimSpace(string(out))
}

// remoteFile returns the contents of `path` at origin/main on the bare remote.
func (f gitFixture) remoteFile(t *testing.T, path string) string {
	out := mustGitOutput(t, f.remoteDir, "show", "main:"+path)
	return string(out)
}

// remoteCommitsSinceSeed returns commit SHAs reachable from origin/main newer
// than the first (seed) commit, in chronological order.
func (f gitFixture) remoteCommitsSinceSeed(t *testing.T) []string {
	out := mustGitOutput(t, f.remoteDir, "log", "--reverse", "--format=%H", "main")
	all := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(all) <= 1 {
		return nil
	}
	return all[1:]
}

// parents returns the parent SHAs recorded on the commit at sha on the remote.
func (f gitFixture) parents(t *testing.T, sha string) []string {
	t.Helper()
	out := mustGitOutput(t, f.remoteDir, "rev-list", "--parents", "-n", "1", sha)
	fields := strings.Fields(strings.TrimSpace(string(out)))
	require.NotEmpty(t, fields)
	return fields[1:]
}

// remoteIdent returns the author and committer recorded on a commit, each
// formatted "Name <email>". The two differ whenever the merger creates a commit
// for someone else's change.
func (f gitFixture) remoteIdent(t *testing.T, sha string) (author, committer string) {
	t.Helper()
	out := mustGitOutput(t, f.remoteDir, "show", "--no-patch", "--format=%an <%ae>%n%cn <%ce>", sha)
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	require.Len(t, lines, 2)
	return lines[0], lines[1]
}

// authoredCommit is a commitSpec plus the identity to author it with, for the
// tests that need a change whose commits are not all by the same person.
type authoredCommit struct {
	spec  commitSpec
	name  string
	email string
}

// pushMultiCommitPRAs is pushMultiCommitPR with an explicit author per commit.
func (f gitFixture) pushMultiCommitPRAs(t *testing.T, branch string, commits ...authoredCommit) string {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	mustGit(t, f.authorDir, "checkout", "-B", branch, "origin/main")
	for _, c := range commits {
		require.NoError(t, writeFile(filepath.Join(f.authorDir, c.spec.path), c.spec.contents))
		mustGit(t, f.authorDir, "add", ".")
		mustGit(t, f.authorDir, "commit", "-m", c.spec.message,
			"--author", fmt.Sprintf("%s <%s>", c.name, c.email))
	}
	mustGit(t, f.authorDir, "push", "-f", "origin", branch)
	return strings.TrimSpace(string(mustGitOutput(t, f.authorDir, "rev-parse", "HEAD")))
}

func mustGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := newGitCommand(context.Background(), testGitRuntime(t), dir, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "git %s: %s", strings.Join(args, " "), stderr.String())
}

func mustGitOutput(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	cmd := newGitCommand(context.Background(), testGitRuntime(t), dir, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "git %s: %s", strings.Join(args, " "), stderr.String())
	return stdout.Bytes()
}

func testGitRuntime(t *testing.T) GitRuntime {
	t.Helper()
	executable := gitexectest.Git(t)
	templateDescription := gitexectest.Runfile(t, "SUBMITQUEUE_TEST_GIT_TEMPLATE_DESCRIPTION")
	return GitRuntime{
		Executable:  executable,
		ExecPath:    filepath.Dir(executable),
		TemplateDir: filepath.Dir(templateDescription),
	}
}

func writeFile(path, contents string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(contents), 0o644)
}
