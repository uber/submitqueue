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
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/pusher"
)

// gitFixture provides a bare "remote" repository plus a working checkout
// that pushes to it. Tests run real `git` commands so we exercise the same
// code path as production.
//
// The fixture also exposes helpers for pushing additional commits to side
// branches on the remote, so each test can build the SHAs it needs the
// Pusher to cherry-pick.
type gitFixture struct {
	root        string
	remoteDir   string
	checkoutDir string
	authorDir   string // a separate working clone used to author "PR" commits
}

func setupGitFixture(t *testing.T) gitFixture {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available on PATH")
	}

	root := t.TempDir()
	remoteDir := filepath.Join(root, "remote.git")
	checkoutDir := filepath.Join(root, "checkout")
	authorDir := filepath.Join(root, "author")

	mustGit(t, root, "init", "--bare", "-b", "main", remoteDir)

	// Seed main with one initial commit so the Pusher's reset/fetch flow has
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

// configRepo applies the test-only config that lets git commit work in a
// sandbox without GPG signing, system identity, or system git hooks. The
// devpod environment installs hooks via the system git config (core.hooksPath
// = /etc/git-hooks) that interfere with test commits — point each test repo
// at an empty hooks dir to disarm them without resorting to --no-verify.
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

// pushPRCommitFrom creates a single commit on `branch` based on the given
// base ref, returning the resulting SHA. Use this to create branches that
// diverge from a specific ancestor (so they conflict when cherry-picked
// onto a newer main).
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

// remoteSHA returns the SHA at refs/heads/<branch> on the bare remote.
func (f gitFixture) remoteSHA(t *testing.T, branch string) string {
	t.Helper()
	out := mustGitOutput(t, f.remoteDir, "rev-parse", branch)
	return strings.TrimSpace(string(out))
}

// landOnMain cherry-picks an arbitrary SHA directly onto main on the
// remote, simulating "this content is already on the target branch" for
// rebased-out tests.
func (f gitFixture) landOnMain(t *testing.T, sha string) {
	t.Helper()
	mustGit(t, f.authorDir, "fetch", "origin")
	mustGit(t, f.authorDir, "checkout", "-B", "land", "origin/main")
	mustGit(t, f.authorDir, "cherry-pick", sha)
	mustGit(t, f.authorDir, "push", "origin", "land:main")
}

// uri builds a github-format URI ending in `sha` so the Pusher's parser
// resolves it to that SHA.
func uri(sha string) string {
	return fmt.Sprintf("github://uber/submitqueue/1/%s", sha)
}

func (f gitFixture) newPusher(t *testing.T) pusher.Pusher {
	return NewPusher(Params{
		CheckoutPath: f.checkoutDir,
		Remote:       "origin",
		Target:       "main",
		Logger:       zaptest.NewLogger(t).Sugar(),
		MetricsScope: tally.NoopScope,
	})
}

// installRaceHook writes a pre-receive hook on the bare remote that
// simulates concurrent pushes. On its Nth invocation it reads the Nth line
// of race-shas, points refs/heads/main at that SHA via update-ref, and
// exits 1 (rejecting the current push). Once race-shas is exhausted it
// exits 0 and the push goes through.
//
// Combined with the gitPusher's contention-detection (refetch + compare
// base SHA), this lets a test drive the full retry loop using only real
// git mechanics — the second-attempt's reset picks up the moved tip and
// proceeds from there.
func (f gitFixture) installRaceHook(t *testing.T, raceSHAs []string) {
	t.Helper()
	hookDir := filepath.Join(f.remoteDir, "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0o755))
	// Override the system-wide core.hooksPath so the hook we just wrote
	// actually fires on the bare remote (the devpod sets a global
	// /etc/git-hooks directory that would otherwise win).
	mustGit(t, f.remoteDir, "config", "core.hooksPath", hookDir)
	require.NoError(t, writeFile(
		filepath.Join(hookDir, "race-shas"),
		strings.Join(raceSHAs, "\n")+"\n",
	))
	const script = `#!/bin/sh
counter_file="$GIT_DIR/hooks/race-counter"
race_sha_file="$GIT_DIR/hooks/race-shas"
count=$(cat "$counter_file" 2>/dev/null || echo 0)
count=$((count + 1))
echo "$count" > "$counter_file"
next_sha=$(sed -n "${count}p" "$race_sha_file")
if [ -z "$next_sha" ]; then
  exit 0
fi
# Pre-receive runs in git's quarantine env; unset its markers so update-ref
# is allowed to mutate the live ref store.
unset GIT_QUARANTINE_PATH GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES
git update-ref refs/heads/main "$next_sha"
echo "race hook moved main to $next_sha and rejected push" >&2
exit 1
`
	hookPath := filepath.Join(hookDir, "pre-receive")
	require.NoError(t, os.WriteFile(hookPath, []byte(script), 0o755))
}

// hookInvocations returns the number of times the pre-receive race hook
// has fired. Used by retry tests to verify the loop ran the expected
// number of attempts.
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

// remoteCommitsSinceSeed returns commit SHAs on origin/main newer than the
// first (seed) commit, in chronological order.
func (f gitFixture) remoteCommitsSinceSeed(t *testing.T) []string {
	out := mustGitOutput(t, f.remoteDir, "log", "--reverse", "--format=%H", "main")
	all := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(all) <= 1 {
		return nil
	}
	return all[1:]
}

func TestPusher_Push_SingleChangeSingleURIProducesOneCommit(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	p := f.newPusher(t)

	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(sha)}},
	})
	require.NoError(t, err)
	require.Len(t, res.Outcomes, 1)

	out := res.Outcomes[0]
	assert.Equal(t, pusher.OutcomeStatusCommitted, out.Status)
	require.Len(t, out.CommitSHAs, 1)
	assert.Equal(t, []string{out.CommitSHAs[0]}, f.remoteCommitsSinceSeed(t))
	assert.Equal(t, "hello\nearth\n", f.remoteFile(t, "hello.txt"))
}

func TestPusher_Push_StackedURIsProduceMultipleCommitsForOneChange(t *testing.T) {
	f := setupGitFixture(t)
	// Build a stack on a single branch so the second SHA's parent is the first SHA.
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

	p := f.newPusher(t)
	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(sha1), uri(sha2)}},
	})
	require.NoError(t, err)
	require.Len(t, res.Outcomes, 1)

	out := res.Outcomes[0]
	assert.Equal(t, pusher.OutcomeStatusCommitted, out.Status)
	require.Len(t, out.CommitSHAs, 2)
	assert.Equal(t, out.CommitSHAs, f.remoteCommitsSinceSeed(t))
	assert.Equal(t, "hello\nearth\ngoodbye\n", f.remoteFile(t, "hello.txt"))
}

func TestPusher_Push_AlreadyLandedChangeIsRebasedOut(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")

	// Land the same content on main outside the Pusher so the cherry-pick
	// finds nothing new to add.
	f.landOnMain(t, sha)
	mainBeforePush := f.remoteHEAD(t)

	p := f.newPusher(t)
	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(sha)}},
	})
	require.NoError(t, err)
	require.Len(t, res.Outcomes, 1)

	out := res.Outcomes[0]
	assert.Equal(t, pusher.OutcomeStatusAlreadyExisted, out.Status)
	assert.Empty(t, out.CommitSHAs)
	assert.Equal(t, mainBeforePush, f.remoteHEAD(t),
		"rebased-out push should not advance the remote tip")
}

func TestPusher_Push_MixedChangesPartiallyRebasedOut(t *testing.T) {
	f := setupGitFixture(t)
	subsumedSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.landOnMain(t, subsumedSHA)

	freshSHA := f.pushPRCommit(t, "feature/b", "extra.txt", "extra\n", "add extra")

	p := f.newPusher(t)
	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(subsumedSHA)}},
		{URIs: []string{uri(freshSHA)}},
	})
	require.NoError(t, err)
	require.Len(t, res.Outcomes, 2)

	assert.Equal(t, pusher.OutcomeStatusAlreadyExisted, res.Outcomes[0].Status)
	assert.Empty(t, res.Outcomes[0].CommitSHAs)

	assert.Equal(t, pusher.OutcomeStatusCommitted, res.Outcomes[1].Status)
	require.Len(t, res.Outcomes[1].CommitSHAs, 1)

	assert.Equal(t, "extra\n", f.remoteFile(t, "extra.txt"))
}

func TestPusher_Push_ConflictReturnsErrConflictAndDoesNotPush(t *testing.T) {
	f := setupGitFixture(t)
	seedSHA := f.remoteSHA(t, "main")

	// Both branches start from the same seed and change the same line in
	// different ways, then "earth" lands first directly on main. The
	// Pusher's attempt to land "mars" must conflict.
	earthSHA := f.pushPRCommitFrom(t, seedSHA, "feature/a", "hello.txt", "hello\nearth\n", "earth")
	f.landOnMain(t, earthSHA)
	mainBefore := f.remoteHEAD(t)

	conflictingSHA := f.pushPRCommitFrom(t, seedSHA, "feature/b", "hello.txt", "hello\nmars\n", "mars")

	p := f.newPusher(t)
	_, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(conflictingSHA)}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, pusher.ErrConflict))

	assert.Equal(t, mainBefore, f.remoteHEAD(t),
		"on conflict the remote tip must not move")
}

func TestPusher_Push_ResetsBetweenCalls(t *testing.T) {
	f := setupGitFixture(t)
	sha := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	p := f.newPusher(t)

	// Dirty the checkout so that, without a reset, subsequent operations
	// would fail or include unrelated changes.
	require.NoError(t, writeFile(filepath.Join(f.checkoutDir, "stray.txt"), "leftover\n"))

	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(sha)}},
	})
	require.NoError(t, err)
	require.Len(t, res.Outcomes[0].CommitSHAs, 1)

	out := mustGitOutput(t, f.remoteDir, "ls-tree", "--name-only", res.Outcomes[0].CommitSHAs[0])
	assert.NotContains(t, string(out), "stray.txt", "unrelated file should not have landed")
	assert.Contains(t, string(out), "hello.txt")
}

func TestPusher_Push_RecoversAfterPriorConflict(t *testing.T) {
	f := setupGitFixture(t)
	seedSHA := f.remoteSHA(t, "main")

	first := f.pushPRCommitFrom(t, seedSHA, "feature/a", "hello.txt", "hello\nearth\n", "earth")
	f.landOnMain(t, first)
	conflictingSHA := f.pushPRCommitFrom(t, seedSHA, "feature/b", "hello.txt", "hello\nmars\n", "mars")

	p := f.newPusher(t)
	_, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(conflictingSHA)}},
	})
	require.Error(t, err)

	// A subsequent, clean push must succeed even though the prior call left
	// a cherry-pick in progress before its rollback.
	freshSHA := f.pushPRCommit(t, "feature/c", "extra.txt", "extra\n", "add extra")
	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(freshSHA)}},
	})
	require.NoError(t, err)
	assert.Equal(t, pusher.OutcomeStatusCommitted, res.Outcomes[0].Status)
	assert.Equal(t, "extra\n", f.remoteFile(t, "extra.txt"))
}

func TestPusher_Push_RejectsEmptyChanges(t *testing.T) {
	f := setupGitFixture(t)
	p := f.newPusher(t)

	_, err := p.Push(context.Background(), nil)
	require.Error(t, err)
	assert.False(t, errors.Is(err, pusher.ErrConflict))
}

func TestPusher_Push_InvalidURIErrors(t *testing.T) {
	f := setupGitFixture(t)
	p := f.newPusher(t)

	_, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{"not a uri"}},
	})
	require.Error(t, err)
}

func TestPusher_Push_RetriesWhenRemoteMovesUnderUs(t *testing.T) {
	f := setupGitFixture(t)
	// Pre-stage one race commit, install the hook, then build the feature
	// commit. Order matters: pushPRCommit also goes through the hook, so
	// race + feature must be on the remote before the hook is armed.
	raceSHA := f.pushPRCommit(t, "race", "race.txt", "race\n", "race commit")
	featureSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.installRaceHook(t, []string{raceSHA})

	p := f.newPusher(t)
	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(featureSHA)}},
	})
	require.NoError(t, err)
	require.Len(t, res.Outcomes, 1)
	require.Len(t, res.Outcomes[0].CommitSHAs, 1)

	assert.Equal(t, 2, f.hookInvocations(t),
		"first attempt rejected by hook, second attempt allowed through")

	commits := f.remoteCommitsSinceSeed(t)
	require.Len(t, commits, 2)
	assert.Equal(t, raceSHA, commits[0],
		"race commit landed first via the hook")
	assert.Equal(t, res.Outcomes[0].CommitSHAs[0], commits[1],
		"our cherry-pick landed on top after the retry")
	assert.Equal(t, "hello\nearth\n", f.remoteFile(t, "hello.txt"))
}

func TestPusher_Push_GivesUpAfterMaxAttempts(t *testing.T) {
	f := setupGitFixture(t)
	raceSHAs := []string{
		f.pushPRCommit(t, "race1", "r1.txt", "1\n", "r1"),
		f.pushPRCommit(t, "race2", "r2.txt", "2\n", "r2"),
	}
	featureSHA := f.pushPRCommit(t, "feature/a", "hello.txt", "hello\nearth\n", "tweak hello")
	f.installRaceHook(t, raceSHAs)

	p := NewPusher(Params{
		CheckoutPath:    f.checkoutDir,
		Remote:          "origin",
		Target:          "main",
		Logger:          zaptest.NewLogger(t).Sugar(),
		MetricsScope:    tally.NoopScope,
		MaxPushAttempts: 2,
	})
	_, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{uri(featureSHA)}},
	})
	require.Error(t, err)
	assert.Equal(t, 2, f.hookInvocations(t),
		"both attempts hit the hook before the cap kicked in")
	// The remote ended up at race2 (the last hook injection), and our
	// feature commit never landed.
	assert.Equal(t, raceSHAs[1], f.remoteHEAD(t))
}

// --- helpers ---

func mustGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "git %s: %s", strings.Join(args, " "), stderr.String())
}

func mustGitOutput(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "git %s: %s", strings.Join(args, " "), stderr.String())
	return stdout.Bytes()
}

func writeFile(path, contents string) error {
	return os.WriteFile(path, []byte(contents), 0o644)
}
