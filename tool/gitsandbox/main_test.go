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

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/gitexec"
)

// testGit resolves the pinned git the test target supplies, so these assertions
// exercise the same build the sandbox is seeded with rather than the host's.
//
// rules_go expands $(location) to an execroot-relative path, which for an
// external output has to be re-rooted under the runfiles directory a test runs
// from — the same re-rooting the git E2E does. Under `bazel run` the binary
// needs none of this, because it already runs from the runfiles tree.
func testGit(t *testing.T) string {
	t.Helper()

	supplied := os.Getenv("SUBMITQUEUE_TEST_GIT")
	require.NotEmpty(t, supplied, "the test target must supply SUBMITQUEUE_TEST_GIT")

	if git, err := gitexec.Resolve(supplied); err == nil {
		return git
	}

	slashed := filepath.ToSlash(supplied)
	index := strings.Index(slashed, "/external/")
	require.GreaterOrEqual(t, index, 0, "SUBMITQUEUE_TEST_GIT=%q is not a runfile", supplied)
	external := slashed[index+len("/external/"):]

	root := os.Getenv("TEST_SRCDIR")
	require.NotEmpty(t, root)

	git, err := gitexec.Resolve(filepath.Join(root, filepath.FromSlash(external)))
	require.NoError(t, err, "the test target must supply a git binary")
	return git
}

func TestProvision_SeedsABareRepositoryOnTheTargetBranch(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	created, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)
	assert.True(t, created, "a fresh directory must report that it was created")

	head, err := gitexec.Output(ctx, git, bare, "rev-parse", "refs/heads/main")
	require.NoError(t, err, "the target branch must exist after seeding")
	assert.Len(t, head, 40)

	subject, err := gitexec.Output(ctx, git, bare, "log", "-1", "--format=%s", "refs/heads/main")
	require.NoError(t, err)
	assert.Equal(t, "seed the sandbox", subject)

	contents, err := gitexec.Output(ctx, git, bare, "show", "refs/heads/main:"+seedFile)
	require.NoError(t, err)
	assert.Contains(t, contents, "SubmitQueue sandbox")
}

func TestProvision_LeavesAnExistingRepositoryAlone(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	_, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)
	before, err := gitexec.Output(ctx, git, bare, "rev-parse", "refs/heads/main")
	require.NoError(t, err)

	created, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)
	assert.False(t, created, "an initialized repository must not be re-created")

	after, err := gitexec.Output(ctx, git, bare, "rev-parse", "refs/heads/main")
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"re-running must preserve whatever landed since the first run")
}

// A restarted stack must not lose the history someone is looking at, which is
// the whole reason provisioning is idempotent rather than a reset.
func TestProvision_PreservesCommitsLandedAfterSeeding(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")
	require.NoError(t, func() error { _, err := provision(ctx, git, bare, "main"); return err }())

	work := t.TempDir()
	require.NoError(t, gitexec.Run(ctx, git, "", "clone", bare, work))
	require.NoError(t, gitexec.Run(ctx, git, work, "config", "user.name", "Test"))
	require.NoError(t, gitexec.Run(ctx, git, work, "config", "user.email", "test@example.invalid"))
	require.NoError(t, os.WriteFile(filepath.Join(work, "landed.txt"), []byte("landed\n"), 0o644))
	require.NoError(t, gitexec.Run(ctx, git, work, "add", "."))
	require.NoError(t, gitexec.Run(ctx, git, work, "commit", "-m", "a landed change"))
	require.NoError(t, gitexec.Run(ctx, git, work, "push", "origin", "main"))

	_, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)

	subject, err := gitexec.Output(ctx, git, bare, "log", "-1", "--format=%s", "refs/heads/main")
	require.NoError(t, err)
	assert.Equal(t, "a landed change", subject)
}

func TestProvision_HonorsTheBranchName(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	_, err := provision(ctx, git, bare, "trunk")
	require.NoError(t, err)

	_, err = gitexec.Output(ctx, git, bare, "rev-parse", "refs/heads/trunk")
	assert.NoError(t, err)
}

// The reflog is how a reader confirms a stack landed in one push, and bare
// repositories do not keep one unless asked.
func TestProvision_EnablesTheReflog(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	_, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)

	value, err := gitexec.Output(ctx, git, bare, "config", "core.logAllRefUpdates")
	require.NoError(t, err)
	assert.Equal(t, "true", value)
}

// Git packs refs after a push by default, rewriting packed-refs wholesale. The
// host writes this repository and a container reads it through a bind mount,
// which does not preserve the atomicity that makes the rewrite safe — a reader
// can catch the file mid-name and fail a land for no reason. Nothing to repack,
// nothing to catch.
func TestProvision_DisablesAutomaticHousekeeping(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	_, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)

	for setting, want := range map[string]string{
		"gc.auto":          "0",
		"receive.autogc":   "false",
		"maintenance.auto": "false",
	} {
		got, err := gitexec.Output(ctx, git, bare, "config", "--get", setting)
		require.NoError(t, err, "%s must be set", setting)
		assert.Equal(t, want, got, setting)
	}
}

// A sandbox made before these settings existed is reused rather than recreated,
// so it has to gain them on the next start instead of staying broken until
// someone deletes it.
func TestProvision_AppliesSettingsToAnExistingSandbox(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	_, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)
	require.NoError(t, gitexec.Run(ctx, git, bare, "config", "receive.autogc", "true"))

	created, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)
	require.False(t, created, "an existing sandbox is reused, not recreated")

	got, err := gitexec.Output(ctx, git, bare, "config", "--get", "receive.autogc")
	require.NoError(t, err)
	assert.Equal(t, "false", got)
}

func TestRun_RequiresASandboxDirectory(t *testing.T) {
	err := run(context.Background(), testGit(t), "", "", "sandbox", "main")
	require.Error(t, err)
}

func TestRun_CreatesTheCheckoutDirectory(t *testing.T) {
	checkout := filepath.Join(t.TempDir(), "checkouts")

	err := run(context.Background(), testGit(t), t.TempDir(), checkout, "sandbox", "main")
	require.NoError(t, err)

	info, err := os.Stat(checkout)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

// A repository that exists but carries no commit cannot be merged into, and the
// next run would skip it as already provisioned rather than repair it.
func TestProvision_LeavesNothingBehindWhenCreationFails(t *testing.T) {
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	// A branch name git refuses, so creation fails partway.
	_, err := provision(ctx, testGit(t), bare, "refs/heads/")
	require.Error(t, err)

	_, statErr := os.Stat(bare)
	assert.True(t, os.IsNotExist(statErr),
		"a repository that could not be created must not be left behind")
	assert.NotContains(t, strings.ToLower(err.Error()), "could not be removed")
}

// The failure above must be recoverable: a corrected run provisions cleanly
// rather than tripping over remnants of the one before it.
func TestProvision_SucceedsAfterAFailedAttempt(t *testing.T) {
	git := testGit(t)
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	_, err := provision(ctx, git, bare, "refs/heads/")
	require.Error(t, err)

	created, err := provision(ctx, git, bare, "main")
	require.NoError(t, err)
	assert.True(t, created)

	_, err = gitexec.Output(ctx, git, bare, "rev-parse", "refs/heads/main")
	assert.NoError(t, err)
}
