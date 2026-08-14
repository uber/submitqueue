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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	gitmerger "github.com/uber/submitqueue/runway/extension/merger/git"
)

// testRuntime resolves the pinned git the Bazel target supplies. Provisioning
// runs real git, so these tests use the same runtime the merger will.
func testRuntime(t *testing.T) gitmerger.GitRuntime {
	t.Helper()
	executable := runfilePath(t, "SUBMITQUEUE_TEST_GIT")
	templateDescription := runfilePath(t, "SUBMITQUEUE_TEST_GIT_TEMPLATE_DESCRIPTION")
	return gitmerger.GitRuntime{
		Executable:  executable,
		ExecPath:    filepath.Dir(executable),
		TemplateDir: filepath.Dir(templateDescription),
	}
}

// runfilePath resolves a $(location)-expanded path from the test environment.
// rules_go emits an execroot-relative path, which for an external output has to
// be re-rooted under the runfiles directory the test actually runs from.
func runfilePath(t *testing.T, name string) string {
	t.Helper()
	path := os.Getenv(name)
	require.NotEmpty(t, path, "%s must be set by the test target", name)
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
	require.NotEmpty(t, external, "%s=%q is not a runfile", name, path)

	root := os.Getenv("TEST_SRCDIR")
	require.NotEmpty(t, root)
	candidate := filepath.Join(root, filepath.FromSlash(external))
	_, err := os.Stat(candidate)
	require.NoError(t, err)
	return candidate
}

// seedBareRepo creates a bare repository holding one commit on branch and
// returns its path — the shape of the remote a merge target points at.
func seedBareRepo(t *testing.T, branch string) string {
	t.Helper()
	runtime := testRuntime(t)
	root := t.TempDir()
	bare := filepath.Join(root, "remote.git")
	work := filepath.Join(root, "seed")

	ctx := context.Background()
	require.NoError(t, os.MkdirAll(work, 0o755))
	mustRunGit(t, ctx, runtime, root, "init", "--bare", "-b", branch, bare)
	mustRunGit(t, ctx, runtime, work, "init", "-b", branch)
	mustRunGit(t, ctx, runtime, work, "config", "user.name", "Seed")
	mustRunGit(t, ctx, runtime, work, "config", "user.email", "seed@example.com")
	mustRunGit(t, ctx, runtime, work, "config", "commit.gpgsign", "false")
	require.NoError(t, os.WriteFile(filepath.Join(work, "seed.txt"), []byte("seed\n"), 0o644))
	mustRunGit(t, ctx, runtime, work, "add", ".")
	mustRunGit(t, ctx, runtime, work, "commit", "-m", "seed")
	mustRunGit(t, ctx, runtime, work, "remote", "add", "origin", bare)
	mustRunGit(t, ctx, runtime, work, "push", "origin", branch)

	return bare
}

func mustRunGit(t *testing.T, ctx context.Context, runtime gitmerger.GitRuntime, dir string, args ...string) string {
	t.Helper()
	out, err := runGit(ctx, runtime, dir, args...)
	require.NoError(t, err, "git %s", strings.Join(args, " "))
	return strings.TrimSpace(string(out))
}

// localCheckout builds a config pointing at a freshly seeded bare repo.
func localCheckout(t *testing.T, bare string) mergerConfig {
	t.Helper()
	cfg := mergerConfig{
		Type:         mergerTypeGit,
		RemoteURL:    "file://" + bare,
		CheckoutPath: filepath.Join(t.TempDir(), "checkout"),
	}
	require.NoError(t, cfg.normalizeAndValidate("test"))
	return cfg
}

func TestProvisionCheckout_CreatesUsableWorkingTree(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t)
	cfg := localCheckout(t, seedBareRepo(t, "main"))

	require.NoError(t, provisionCheckout(ctx, zaptest.NewLogger(t).Sugar(), runtime, cfg))

	// The merger resets to refs/remotes/<remote>/<target> on every cycle, so
	// that ref existing is what makes the checkout usable at all.
	assert.NotEmpty(t, mustRunGit(t, ctx, runtime, cfg.CheckoutPath, "rev-parse", "origin/main"))
	assert.Equal(t, "main", mustRunGit(t, ctx, runtime, cfg.CheckoutPath, "rev-parse", "--abbrev-ref", "HEAD"))

	content, err := os.ReadFile(filepath.Join(cfg.CheckoutPath, "seed.txt"))
	require.NoError(t, err)
	assert.Equal(t, "seed\n", string(content))
}

func TestProvisionCheckout_NonDefaultTargetBranch(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t)
	cfg := localCheckout(t, seedBareRepo(t, "trunk"))
	cfg.Target = "trunk"

	require.NoError(t, provisionCheckout(ctx, zaptest.NewLogger(t).Sugar(), runtime, cfg))
	assert.Equal(t, "trunk", mustRunGit(t, ctx, runtime, cfg.CheckoutPath, "rev-parse", "--abbrev-ref", "HEAD"))
}

func TestProvisionCheckout_IsIdempotent(t *testing.T) {
	// A restart against a persisted volume must cost nothing and must not
	// discard the objects the previous run fetched.
	ctx := context.Background()
	runtime := testRuntime(t)
	logger := zaptest.NewLogger(t).Sugar()
	cfg := localCheckout(t, seedBareRepo(t, "main"))

	require.NoError(t, provisionCheckout(ctx, logger, runtime, cfg))
	first := mustRunGit(t, ctx, runtime, cfg.CheckoutPath, "rev-parse", "HEAD")

	require.NoError(t, provisionCheckout(ctx, logger, runtime, cfg))
	assert.Equal(t, first, mustRunGit(t, ctx, runtime, cfg.CheckoutPath, "rev-parse", "HEAD"))
}

func TestProvisionCheckout_CorrectsDriftedRemoteURL(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t)
	logger := zaptest.NewLogger(t).Sugar()

	cfg := localCheckout(t, seedBareRepo(t, "main"))
	require.NoError(t, provisionCheckout(ctx, logger, runtime, cfg))

	moved := cfg
	moved.RemoteURL = "file://" + seedBareRepo(t, "main")
	require.NoError(t, provisionCheckout(ctx, logger, runtime, moved))

	got := mustRunGit(t, ctx, runtime, cfg.CheckoutPath, "remote", "get-url", "origin")
	assert.Equal(t, moved.RemoteURL, got)
}

func TestProvisionCheckout_LocalRemoteCarriesNoCredential(t *testing.T) {
	ctx := context.Background()
	cfg := localCheckout(t, seedBareRepo(t, "main"))
	cfg.TokenEnv = "" // a local path authenticates by nothing at all

	require.NoError(t, provisionCheckout(ctx, zaptest.NewLogger(t).Sugar(), testRuntime(t), cfg))

	_, err := os.Stat(filepath.Join(cfg.CheckoutPath, ".git", credentialFile))
	assert.True(t, os.IsNotExist(err))
}

func TestWriteCredential_KeepsTokenOutOfTheRemoteURL(t *testing.T) {
	// The merger folds git's stderr into the errors it returns, so a token in
	// the remote URL would be reprinted by any failed fetch. This is the
	// property that keeps it out of logs and dead-letter payloads.
	const token = "s3cret-token-value"
	t.Setenv("TEST_FORGE_TOKEN", token)

	ctx := context.Background()
	runtime := testRuntime(t)
	logger := zaptest.NewLogger(t).Sugar()

	// Provision from a local remote first so there is a repository to write
	// into, then point it at an HTTPS URL carrying a credential.
	cfg := localCheckout(t, seedBareRepo(t, "main"))
	require.NoError(t, provisionCheckout(ctx, logger, runtime, cfg))

	authed := cfg
	authed.RemoteURL = "https://example.com/o/r.git"
	authed.TokenEnv = "TEST_FORGE_TOKEN"
	require.NoError(t, configureRemote(ctx, runtime, authed))
	require.NoError(t, writeCredential(authed))

	url := mustRunGit(t, ctx, runtime, authed.CheckoutPath, "remote", "get-url", "origin")
	assert.NotContains(t, url, token)
	assert.Equal(t, "https://example.com/o/r.git", url)

	path := filepath.Join(authed.CheckoutPath, ".git", credentialFile)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "credential must not be world-readable")

	// git resolves the fragment through the include, which is what makes the
	// header reach the remote without the repository config holding it.
	header := mustRunGit(t, ctx, runtime, authed.CheckoutPath,
		"config", "--get", "http.https://example.com/o/r.git.extraheader")
	assert.Contains(t, header, "Authorization: Basic ")
	assert.NotContains(t, header, token, "the token is base64-encoded, never literal")
}

func TestWriteCredential_RemovesFragmentWhenNoLongerNeeded(t *testing.T) {
	t.Setenv("TEST_FORGE_TOKEN", "value")
	ctx := context.Background()
	runtime := testRuntime(t)

	cfg := localCheckout(t, seedBareRepo(t, "main"))
	require.NoError(t, provisionCheckout(ctx, zaptest.NewLogger(t).Sugar(), runtime, cfg))

	authed := cfg
	authed.RemoteURL = "https://example.com/o/r.git"
	authed.TokenEnv = "TEST_FORGE_TOKEN"
	require.NoError(t, writeCredential(authed))
	require.FileExists(t, filepath.Join(cfg.CheckoutPath, ".git", credentialFile))

	// Switching the target back to one needing no credential must not leave the
	// old secret lying in the checkout.
	require.NoError(t, writeCredential(cfg))
	_, err := os.Stat(filepath.Join(cfg.CheckoutPath, ".git", credentialFile))
	assert.True(t, os.IsNotExist(err))
}

func TestWriteCredential_RejectsUnsetToken(t *testing.T) {
	// Failing here beats cloning anonymously and reporting a confusing 404 on a
	// private repository later.
	cfg := mergerConfig{
		Type:         mergerTypeGit,
		RemoteURL:    "https://example.com/o/r.git",
		CheckoutPath: t.TempDir(),
		TokenEnv:     "TEST_TOKEN_DEFINITELY_NOT_SET",
		TokenUser:    "x-access-token",
	}
	require.Error(t, writeCredential(cfg))

	t.Setenv("TEST_TOKEN_DEFINITELY_NOT_SET", "")
	require.Error(t, writeCredential(cfg), "an empty value is a deployment mistake, not a valid credential")
}

func TestResolveGitRuntime_HonorsEnvironmentOverrides(t *testing.T) {
	pinned := testRuntime(t)
	t.Setenv("GIT_EXECUTABLE", pinned.Executable)
	t.Setenv("GIT_EXEC_PATH", pinned.ExecPath)
	t.Setenv("GIT_TEMPLATE_DIR", pinned.TemplateDir)

	got, err := resolveGitRuntime(context.Background())
	require.NoError(t, err)
	assert.Equal(t, pinned, got)
}

func TestResolveGitRuntime_DerivesExecPathFromTheBinary(t *testing.T) {
	// The trap this removes: the merger requires all three paths, so a
	// deployment that sets only MERGE_CHECKOUT_PATH used to fail at startup.
	pinned := testRuntime(t)
	t.Setenv("GIT_EXECUTABLE", pinned.Executable)
	t.Setenv("GIT_EXEC_PATH", "")
	t.Setenv("GIT_TEMPLATE_DIR", pinned.TemplateDir)

	got, err := resolveGitRuntime(context.Background())
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(got.ExecPath))
	assert.NotEmpty(t, got.ExecPath)
}

func TestResolveGitRuntime_ReportsMissingGit(t *testing.T) {
	t.Setenv("GIT_EXECUTABLE", "")
	t.Setenv("GIT_EXEC_PATH", "")
	t.Setenv("GIT_TEMPLATE_DIR", "")
	t.Setenv("PATH", t.TempDir())

	_, err := resolveGitRuntime(context.Background())
	require.Error(t, err)
}

// Verify the pinned git is what the tests actually exercised.
func TestTestRuntimeIsUsable(t *testing.T) {
	out, err := exec.Command(testRuntime(t).Executable, "--version").Output()
	require.NoError(t, err)
	assert.Contains(t, string(out), "git version")
}
