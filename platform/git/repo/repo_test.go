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

package gitrepo

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gitexec "github.com/uber/submitqueue/platform/git/exec"
	gitexectest "github.com/uber/submitqueue/platform/git/exectest"
)

// remote is a bare "remote" plus a working clone the test commits into, driving
// the Bazel-pinned git so the assertions describe the build the services run.
type remote struct {
	t    *testing.T
	git  string
	url  string
	work string
}

func newRemote(t *testing.T) *remote {
	t.Helper()
	root := t.TempDir()
	r := &remote{
		t:    t,
		git:  gitexectest.Git(t),
		url:  filepath.Join(root, "remote.git"),
		work: filepath.Join(root, "work"),
	}
	r.run("", "init", "--bare", "-b", "main", r.url)
	r.run("", "clone", r.url, r.work)
	r.run(r.work, "config", "user.name", "Test")
	r.run(r.work, "config", "user.email", "test@example.invalid")
	require.NoError(t, os.WriteFile(filepath.Join(r.work, "seed.txt"), []byte("seed\n"), 0o644))
	r.run(r.work, "add", ".")
	r.run(r.work, "commit", "-m", "seed")
	r.run(r.work, "push", "origin", "main")
	return r
}

func (r *remote) run(dir string, args ...string) string {
	r.t.Helper()
	out, err := gitexec.Command(context.Background(), r.git, dir, args...).CombinedOutput()
	require.NoError(r.t, err, "git %s: %s", strings.Join(args, " "), out)
	return strings.TrimSpace(string(out))
}

// pushFeature commits a file on a new branch and returns its head SHA and ref.
func (r *remote) pushFeature(branch, contents string) (sha, ref string) {
	r.t.Helper()
	r.run(r.work, "checkout", "-B", branch, "main")
	require.NoError(r.t, os.WriteFile(filepath.Join(r.work, "feature.txt"), []byte(contents), 0o644))
	r.run(r.work, "add", "-A")
	r.run(r.work, "commit", "-m", "feature")
	r.run(r.work, "push", "-f", "origin", branch)
	return r.run(r.work, "rev-parse", "HEAD"), "refs/heads/" + branch
}

func TestProvision_IsIdempotentAndKeepsWhatItFetched(t *testing.T) {
	ctx := context.Background()
	rem := newRemote(t)
	head, ref := rem.pushFeature("feature/keep", "keep\n")

	repo, err := NewRepo(RepoConfig{
		Git:       rem.git,
		Path:      filepath.Join(t.TempDir(), "copy.git"),
		RemoteURL: rem.url,
		Target:    "main",
	})
	require.NoError(t, err)
	require.NoError(t, repo.Provision(ctx))

	require.False(t, repo.HasCommit(ctx, head), "provisioning fetches only the target")
	require.NoError(t, repo.EnsureCommit(ctx, head, ref))
	require.True(t, repo.HasCommit(ctx, head))

	require.NoError(t, repo.Provision(ctx))
	assert.True(t, repo.HasCommit(ctx, head),
		"re-provisioning must not discard objects already fetched")
}

func TestNewRepo_RejectsAnIncompleteConfiguration(t *testing.T) {
	git := gitexectest.Git(t)
	for _, tt := range []struct {
		name string
		cfg  RepoConfig
	}{
		{name: "no path", cfg: RepoConfig{Git: git, RemoteURL: "u", Target: "main"}},
		{name: "no remote url", cfg: RepoConfig{Git: git, Path: "p", Target: "main"}},
		{name: "no target", cfg: RepoConfig{Git: git, Path: "p", RemoteURL: "u"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRepo(tt.cfg)
			require.Error(t, err)
		})
	}
}
