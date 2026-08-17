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
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/platform/base/change"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
	gitexectest "github.com/uber/submitqueue/platform/git/exectest"
	gitrepo "github.com/uber/submitqueue/platform/git/repo"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// fixture is a bare "remote" plus a working clone the test authors changes in,
// and a provider reading through its own separate copy of that remote — the
// same three-way arrangement the real deployment has.
type fixture struct {
	t        *testing.T
	git      string
	remote   string
	work     string
	provider changeprovider.ChangeProvider
	repo     *gitrepo.Repo
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	git := gitexectest.Git(t)

	root := t.TempDir()
	f := &fixture{
		t:      t,
		git:    git,
		remote: filepath.Join(root, "remote.git"),
		work:   filepath.Join(root, "work"),
	}

	f.run("", "init", "--bare", "-b", "main", f.remote)
	f.run("", "clone", f.remote, f.work)
	f.run(f.work, "config", "user.name", "Test")
	f.run(f.work, "config", "user.email", "test@example.invalid")
	f.write("seed.txt", "seed\n")
	f.run(f.work, "add", ".")
	f.run(f.work, "commit", "-m", "seed")
	f.run(f.work, "push", "origin", "main")

	repo, err := gitrepo.NewRepo(gitrepo.RepoConfig{
		Git:       git,
		Path:      filepath.Join(root, "copy.git"),
		RemoteURL: f.remote,
		Target:    "main",
	})
	require.NoError(t, err)
	require.NoError(t, repo.Provision(ctx))

	f.repo = repo
	f.provider = New(Params{
		Config:       changeprovider.Config{QueueName: "q"},
		Repo:         repo,
		Logger:       zap.NewNop().Sugar(),
		MetricsScope: tally.NoopScope,
	})
	return f
}

func (f *fixture) run(dir string, args ...string) string {
	f.t.Helper()
	cmd := gitexec.Command(context.Background(), f.git, dir, args...)
	out, err := cmd.CombinedOutput()
	require.NoError(f.t, err, "git %s: %s", strings.Join(args, " "), out)
	return strings.TrimSpace(string(out))
}

func (f *fixture) write(path, contents string) {
	f.t.Helper()
	full := filepath.Join(f.work, path)
	require.NoError(f.t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(f.t, os.WriteFile(full, []byte(contents), 0o644))
}

// push commits the given files on a branch cut from base and pushes it,
// returning the head SHA.
func (f *fixture) push(branch, base string, files map[string]string, message string) string {
	f.t.Helper()
	f.run(f.work, "checkout", "-B", branch, base)
	for path, contents := range files {
		f.write(path, contents)
	}
	f.run(f.work, "add", "-A")
	f.run(f.work, "commit", "-m", message)
	f.run(f.work, "push", "-f", "origin", branch)
	return f.run(f.work, "rev-parse", "HEAD")
}

func (f *fixture) mainSHA() string {
	f.t.Helper()
	return f.run(f.work, "rev-parse", "origin/main")
}

// uri builds the change URI for a branch head, percent-encoding the ref the way
// a real caller must.
func (f *fixture) uri(branch, sha string) string {
	return fmt.Sprintf("git://git.example.com/demo/%s/%s",
		url.PathEscape("refs/heads/"+branch), sha)
}

func (f *fixture) get(uris ...string) []entity.ChangeInfo {
	f.t.Helper()
	infos, err := f.provider.Get(context.Background(), entity.Request{
		Queue:  "q",
		Change: change.Change{URIs: uris},
	})
	require.NoError(f.t, err)
	return infos
}

// pathsOf reduces a result to file paths, which is what a path-keyed analyzer
// reads and the easiest thing to assert exactly.
func pathsOf(info entity.ChangeInfo) []string {
	paths := make([]string, 0, len(info.Details.ChangedFiles))
	for _, f := range info.Details.ChangedFiles {
		paths = append(paths, f.Path)
	}
	return paths
}

func TestGet_ReportsFilesAndLineCounts(t *testing.T) {
	f := newFixture(t)
	head := f.push("feature/one", f.mainSHA(), map[string]string{
		"pkg/a/one.go": "one\ntwo\nthree\n",
	}, "add one")

	infos := f.get(f.uri("feature/one", head))
	require.Len(t, infos, 1)

	require.Len(t, infos[0].Details.ChangedFiles, 1)
	file := infos[0].Details.ChangedFiles[0]
	assert.Equal(t, "pkg/a/one.go", file.Path)
	assert.Equal(t, 3, file.LinesAdded)
	assert.Equal(t, 0, file.LinesDeleted)
	assert.Equal(t, "Test", infos[0].Details.Author.Name)
	assert.Equal(t, "test@example.invalid", infos[0].Details.Author.Email)
}

// TestGet_StackIsReportedPerChange is the assertion the whole baseline design
// exists for. A stack's branches are cut one from the next, so measuring every
// change against the target would report the second as containing the first —
// and a consumer summing line counts over a batch would count them twice.
func TestGet_StackIsReportedPerChange(t *testing.T) {
	f := newFixture(t)
	base := f.mainSHA()

	first := f.push("feature/s1", base, map[string]string{"pkg/a/one.go": "a\n"}, "first")
	second := f.push("feature/s2", first, map[string]string{"pkg/b/two.go": "b\nb\n"}, "second")
	third := f.push("feature/s3", second, map[string]string{"pkg/c/three.go": "c\nc\nc\n"}, "third")

	infos := f.get(
		f.uri("feature/s1", first),
		f.uri("feature/s2", second),
		f.uri("feature/s3", third),
	)
	require.Len(t, infos, 3)

	assert.Equal(t, []string{"pkg/a/one.go"}, pathsOf(infos[0]))
	assert.Equal(t, []string{"pkg/b/two.go"}, pathsOf(infos[1]),
		"the second change must not carry the first's files")
	assert.Equal(t, []string{"pkg/c/three.go"}, pathsOf(infos[2]),
		"nor the third the first two's")

	assert.Equal(t, 1, infos[0].Details.TotalLinesChanged())
	assert.Equal(t, 2, infos[1].Details.TotalLinesChanged())
	assert.Equal(t, 3, infos[2].Details.TotalLinesChanged())
}

func TestGet_MultiCommitChangeIsReportedWhole(t *testing.T) {
	// A change is the whole branch against the target, not its tip commit — so a
	// change built from several commits reports every file it touched.
	f := newFixture(t)
	base := f.mainSHA()

	f.run(f.work, "checkout", "-B", "feature/multi", base)
	for _, name := range []string{"pkg/a/one.go", "pkg/a/two.go", "pkg/a/three.go"} {
		f.write(name, "x\n")
		f.run(f.work, "add", "-A")
		f.run(f.work, "commit", "-m", "add "+name)
	}
	f.run(f.work, "push", "-f", "origin", "feature/multi")
	head := f.run(f.work, "rev-parse", "HEAD")

	infos := f.get(f.uri("feature/multi", head))
	assert.ElementsMatch(t,
		[]string{"pkg/a/one.go", "pkg/a/two.go", "pkg/a/three.go"},
		pathsOf(infos[0]))
}

func TestGet_BinaryFileIsReportedWithoutLineCounts(t *testing.T) {
	f := newFixture(t)
	head := f.push("feature/bin", f.mainSHA(), map[string]string{
		"assets/blob.bin": "\x00\x01\x02\x00binary\x00",
	}, "add a binary")

	infos := f.get(f.uri("feature/bin", head))
	require.Len(t, infos[0].Details.ChangedFiles, 1)

	file := infos[0].Details.ChangedFiles[0]
	assert.Equal(t, "assets/blob.bin", file.Path, "a binary must still be reported as touched")
	assert.Equal(t, 0, file.LinesAdded)
	assert.Equal(t, 0, file.LinesDeleted)
}

// A rename is one record split across extra NUL-delimited fields, which is the
// case a naive parse silently mangles into a path of "".
func TestGet_RenameIsReportedAtItsNewPath(t *testing.T) {
	f := newFixture(t)

	f.run(f.work, "checkout", "main")
	f.write("pkg/a/original.go", strings.Repeat("keep\n", 20))
	f.run(f.work, "add", "-A")
	f.run(f.work, "commit", "-m", "add original")
	f.run(f.work, "push", "origin", "main")

	f.run(f.work, "checkout", "-B", "feature/rename", f.mainSHA())
	require.NoError(t, os.MkdirAll(filepath.Join(f.work, "pkg/b"), 0o755))
	f.run(f.work, "mv", "pkg/a/original.go", "pkg/b/moved.go")
	f.run(f.work, "commit", "-m", "move it")
	f.run(f.work, "push", "-f", "origin", "feature/rename")
	head := f.run(f.work, "rev-parse", "HEAD")

	infos := f.get(f.uri("feature/rename", head))
	assert.Equal(t, []string{"pkg/b/moved.go"}, pathsOf(infos[0]))
}

// The provider's copy is its own, so a change pushed after the copy was made is
// not there until it fetches — the normal case, not an edge one.
func TestGet_FetchesACommitItHasNotSeen(t *testing.T) {
	f := newFixture(t)
	head := f.push("feature/later", f.mainSHA(), map[string]string{"late.txt": "late\n"}, "later")

	require.False(t, f.repo.HasCommit(context.Background(), head),
		"the fixture must start without the commit for this to prove anything")

	infos := f.get(f.uri("feature/later", head))
	assert.Equal(t, []string{"late.txt"}, pathsOf(infos[0]))
}

func TestGet_UnknownCommitIsAnError(t *testing.T) {
	f := newFixture(t)
	missing := "0123456789abcdef0123456789abcdef01234567"

	_, err := f.provider.Get(context.Background(), entity.Request{
		Queue:  "q",
		Change: change.Change{URIs: []string{f.uri("feature/nope", missing)}},
	})
	require.Error(t, err)
}

func TestGet_MalformedURIIsAnError(t *testing.T) {
	f := newFixture(t)

	_, err := f.provider.Get(context.Background(), entity.Request{
		Queue:  "q",
		Change: change.Change{URIs: []string{"not-a-change-uri"}},
	})
	require.Error(t, err)
}

// A change sharing no history with the target is reported as an error rather
// than as a change that happens to touch everything.
func TestGet_UnrelatedHistoryIsAnError(t *testing.T) {
	f := newFixture(t)

	orphan := filepath.Join(t.TempDir(), "orphan")
	f.run("", "clone", f.remote, orphan)
	f.run(orphan, "config", "user.name", "Test")
	f.run(orphan, "config", "user.email", "test@example.invalid")
	f.run(orphan, "checkout", "--orphan", "feature/orphan")
	f.run(orphan, "rm", "-rf", ".")
	require.NoError(t, os.WriteFile(filepath.Join(orphan, "alone.txt"), []byte("alone\n"), 0o644))
	f.run(orphan, "add", "-A")
	f.run(orphan, "commit", "-m", "unrelated")
	f.run(orphan, "push", "-f", "origin", "feature/orphan")
	head := f.run(orphan, "rev-parse", "HEAD")

	_, err := f.provider.Get(context.Background(), entity.Request{
		Queue:  "q",
		Change: change.Change{URIs: []string{f.uri("feature/orphan", head)}},
	})
	require.Error(t, err)
}

// Two queues sharing a repository share its lock, so concurrent reads must not
// interleave git commands against one directory.
func TestGet_ConcurrentReadsAreSerialized(t *testing.T) {
	f := newFixture(t)
	base := f.mainSHA()

	heads := make([]string, 4)
	for i := range heads {
		heads[i] = f.push(fmt.Sprintf("feature/c%d", i), base,
			map[string]string{fmt.Sprintf("pkg/c%d/f.go", i): "x\n"}, fmt.Sprintf("c%d", i))
	}

	var wg sync.WaitGroup
	results := make([][]entity.ChangeInfo, len(heads))
	errs := make([]error, len(heads))
	for i, head := range heads {
		wg.Add(1)
		go func(i int, head string) {
			defer wg.Done()
			results[i], errs[i] = f.provider.Get(context.Background(), entity.Request{
				Queue:  "q",
				Change: change.Change{URIs: []string{f.uri(fmt.Sprintf("feature/c%d", i), head)}},
			})
		}(i, head)
	}
	wg.Wait()

	for i := range heads {
		require.NoError(t, errs[i])
		assert.Equal(t, []string{fmt.Sprintf("pkg/c%d/f.go", i)}, pathsOf(results[i][0]))
	}
}
