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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitchange "github.com/uber/submitqueue/platform/base/change/git"
	"github.com/uber/submitqueue/platform/fakemarker"
	"github.com/uber/submitqueue/platform/gitexec"
	"github.com/uber/submitqueue/platform/gitexec/gitexectest"
)

// quietSpec is a changeSpec whose progress notes go nowhere, for tests that
// care about what was created rather than what was reported.
func quietSpec(branch, parentBranch, parentSHA string, files ...changeFile) changeSpec {
	return changeSpec{
		branch:       branch,
		parentBranch: parentBranch,
		parentSHA:    parentSHA,
		title:        "demo change",
		files:        files,
		note:         func(string, ...any) {},
	}
}

func TestWithFiles_NamesOnePathPerDirectory(t *testing.T) {
	uri := withFiles("git://git.example.com/r/x/y", []changeFile{
		{path: "demo/alpha/one.txt"},
		{path: "demo/alpha/two.txt"},
		{path: "demo/beta/three.txt"},
	})

	assert.Equal(t, []string{"demo/alpha/one.txt", "demo/beta/three.txt"}, fakemarker.Files([]string{uri}),
		"a second file in a directory already named adds no key the analyzer does not have")
}

// The gateway rejects a change URI over 255 bytes, so a change touching many
// directories must lose paths rather than produce a request that cannot be
// submitted at all.
func TestWithFiles_StaysWithinTheURIBudget(t *testing.T) {
	base := "git://git.example.com/sandbox/refs%2Fheads%2Fdemo%2F0814-154238%2F1/" + strings.Repeat("a", 40)
	files := make([]changeFile, 0, 20)
	for i := range 20 {
		files = append(files, changeFile{path: fmt.Sprintf("demo/area-%02d/0814-154238-1-%d.txt", i, i)})
	}

	uri := withFiles(base, files)
	assert.LessOrEqual(t, len(uri), maxChangeURIBytes)
	assert.NotEmpty(t, fakemarker.Files([]string{uri}), "some paths must still be reported")
}

func TestWithFiles_LeavesTheURIAloneWithNoFiles(t *testing.T) {
	assert.Equal(t, "git://git.example.com/r/x/y", withFiles("git://git.example.com/r/x/y", nil))
}

// Every URI a run submits has to survive the gateway's validation, marker and
// all — which is what the first attempt at this got wrong.
func TestSources_ProduceSubmittableURIs(t *testing.T) {
	files := make([]changeFile, 0, 8)
	for k := 1; k <= 8; k++ {
		files = append(files, changeFile{path: changeFilePath("0814-154238", 5, 1, k)})
	}
	spec := quietSpec("demo/0814-154238/1", "main", strings.Repeat("b", 40), files...)

	opened, err := fakeSource{}.open(context.Background(), spec)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(opened.uri), maxChangeURIBytes)
	_, err = gitchange.ParseChangeID(opened.uri)
	assert.NoError(t, err)
}

func TestFakeSource_MintsAParsableChangeURI(t *testing.T) {
	opened, err := fakeSource{}.open(context.Background(), quietSpec("demo/0102-030405/1", "main", "abc"))
	require.NoError(t, err)

	// The gateway parses what this mints, so a URI it would reject is a bug
	// here rather than a surprise at land time.
	id, err := gitchange.ParseChangeID(opened.uri)
	require.NoError(t, err)
	assert.Equal(t, "refs/heads/demo/0102-030405/1", id.Ref,
		"the ref must survive percent-encoding, slashes and all")
	assert.Equal(t, opened.headSHA, id.CommitSHA)
	assert.Len(t, opened.headSHA, 40)
}

func TestFakeSource_GivesEachChangeItsOwnCommit(t *testing.T) {
	ctx := context.Background()
	first, err := fakeSource{}.open(ctx, quietSpec("demo/run/1", "main", "abc"))
	require.NoError(t, err)
	second, err := fakeSource{}.open(ctx, quietSpec("demo/run/2", "main", "abc"))
	require.NoError(t, err)

	assert.NotEqual(t, first.headSHA, second.headSHA,
		"two changes sharing a head would land as one")
}

// A change with no repository behind it has nothing to open, and a cell with a
// URL renders as a link — so it must not carry one.
func TestFakeSource_LabelsChangesWithTheBranchAndNoLink(t *testing.T) {
	opened, err := fakeSource{}.open(context.Background(), quietSpec("demo/run/1", "main", "abc"))
	require.NoError(t, err)

	assert.Equal(t, "demo/run/1", opened.cell.Text)
	assert.Empty(t, opened.cell.URL)
}

func TestFakeSource_IsReproducible(t *testing.T) {
	ctx := context.Background()
	first, err := fakeSource{}.open(ctx, quietSpec("demo/run/1", "main", "abc"))
	require.NoError(t, err)
	again, err := fakeSource{}.open(ctx, quietSpec("demo/run/1", "main", "abc"))
	require.NoError(t, err)

	assert.Equal(t, first.uri, again.uri, "replaying a run must submit the same change")
}

// sandbox creates a bare repository with one commit on main, standing in for
// what tool/gitsandbox provisions.
func sandbox(t *testing.T, git string) string {
	t.Helper()
	ctx := context.Background()
	bare := filepath.Join(t.TempDir(), "sandbox.git")

	require.NoError(t, gitexec.Run(ctx, git, "", "init", "--bare", "-b", "main", bare))
	seed := t.TempDir()
	require.NoError(t, gitexec.Run(ctx, git, "", "clone", bare, seed))
	require.NoError(t, gitexec.Run(ctx, git, seed, "config", "user.name", "Test"))
	require.NoError(t, gitexec.Run(ctx, git, seed, "config", "user.email", "test@example.invalid"))
	require.NoError(t, os.WriteFile(filepath.Join(seed, "seed.txt"), []byte("seed\n"), 0o644))
	require.NoError(t, gitexec.Run(ctx, git, seed, "add", "."))
	require.NoError(t, gitexec.Run(ctx, git, seed, "commit", "-m", "seed"))
	require.NoError(t, gitexec.Run(ctx, git, seed, "push", "origin", "main"))
	return bare
}

func TestGitSource_PushesABranchWithACommitPerFile(t *testing.T) {
	git := gitexectest.Git(t)
	ctx := context.Background()
	bare := sandbox(t, git)

	src, err := newGitSource(ctx, git, bare, "sandbox")
	require.NoError(t, err)
	defer src.close()

	base, err := src.baseSHA(ctx, "main")
	require.NoError(t, err)

	opened, err := src.open(ctx, quietSpec("demo/run/1", "main", base,
		changeFile{path: "a/one.txt", body: "one\n", message: "add one"},
		changeFile{path: "b/two.txt", body: "two\n", message: "add two"},
	))
	require.NoError(t, err)

	subjects, err := gitexec.Output(ctx, git, bare, "log", "--reverse", "--format=%s", "main..refs/heads/demo/run/1")
	require.NoError(t, err)
	assert.Equal(t, []string{"add one", "add two"}, strings.Split(subjects, "\n"),
		"each file must arrive as its own commit, in order")

	head, err := gitexec.Output(ctx, git, bare, "rev-parse", "refs/heads/demo/run/1")
	require.NoError(t, err)
	assert.Equal(t, head, opened.headSHA, "the URI must pin the commit that was pushed")

	contents, err := gitexec.Output(ctx, git, bare, "show", "refs/heads/demo/run/1:a/one.txt")
	require.NoError(t, err)
	assert.Equal(t, "one", contents)
}

func TestGitSource_MintsAURIPinningTheHead(t *testing.T) {
	git := gitexectest.Git(t)
	ctx := context.Background()
	src, err := newGitSource(ctx, git, sandbox(t, git), "sandbox")
	require.NoError(t, err)
	defer src.close()

	base, err := src.baseSHA(ctx, "main")
	require.NoError(t, err)
	opened, err := src.open(ctx, quietSpec("demo/run/1", "main", base,
		changeFile{path: "one.txt", body: "one\n", message: "add one"}))
	require.NoError(t, err)

	id, err := gitchange.ParseChangeID(opened.uri)
	require.NoError(t, err)
	assert.Equal(t, "sandbox", id.Repo)
	assert.Equal(t, "refs/heads/demo/run/1", id.Ref)
	assert.Equal(t, opened.headSHA, id.CommitSHA)
	assert.Equal(t, "demo/run/1", opened.cell.Text)
	assert.Empty(t, opened.cell.URL, "a branch in a bare repository has nothing to open")
}

// A stack is cut from the previous change's head rather than the base, which is
// what makes the chain land in the order it was built.
func TestGitSource_StacksOnAPreviousChange(t *testing.T) {
	git := gitexectest.Git(t)
	ctx := context.Background()
	bare := sandbox(t, git)
	src, err := newGitSource(ctx, git, bare, "sandbox")
	require.NoError(t, err)
	defer src.close()

	base, err := src.baseSHA(ctx, "main")
	require.NoError(t, err)
	first, err := src.open(ctx, quietSpec("demo/run/1", "main", base,
		changeFile{path: "one.txt", body: "one\n", message: "add one"}))
	require.NoError(t, err)
	second, err := src.open(ctx, quietSpec("demo/run/2", "demo/run/1", first.headSHA,
		changeFile{path: "two.txt", body: "two\n", message: "add two"}))
	require.NoError(t, err)

	require.NoError(t, gitexec.Run(ctx, git, bare, "merge-base", "--is-ancestor", first.headSHA, second.headSHA),
		"the second change must build on the first")
}

// One working tree cannot take concurrent checkouts and commits. createIndependent
// runs up to -concurrency of these at once, so the source has to serialize them
// itself; without the lock this corrupts the index or races the branch.
func TestGitSource_SerializesConcurrentChanges(t *testing.T) {
	git := gitexectest.Git(t)
	ctx := context.Background()
	bare := sandbox(t, git)
	src, err := newGitSource(ctx, git, bare, "sandbox")
	require.NoError(t, err)
	defer src.close()

	base, err := src.baseSHA(ctx, "main")
	require.NoError(t, err)

	const changes = 5
	var wg sync.WaitGroup
	errs := make([]error, changes)
	for i := range changes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			branch := fmt.Sprintf("demo/run/%d", i+1)
			_, errs[i] = src.open(ctx, quietSpec(branch, "main", base,
				changeFile{path: fmt.Sprintf("file-%d.txt", i+1), body: "x\n", message: "add " + branch}))
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "change %d failed", i+1)
	}
	for i := range changes {
		branch := fmt.Sprintf("refs/heads/demo/run/%d", i+1)
		_, err := gitexec.Output(ctx, git, bare, "rev-parse", branch)
		assert.NoError(t, err, "%s must exist", branch)
	}
}

func TestNewChangeSource_GitRejectsAMissingSandbox(t *testing.T) {
	_, _, err := newChangeSource(context.Background(), config{
		provider:   providerGit,
		git:        gitexectest.Git(t),
		sandboxDir: filepath.Join(t.TempDir(), "nothing-here"),
	})
	require.Error(t, err, "a missing sandbox must say so rather than fail later mid-run")
}
