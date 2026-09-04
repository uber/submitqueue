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
	"sync"

	gitchange "github.com/uber/submitqueue/platform/base/change/git"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
	"github.com/uber/submitqueue/submitqueue/client"
)

// gitRemote is the authority in the change URIs this source mints. The lander
// reads the ref and commit out of a URI and reaches the repository through its
// own configured remote, so this identifies a change rather than routing to it
// — which is why a local sandbox can carry a hostname it does not answer to.
const gitRemote = "git.example.com"

// gitSource opens changes as branches in a bare repository on disk. There is no
// provider and no pull request: a change is a branch, and landing it is a real
// fetch, cherry-pick and push.
type gitSource struct {
	git  string
	repo string
	// work is the clone changes are authored in, owned by this source and
	// removed when the run ends.
	work string
	// mu serializes git commands. A single working tree cannot take concurrent
	// checkouts and commits — the index is one file — so -concurrency stops at
	// this boundary. That costs nothing: the flag exists because opening a pull
	// request is several round trips to a provider, and there is no provider
	// here.
	mu sync.Mutex
}

// newGitSource clones the sandbox repository into a scratch directory the
// caller must close.
func newGitSource(ctx context.Context, git, bare, repo string) (*gitSource, error) {
	if _, err := os.Stat(bare); err != nil {
		return nil, fmt.Errorf(
			"no sandbox repository at %s; start the stack with `make local-submitqueue-start PROVIDER=git`: %w", bare, err)
	}

	work, err := os.MkdirTemp("", "sq-demo-work-")
	if err != nil {
		return nil, fmt.Errorf("could not create a working clone: %w", err)
	}
	if err := gitexec.Run(ctx, git, "", "clone", bare, work); err != nil {
		os.RemoveAll(work)
		return nil, err
	}
	// An identity is needed to commit, and gitexec strips the ambient config so
	// a developer's own settings cannot fail the run.
	for _, kv := range [][2]string{
		{"user.name", "SubmitQueue Demo"},
		{"user.email", "demo@submitqueue.invalid"},
	} {
		if err := gitexec.Run(ctx, git, work, "config", kv[0], kv[1]); err != nil {
			os.RemoveAll(work)
			return nil, err
		}
	}
	return &gitSource{git: git, repo: repo, work: work}, nil
}

func (s *gitSource) close() {
	os.RemoveAll(s.work)
}

func (s *gitSource) baseSHA(ctx context.Context, branch string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := gitexec.Run(ctx, s.git, s.work, "fetch", "origin"); err != nil {
		return "", err
	}
	return gitexec.Output(ctx, s.git, s.work, "rev-parse", "origin/"+branch)
}

func (s *gitSource) open(ctx context.Context, spec changeSpec) (openedChange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	spec.note("creating branch %s", spec.branch)
	if err := gitexec.Run(ctx, s.git, s.work, "fetch", "origin"); err != nil {
		return openedChange{}, err
	}
	// Cut from the parent commit rather than the parent branch: in a stack that
	// commit was pushed a moment ago, and naming it exactly is what keeps the
	// chain in the order the run intended.
	if err := gitexec.Run(ctx, s.git, s.work, "checkout", "-B", spec.branch, spec.parentSHA); err != nil {
		return openedChange{}, fmt.Errorf("branch %s from %s: %w", spec.branch, spec.parentSHA, err)
	}

	for k, file := range spec.files {
		spec.note("committing %s (%d/%d)", file.path, k+1, len(spec.files))
		full := filepath.Join(s.work, file.path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return openedChange{}, fmt.Errorf("create %s: %w", filepath.Dir(file.path), err)
		}
		if err := os.WriteFile(full, []byte(file.body), 0o644); err != nil {
			return openedChange{}, fmt.Errorf("write %s: %w", file.path, err)
		}
		if err := gitexec.Run(ctx, s.git, s.work, "add", "--", file.path); err != nil {
			return openedChange{}, err
		}
		if err := gitexec.Run(ctx, s.git, s.work, "commit", "-m", file.message); err != nil {
			return openedChange{}, err
		}
	}

	spec.note("pushing %s", spec.branch)
	if err := gitexec.Run(ctx, s.git, s.work, "push", "-f", "origin", spec.branch); err != nil {
		return openedChange{}, fmt.Errorf("push %s: %w", spec.branch, err)
	}
	headSHA, err := gitexec.Output(ctx, s.git, s.work, "rev-parse", "HEAD")
	if err != nil {
		return openedChange{}, err
	}

	return openedChange{
		headSHA: headSHA,
		// Nothing is stated about what this change touches. The orchestrator
		// keeps its own copy of this repository and reads that out of the
		// commits, which is the whole difference between this rung and the fake
		// one — and what makes a change pushed by hand behave the same as one
		// from here.
		uri: gitchange.ChangeID{
			Scheme: "git", Remote: gitRemote, Repo: s.repo,
			Ref: "refs/heads/" + spec.branch, CommitSHA: headSHA,
		}.String(),
		// No pull request to number, so the branch names the change. Empty URL:
		// a branch in a bare repository has nothing to open.
		cell: client.Cell{Text: spec.branch},
	}, nil
}
