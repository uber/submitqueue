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

// Package baserevision resolves the commit a new batch is analyzed against by
// asking a git workspace for the tip of the queue's target branch. Any
// gitworkspace.Factory works, so where git runs is the deployment's choice.
package baserevision

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/uber/submitqueue/platform/extension/gitworkspace"
)

const defaultRemote = "origin"

// fullCommitSHA matches the SHA-1 and SHA-256 object formats. Tango needs the
// full SHA, so an abbreviated one is rejected.
var fullCommitSHA = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// Target locates the branch whose tip is a queue's base revision.
type Target struct {
	// Repo identifies the repository to the workspace factory.
	Repo string

	// Remote is the git remote the branch is fetched from. It defaults to
	// origin.
	Remote string

	// Branch is the target branch.
	Branch string
}

// FromGit returns a function for tango.Params.BaseRevision that fetches the
// target branch of the queue and returns its tip as a full commit SHA, as of
// the call. A queue without a target, or any git failure, is an error. Calls
// that share a Repo are serialized, because concurrent fetches into one clone
// can fail on a ref lock and overwrite each other's FETCH_HEAD.
func FromGit(factory gitworkspace.Factory, targets map[string]Target) func(ctx context.Context, queue string) (string, error) {
	locks := make(map[string]*sync.Mutex, len(targets))
	for _, target := range targets {
		locks[target.Repo] = &sync.Mutex{}
	}

	return func(ctx context.Context, queue string) (string, error) {
		target, ok := targets[queue]
		if !ok {
			return "", fmt.Errorf("no target branch configured for queue %s", queue)
		}
		remote := target.Remote
		if remote == "" {
			remote = defaultRemote
		}

		lock := locks[target.Repo]
		lock.Lock()
		defer lock.Unlock()

		workspace, err := factory.For(ctx, target.Repo)
		if err != nil {
			return "", fmt.Errorf("failed to open a workspace for repo %s: %w", target.Repo, err)
		}
		defer workspace.Close()

		outputs, err := workspace.Exec([]gitworkspace.Command{
			{Alias: "fetch", Bin: "git", Args: []string{"fetch", remote, target.Branch}},
			{Alias: "tip", Bin: "git", Args: []string{"rev-parse", "FETCH_HEAD^{commit}"}},
		})
		if err != nil {
			return "", fmt.Errorf("failed to run git in repo %s: %w", target.Repo, err)
		}
		for _, out := range outputs {
			if out.ExitCode != 0 {
				return "", fmt.Errorf("git %s failed with exit code %d: %s", out.Alias, out.ExitCode, strings.TrimSpace(out.Stderr))
			}
		}
		if len(outputs) != 2 {
			return "", fmt.Errorf("expected 2 git outputs, got %d", len(outputs))
		}

		sha := strings.TrimSpace(outputs[1].Stdout)
		if !fullCommitSHA.MatchString(sha) {
			return "", fmt.Errorf("git returned %q, not a full commit SHA", sha)
		}
		return sha, nil
	}
}
