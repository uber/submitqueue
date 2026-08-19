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

// Command gitsandbox provisions the bare repository the git provider merges
// into (`make local-submitqueue-start PROVIDER=git`).
//
// A merge target has to exist before Runway starts: the merger clones it at
// boot and fails outright if it cannot. So this runs ahead of the stack,
// creating a bare repository with one commit on the target branch — the
// smallest thing a merge can be performed against.
//
// It is idempotent. An already-initialized repository is left exactly as it is,
// so restarting the stack keeps whatever previous runs landed rather than
// resetting the history someone may be looking at.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

// seedFile is committed so the target branch exists with something on it. A
// branch with no commits cannot be cloned or merged into.
const seedFile = "README.md"

const seedContents = `# SubmitQueue sandbox

Created by ` + "`make local-submitqueue-start PROVIDER=git`" + `. Everything below the
seed commit was landed by SubmitQueue.
`

func main() {
	git := flag.String("git", "", "path to the git binary; defaults to GIT_EXECUTABLE, then PATH")
	sandboxDir := flag.String("sandbox-dir", "", "directory holding the bare repository (required)")
	checkoutDir := flag.String("checkout-dir", "", "directory Runway provisions its working trees in")
	repoName := flag.String("repo-name", "sandbox", "bare repository name, without the .git suffix")
	branch := flag.String("branch", "main", "target branch created by the seed commit")
	flag.Parse()

	if err := run(context.Background(), *git, *sandboxDir, *checkoutDir, *repoName, *branch); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, git, sandboxDir, checkoutDir, repoName, branch string) error {
	if sandboxDir == "" {
		return fmt.Errorf("-sandbox-dir is required")
	}

	resolved, err := gitexec.Resolve(git)
	if err != nil {
		return err
	}

	bare := filepath.Join(sandboxDir, repoName+".git")
	created, err := provision(ctx, resolved, bare, branch)
	if err != nil {
		return err
	}

	// Docker creates a missing bind-mount source itself, owned by root, which
	// on rootful Docker leaves a directory the host user cannot clean up.
	if checkoutDir != "" {
		if err := os.MkdirAll(checkoutDir, 0o755); err != nil {
			return fmt.Errorf("could not create checkout directory %q: %w", checkoutDir, err)
		}
	}

	if created {
		fmt.Printf("Initialized sandbox repository at %s (branch %s)\n", bare, branch)
	} else {
		fmt.Printf("Reusing sandbox repository at %s\n", bare)
	}
	return nil
}

// provision creates and seeds the bare repository, reporting whether it did any
// work. An existing repository is left alone.
func provision(ctx context.Context, git, bare, branch string) (bool, error) {
	// HEAD is written by `git init` before anything else, so its presence marks
	// a repository that has at least been initialized.
	if _, err := os.Stat(filepath.Join(bare, "HEAD")); err == nil {
		// Settings are re-applied rather than assumed, so a sandbox created by
		// an older version gains them without having to be thrown away.
		return false, configure(ctx, git, bare)
	}

	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		return false, fmt.Errorf("could not create sandbox directory: %w", err)
	}

	if err := create(ctx, git, bare, branch); err != nil {
		// A repository that is initialized but has no commit cannot be merged
		// into, and the next run would skip it as already provisioned — so the
		// stack would fail at boot instead of being repaired. Leave nothing
		// behind rather than something unusable.
		if removeErr := os.RemoveAll(bare); removeErr != nil {
			return false, fmt.Errorf("%w (and the partial repository at %s could not be removed: %v)", err, bare, removeErr)
		}
		return false, err
	}
	return true, nil
}

// create initializes the bare repository and puts the first commit on its
// target branch.
func create(ctx context.Context, git, bare, branch string) error {
	if err := gitexec.Run(ctx, git, "", "init", "--bare", "-b", branch, bare); err != nil {
		return err
	}
	if err := configure(ctx, git, bare); err != nil {
		return err
	}
	return seed(ctx, git, bare, branch)
}

// configure applies the settings the sandbox needs to be both readable and
// safe to share between the host and the containers.
func configure(ctx context.Context, git, bare string) error {
	settings := [][2]string{
		// Bare repositories do not log ref updates by default, and the reflog
		// is how a reader confirms a whole stack landed in a single push.
		{"core.logAllRefUpdates", "true"},

		// No automatic housekeeping, which here is a correctness setting rather
		// than a tuning one.
		//
		// Git runs `gc --auto` after every push by default, and packing refs
		// rewrites packed-refs wholesale. That is safe on one filesystem, where
		// the replacement is a rename nobody can observe halfway — but this
		// repository is written from the host and read from inside a container
		// through a bind mount, and that boundary does not preserve the
		// guarantee. A run pushing fifty branches gives fifty chances for the
		// merger to read a packed-refs truncated mid-name and fail a land that
		// had nothing wrong with it. Left unpacked, there is nothing to rewrite;
		// loose refs cost a sandbox that gets deleted nothing.
		{"gc.auto", "0"},
		{"receive.autogc", "false"},
		{"maintenance.auto", "false"},
	}

	for _, s := range settings {
		if err := gitexec.Run(ctx, git, bare, "config", s[0], s[1]); err != nil {
			return err
		}
	}
	return nil
}

// seed commits an initial file on the target branch, through a throwaway clone
// because a bare repository has no working tree to commit from.
func seed(ctx context.Context, git, bare, branch string) error {
	work, err := os.MkdirTemp("", "sq-sandbox-seed-")
	if err != nil {
		return fmt.Errorf("could not create a working clone: %w", err)
	}
	defer os.RemoveAll(work)

	if err := gitexec.Run(ctx, git, "", "clone", bare, work); err != nil {
		return err
	}
	// An identity is required to commit, and the ambient one is unavailable:
	// gitexec strips the global config precisely so a developer's hooks and
	// signing settings cannot fail this.
	for _, kv := range [][2]string{
		{"user.name", "SubmitQueue Sandbox"},
		{"user.email", "sandbox@submitqueue.invalid"},
	} {
		if err := gitexec.Run(ctx, git, work, "config", kv[0], kv[1]); err != nil {
			return err
		}
	}

	if err := os.WriteFile(filepath.Join(work, seedFile), []byte(seedContents), 0o644); err != nil {
		return fmt.Errorf("could not write the seed file: %w", err)
	}
	if err := gitexec.Run(ctx, git, work, "add", seedFile); err != nil {
		return err
	}
	if err := gitexec.Run(ctx, git, work, "commit", "-m", "seed the sandbox"); err != nil {
		return err
	}
	return gitexec.Run(ctx, git, work, "push", "origin", branch)
}
