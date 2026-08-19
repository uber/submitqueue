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

// Package gitrepo keeps a local, bare copy of a git remote and answers
// questions about its commits.
//
// It is transport plumbing, not domain logic: it fetches, resolves commits,
// and computes merge bases, but never decides what those facts mean. A reader
// that derives change metadata — files, line counts, author — drives a copy
// through this package and interprets the raw git output itself.
//
// The copy is bare because nothing here checks anything out, so there is no
// working tree to leave dirty and no index to corrupt. Git commands against one
// copy cannot safely interleave, so a Repo carries a lock (it embeds
// sync.Mutex) that every reader sharing the copy holds across a sequence of
// commands. Command environment and git-binary resolution come from
// platform/git/exec, the one source of truth every git caller shares.
package gitrepo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

// RepoConfig describes one local copy of a remote.
type RepoConfig struct {
	// Git is the path to the git binary. Empty resolves through GIT_EXECUTABLE
	// and then PATH.
	Git string
	// Path is where this copy lives on disk. It belongs to one owner: another
	// reader of the same remote keeps its own.
	Path string
	// RemoteURL is where the copy fetches from — a URL or a local path.
	RemoteURL string
	// Remote is the name the copy records RemoteURL under.
	Remote string
	// Target is the branch a change's diff is measured against.
	Target string
	// Auth prepares the copy to reach RemoteURL. Nil when it needs nothing.
	Auth Auth
}

// Repo is one local, bare copy of a remote, shared by every reader built over
// it. The embedded mutex serializes git commands against the copy; a reader
// holds it across any sequence that must see a consistent object set.
type Repo struct {
	sync.Mutex
	cfg RepoConfig
}

// NewRepo returns a Repo for cfg, resolving the git binary. It touches no disk;
// Provision does that.
func NewRepo(cfg RepoConfig) (*Repo, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("gitrepo: a repository path is required")
	}
	if cfg.RemoteURL == "" {
		return nil, fmt.Errorf("gitrepo: a remote URL is required")
	}
	if cfg.Target == "" {
		return nil, fmt.Errorf("gitrepo: a target branch is required")
	}
	if cfg.Remote == "" {
		cfg.Remote = "origin"
	}

	git, err := gitexec.Resolve(cfg.Git)
	if err != nil {
		return nil, err
	}
	cfg.Git = git
	return &Repo{cfg: cfg}, nil
}

// Remote is the name the copy records its remote URL under.
func (r *Repo) Remote() string { return r.cfg.Remote }

// Target is the branch a change's diff is measured against.
func (r *Repo) Target() string { return r.cfg.Target }

// Provision creates the copy if it is not already there and points it at the
// remote, leaving an existing copy's objects alone.
//
// Callers run this at wiring time rather than on first use: a reader is often
// resolved once per message on a retry-driven path, so a copy created there
// would put a clone inside a retry loop and hide a bad remote behind queue
// processing rather than failing the service that owns it.
func (r *Repo) Provision(ctx context.Context) error {
	r.Lock()
	defer r.Unlock()

	if err := os.MkdirAll(r.cfg.Path, 0o755); err != nil {
		return fmt.Errorf("could not create repository directory %q: %w", r.cfg.Path, err)
	}
	// HEAD is written by `git init` before anything else, so its presence marks
	// a repository that exists. An existing one keeps whatever it has fetched.
	if _, err := os.Stat(filepath.Join(r.cfg.Path, "HEAD")); err != nil {
		if _, err := r.run(ctx, "init", "--bare", "-b", r.cfg.Target); err != nil {
			// A half-initialized directory would be skipped as existing on the
			// next run, so leave nothing rather than something unusable.
			os.RemoveAll(r.cfg.Path)
			return err
		}
	}
	if err := r.configureRemote(ctx); err != nil {
		return err
	}

	// Fetch once here, which is what makes provisioning worth doing at startup
	// at all: an unreachable remote, a wrong URL, or a credential that does not
	// work fails the service that is misconfigured. Initializing a directory and
	// recording a remote would succeed against a remote that does not exist.
	return r.FetchTarget(ctx)
}

// configureRemote records the remote, correcting it if the configuration
// changed since the copy was made.
func (r *Repo) configureRemote(ctx context.Context) error {
	existing, err := r.run(ctx, "remote")
	if err != nil {
		return err
	}
	for _, name := range strings.Fields(existing) {
		if name == r.cfg.Remote {
			_, err := r.run(ctx, "remote", "set-url", r.cfg.Remote, r.cfg.RemoteURL)
			return err
		}
	}
	_, err = r.run(ctx, "remote", "add", r.cfg.Remote, r.cfg.RemoteURL)
	return err
}

// EnsureCommit guarantees sha is present locally, fetching if it is not.
//
// By SHA first, which needs the server to allow a want for an object it does
// not advertise (github.com does); the change's own ref is the fallback for a
// server that does not. Neither is shallow — a merge base needs ancestry.
func (r *Repo) EnsureCommit(ctx context.Context, sha, ref string) error {
	if r.HasCommit(ctx, sha) {
		return nil
	}
	if err := r.applyAuth(ctx); err != nil {
		return err
	}

	if _, err := r.run(ctx, "fetch", r.cfg.Remote, sha); err == nil && r.HasCommit(ctx, sha) {
		return nil
	}
	if ref != "" {
		if _, err := r.run(ctx, "fetch", r.cfg.Remote, ref); err == nil && r.HasCommit(ctx, sha) {
			return nil
		}
	}

	// Separate "the remote cannot be reached" from "the remote does not have
	// this commit". Only the first is worth trying again.
	if _, err := r.run(ctx, "ls-remote", "--exit-code", r.cfg.Remote, "HEAD"); err != nil {
		return fmt.Errorf("remote %s unreachable while resolving commit %s: %w", r.cfg.Remote, sha, err)
	}
	return fmt.Errorf("commit %s is not available from remote %s (tried by SHA and via %q)", sha, r.cfg.Remote, ref)
}

// FetchTarget updates the target branch, which is the baseline a change's first
// commit is measured from and moves as other changes land.
func (r *Repo) FetchTarget(ctx context.Context) error {
	if err := r.applyAuth(ctx); err != nil {
		return err
	}
	_, err := r.run(ctx, "fetch", r.cfg.Remote, r.cfg.Target)
	return err
}

func (r *Repo) applyAuth(ctx context.Context) error {
	if r.cfg.Auth == nil {
		return nil
	}
	return r.cfg.Auth.Apply(ctx, r.cfg.Path, r.cfg.RemoteURL)
}

// HasCommit reports whether sha is present in the copy.
func (r *Repo) HasCommit(ctx context.Context, sha string) bool {
	_, err := r.run(ctx, "cat-file", "-e", sha+"^{commit}")
	return err == nil
}

// MergeBase returns the commit two revisions diverged from. Absence of one is
// reported as an error rather than an empty diff: a change sharing no history
// with what it claims to land on is a fact worth surfacing, not a change that
// touches nothing.
func (r *Repo) MergeBase(ctx context.Context, a, b string) (string, error) {
	base, err := r.run(ctx, "merge-base", a, b)
	if err != nil {
		return "", fmt.Errorf("%s and %s share no history: %w", a, b, err)
	}
	return base, nil
}

// command builds a git invocation inside the copy, carrying the shared scrub set
// plus the transport variables a fetch needs. HOME is passed through so git can
// find the user's SSH known_hosts and credential store; it is not in the shared
// transport set, so this package asks for it explicitly.
func (r *Repo) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, r.cfg.Git, args...)
	cmd.Dir = r.cfg.Path
	cmd.Env = gitexec.Env(gitexec.EnvOptions{Transport: true, Passthrough: []string{"HOME"}})
	return cmd
}

// run executes git inside the copy and returns trimmed stdout.
func (r *Repo) run(ctx context.Context, args ...string) (string, error) {
	out, err := r.outputOf(ctx, args...)
	return strings.TrimSpace(out), err
}

// RunRaw executes git inside the copy and returns stdout untrimmed, for commands
// whose output is NUL-delimited and whose trailing separator is part of the
// format.
func (r *Repo) RunRaw(ctx context.Context, args ...string) (string, error) {
	return r.outputOf(ctx, args...)
}

func (r *Repo) outputOf(ctx context.Context, args ...string) (string, error) {
	cmd := r.command(ctx, args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
	}
	return string(out), nil
}

// SetConfig writes one local configuration value into the repository at path.
//
// Exported for an Auth implementation, which configures a repository from
// outside this package and would otherwise have to find and run git itself.
func SetConfig(ctx context.Context, path, key, value string) error {
	git, err := gitexec.Resolve("")
	if err != nil {
		return err
	}
	cmd := gitexec.Command(ctx, git, path, "config", key, value)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("git config %s: %s", key, message)
	}
	return nil
}
