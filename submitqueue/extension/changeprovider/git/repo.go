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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// RepoConfig describes one local copy of a remote.
type RepoConfig struct {
	// Git is the path to the git binary. Empty resolves through GIT_EXECUTABLE
	// and then PATH.
	Git string
	// Path is where this service keeps its own copy. It belongs to this service
	// alone: another service reading the same remote keeps its own.
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

// Repo is one local copy of a remote, shared by every provider built over it.
//
// Bare, because nothing here checks anything out: the copy answers questions
// about commits and never produces one. That also means no index and no working
// tree to leave dirty between operations.
type Repo struct {
	// mu serializes access. Git commands against one repository cannot safely
	// interleave, and every provider sharing this copy shares this lock.
	mu  sync.Mutex
	cfg RepoConfig
}

// NewRepo returns a Repo for cfg, resolving the git binary. It touches no disk;
// Provision does that.
func NewRepo(cfg RepoConfig) (*Repo, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("git change provider: a repository path is required")
	}
	if cfg.RemoteURL == "" {
		return nil, fmt.Errorf("git change provider: a remote URL is required")
	}
	if cfg.Target == "" {
		return nil, fmt.Errorf("git change provider: a target branch is required")
	}
	if cfg.Remote == "" {
		cfg.Remote = "origin"
	}

	git, err := resolveGit(cfg.Git)
	if err != nil {
		return nil, err
	}
	cfg.Git = git
	return &Repo{cfg: cfg}, nil
}

// Provision creates the copy if it is not already there and points it at the
// remote, leaving an existing copy's objects alone.
//
// Callers run this at wiring time rather than on first use: resolving a
// provider happens once per message on the validate path, so a copy created
// there would put a clone inside a retry loop and hide a bad remote behind
// queue processing rather than failing the service that owns it.
func (r *Repo) Provision(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

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
	return r.configureRemote(ctx)
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

// ensureCommit guarantees sha is present locally, fetching if it is not.
//
// By SHA first, which needs the server to allow a want for an object it does
// not advertise (github.com does); the change's own ref is the fallback for a
// server that does not. Neither is shallow — a merge base needs ancestry.
func (r *Repo) ensureCommit(ctx context.Context, sha, ref string) error {
	if r.hasCommit(ctx, sha) {
		return nil
	}
	if err := r.applyAuth(ctx); err != nil {
		return err
	}

	if _, err := r.run(ctx, "fetch", r.cfg.Remote, sha); err == nil && r.hasCommit(ctx, sha) {
		return nil
	}
	if ref != "" {
		if _, err := r.run(ctx, "fetch", r.cfg.Remote, ref); err == nil && r.hasCommit(ctx, sha) {
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

// fetchTarget updates the target branch, which is the baseline a change's first
// commit is measured from and moves as other changes land.
func (r *Repo) fetchTarget(ctx context.Context) error {
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

func (r *Repo) hasCommit(ctx context.Context, sha string) bool {
	_, err := r.run(ctx, "cat-file", "-e", sha+"^{commit}")
	return err == nil
}

// mergeBase returns the commit two revisions diverged from. Absence of one is
// reported as an error rather than an empty diff: a change sharing no history
// with what it claims to land on is a fact worth surfacing, not a change that
// touches nothing.
func (r *Repo) mergeBase(ctx context.Context, a, b string) (string, error) {
	base, err := r.run(ctx, "merge-base", a, b)
	if err != nil {
		return "", fmt.Errorf("%s and %s share no history: %w", a, b, err)
	}
	return base, nil
}

// run executes git inside the copy.
//
// The environment is replaced rather than inherited, for the reason the merger
// records: ambient configuration — a hooks path, a commit template, a signing
// requirement — is exactly what makes a scripted git behave differently on two
// machines. What survives is what reaching a remote needs and what cannot
// change an answer: the SSH agent, TLS roots, and proxy settings.
func (r *Repo) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, r.cfg.Git, args...)
	cmd.Dir = r.cfg.Path
	cmd.Env = commandEnv()

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
	return strings.TrimSpace(string(out)), nil
}

// output runs git and returns stdout untrimmed, for commands whose output is
// NUL-delimited and whose trailing separator is part of the format.
func (r *Repo) output(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, r.cfg.Git, args...)
	cmd.Dir = r.cfg.Path
	cmd.Env = commandEnv()

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

// scrubbedEnv is the configuration-denying half of a git invocation.
var scrubbedEnv = []string{
	"GIT_CONFIG_NOSYSTEM=1",
	"GIT_CONFIG_GLOBAL=" + os.DevNull,
	"GIT_ATTR_NOSYSTEM=1",
	"GIT_TERMINAL_PROMPT=0",
	"GIT_PAGER=cat",
	"GIT_EDITOR=:",
}

// transportEnvNames are inherited when set. None can change what a diff says;
// all of them decide whether a remote can be reached at all.
var transportEnvNames = []string{
	"SSH_AUTH_SOCK",
	"SSH_AGENT_PID",
	"PATH",
	"HOME",
	"GIT_SSH",
	"GIT_SSH_COMMAND",
	"GIT_SSH_VARIANT",
	"GIT_SSL_CAINFO",
	"GIT_SSL_CAPATH",
	"SSL_CERT_DIR",
	"SSL_CERT_FILE",
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
	"http_proxy", "https_proxy", "no_proxy",
}

func commandEnv() []string {
	env := make([]string, 0, len(scrubbedEnv)+len(transportEnvNames))
	env = append(env, scrubbedEnv...)
	for _, name := range transportEnvNames {
		if value, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+value)
		}
	}
	return env
}

// resolveGit locates the git binary, preferring an explicit path, then
// GIT_EXECUTABLE, then PATH — the convention the rest of the repository uses.
func resolveGit(path string) (string, error) {
	candidate := strings.TrimSpace(path)
	if candidate == "" {
		candidate = strings.TrimSpace(os.Getenv("GIT_EXECUTABLE"))
	}
	if candidate == "" {
		found, err := exec.LookPath("git")
		if err != nil {
			return "", fmt.Errorf("git change provider: no git binary found: %w", err)
		}
		candidate = found
	}
	absolute, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("git change provider: %q is not a usable path: %w", candidate, err)
	}
	if info, err := os.Stat(absolute); err != nil || info.IsDir() {
		return "", fmt.Errorf("git change provider: %q is not an executable file", absolute)
	}
	return absolute, nil
}
