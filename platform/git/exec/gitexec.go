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

// Package gitexec locates a git binary and composes the environment git runs
// in. It is the single source of truth for that environment across every
// SubmitQueue caller — demo tooling, the change provider's repository, and the
// Runway merger.
//
// The environment has two halves. The scrub set denies git all ambient
// configuration that could change what a command produces — a global hooks
// path, a signing requirement, a commit template — which is what makes a
// scripted run behave the same on every machine. The transport set carries
// what a command needs to reach a remote — the SSH agent socket, git's ssh and
// credential helpers on PATH, TLS roots, proxy settings — none of which can
// change an answer. Every caller shares both halves; they differ only in the
// literal entries they add (a pinned exec path, an isolated HOME), which is why
// Env takes those as options rather than baking one caller's policy in.
//
// HOME is deliberately not in the transport set: a caller that isolates HOME
// and a caller that inherits it disagree, so each supplies it itself.
package gitexec

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Resolve returns an absolute path to a git binary.
//
// Preference order is the supplied path, then GIT_EXECUTABLE, then PATH — the
// same convention the Runway server's runtime resolution follows, so a
// deployment or a Bazel target can pin git without the caller knowing which
// did.
func Resolve(path string) (string, error) {
	candidate := strings.TrimSpace(path)
	source := "-git"
	if candidate == "" {
		candidate, source = strings.TrimSpace(os.Getenv("GIT_EXECUTABLE")), "GIT_EXECUTABLE"
	}
	if candidate == "" {
		found, err := exec.LookPath("git")
		if err != nil {
			return "", fmt.Errorf("no git on PATH, and neither -git nor GIT_EXECUTABLE is set: %w", err)
		}
		candidate, source = found, "git resolved from PATH"
	}

	absolute, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("%s: could not resolve %q to an absolute path: %w", source, candidate, err)
	}
	if info, err := os.Stat(absolute); err != nil || info.IsDir() {
		return "", fmt.Errorf("%s: %q is not an executable file", source, absolute)
	}
	return absolute, nil
}

// scrubEnv denies git every ambient configuration input that could change what
// a command produces. Always applied, first, so a later entry can override it.
var scrubEnv = []string{
	"GIT_CONFIG_NOSYSTEM=1",
	"GIT_CONFIG_GLOBAL=" + os.DevNull,
	"GIT_ATTR_NOSYSTEM=1",
	"GIT_TERMINAL_PROMPT=0",
	"GIT_PAGER=cat",
	"GIT_EDITOR=:",
}

// transportEnvNames are inherited from the parent process when set. None can
// change what a command produces; each decides whether a remote is reachable.
// HOME is intentionally absent — see the package doc.
var transportEnvNames = []string{
	"PATH",
	"SSH_AUTH_SOCK", "SSH_AGENT_PID",
	"GIT_SSH", "GIT_SSH_COMMAND", "GIT_SSH_VARIANT",
	"GIT_SSL_CAINFO", "GIT_SSL_CAPATH",
	"SSL_CERT_DIR", "SSL_CERT_FILE",
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
	"http_proxy", "https_proxy", "no_proxy",
}

// EnvOptions selects what, on top of the always-applied scrub set, a git
// command's environment carries.
type EnvOptions struct {
	// Transport inherits the transport variables from the parent when set.
	Transport bool
	// Passthrough names further variables to inherit from the parent when set.
	Passthrough []string
	// Literal entries are appended last as "NAME=value", so they override any
	// inherited value of the same name.
	Literal []string
}

// Env composes a git command environment: the scrub set, then the requested
// variables inherited from the parent (only those actually set, so an unset
// SSH_AUTH_SOCK stays absent rather than becoming empty), then the literals.
func Env(opts EnvOptions) []string {
	env := make([]string, 0, len(scrubEnv)+len(transportEnvNames)+len(opts.Passthrough)+len(opts.Literal))
	env = append(env, scrubEnv...)

	names := make([]string, 0, len(transportEnvNames)+len(opts.Passthrough))
	if opts.Transport {
		names = append(names, transportEnvNames...)
	}
	names = append(names, opts.Passthrough...)

	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	return append(env, opts.Literal...)
}

// Command builds a git invocation in dir with the ambient environment removed.
// An empty dir runs in the current working directory. PATH is passed so git can
// find its helpers; nothing else the host sets reaches the command.
func Command(ctx context.Context, git, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	cmd.Env = Env(EnvOptions{Literal: []string{"PATH=" + os.Getenv("PATH")}})
	return cmd
}

// Run executes a git command and discards its output, reporting stderr in the
// error so a failure says what git said rather than only that it exited.
func Run(ctx context.Context, git, dir string, args ...string) error {
	_, err := Output(ctx, git, dir, args...)
	return err
}

// Output executes a git command and returns its trimmed stdout.
func Output(ctx context.Context, git, dir string, args ...string) (string, error) {
	cmd := Command(ctx, git, dir, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), NewCommandError(commandOperation(args), message, err))
	}
	return strings.TrimSpace(string(out)), nil
}
