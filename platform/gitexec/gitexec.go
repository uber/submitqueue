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

// Package gitexec locates a git binary and runs it with the ambient
// environment stripped out.
//
// Demo and development tooling drives git on a developer's own machine, where
// hooks, a signing key, or a commit template configured globally would each
// break a run in a way that has nothing to do with SubmitQueue. Every command
// built here therefore carries the same scrubbed environment the git merger
// uses (see runway/extension/merger/git), so tooling behaves the same on every
// machine.
//
// This resolves only the executable, because tooling runs porcelain
// (init, clone, commit, push) rather than constructing a merger's GitRuntime,
// which additionally pins the exec path and template directory.
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

// Command builds a git invocation in dir with the ambient environment removed.
// An empty dir runs in the current working directory.
func Command(ctx context.Context, git, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, git, args...)
	cmd.Dir = dir
	// A developer's global config is the usual reason a scripted git run fails
	// on one machine and not another: a hooks path, a signing requirement, or a
	// commit template. None of it is relevant to seeding a sandbox.
	cmd.Env = []string{
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_ATTR_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_PAGER=cat",
		"GIT_EDITOR=:",
		"PATH=" + os.Getenv("PATH"),
	}
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
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
	}
	return strings.TrimSpace(string(out)), nil
}
