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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	gitmerger "github.com/uber/submitqueue/runway/extension/merger/git"
)

// resolveGitRuntime determines the pinned git runtime the merger invokes.
//
// The merger requires all three paths to be absolute and present — it refuses
// to construct otherwise, deliberately, so that a merge can never silently pick
// up a different git than the one the deployment intended. Requiring an
// operator to supply all three by hand turns that safeguard into a boot-time
// trap, so each is derived from the installed git when its environment
// override is unset. GIT_EXECUTABLE, GIT_EXEC_PATH, and GIT_TEMPLATE_DIR still
// take precedence, which is how a deployment pins a git other than the one on
// PATH.
func resolveGitRuntime(ctx context.Context) (gitmerger.GitRuntime, error) {
	executable, err := resolveGitExecutable()
	if err != nil {
		return gitmerger.GitRuntime{}, err
	}

	execPath, err := resolveGitExecPath(ctx, executable)
	if err != nil {
		return gitmerger.GitRuntime{}, err
	}

	templateDir, err := resolveGitTemplateDir(execPath)
	if err != nil {
		return gitmerger.GitRuntime{}, err
	}

	return gitmerger.GitRuntime{
		Executable:  executable,
		ExecPath:    execPath,
		TemplateDir: templateDir,
	}, nil
}

// resolveGitExecutable returns the absolute path to the git binary.
func resolveGitExecutable() (string, error) {
	if v := strings.TrimSpace(os.Getenv("GIT_EXECUTABLE")); v != "" {
		return absolutePath("GIT_EXECUTABLE", v)
	}
	found, err := exec.LookPath("git")
	if err != nil {
		return "", fmt.Errorf("no git on PATH and GIT_EXECUTABLE is unset: %w", err)
	}
	return absolutePath("git resolved from PATH", found)
}

// resolveGitExecPath returns the directory holding git's helper executables,
// asking the resolved binary itself rather than guessing.
func resolveGitExecPath(ctx context.Context, executable string) (string, error) {
	if v := strings.TrimSpace(os.Getenv("GIT_EXEC_PATH")); v != "" {
		return absolutePath("GIT_EXEC_PATH", v)
	}

	cmd := exec.CommandContext(ctx, executable, "--exec-path")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s --exec-path: %w: %s", executable, err, strings.TrimSpace(stderr.String()))
	}
	out := strings.TrimSpace(stdout.String())
	if out == "" {
		return "", fmt.Errorf("%s --exec-path returned nothing; set GIT_EXEC_PATH", executable)
	}
	return absolutePath("git --exec-path", out)
}

// resolveGitTemplateDir locates git's repository templates.
//
// Git exposes no query for this the way it does for the exec path, but it does
// install both under one prefix — the exec path is <prefix>/lib/git-core or
// <prefix>/libexec/git-core, and the templates sit at
// <prefix>/share/git-core/templates. Deriving from the exec path therefore
// keeps the templates matched to the same installation, which a hardcoded
// system path would not.
func resolveGitTemplateDir(execPath string) (string, error) {
	if v := strings.TrimSpace(os.Getenv("GIT_TEMPLATE_DIR")); v != "" {
		return absolutePath("GIT_TEMPLATE_DIR", v)
	}

	prefix := filepath.Dir(filepath.Dir(execPath))
	candidates := []string{
		filepath.Join(prefix, "share", "git-core", "templates"),
		"/usr/share/git-core/templates",
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf(
		"could not locate git templates near exec path %q (tried %s); set GIT_TEMPLATE_DIR",
		execPath, strings.Join(candidates, ", "),
	)
}

// absolutePath makes a resolved runtime path absolute, naming where it came
// from so a bad value points at the thing that supplied it.
func absolutePath(source, path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%s: could not resolve %q to an absolute path: %w", source, path, err)
	}
	return abs, nil
}
