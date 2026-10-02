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

package gitexec

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
)

// Runtime identifies the explicitly provided Git runtime that commands run on.
type Runtime struct {
	// Executable is the absolute path to the Git executable.
	Executable string
	// ExecPath is the absolute directory containing Git's helper executables.
	ExecPath string
	// TemplateDir is the absolute directory containing Git's repository
	// templates.
	TemplateDir string
	// PassthroughEnv names additional environment variables to inherit from
	// the parent process, on top of the auth and transport ones always passed
	// through. For a deployment whose remote needs something unusual; leave
	// empty otherwise. Names that could alter merge semantics do not belong
	// here — the scrubbed environment is what keeps a merge reproducible.
	PassthroughEnv []string
}

// Validate reports whether every path of the runtime is set and absolute.
func (r Runtime) Validate() error {
	for _, field := range []struct{ name, path string }{
		{"executable", r.Executable},
		{"exec path", r.ExecPath},
		{"template dir", r.TemplateDir},
	} {
		if field.path == "" {
			return fmt.Errorf("git runtime %s is required", field.name)
		}
		if !filepath.IsAbs(field.path) {
			return fmt.Errorf("git runtime %s must be absolute: %q", field.name, field.path)
		}
	}
	return nil
}

// Command constructs a Git command without inheriting the caller's
// environment. The executable and helper paths come from the pinned runtime;
// repository-local configuration remains an intentional input.
func (r Runtime) Command(ctx context.Context, dir string, args ...string) *exec.Cmd {
	gitArgs := make([]string, 0, len(args)+3)
	gitArgs = append(gitArgs,
		"--exec-path="+r.ExecPath,
		"-c", "init.templateDir="+r.TemplateDir,
	)
	gitArgs = append(gitArgs, args...)

	cmd := exec.CommandContext(ctx, r.Executable, gitArgs...)
	cmd.Dir = dir
	// HOME and XDG_CONFIG_HOME are isolated to the checkout rather than inherited,
	// so the runtime's literals — appended last — override any HOME a deployment
	// passed through. The remaining literals pin git's runtime; the scrub set and
	// transport variables come from Env.
	cmd.Env = Env(EnvOptions{
		Transport:   true,
		Passthrough: r.PassthroughEnv,
		Literal: []string{
			"HOME=" + filepath.Join(dir, ".submitqueue-git-home"),
			"XDG_CONFIG_HOME=" + filepath.Join(dir, ".submitqueue-git-home", "xdg"),
			"GIT_EXEC_PATH=" + r.ExecPath,
			"GIT_TEMPLATE_DIR=" + r.TemplateDir,
			"LC_ALL=C",
			"LANG=C",
		},
	})
	return cmd
}
