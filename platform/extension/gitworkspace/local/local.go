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

// Package local implements gitworkspace.Workspace by executing commands as
// local subprocesses against a git clone on disk.
package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/uber/submitqueue/platform/extension/gitworkspace"
)

// notRunExitCode is the exit code reported for a command that never ran to
// completion: one skipped after a failure, or one the OS could not start.
const notRunExitCode = -1

// gitBin is the Bin that the runtime and committer identity apply to.
const gitBin = "git"

// Params configures a local workspace factory.
type Params struct {
	// Dir is the path to the local git clone.
	Dir string
	// Runtime pins the executable, helper paths and scrubbed environment that
	// git commands run on. When zero, git is resolved from PATH and inherits the
	// caller's environment.
	Runtime Runtime
	// CommitterName and CommitterEmail identify the committer of every commit a
	// git command creates. Set both or neither; when unset, the repository and
	// environment configuration decide.
	CommitterName  string
	CommitterEmail string
}

type factory struct {
	dir     string
	runtime Runtime
	// gitConfig is prepended to every git command's arguments.
	gitConfig []string
}

// NewFactory creates a gitworkspace.Factory that runs commands as local
// subprocesses in the given directory. It fails when a set Runtime is invalid
// or only half of the committer identity is set.
func NewFactory(p Params) (gitworkspace.Factory, error) {
	if p.Runtime.Executable != "" {
		if err := p.Runtime.Validate(); err != nil {
			return nil, err
		}
	}
	if (p.CommitterName == "") != (p.CommitterEmail == "") {
		return nil, fmt.Errorf("committer name and email must be set together")
	}
	f := &factory{dir: p.Dir, runtime: p.Runtime}
	if p.CommitterName != "" {
		f.gitConfig = []string{
			"-c", "user.name=" + p.CommitterName,
			"-c", "user.email=" + p.CommitterEmail,
			"-c", "commit.gpgsign=false",
		}
	}
	return f, nil
}

func (f *factory) For(ctx context.Context, _ string) (gitworkspace.Workspace, error) {
	return &workspace{ctx: ctx, factory: f}, nil
}

type workspace struct {
	ctx context.Context
	*factory
}

func (w *workspace) Exec(commands []gitworkspace.Command) ([]gitworkspace.Output, error) {
	outputs := make([]gitworkspace.Output, 0, len(commands))
	for i, cmd := range commands {
		out := w.run(cmd)
		outputs = append(outputs, out)
		if out.ExitCode != 0 {
			for _, remaining := range commands[i+1:] {
				outputs = append(outputs, gitworkspace.Output{
					Alias:    remaining.Alias,
					ExitCode: notRunExitCode,
					Skipped:  true,
				})
			}
			break
		}
	}
	return outputs, nil
}

func (w *workspace) command(cmd gitworkspace.Command) *exec.Cmd {
	if cmd.Bin != gitBin {
		c := exec.CommandContext(w.ctx, cmd.Bin, cmd.Args...)
		c.Dir = w.dir
		return c
	}
	args := append(append(make([]string, 0, len(w.gitConfig)+len(cmd.Args)), w.gitConfig...), cmd.Args...)
	if w.runtime.Executable == "" {
		c := exec.CommandContext(w.ctx, gitBin, args...)
		c.Dir = w.dir
		return c
	}
	return w.runtime.Command(w.ctx, w.dir, args...)
}

func (w *workspace) run(cmd gitworkspace.Command) gitworkspace.Output {
	c := w.command(cmd)
	if cmd.Stdin != "" {
		c.Stdin = strings.NewReader(cmd.Stdin)
	}
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr

	err := c.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return gitworkspace.Output{
				Alias:    cmd.Alias,
				ExitCode: int32(exitErr.ExitCode()),
				Stdout:   stdout.String(),
				Stderr:   stderr.String(),
			}
		}
		return gitworkspace.Output{
			Alias:    cmd.Alias,
			ExitCode: notRunExitCode,
			Stderr:   err.Error(),
		}
	}
	return gitworkspace.Output{
		Alias:    cmd.Alias,
		ExitCode: 0,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
	}
}

func (w *workspace) Close() error { return nil }
