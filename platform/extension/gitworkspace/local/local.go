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
	"os/exec"
	"strings"

	"github.com/uber/submitqueue/platform/extension/gitworkspace"
)

// notRunExitCode is the exit code reported for a command that never ran to
// completion: one skipped after a failure, or one the OS could not start.
const notRunExitCode = -1

// Params configures a local workspace factory.
type Params struct {
	// Dir is the path to the local git clone.
	Dir string
}

type factory struct {
	dir string
}

// NewFactory creates a gitworkspace.Factory that runs commands as local
// subprocesses in the given directory.
func NewFactory(p Params) gitworkspace.Factory {
	return &factory{dir: p.Dir}
}

func (f *factory) For(ctx context.Context, _ string) (gitworkspace.Workspace, error) {
	return &workspace{ctx: ctx, dir: f.dir}, nil
}

type workspace struct {
	ctx context.Context
	dir string
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

func (w *workspace) run(cmd gitworkspace.Command) gitworkspace.Output {
	c := exec.CommandContext(w.ctx, cmd.Bin, cmd.Args...)
	c.Dir = w.dir
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
