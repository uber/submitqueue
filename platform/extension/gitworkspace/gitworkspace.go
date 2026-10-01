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

// Package gitworkspace defines the pluggable interface for executing git
// commands in a stateful workspace. The OSS implementation runs commands as
// local subprocesses; alternative deployments wrap their backend's exec API
// in the same contract.
package gitworkspace

//go:generate mockgen -source=gitworkspace.go -destination=mock/gitworkspace_mock.go -package=mock

import "context"

// Command is one command to execute in a git workspace.
type Command struct {
	// Alias identifies this command's Output in the response.
	Alias string
	// Bin is the executable name.
	Bin string
	// Args are the command-line arguments.
	Args []string
	// Stdin is optional standard input.
	Stdin string
}

// Output is the result of one executed command.
type Output struct {
	// Alias matches the Command that produced this output.
	Alias string
	// ExitCode is the process exit code (0 = success, -1 = skipped).
	ExitCode int32
	// Stdout is the captured standard output.
	Stdout string
	// Stderr is the captured standard error.
	Stderr string
}

// Workspace is a stateful git environment where command batches execute
// sequentially. Commands within one Exec call run in order; a non-zero
// exit code skips the remaining commands in that batch. State persists
// across Exec calls on the same Workspace.
type Workspace interface {
	// Exec sends a batch of commands and returns their outputs.
	Exec(commands []Command) ([]Output, error)
	// Close releases the workspace and its resources.
	Close() error
}

// Factory creates Workspace instances bound to a repository.
type Factory interface {
	// For returns a Workspace for the given repository.
	For(ctx context.Context, repo string) (Workspace, error)
}
