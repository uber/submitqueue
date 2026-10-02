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
	// Alias identifies this command's Output in the response. It must be unique
	// within one Exec batch.
	Alias string
	// Bin is the executable name, such as "git". The backend decides which
	// binary it resolves to.
	Bin string
	// Args are the command-line arguments. For git, Args[0] is the subcommand;
	// global options and identity belong to the backend, not here.
	Args []string
	// Stdin is optional standard input.
	Stdin string
}

// Output is the result of one executed command.
type Output struct {
	// Alias matches the Command that produced this output.
	Alias string
	// ExitCode is the process exit code. It is unspecified when Skipped is set.
	ExitCode int32
	// Skipped reports that the command did not run because an earlier command
	// in the same batch failed.
	Skipped bool
	// Stdout is the captured standard output.
	Stdout string
	// Stderr is the captured standard error.
	Stderr string
}

// Workspace is a stateful git environment where command batches execute
// sequentially. Commands within one Exec call run in order; after the first
// non-zero exit, every remaining command is reported Skipped. State persists
// across Exec calls on the same Workspace. A Workspace is used by one
// goroutine at a time.
type Workspace interface {
	// Exec runs a batch and returns one Output per command, in order. A
	// command that exits non-zero is reported in its Output, not as an error;
	// the error is reserved for failing to run the batch at all.
	Exec(commands []Command) ([]Output, error)
	// Close releases the workspace and its resources.
	Close() error
}

// Factory creates Workspace instances bound to a repository.
type Factory interface {
	// For returns a Workspace for the given repository. Cancelling ctx aborts
	// any running command and ends the Workspace, so ctx bounds its lifetime.
	For(ctx context.Context, repo string) (Workspace, error)
}
