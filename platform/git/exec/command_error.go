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

import "os/exec"

// CommandError preserves the failed Git operation and its process error for
// backend-specific classification after callers add contextual wrapping.
type CommandError struct {
	operation string
	message   string
	cause     error
}

// NewCommandError records a failed Git operation without assigning retry
// policy. Callers supply the rendered diagnostic they want Error to expose.
func NewCommandError(operation, message string, cause error) *CommandError {
	return &CommandError{
		operation: operation,
		message:   message,
		cause:     cause,
	}
}

// Error returns the command diagnostic supplied by the execution boundary.
func (e *CommandError) Error() string {
	if e.message != "" {
		return e.message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return "git command failed"
}

// Unwrap returns the process error reported by os/exec.
func (e *CommandError) Unwrap() error {
	return e.cause
}

// Operation returns the Git subcommand, such as fetch or cherry-pick.
func (e *CommandError) Operation() string {
	return e.operation
}

// Diagnostic returns Git's rendered failure output.
func (e *CommandError) Diagnostic() string {
	return e.message
}

// ProcessExited reports whether Git started and returned a non-zero exit.
func (e *CommandError) ProcessExited() bool {
	_, ok := e.cause.(*exec.ExitError)
	return ok
}

func commandOperation(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}
