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
)

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

// Operation returns the Git subcommand in args, or "" when args is empty.
func Operation(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

// CommandFailure builds the error for a failed git invocation.
//
// A context that has ended takes precedence over whatever git reported. When
// os/exec kills a child because its context was cancelled, Wait reports only
// the death — a bare *exec.ExitError reading "signal: killed" — and neither
// context.Canceled nor context.DeadlineExceeded appears anywhere in the
// chain, so nothing downstream can tell an interrupted command apart from one
// that genuinely failed. Reading ctx.Err() here is the only place that
// distinction still exists; surfacing it puts cancellation back in the chain
// where the generic classifier can recognise it.
func CommandFailure(ctx context.Context, args []string, message string, cause error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("git %s: %s: %w", Operation(args), message, ctxErr)
	}
	return NewCommandError(Operation(args), message, cause)
}
