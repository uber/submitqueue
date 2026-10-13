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

package gitworkspace

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	giterrs "github.com/uber/submitqueue/platform/errs/git"
)

func TestCommandFailure(t *testing.T) {
	tests := []struct {
		name          string
		cmd           Command
		out           Output
		wantOperation string
		wantContains  []string
	}{
		{
			name:          "operation is the git subcommand",
			cmd:           Command{Alias: "fetch-target", Bin: "git", Args: []string{"fetch", "origin", "main"}},
			out:           Output{ExitCode: 128, Stderr: "fatal: unable to access remote"},
			wantOperation: "fetch",
			wantContains:  []string{"exit 128", "unable to access remote"},
		},
		{
			name:          "diagnostic keeps stdout and stderr",
			cmd:           Command{Alias: "pick", Bin: "git", Args: []string{"cherry-pick", "abc"}},
			out:           Output{ExitCode: 1, Stderr: "hint: resolve conflicts", Stdout: "CONFLICT (content)"},
			wantOperation: "cherry-pick",
			wantContains:  []string{"hint: resolve conflicts", "CONFLICT (content)"},
		},
		{
			name:          "no output still reports the exit code",
			cmd:           Command{Alias: "rev", Bin: "git", Args: []string{"rev-parse", "HEAD"}},
			out:           Output{ExitCode: 1},
			wantOperation: "rev-parse",
			wantContains:  []string{"exit 1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CommandFailure(context.Background(), tt.cmd, tt.out)

			var failure giterrs.CommandFailure
			require.True(t, errors.As(err, &failure))
			assert.Equal(t, tt.wantOperation, failure.Operation())
			for _, fragment := range tt.wantContains {
				assert.Contains(t, failure.Diagnostic(), fragment)
			}
		})
	}
}

func TestCommandFailure_CancelledContextTakesPrecedence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := CommandFailure(ctx, Command{Args: []string{"fetch", "origin"}}, Output{ExitCode: -1, Stderr: "signal: killed"})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
