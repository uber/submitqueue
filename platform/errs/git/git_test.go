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

package git

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

type classifierFixtures struct {
	exitError  error
	startError error
}

func setupClassifierFixtures(t *testing.T) classifierFixtures {
	t.Helper()

	err := exec.Command(os.Args[0], "-test.run=[").Run()
	require.Error(t, err)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)

	return classifierFixtures{
		exitError:  exitErr,
		startError: &exec.Error{Name: "git", Err: exec.ErrNotFound},
	}
}

func TestClassifier(t *testing.T) {
	fixtures := setupClassifierFixtures(t)
	tests := []struct {
		name string
		err  error
		want errs.Verdict
	}{
		{
			name: "remote fetch exit is retryable dependency failure",
			err:  gitexec.NewCommandError("fetch", "temporary remote failure", fixtures.exitError),
			want: errs.InfraDependencyRetryable,
		},
		{
			name: "remote push exit is retryable dependency failure",
			err:  gitexec.NewCommandError("push", "temporary remote failure", fixtures.exitError),
			want: errs.InfraDependencyRetryable,
		},
		{
			name: "remote authentication failure is permanent dependency failure",
			err:  gitexec.NewCommandError("fetch", "fatal: Authentication failed", fixtures.exitError),
			want: errs.InfraDependency,
		},
		{
			name: "local reset exit is retryable infrastructure failure",
			err:  gitexec.NewCommandError("reset", "checkout unavailable", fixtures.exitError),
			want: errs.InfraRetryable,
		},
		{
			name: "invalid local refspec is permanent infrastructure failure",
			err:  gitexec.NewCommandError("reset", "fatal: invalid refspec", fixtures.exitError),
			want: errs.Infra,
		},
		{
			name: "local configuration exit is permanent infrastructure failure",
			err:  gitexec.NewCommandError("config", "invalid configuration", fixtures.exitError),
			want: errs.Infra,
		},
		{
			name: "process start failure remains non-retryable",
			err:  gitexec.NewCommandError("fetch", "git executable missing", fixtures.startError),
			want: errs.Unknown,
		},
		{
			name: "unknown operation remains non-retryable",
			err:  gitexec.NewCommandError("unknown", "unsupported command", fixtures.exitError),
			want: errs.Unknown,
		},
		{
			name: "plain error remains unknown",
			err:  errors.New("anything"),
			want: errs.Unknown,
		},
		{
			name: "nil remains unknown",
			err:  nil,
			want: errs.Unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_AppliedViaProcessor(t *testing.T) {
	fixtures := setupClassifierFixtures(t)
	tests := []struct {
		name           string
		err            error
		wantRetryable  bool
		wantDependency bool
		wantSame       bool
	}{
		{
			name:           "wrapped fetch failure is retryable dependency",
			err:            fmt.Errorf("reset checkout: %w", gitexec.NewCommandError("fetch", "connection reset", fixtures.exitError)),
			wantRetryable:  true,
			wantDependency: true,
		},
		{
			name:          "wrapped cherry-pick process failure is retryable locally",
			err:           fmt.Errorf("apply change: %w", gitexec.NewCommandError("cherry-pick", "process killed", fixtures.exitError)),
			wantRetryable: true,
		},
		{
			name:     "configuration failure stays non-retryable",
			err:      fmt.Errorf("prepare checkout: %w", gitexec.NewCommandError("config", "invalid key", fixtures.exitError)),
			wantSame: true,
		},
		{
			name:           "authentication failure stays non-retryable dependency",
			err:            fmt.Errorf("fetch target: %w", gitexec.NewCommandError("fetch", "fatal: Authentication failed", fixtures.exitError)),
			wantDependency: true,
			wantSame:       false,
		},
		{
			name:     "unknown error stays non-retryable",
			err:      errors.New("anything"),
			wantSame: true,
		},
	}

	processor := errs.NewClassifierProcessor(Classifier)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := processor.Process(tt.err)
			assert.Equal(t, tt.wantRetryable, errs.IsRetryable(got))
			assert.Equal(t, tt.wantDependency, errs.IsDependencyError(got))
			if tt.wantSame {
				assert.Same(t, tt.err, got)
			}
		})
	}
}
