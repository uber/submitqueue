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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"

	"github.com/uber/submitqueue/platform/errs"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

func gitError(operation, diagnostic string) error {
	return gitexec.NewCommandError(operation, diagnostic, errors.New("exit status 128"))
}

type remoteCommandFailure struct {
	operation  string
	diagnostic string
}

func (e remoteCommandFailure) Error() string {
	return e.diagnostic
}

func (e remoteCommandFailure) Operation() string {
	return e.operation
}

func (e remoteCommandFailure) Diagnostic() string {
	return e.diagnostic
}

func TestClassifyCommand(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		diagnostic string
		want       errs.Verdict
	}{
		{
			name:       "transport fault on fetch is a retryable dependency failure",
			operation:  "fetch",
			diagnostic: "fatal: unable to access 'https://host/r.git/': Connection reset by peer",
			want:       errs.InfraDependencyRetryable,
		},
		{
			name:       "unresolvable host on ls-remote is a retryable dependency failure",
			operation:  "ls-remote",
			diagnostic: "fatal: Could not resolve host: github.example.com",
			want:       errs.InfraDependencyRetryable,
		},
		{
			name:       "checkout contention on a local commit is a retryable local failure",
			operation:  "commit",
			diagnostic: "fatal: Unable to create '/checkout/.git/index.lock': File exists.",
			want:       errs.InfraRetryable,
		},
		{
			name:       "checkout contention during fetch is attributed to the remote it ran against",
			operation:  "fetch",
			diagnostic: "error: cannot lock ref 'refs/remotes/origin/main'",
			want:       errs.InfraDependencyRetryable,
		},
		{
			name:       "transport fragment on a local operation is not evidence of a transient failure",
			operation:  "merge",
			diagnostic: "error: could not resolve host mentioned in a commit message",
			want:       errs.Infra,
		},
		{
			name:       "unknown revision is a permanent local failure",
			operation:  "rev-parse",
			diagnostic: "fatal: ambiguous argument 'origin/main': unknown revision or path not in the working tree.",
			want:       errs.Infra,
		},
		{
			name:       "empty squash commit is a permanent local failure",
			operation:  "commit",
			diagnostic: "exit status 1",
			want:       errs.Infra,
		},
		{
			name:      "failure with no diagnostic at all is a permanent local failure",
			operation: "cat-file",
			want:      errs.Infra,
		},
		{
			name:       "path outside the repository is a permanent local failure",
			operation:  "clean",
			diagnostic: "fatal: '/etc': '/etc' is outside repository at '/checkout'",
			want:       errs.Infra,
		},
		{
			name:       "non-fast-forward push is a permanent dependency failure",
			operation:  "push",
			diagnostic: "! [rejected]        main -> main (fetch first)",
			want:       errs.InfraDependency,
		},
		{
			name:       "authentication failure is a permanent dependency failure",
			operation:  "fetch",
			diagnostic: "fatal: Authentication failed for 'https://host/r.git/'",
			want:       errs.InfraDependency,
		},
		{
			// Git prints this trailer under permanent failures too — a missing
			// remote, absent access rights — so it is not evidence of a
			// transient one.
			name:       "generic remote trailer is a permanent dependency failure",
			operation:  "fetch",
			diagnostic: "fatal: 'origin' does not appear to be a git repository\nfatal: Could not read from remote repository.",
			want:       errs.InfraDependency,
		},
		{
			name:       "unknown subcommand is a permanent local failure",
			operation:  "bisect",
			diagnostic: "fatal: something went wrong",
			want:       errs.Infra,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ClassifyCommand(tt.operation, tt.diagnostic))
		})
	}
}

func TestClassifier(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want errs.Verdict
	}{
		{
			name: "local process failure",
			err:  gitError("fetch", "fatal: Connection reset by peer"),
			want: errs.InfraDependencyRetryable,
		},
		{
			name: "remote execution failure",
			err: remoteCommandFailure{
				operation:  "commit",
				diagnostic: "fatal: Unable to create '/checkout/.git/index.lock': File exists.",
			},
			want: errs.InfraRetryable,
		},
		{
			name: "non-Git error is not this classifier's node",
			err:  errors.New("anything"),
			want: errs.Unknown,
		},
		{
			name: "nil is not this classifier's node",
			want: errs.Unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_FragmentsMatchRegardlessOfCase(t *testing.T) {
	for _, fragment := range transientTransportFragments {
		t.Run(fragment, func(t *testing.T) {
			shouted := "fatal: " + strings.ToUpper(fragment)
			assert.Equal(t, errs.InfraDependencyRetryable, ClassifyCommand("fetch", shouted))
		})
	}
	for _, fragment := range transientCheckoutFragments {
		t.Run(fragment, func(t *testing.T) {
			shouted := "fatal: " + strings.ToUpper(fragment)
			assert.Equal(t, errs.InfraRetryable, ClassifyCommand("commit", shouted))
		})
	}
}

func TestClassifier_AppliedViaProcessor(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantRetryable  bool
		wantDependency bool
		wantUnwrapped  bool
	}{
		{
			name:           "wrapped transport fault is a retryable dependency",
			err:            fmt.Errorf("reset checkout: %w", gitError("fetch", "fatal: Connection reset by peer")),
			wantRetryable:  true,
			wantDependency: true,
		},
		{
			name:          "wrapped checkout contention is retryable locally",
			err:           fmt.Errorf("apply change: %w", gitError("cherry-pick", "fatal: Unable to create '.git/index.lock': File exists.")),
			wantRetryable: true,
		},
		{
			name: "wrapped unknown revision is non-retryable",
			err:  fmt.Errorf("resolve tip: %w", gitError("rev-parse", "fatal: ambiguous argument 'origin/main'")),
		},
		{
			name:           "wrapped rejected push is a non-retryable dependency",
			err:            fmt.Errorf("promote: %w", gitError("push", "! [rejected] main -> main (fetch first)")),
			wantDependency: true,
		},
		{
			name:          "explicit user wrap wins over the classifier",
			err:           errs.NewUserError(gitError("fetch", "fatal: Connection reset by peer")),
			wantUnwrapped: true,
		},
		{
			name:          "unrecognised error is returned unchanged",
			err:           errors.New("anything"),
			wantUnwrapped: true,
		},
	}

	processor := errs.NewClassifierProcessor(Classifier)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := processor.Process(tt.err)
			assert.Equal(t, tt.wantRetryable, errs.IsRetryable(got))
			assert.Equal(t, tt.wantDependency, errs.IsDependencyError(got))
			if tt.wantUnwrapped {
				assert.Same(t, tt.err, got)
			}
		})
	}
}

// A git process killed because its context ended reports only "signal: killed",
// so gitexec.CommandFailure is what puts the cancellation back in the chain.
// This classifier must stay out of the way for the generic one to see it.
func TestClassifier_LeavesCancellationToTheGenericClassifier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := gitexec.CommandFailure(ctx, []string{"fetch", "origin"}, "signal: killed", errors.New("signal: killed"))
	assert.Equal(t, errs.Unknown, Classifier.Classify(err), "a cancelled command is not a CommandError node")

	processed := errs.NewClassifierProcessor(genericerrs.Classifier, Classifier).Process(err)
	assert.True(t, errs.IsRetryable(processed), "the generic classifier recognises the cancellation")
}
