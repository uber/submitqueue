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

// Package git provides an errs.Classifier for failures from Git processes.
package git

import (
	"strings"

	"github.com/uber/submitqueue/platform/errs"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

// Classifier recognises structured Git command failures. Remote exchanges are
// retryable dependency failures; local repository operations are retryable
// infrastructure failures. Commands that never started and unknown operations
// remain unclassified and therefore fail fast.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

var permanentDiagnosticFragments = []string{
	"authentication failed",
	"bad config line",
	"does not appear to be a git repository",
	"invalid refspec",
	"not a git repository",
	"permission denied (publickey)",
	"repository not found",
	"unknown option",
	"unknown switch",
}

func (classifier) Classify(err error) errs.Verdict {
	commandErr, ok := err.(*gitexec.CommandError)
	if !ok || !commandErr.ProcessExited() {
		return errs.Unknown
	}

	if isPermanentGitDiagnostic(commandErr.Diagnostic()) {
		switch commandErr.Operation() {
		case "fetch", "ls-remote", "push":
			return errs.InfraDependency
		default:
			return errs.Infra
		}
	}

	switch commandErr.Operation() {
	case "fetch", "ls-remote", "push":
		return errs.InfraDependencyRetryable
	case "cat-file", "cherry-pick", "clean", "commit", "ls-files", "merge", "merge-base", "reset", "rev-list", "rev-parse", "show":
		return errs.InfraRetryable
	case "config":
		return errs.Infra
	default:
		return errs.Unknown
	}
}

func isPermanentGitDiagnostic(diagnostic string) bool {
	diagnostic = strings.ToLower(diagnostic)
	for _, fragment := range permanentDiagnosticFragments {
		if strings.Contains(diagnostic, fragment) {
			return true
		}
	}
	return false
}
