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
//
// Git has no typed status to read: it reports almost everything as a non-zero
// exit and a line of prose, so a connection reset and a deleted branch both
// leave `git fetch` looking identical to a caller that only checks the code.
// The subcommand that was run and the diagnostic git printed are therefore the
// only signals available, and the classification pairs them: a fragment is
// evidence of a transient failure only for the operations it can actually
// arise from, so a transport fault counts against a command that talks to the
// remote and lock contention counts against any command that writes to the
// checkout.
//
// Only a recognised pair is retryable. Everything else is a permanent
// infrastructure failure, including a diagnostic this package has never seen.
// The direction is deliberate: an unlisted transient failure costs one lost
// retry, while a permanent failure that defaulted to retryable would replay a
// deterministic error — a deleted target branch, an empty squash commit, a
// rejected push — through the whole retry budget, re-running the fetch, reset
// and cherry-picks behind it each time, before dead-lettering anyway.
//
// Git's wording drifts between versions, so the fragment lists are expected to
// grow. Adding one is cheap and safe; the cost of a missing fragment is bounded
// at a single lost retry, which is what makes the allowlist maintainable.
//
// Cancellation is deliberately absent. A git process killed because its
// context ended dies with "signal: killed" and no trace of the cancellation,
// so it is gitexec.CommandFailure — not this classifier — that puts
// context.Canceled back in the chain, leaving the generic classifier to
// recognise it as it does for every other cancelled operation.
package git

import (
	"strings"

	"github.com/uber/submitqueue/platform/errs"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
)

// Classifier recognises Git process failures, reporting a known transient
// diagnostic on an operation it can arise from as retryable and every other
// Git failure as permanent. See the package doc for why the default runs that
// way.
//
// The classifier is stateless; this package-level singleton is the canonical
// handle. Pass it as one of the variadic classifiers to
// errs.NewClassifierProcessor; the resulting processor is what gets handed to
// consumer.New.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

// remoteOperations are the Git subcommands that exchange data with the
// configured remote. They attribute their failures to that remote, and they
// are the only operations a transport fragment can legitimately describe.
var remoteOperations = map[string]bool{
	"clone":     true,
	"fetch":     true,
	"ls-remote": true,
	"pull":      true,
	"push":      true,
}

// transientTransportFragments are diagnostics that mean the exchange with the
// remote did not complete, weighed only for a remoteOperations subcommand.
// A rejected push or a failed authentication is the remote answering, not
// failing to answer, and stays permanent.
var transientTransportFragments = []string{
	"502 bad gateway",
	"503 service unavailable",
	"504 gateway timeout",
	"broken pipe",
	"connection refused",
	"connection reset by peer",
	"connection timed out",
	"could not resolve host",
	"early eof",
	"network is unreachable",
	"no route to host",
	"operation timed out",
	"ssh_exchange_identification",
	"temporary failure in name resolution",
	"transfer closed with outstanding read data remaining",
}

// transientCheckoutFragments are diagnostics that mean another process held
// the checkout, weighed for every subcommand: a remote operation writes refs
// and the index too, so it can lose the same race a local one can.
var transientCheckoutFragments = []string{
	".lock': file exists",
	"cannot lock ref",
	"index.lock",
	"resource temporarily unavailable",
}

// Classify inspects a single node. Per the errs.Classifier contract, this must
// not call errors.Is / errors.As — the classifier-processor owns the chain
// walk.
func (classifier) Classify(err error) errs.Verdict {
	commandErr, ok := err.(*gitexec.CommandError)
	if !ok {
		// The only Unknown this classifier returns, and it means "not my
		// node" rather than "no opinion on this failure". Returning a verdict
		// here would claim every error the walk passes — a MySQL driver error
		// among them — before its own classifier were asked.
		return errs.Unknown
	}

	diagnostic := strings.ToLower(commandErr.Diagnostic())
	remote := remoteOperations[commandErr.Operation()]

	transient := containsAny(diagnostic, transientCheckoutFragments) ||
		(remote && containsAny(diagnostic, transientTransportFragments))

	switch {
	case transient && remote:
		return errs.InfraDependencyRetryable
	case transient:
		return errs.InfraRetryable
	case remote:
		return errs.InfraDependency
	default:
		return errs.Infra
	}
}

func containsAny(diagnostic string, fragments []string) bool {
	for _, fragment := range fragments {
		if strings.Contains(diagnostic, fragment) {
			return true
		}
	}
	return false
}
