// Copyright (c) 2025 Uber Technologies, Inc.
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

// Package generic provides an errs.Classifier for errors that are not tied to
// any particular backend. Servers should wire this classifier in alongside
// any backend-specific classifiers (e.g. core/errs/mysql).
package generic

import (
	"context"

	"github.com/uber/submitqueue/core/errs"
)

// Classifier recognises generic, non-backend-specific errors and returns
// errs.Unknown for anything it does not recognise so the surrounding
// errs.Classify walker can keep looking down the unwrap chain.
//
// The classifier is stateless; this package-level singleton is the canonical
// handle. Pass it into consumer.New as a vararg.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

// Classify inspects a single node. Per the errs.Classifier contract, this
// must not call errors.Is / errors.As — errs.Classify owns the chain walk.
func (classifier) Classify(err error) errs.Verdict {
	// Cancellation signals that the caller aborted the work in flight
	// (process shutdown, deadline on the inbound RPC, parent operation gone) —
	// it is not a statement about the work itself being invalid. The same
	// message handed to a fresh process with an uncancelled context is
	// expected to succeed, so nacking for redelivery is the correct response.
	// Cases where cancellation truly means "do not run this again" are
	// caller-specific and should be expressed by wrapping with an explicit
	// NewUserError / NewDependencyError before returning; the pass-1
	// framework-wrap check in errs.Classify will then short-circuit before
	// this classifier is consulted.
	if err == context.Canceled {
		return errs.InfraRetryable
	}
	return errs.Unknown
}
