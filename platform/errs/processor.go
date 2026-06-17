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

package errs

import "errors"

// ErrorProcessor transforms an error returned by a controller into the error
// the surrounding transport will react to. It runs exactly once per failing
// delivery — typically called by the consumer immediately after a controller
// returns — and the result is what IsRetryable / IsUserError / IsDependencyError
// will subsequently inspect.
//
// Two implementations ship in this package:
//
//   - NewClassifierProcessor runs a per-node classifier walk. This preserves
//     controller-attached framework wraps (NewUserError, NewDependencyError,
//     ...) verbatim and only invokes the supplied classifiers when the chain
//     carries no existing framework type. Use it for primary pipeline
//     consumers where controller-driven classification is the source of truth.
//
//   - AlwaysRetryableProcessor unconditionally wraps every non-nil error with
//     NewRetryableError, overriding any inner framework wrap. Use it for
//     narrowly-scoped consumers — typically DLQ reconciliation — that must
//     redeliver on any failure because there is no further dead-letter
//     destination.
//
// Separating "decide how an error is interpreted" from "decide what to do
// with the interpreted error" lets the same consumer implementation host
// transports with very different retry policies without leaking the policy
// into each Controller.
type ErrorProcessor interface {
	Process(err error) error
}

// NewClassifierProcessor returns an ErrorProcessor that runs the supplied
// classifiers over the chain of any non-nil error.
//
// Semantics of Process on the returned processor:
//
//   - nil in, nil out.
//   - If err's chain already carries a framework classification (*userError
//     or *infraError anywhere in the chain), returns err unchanged — the chain
//     is already interpretable by IsUserError / IsRetryable /
//     IsDependencyError.
//   - Otherwise, walks the chain from outermost to innermost, asking each
//     classifier per node. The FIRST non-Unknown verdict wins; the outermost
//     such node determines the wrap. err is wrapped with the framework
//     constructor matching that verdict (User -> NewUserError, InfraRetryable
//     -> NewRetryableError, etc.) and the wrapped error is returned.
//   - Verdict Infra means "non-retryable infra" — which is already the default
//     behavior for an unwrapped chain, so no wrap is added.
//   - If no classifier recognises anything, err is returned unchanged.
//
// Implementation: two passes over the chain. Pass 1 is a cheap type check
// looking for an existing framework wrap and short-circuits if one is found —
// no classifier is invoked. Pass 2 runs the configured classifiers per node.
// Walking the chain is cheap relative to a classifier call, so this avoids
// running classifiers whenever the chain is already classified deeper down.
//
// Passing no classifiers is valid — the processor will still honour any
// framework wrap already in the chain and otherwise return err unchanged.
//
// NOTE: this central classifier model cannot disambiguate errors of the same
// underlying type produced by different extensions (e.g. a net.OpError from a
// mysql connection vs the same type from an HTTP caller would both match the
// mysql classifier here). Resolving that requires per-extension provenance
// tagging; intentionally deferred.
func NewClassifierProcessor(classifiers ...Classifier) ErrorProcessor {
	return classifierProcessor{classifiers: classifiers}
}

type classifierProcessor struct {
	classifiers []Classifier
}

func (p classifierProcessor) Process(err error) error {
	if err == nil {
		return nil
	}

	// Pass 1 — cheap framework-wrap check. If any node already carries a
	// framework type, the chain is interpretable as-is and classifiers are
	// never invoked.
	for cur := err; cur != nil; cur = errors.Unwrap(cur) {
		switch cur.(type) {
		case *userError, *infraError:
			return err
		}
	}

	// Pass 2 — run classifiers per node from outermost to innermost. Stop at
	// the first non-Unknown verdict.
	var verdict Verdict
	for cur := err; cur != nil && verdict == Unknown; cur = errors.Unwrap(cur) {
		for _, c := range p.classifiers {
			if v := c.Classify(cur); v != Unknown {
				verdict = v
				break
			}
		}
	}

	switch verdict {
	case User:
		return NewUserError(err)
	case InfraRetryable:
		return NewRetryableError(err)
	case InfraDependency:
		return NewDependencyError(err)
	case InfraDependencyRetryable:
		return NewRetryableDependencyError(err)
	}
	// Unknown or Infra — no wrap needed; the existing chain already behaves as
	// non-retryable infra at the IsRetryable / IsUserError layer.
	return err
}

// AlwaysRetryableProcessor classifies every non-nil error as InfraRetryable by
// wrapping it with NewRetryableError. The wrap is unconditional: an inner
// *userError or non-retryable *infraError is overridden because errors.As
// (used by IsRetryable) matches the outermost *infraError first, and that
// outer wrap is always retryable=true.
//
// Side-effect: an inner *infraError carrying dependency=true is masked. The
// outer wrap is constructed with dependency=false, so IsDependencyError on
// the result returns false even though the original chain originated in a
// dependency. This is acceptable for the intended DLQ-reconciliation use
// case where only IsRetryable drives transport behavior; if dependency
// provenance ever needs to survive this processor it must be added here
// explicitly.
//
// Pair this only with consumers whose controllers should retry on any returned
// error. On a primary pipeline consumer this would loop forever on genuine
// user errors and prevent them from reaching the DLQ.
var AlwaysRetryableProcessor ErrorProcessor = alwaysRetryableProcessor{}

type alwaysRetryableProcessor struct{}

func (alwaysRetryableProcessor) Process(err error) error {
	if err == nil {
		return nil
	}
	return NewRetryableError(err)
}
