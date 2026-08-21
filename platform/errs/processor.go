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
//   - If err carries a framework classification (*userError or *infraError) on
//     its single-cause spine, returns err unchanged — the chain is already
//     interpretable by IsUserError / IsRetryable / IsDependencyError.
//   - Otherwise, walks the chain from outermost to innermost, asking each
//     classifier per node. The FIRST non-Unknown verdict wins; the outermost
//     such node determines the wrap. err is wrapped with the framework
//     constructor matching that verdict (User -> NewUserError, InfraRetryable
//     -> NewRetryableError, etc.) and the wrapped error is returned.
//   - Verdict Infra means "non-retryable infra" — which is already the default
//     behavior for an unwrapped chain, so no wrap is added.
//   - If no classifier recognizes anything, err is returned unchanged.
//
// Implementation: two passes over the chain. Pass 1 looks for an existing
// framework wrap and short-circuits if one is found — no classifier is invoked.
// Pass 2 runs the configured classifiers per node. Walking the chain is cheap
// relative to a classifier call, so this avoids running classifiers whenever
// the chain is already classified deeper down.
//
// Both passes traverse joined errors (errors.Join, or anything else exposing
// Unwrap() []error) as well as ordinary single-cause wraps. A wrap classifies
// the subtree beneath it and no more, so a wrapped branch of a join is weighed
// against its siblings instead of answering for them; classify documents how.
//
// Passing no classifiers is valid — the processor will still honor any
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

	// Pass 1 — framework-wrap check, along the single-cause spine only. A wrap
	// found here sits above everything else in err, so it classifies the whole
	// error and is returned verbatim. The walk stops at a join because
	// errors.Unwrap yields nothing for one, which is the behavior we want: a
	// wrap inside one branch speaks only for that branch, and classify weighs
	// it against its siblings rather than letting it silently answer for them.
	for cur := err; cur != nil; cur = errors.Unwrap(cur) {
		if wrapVerdict(cur) != Unknown {
			return err
		}
	}

	// Pass 2 — classify the chain and wrap with the verdict it reaches.
	switch p.classify(err) {
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

// classify returns the verdict for err — from a framework wrap if it carries
// one, otherwise from the configured classifiers — or Unknown when nothing
// recognizes any node in it.
//
// The walk treats the two ways an error can contain another differently,
// because they mean different things:
//
//   - Down a wrap chain (Unwrap() error) the outermost verdict wins. A wrapper
//     saw the error it wrapped and chose to add context on top of it, so it
//     speaks with more knowledge than its cause.
//   - Across the branches of a join (Unwrap() []error) nothing shadows anything.
//     The branches are independent failures that happened to be reported
//     together, and their order is the order the caller ran them in, not a
//     precedence. They combine by verdictRank so that the result cannot depend
//     on which branch failed first.
//
// The second rule is why a framework wrap does not get the short-circuit it
// gets on the spine: within a join it is one branch's account of one failure,
// with no standing to classify the failures beside it.
func (p classifierProcessor) classify(err error) Verdict {
	for cur := err; cur != nil; {
		// A wrap is authoritative for everything it contains, so it answers for
		// this subtree without consulting a classifier.
		if v := wrapVerdict(cur); v != Unknown {
			return v
		}

		for _, c := range p.classifiers {
			if v := c.Classify(cur); v != Unknown {
				return v
			}
		}

		if joined, ok := cur.(interface{ Unwrap() []error }); ok {
			verdict := Unknown
			for _, branch := range joined.Unwrap() {
				if v := p.classify(branch); verdictRank(v) > verdictRank(verdict) {
					verdict = v
				}
			}
			return verdict
		}

		cur = errors.Unwrap(cur)
	}
	return Unknown
}

// wrapVerdict returns the verdict the framework wrap err carries, or Unknown if
// err is not a wrap. It inspects the single node it is given, never the chain.
// Every wrap maps to a real verdict, which is what makes Unknown usable as the
// "not a wrap" answer.
func wrapVerdict(err error) Verdict {
	switch e := err.(type) {
	case *userError:
		return User
	case *infraError:
		switch {
		case e.retryable && e.dependency:
			return InfraDependencyRetryable
		case e.retryable:
			return InfraRetryable
		case e.dependency:
			return InfraDependency
		default:
			return Infra
		}
	}
	return Unknown
}

// verdictRank orders verdicts for combining the independent branches of a
// joined error. The highest-ranked branch verdict becomes the verdict for the
// join as a whole.
//
// It must be an explicit table rather than a comparison on Verdict itself,
// whose constants are declaration-ordered and not severity-ordered:
// InfraDependency numerically exceeds InfraRetryable, so ranking by value would
// let a non-retryable branch discard a retryable sibling.
//
// Two principles set the order. Retryable outranks non-retryable because the
// two mistakes cost differently — a wrong "retryable" spends a bounded retry
// budget and then dead-letters anyway, while a wrong "non-retryable" throws
// away a failure that would have cleared on its own and leaves it for someone
// to find and replay by hand. Within a retryability tier the verdict that
// implicates this service outranks the one that points elsewhere, so a failure
// that is partly ours is not reported as a pure dependency or user problem and
// routed away from the people who can fix it. That second ordering only moves
// attribution: every verdict in the non-retryable tier yields the same transport
// outcome.
func verdictRank(v Verdict) int {
	switch v {
	case InfraRetryable:
		return 5
	case InfraDependencyRetryable:
		return 4
	case Infra:
		return 3
	case User:
		return 2
	case InfraDependency:
		return 1
	default: // Unknown
		return 0
	}
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
