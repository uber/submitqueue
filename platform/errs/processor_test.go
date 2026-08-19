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

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubClassifier returns a fixed verdict regardless of the error inspected.
// Used to verify that NewClassifierProcessor wires Classify correctly.
type stubClassifier struct{ verdict Verdict }

func (s stubClassifier) Classify(error) Verdict { return s.verdict }

// verdictByError recognizes only the exact nodes it was built with, the way a
// real backend classifier recognizes only its own driver's errors. Everything
// else classifies Unknown.
type verdictByError map[error]Verdict

func (m verdictByError) Classify(err error) Verdict { return m[err] }

// singleWrap is a classifiable node with exactly one cause. fmt.Errorf cannot
// stand in for it: its wrapper node is opaque to a classifier, and two %w verbs
// produce a join rather than a chain.
type singleWrap struct{ cause error }

func (w singleWrap) Error() string { return "wrapped: " + w.cause.Error() }
func (w singleWrap) Unwrap() error { return w.cause }

func TestNewClassifierProcessor_NilIn(t *testing.T) {
	p := NewClassifierProcessor()
	assert.NoError(t, p.Process(nil))
}

func TestNewClassifierProcessor_PreservesFrameworkWrap(t *testing.T) {
	// An error already carrying a framework wrap must pass through unchanged,
	// even if a classifier would otherwise contradict it. This mirrors the
	// pass-1 short-circuit in Classify.
	p := NewClassifierProcessor(stubClassifier{verdict: InfraRetryable})

	userErr := NewUserError(errors.New("bad input"))
	out := p.Process(userErr)
	assert.Same(t, userErr, out)
	assert.True(t, IsUserError(out))
	assert.False(t, IsRetryable(out))
}

func TestNewClassifierProcessor_AppliesClassifierWhenNoWrap(t *testing.T) {
	p := NewClassifierProcessor(stubClassifier{verdict: InfraRetryable})

	raw := errors.New("transient")
	out := p.Process(raw)
	require.Error(t, out)
	assert.True(t, IsRetryable(out))
}

func TestNewClassifierProcessor_NoClassifiersReturnsUnchanged(t *testing.T) {
	// Empty classifier list still walks pass 1 (framework wraps preserved) but
	// produces no wrap of its own — the chain stays as the caller passed it.
	p := NewClassifierProcessor()

	raw := errors.New("transient")
	out := p.Process(raw)
	assert.Same(t, raw, out)
	assert.False(t, IsRetryable(out))
	assert.False(t, IsUserError(out))
}

// TestNewClassifierProcessor_JoinedBranches covers the reason the walk knows
// about Unwrap() []error at all: a caller that fans work out to several
// children reports their failures with errors.Join, and errors.Unwrap cannot
// see into one, so classifiers used to never be offered any branch.
func TestNewClassifierProcessor_JoinedBranches(t *testing.T) {
	transient := errors.New("deadlock")
	permanent := errors.New("schema mismatch")
	badInput := errors.New("malformed payload")
	upstreamBlip := errors.New("upstream 503")
	upstreamGone := errors.New("upstream decommissioned")

	p := NewClassifierProcessor(verdictByError{
		transient:    InfraRetryable,
		permanent:    Infra,
		badInput:     User,
		upstreamBlip: InfraDependencyRetryable,
		upstreamGone: InfraDependency,
	})

	tests := []struct {
		name           string
		err            error
		wantRetryable  bool
		wantUser       bool
		wantDependency bool
	}{
		{
			// errors.Join builds a join node even for one error, so a lone
			// failing child is just as opaque to errors.Unwrap as several.
			name:          "single branch",
			err:           errors.Join(fmt.Errorf("child a: %w", transient)),
			wantRetryable: true,
		},
		{
			name:          "retryable branch last",
			err:           errors.Join(fmt.Errorf("child a: %w", permanent), fmt.Errorf("child b: %w", transient)),
			wantRetryable: true,
		},
		{
			// Same branches reversed: the verdict must come from rank, not from
			// the order the children happened to run in.
			name:          "retryable branch first",
			err:           errors.Join(fmt.Errorf("child a: %w", transient), fmt.Errorf("child b: %w", permanent)),
			wantRetryable: true,
		},
		{
			name:          "retryable outranks user",
			err:           errors.Join(badInput, transient),
			wantRetryable: true,
		},
		{
			name:          "join nested below a wrap",
			err:           fmt.Errorf("dispatch: %w", errors.Join(permanent, fmt.Errorf("child b: %w", transient))),
			wantRetryable: true,
		},
		{
			name:          "join nested inside another join",
			err:           errors.Join(permanent, errors.Join(badInput, transient)),
			wantRetryable: true,
		},
		{
			// Both branches are retryable, so only attribution is in question:
			// a failure that is partly local is not blamed on the dependency.
			name:          "local retryable outranks dependency retryable",
			err:           errors.Join(upstreamBlip, transient),
			wantRetryable: true,
		},
		{
			name:           "dependency retryable alone keeps its provenance",
			err:            errors.Join(permanent, upstreamBlip),
			wantRetryable:  true,
			wantDependency: true,
		},
		{
			name:     "user outranks non-retryable dependency",
			err:      errors.Join(upstreamGone, badInput),
			wantUser: true,
		},
		{
			name: "no branch recognized",
			err:  errors.Join(errors.New("who knows"), errors.New("nor this")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := p.Process(tt.err)
			require.Error(t, out)
			assert.Equal(t, tt.wantRetryable, IsRetryable(out))
			assert.Equal(t, tt.wantUser, IsUserError(out))
			assert.Equal(t, tt.wantDependency, IsDependencyError(out))
		})
	}
}

func TestNewClassifierProcessor_JoinedBranchesKeepEveryCause(t *testing.T) {
	transient := errors.New("deadlock")
	permanent := errors.New("schema mismatch")
	p := NewClassifierProcessor(verdictByError{transient: InfraRetryable})

	out := p.Process(errors.Join(fmt.Errorf("child a: %w", permanent), fmt.Errorf("child b: %w", transient)))

	require.True(t, IsRetryable(out))
	assert.True(t, errors.Is(out, transient))
	assert.True(t, errors.Is(out, permanent), "the branch that lost the rank must stay in the chain for diagnostics")
}

// TestNewClassifierProcessor_WrappedBranchesAreWeighed covers branches that
// arrive already classified. A wrap speaks for the subtree beneath it, so it
// contributes a verdict to the join like any other branch rather than deciding
// for its siblings — which is what makes the outcome independent of the order
// the branches were reported in.
func TestNewClassifierProcessor_WrappedBranchesAreWeighed(t *testing.T) {
	transient := errors.New("deadlock")
	p := NewClassifierProcessor(verdictByError{transient: InfraRetryable})

	tests := []struct {
		name          string
		err           error
		wantRetryable bool
		wantUser      bool
	}{
		{
			// IsUserError stays true alongside it: the losing branch keeps its
			// wrap in the chain, and only the outer one drives the retry.
			name:          "wrapped user error does not suppress a classifiable sibling",
			err:           errors.Join(NewUserError(errors.New("malformed payload")), transient),
			wantRetryable: true,
			wantUser:      true,
		},
		{
			name:          "retryable wrap ranked ahead of a non-retryable one",
			err:           errors.Join(NewDependencyError(errors.New("upstream 503")), NewRetryableError(errors.New("blip"))),
			wantRetryable: true,
		},
		{
			// The same two wraps reversed. Before wraps were weighed, this pair
			// resolved by whichever branch errors.As reached first.
			name:          "retryable wrap ranked ahead of a non-retryable one, reversed",
			err:           errors.Join(NewRetryableError(errors.New("blip")), NewDependencyError(errors.New("upstream 503"))),
			wantRetryable: true,
		},
		{
			name:     "sole wrapped branch still classifies the join",
			err:      errors.Join(NewUserError(errors.New("malformed payload")), errors.New("unrecognized")),
			wantUser: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := p.Process(tt.err)
			require.Error(t, out)
			assert.Equal(t, tt.wantRetryable, IsRetryable(out))
			assert.Equal(t, tt.wantUser, IsUserError(out))
		})
	}
}

// TestNewClassifierProcessor_SpineWrapStillShortCircuits pins the other half of
// the rule: a wrap above a join covers the whole join, so it is returned
// verbatim and no branch is consulted.
func TestNewClassifierProcessor_SpineWrapStillShortCircuits(t *testing.T) {
	transient := errors.New("deadlock")
	p := NewClassifierProcessor(verdictByError{transient: InfraRetryable})

	wrapped := NewUserError(errors.Join(errors.New("child a"), transient))
	out := p.Process(wrapped)

	assert.Same(t, wrapped, out)
	assert.True(t, IsUserError(out))
	assert.False(t, IsRetryable(out))
}

// TestNewClassifierProcessor_WrapChainKeepsOutermostVerdict guards the
// asymmetry between the two walks: rank decides between the branches of a join,
// but down a wrap chain the outer node still wins outright, because it saw its
// cause and classified anyway. Without this the join rule would leak into
// ordinary chains and let a retryable cause override the verdict a caller
// deliberately put on top of it.
func TestNewClassifierProcessor_WrapChainKeepsOutermostVerdict(t *testing.T) {
	inner := errors.New("deadlock")
	outer := singleWrap{cause: inner}
	p := NewClassifierProcessor(verdictByError{outer: User, inner: InfraRetryable})

	out := p.Process(outer)

	assert.True(t, IsUserError(out))
	assert.False(t, IsRetryable(out))
}

func TestAlwaysRetryableProcessor_NilIn(t *testing.T) {
	assert.NoError(t, AlwaysRetryableProcessor.Process(nil))
}

func TestAlwaysRetryableProcessor_WrapsPlainError(t *testing.T) {
	raw := errors.New("anything")
	out := AlwaysRetryableProcessor.Process(raw)
	require.Error(t, out)
	assert.True(t, IsRetryable(out))
	// The wrap preserves the original cause for diagnostics.
	assert.True(t, errors.Is(out, raw))
}

// TestAlwaysRetryableProcessor_OverridesUserError pins the headline behavior:
// even an explicit NewUserError from a controller must come out retryable so
// the surrounding consumer redelivers it. This is the whole reason this
// processor exists — Classify would short-circuit on the inner *userError and
// IsRetryable would return false.
func TestAlwaysRetryableProcessor_OverridesUserError(t *testing.T) {
	inner := errors.New("bad input")
	userErr := NewUserError(inner)

	out := AlwaysRetryableProcessor.Process(userErr)
	require.Error(t, out)
	assert.True(t, IsRetryable(out), "outer infraError(retryable=true) must win IsRetryable")
	// The inner *userError is preserved in the chain for observability — a
	// caller that explicitly classified its failure as user-caused did so for
	// a reason, even if the transport overrides the retry decision.
	assert.True(t, IsUserError(out))
	assert.True(t, errors.Is(out, inner))
}

func TestAlwaysRetryableProcessor_OverridesNonRetryableDependencyError(t *testing.T) {
	depErr := NewDependencyError(errors.New("upstream 503"))

	out := AlwaysRetryableProcessor.Process(depErr)
	require.Error(t, out)
	assert.True(t, IsRetryable(out), "retryable=true must take precedence over inner non-retryable")
	// The dependency bit is intentionally masked by the outer wrap — see the
	// AlwaysRetryableProcessor doc comment. IsRetryable is the only contract
	// this processor promises to satisfy.
	assert.False(t, IsDependencyError(out))
}

func TestAlwaysRetryableProcessor_PreservesContextCancellation(t *testing.T) {
	// context.Canceled is a special case for the consumer loop (treated as
	// shutdown, not a controller failure) — but classification-wise it should
	// still come back retryable so a non-shutdown caller redelivers.
	out := AlwaysRetryableProcessor.Process(context.Canceled)
	require.Error(t, out)
	assert.True(t, IsRetryable(out))
	assert.True(t, errors.Is(out, context.Canceled))
}

func TestAlwaysRetryableProcessor_DoubleWrapIsBenign(t *testing.T) {
	// Wrapping an already-retryable error is a no-op from the IsRetryable
	// perspective. We do not collapse the wrap; the second layer is cheap.
	already := NewRetryableError(errors.New("already retryable"))
	out := AlwaysRetryableProcessor.Process(already)
	require.Error(t, out)
	assert.True(t, IsRetryable(out))
}

// TestErrorProcessor_InterfaceConformance is a compile-time assertion that
// both shipped implementations satisfy the ErrorProcessor interface.
func TestErrorProcessor_InterfaceConformance(t *testing.T) {
	var _ ErrorProcessor = NewClassifierProcessor()
	var _ ErrorProcessor = AlwaysRetryableProcessor
	var _ ErrorProcessor = classifierProcessor{}
	var _ ErrorProcessor = alwaysRetryableProcessor{}
}

// Smoke-test that the processor result is interpretable by fmt-wrap callers
// that may further annotate the error before it reaches IsRetryable.
func TestAlwaysRetryableProcessor_SurvivesFmtWrap(t *testing.T) {
	out := AlwaysRetryableProcessor.Process(errors.New("boom"))
	wrapped := fmt.Errorf("downstream: %w", out)
	assert.True(t, IsRetryable(wrapped))
}
