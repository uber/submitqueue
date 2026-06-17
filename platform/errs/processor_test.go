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
