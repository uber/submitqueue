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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/failure"
)

// driverError stands in for a backend's own error type — the thing a real
// classifier (e.g. mysqlerrs) recognises at the bottom of a chain. Defined
// here rather than importing a real classifier, which would be an import
// cycle: every classifier package imports errs.
type driverError struct{ code int }

func (e driverError) Error() string { return fmt.Sprintf("driver error %d", e.code) }

// driverClassifier recognises only driverError, so a verdict proves the
// processor actually reached that node rather than stopping earlier.
type driverClassifier struct{}

func (driverClassifier) Classify(err error) Verdict {
	var de driverError
	if errors.As(err, &de) {
		return InfraRetryable
	}
	return Unknown
}

func TestAttributionReadsBackSubjects(t *testing.T) {
	err := Attribute(errors.New("boom"), failure.Subject{Type: "batch", ID: "q/batch/1"})

	got := Attribution(err)
	assert.Equal(t, "boom", got.Message)
	assert.Equal(t, []failure.Subject{{Type: "batch", ID: "q/batch/1"}}, got.Subjects)
}

// An unattributed error still yields a usable failure — the message alone.
// That is what lets a caller build one unconditionally.
func TestAttributionOfPlainError(t *testing.T) {
	got := Attribution(errors.New("boom"))

	assert.Equal(t, "boom", got.Message)
	assert.Empty(t, got.Subjects)
	assert.Empty(t, got.Detail)
}

func TestAttributionOfNil(t *testing.T) {
	assert.Equal(t, failure.Failure{}, Attribution(nil))
}

func TestAttributeNilIsNil(t *testing.T) {
	assert.NoError(t, Attribute(nil, failure.Subject{Type: "batch", ID: "x"}))
	assert.NoError(t, Detail(nil, map[string]any{"k": "v"}))
}

// Attribution survives further wrapping, which is the normal shape: a leaf
// attributes the entity it knows about, outer frames add context with %w.
func TestAttributionThroughOuterWrap(t *testing.T) {
	inner := Attribute(errors.New("boom"), failure.Subject{Type: "batch", ID: "q/batch/1"})
	outer := fmt.Errorf("run failed: %w", inner)

	got := Attribution(outer)
	assert.Equal(t, "run failed: boom", got.Message)
	assert.Equal(t, []failure.Subject{{Type: "batch", ID: "q/batch/1"}}, got.Subjects)
}

func TestAttributionMergesLayers(t *testing.T) {
	err := Attribute(errors.New("boom"), failure.Subject{Type: "batch", ID: "inner"})
	err = Detail(err, map[string]any{"stage": "dispatch", "shared": "inner"})
	err = Attribute(err, failure.Subject{Type: "queue", ID: "q"}, failure.Subject{Type: "batch", ID: "inner"})
	err = Detail(err, map[string]any{"shared": "outer"})

	got := Attribution(err)

	// Outermost first, and the repeated subject appears once.
	assert.Equal(t, []failure.Subject{
		{Type: "queue", ID: "q"},
		{Type: "batch", ID: "inner"},
	}, got.Subjects)
	// Outermost wins on a key collision.
	assert.Equal(t, "outer", got.Detail["shared"])
	assert.Equal(t, "dispatch", got.Detail["stage"])
}

// The regression that matters: attribution must be invisible to classification.
// If the wrapper broke the chain walk, every storage error would silently stop
// being retryable.
func TestAttributionDoesNotHideTheCauseFromClassifiers(t *testing.T) {
	cause := driverError{code: 1213}
	attributed := Attribute(fmt.Errorf("write failed: %w", cause), failure.Subject{Type: "batch", ID: "q/batch/1"})

	var de driverError
	require.True(t, errors.As(attributed, &de), "errors.As must reach the cause through the wrapper")
	assert.Equal(t, 1213, de.code)

	processed := NewClassifierProcessor(driverClassifier{}).Process(attributed)
	assert.True(t, IsRetryable(processed), "classification must survive attribution")

	// And the attribution survives classification wrapping it in turn.
	assert.Equal(t, []failure.Subject{{Type: "batch", ID: "q/batch/1"}}, Attribution(processed).Subjects)
}

// Attribution carries no verdict of its own, so an attributed error with
// nothing else on it stays non-retryable by default.
func TestAttributionAloneIsNotRetryable(t *testing.T) {
	err := Attribute(errors.New("boom"), failure.Subject{Type: "queue", ID: "q"})

	assert.False(t, IsRetryable(err))
	assert.False(t, IsUserError(err))
}

func TestAttributionComposesWithFrameworkWraps(t *testing.T) {
	tests := []struct {
		name      string
		wrap      func(error) error
		retryable bool
		userError bool
	}{
		{"retryable", NewRetryableError, true, false},
		{"user", NewUserError, false, true},
		{"dependency retryable", NewRetryableDependencyError, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.wrap(Attribute(errors.New("boom"), failure.Subject{Type: "batch", ID: "b"}))

			assert.Equal(t, tt.retryable, IsRetryable(err))
			assert.Equal(t, tt.userError, IsUserError(err))
			assert.Equal(t, []failure.Subject{{Type: "batch", ID: "b"}}, Attribution(err).Subjects)
		})
	}
}
