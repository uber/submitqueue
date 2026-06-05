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

package generic

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/core/errs"
)

func TestClassifier_ContextCanceled(t *testing.T) {
	assert.Equal(t, errs.InfraRetryable, Classifier.Classify(context.Canceled))
}

func TestClassifier_Unknown(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		// Per-node contract — Classifier should NOT match a wrapped
		// context.Canceled; the surrounding classifier-processor walk will
		// reach the inner node and ask Classifier again there.
		{"wrapped context.Canceled", fmt.Errorf("op: %w", context.Canceled)},
		{"deadline exceeded", context.DeadlineExceeded},
		{"plain error", errors.New("anything")},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, errs.Unknown, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifier_AppliedViaProcessor(t *testing.T) {
	p := errs.NewClassifierProcessor(Classifier)

	t.Run("bare context.Canceled becomes retryable infra", func(t *testing.T) {
		out := p.Process(context.Canceled)
		assert.True(t, errs.IsRetryable(out))
	})

	t.Run("wrapped context.Canceled becomes retryable infra", func(t *testing.T) {
		// The chain walker reaches the inner context.Canceled node and the
		// classifier matches there.
		wrapped := fmt.Errorf("process: %w", context.Canceled)
		out := p.Process(wrapped)
		assert.True(t, errs.IsRetryable(out))
	})

	t.Run("framework wrap in chain wins", func(t *testing.T) {
		// A controller explicitly classified the shutdown as non-retryable.
		// The pass-1 framework-wrap check short-circuits before Classifier
		// runs.
		err := errs.NewUserError(context.Canceled)
		out := p.Process(err)
		assert.Same(t, err, out)
		assert.False(t, errs.IsRetryable(out))
		assert.True(t, errs.IsUserError(out))
	})
}
