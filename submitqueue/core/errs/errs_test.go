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
	platformerrs "github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func TestClassifier(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want platformerrs.Verdict
	}{
		{
			name: "version mismatch is retryable",
			err:  storage.ErrVersionMismatch,
			want: platformerrs.InfraRetryable,
		},
		{
			name: "other storage sentinel is unknown",
			err:  storage.ErrNotFound,
			want: platformerrs.Unknown,
		},
		{
			name: "wrapped node is left to processor walk",
			err:  fmt.Errorf("update: %w", storage.ErrVersionMismatch),
			want: platformerrs.Unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classifier.Classify(tt.err))
		})
	}
}

func TestClassifierProcessorMarksWrappedVersionMismatchRetryable(t *testing.T) {
	raw := fmt.Errorf("update batch: %w", storage.ErrVersionMismatch)
	processed := platformerrs.NewClassifierProcessor(Classifier).Process(raw)

	assert.True(t, platformerrs.IsRetryable(processed))
	assert.True(t, errors.Is(processed, storage.ErrVersionMismatch))
}
