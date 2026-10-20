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

package storage

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
)

func TestErrVersionMismatchClassification(t *testing.T) {
	err := fmt.Errorf("update request: %w", ErrVersionMismatch)

	assert.ErrorIs(t, err, ErrVersionMismatch)
	assert.True(t, errs.IsRetryable(err))
	assert.False(t, errs.IsUserError(err))
	assert.False(t, errs.IsDependencyError(err))
}

func TestCompleteReadMetricsOp(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		result string
	}{
		{name: "success", result: "success"},
		{name: "wrapped not found", err: WrapNotFound(sql.ErrNoRows), result: MetricsResultNotFound},
		{name: "not found wrapped by a caller", err: fmt.Errorf("get build: %w", ErrNotFound), result: MetricsResultNotFound},
		{name: "other error", err: fmt.Errorf("connection refused"), result: "error"},
		{name: "version mismatch", err: ErrVersionMismatch, result: "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			CompleteReadMetricsOp(metrics.Begin(scope, "get", metrics.StorageLatencyBuckets), tt.err)

			histograms := scope.Snapshot().Histograms()
			assert.Len(t, histograms, 1)
			assert.Contains(t, histograms, "get.finish+result="+tt.result)
		})
	}
}
