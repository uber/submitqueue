// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/sqsim/entity"
)

func TestClassifierClassifiesModeledFaults(t *testing.T) {
	tests := []struct {
		name      string
		kind      entity.FaultKind
		retryable bool
	}{
		{name: "retryable", kind: entity.FaultRetryable, retryable: true},
		{name: "non-retryable", kind: entity.FaultNonRetryable, retryable: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := ErrorForFault(entity.Fault{Kind: tt.kind, Phase: entity.FaultBeforeSideEffect})
			require.Error(t, raw)
			classified := errs.NewClassifierProcessor(Classifier).Process(raw)
			assert.Equal(t, tt.retryable, errs.IsRetryable(classified))
			assert.True(t, errs.IsDependencyError(classified))
		})
	}
}
