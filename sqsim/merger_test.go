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

package sqsim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeInvocationBuilderAppliesFault(t *testing.T) {
	builder := NewMergeBehavior().Invoke(
		MergeSucceededAfter(time.Second).Fault(RetryableErrorAfterSideEffect()),
	)

	behavior, err := builder.build()
	require.NoError(t, err)
	require.Len(t, behavior.Invocations, 1)
	assert.Equal(t, int64(1000), behavior.Invocations[0].DelayMs)
	assert.Equal(t, FaultAfterSideEffect, behavior.Invocations[0].Fault.Phase)
}
