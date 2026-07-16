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

	"github.com/stretchr/testify/require"
)

func TestBehaviorBuilderRequiresEveryExternalOperation(t *testing.T) {
	behavior := NewBehavior().
		BuildRunner(SuccessfulBuildRunner()).
		MergeConflictCheck(SuccessfulMergeConflictCheck())

	_, err := NewScenario().
		Timeout(time.Minute).
		Land(NewLand("l1").Queue("sqsim").Behavior(behavior).Expect(RequestLanded)).
		Build()
	require.Error(t, err)
}
