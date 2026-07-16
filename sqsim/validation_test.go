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

func TestValidateRejectsInvalidScenario(t *testing.T) {
	tests := []struct {
		name    string
		builder *ScenarioBuilder
	}{
		{
			name:    "no lands",
			builder: NewScenario().Timeout(time.Minute),
		},
		{
			name: "duplicate names",
			builder: NewScenario().Timeout(time.Minute).Land(
				NewLand("l1").Queue("sqsim").Behavior(happyBehavior()).Expect(RequestLanded),
				NewLand("l1").Queue("sqsim").Behavior(happyBehavior()).Expect(RequestLanded),
			),
		},
		{
			name: "build timeline does not begin at zero",
			builder: NewScenario().Timeout(time.Minute).Land(
				NewLand("l1").
					Queue("sqsim").
					Behavior(NewBehavior().
						BuildRunner(NewBuildRunnerBehavior().Trigger(BuildCreated(
							StatusAt(time.Second, BuildRunning),
							StatusAt(2*time.Second, BuildSucceeded),
						))).
						MergeConflictCheck(SuccessfulMergeConflictCheck()).
						Merge(SuccessfulMerge())).
					Expect(RequestLanded),
			),
		},
		{
			name: "build timeline offsets are not increasing",
			builder: NewScenario().Timeout(time.Minute).Land(
				NewLand("l1").
					Queue("sqsim").
					Behavior(NewBehavior().
						BuildRunner(NewBuildRunnerBehavior().Trigger(BuildCreated(
							StatusAt(0, BuildRunning),
							StatusAt(0, BuildSucceeded),
						))).
						MergeConflictCheck(SuccessfulMergeConflictCheck()).
						Merge(SuccessfulMerge())).
					Expect(RequestLanded),
			),
		},
		{
			name: "dry run after side effect",
			builder: NewScenario().Timeout(time.Minute).Land(
				NewLand("l1").
					Queue("sqsim").
					Behavior(NewBehavior().
						BuildRunner(SuccessfulBuildRunner()).
						MergeConflictCheck(NewMergeConflictCheckBehavior().Invoke(
							MergeConflictCheckFault(RetryableErrorAfterSideEffect()),
						)).
						Merge(SuccessfulMerge())).
					Expect(RequestLanded),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.builder.Build()
			require.Error(t, err)
		})
	}
}
