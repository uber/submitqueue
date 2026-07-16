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

package scenarios

import (
	"time"

	"github.com/uber/submitqueue/sqsim"
)

// BuildFailure returns a request whose Build Runner reports terminal failure.
func BuildFailure() (sqsim.Scenario, error) {
	behavior := sqsim.NewBehavior().
		BuildRunner(sqsim.BuildFailedAfter(time.Second)).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())
	return singleErrorScenario(behavior)
}

// MergeConflict returns a request rejected by the dry-run mergeability check.
func MergeConflict() (sqsim.Scenario, error) {
	behavior := sqsim.NewBehavior().
		BuildRunner(sqsim.SuccessfulBuildRunner()).
		MergeConflictCheck(sqsim.ConflictingMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())
	return singleErrorScenario(behavior)
}

func singleErrorScenario(behavior *sqsim.BehaviorBuilder) (sqsim.Scenario, error) {
	return sqsim.NewScenario().
		Timeout(30 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(behavior).
				Expect(sqsim.RequestError),
		).
		Build()
}
