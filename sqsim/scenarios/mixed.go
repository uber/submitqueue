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

// MixedConcurrent returns staggered requests with successful, conflict, and build-failure outcomes.
func MixedConcurrent() (sqsim.Scenario, error) {
	success := sqsim.NewBehavior().
		BuildRunner(sqsim.BuildSucceededAfter(8 * time.Second)).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.NewMergeBehavior().Invoke(sqsim.MergeSucceededAfter(time.Second)))
	conflict := sqsim.NewBehavior().
		BuildRunner(sqsim.SuccessfulBuildRunner()).
		MergeConflictCheck(sqsim.ConflictingMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())
	buildFailure := sqsim.NewBehavior().
		BuildRunner(sqsim.BuildFailedAfter(5 * time.Second)).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

	return sqsim.NewScenario().
		Timeout(90*time.Second).
		Land(
			sqsim.NewLand("lands").
				Queue("sqsim").
				Behavior(success).
				Expect(sqsim.RequestLanded),
			sqsim.NewLand("conflicts").
				Queue("sqsim").
				SubmitAfter(2*time.Second).
				Behavior(conflict).
				Expect(sqsim.RequestError),
			sqsim.NewLand("build-fails").
				Queue("sqsim").
				SubmitAfter(4*time.Second).
				Behavior(buildFailure).
				Expect(sqsim.RequestError),
		).
		Build()
}
