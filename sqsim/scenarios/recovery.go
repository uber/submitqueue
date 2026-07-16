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

// BuildStatusRecovery returns a request whose first Build Runner Status call fails transiently.
func BuildStatusRecovery() (sqsim.Scenario, error) {
	buildRunner := sqsim.NewBuildRunnerBehavior().
		Trigger(sqsim.BuildCreated(
			sqsim.StatusAt(0, sqsim.BuildAccepted),
			sqsim.StatusAt(500*time.Millisecond, sqsim.BuildRunning),
			sqsim.StatusAt(2*time.Second, sqsim.BuildSucceeded),
		)).
		StatusFaultOnCall(1, sqsim.RetryableErrorBeforeSideEffect())

	return singleLandedScenario(buildRunner, sqsim.SuccessfulMergeConflictCheck(), sqsim.SuccessfulMerge())
}

// BuildTriggerRecovery returns a request whose first Build Runner Trigger call fails transiently.
func BuildTriggerRecovery() (sqsim.Scenario, error) {
	buildRunner := sqsim.NewBuildRunnerBehavior().Trigger(
		sqsim.BuildTriggerFault(sqsim.RetryableErrorBeforeSideEffect()),
		sqsim.BuildCreated(
			sqsim.StatusAt(0, sqsim.BuildAccepted),
			sqsim.StatusAt(time.Second, sqsim.BuildSucceeded),
		),
	)

	return singleLandedScenario(buildRunner, sqsim.SuccessfulMergeConflictCheck(), sqsim.SuccessfulMerge())
}

// MergeConflictCheckRecovery returns a request whose first dry-run mergeability check fails transiently.
func MergeConflictCheckRecovery() (sqsim.Scenario, error) {
	check := sqsim.NewMergeConflictCheckBehavior().Invoke(
		sqsim.MergeConflictCheckFault(sqsim.RetryableErrorBeforeSideEffect()),
		sqsim.MergeConflictCheckSucceeded(),
	)

	return singleLandedScenario(sqsim.BuildSucceededAfter(time.Second), check, sqsim.SuccessfulMerge())
}

// MergeResponseLost returns a request whose merge succeeds before its first response is lost.
func MergeResponseLost() (sqsim.Scenario, error) {
	merge := sqsim.NewMergeBehavior().Invoke(
		sqsim.MergeSucceededAfter(500 * time.Millisecond).
			Fault(sqsim.RetryableErrorAfterSideEffect()),
	)

	return singleLandedScenario(sqsim.BuildSucceededAfter(time.Second), sqsim.SuccessfulMergeConflictCheck(), merge)
}

func singleLandedScenario(
	buildRunner *sqsim.BuildRunnerBehaviorBuilder,
	check *sqsim.MergeConflictCheckBehaviorBuilder,
	merge *sqsim.MergeBehaviorBuilder,
) (sqsim.Scenario, error) {
	behavior := sqsim.NewBehavior().
		BuildRunner(buildRunner).
		MergeConflictCheck(check).
		Merge(merge)

	return sqsim.NewScenario().
		Timeout(45 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(behavior).
				Expect(sqsim.RequestLanded),
		).
		Build()
}
