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

// HappyPath returns one request whose external operations all succeed.
func HappyPath() (sqsim.Scenario, error) {
	happy := sqsim.NewBehavior().
		BuildRunner(sqsim.NewBuildRunnerBehavior().
			Trigger(sqsim.BuildCreated(
				sqsim.StatusAt(0, sqsim.BuildAccepted),
				sqsim.StatusAt(500*time.Millisecond, sqsim.BuildRunning),
				sqsim.StatusAt(5*time.Second, sqsim.BuildSucceeded),
			))).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.NewMergeBehavior().
			Invoke(sqsim.MergeSucceededAfter(500 * time.Millisecond)))

	return sqsim.NewScenario().
		Timeout(30 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(happy).
				Expect(sqsim.RequestLanded),
		).
		Build()
}
