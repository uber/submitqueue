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
	"fmt"
	"time"

	"github.com/uber/submitqueue/sqsim"
)

const loadRequestCount = 1000

// Load1000 returns an opt-in workload of one thousand successful requests.
func Load1000() (sqsim.Scenario, error) {
	lands := make([]*sqsim.LandBuilder, 0, loadRequestCount)
	for i := 1; i <= loadRequestCount; i++ {
		behavior := sqsim.NewBehavior().
			BuildRunner(sqsim.BuildSucceededAfter(time.Second)).
			MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
			Merge(sqsim.SuccessfulMerge())
		lands = append(lands,
			sqsim.NewLand(fmt.Sprintf("l%04d", i)).
				Queue("sqsim").
				Behavior(behavior).
				Expect(sqsim.RequestLanded),
		)
	}

	return sqsim.NewScenario().
		Timeout(10 * time.Minute).
		Land(lands...).
		Build()
}
