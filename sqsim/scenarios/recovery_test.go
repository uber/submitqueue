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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/sqsim"
)

func TestRecoveryScenariosExpectLanding(t *testing.T) {
	tests := map[string]Builder{
		"build status":         BuildStatusRecovery,
		"build trigger":        BuildTriggerRecovery,
		"merge conflict check": MergeConflictCheckRecovery,
		"merge response lost":  MergeResponseLost,
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			scenario, err := build()
			require.NoError(t, err)
			require.Len(t, scenario.Lands, 1)
			assert.Equal(t, sqsim.RequestLanded, scenario.Lands[0].Expectation.Status)
		})
	}
}
