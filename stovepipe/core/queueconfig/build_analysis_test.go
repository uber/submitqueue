// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queueconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildAnalysisValidate(t *testing.T) {
	valid := BuildAnalysis{
		PollDelayMs: 10,
	}

	require.NoError(t, valid.Validate())

	t.Run("PollDelayMs", func(t *testing.T) {
		for _, value := range []int64{0, -1} {
			cfg := valid
			cfg.PollDelayMs = value

			require.Error(t, cfg.Validate())
		}
	})

}
