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

func TestIssueTrackValidate(t *testing.T) {
	valid := IssueTrack{
		RefreshDelayMs:      10,
		RecoveryWaitDelayMs: 11,
	}

	require.NoError(t, valid.Validate())

	t.Run("RefreshDelayMs", func(t *testing.T) {
		for _, value := range []int64{0, -1} {
			cfg := valid
			cfg.RefreshDelayMs = value

			require.Error(t, cfg.Validate())
		}
	})

	t.Run("RecoveryWaitDelayMs", func(t *testing.T) {
		for _, value := range []int64{0, -1} {
			cfg := valid
			cfg.RecoveryWaitDelayMs = value

			require.Error(t, cfg.Validate())
		}
	})

}
