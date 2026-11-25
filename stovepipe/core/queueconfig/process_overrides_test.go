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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessOverridesApply(t *testing.T) {
	base := Process{
		MaxConcurrent:   10,
		GateWaitDelayMs: 11,
	}

	inherited := (ProcessOverrides{}).Apply(base)
	assert.Equal(t, base, inherited)

	t.Run("MaxConcurrent", func(t *testing.T) {
		value := int32(25)
		original := base
		override := ProcessOverrides{MaxConcurrent: &value}

		got := override.Apply(base)
		want := base
		want.MaxConcurrent = value

		assert.Equal(t, want, got)
		assert.Equal(t, original, base)

		value = 0
		assert.Equal(t, int32(25), got.MaxConcurrent)

		zero := override.Apply(base)
		assert.Zero(t, zero.MaxConcurrent)
	})

	t.Run("GateWaitDelay", func(t *testing.T) {
		value := 25*time.Millisecond + 500*time.Microsecond
		original := base
		override := ProcessOverrides{GateWaitDelay: &value}

		got := override.Apply(base)
		want := base
		want.GateWaitDelayMs = value.Milliseconds()

		assert.Equal(t, want, got)
		assert.Equal(t, original, base)

		value = 0
		assert.Equal(t, int64(25), got.GateWaitDelayMs)

		zero := override.Apply(base)
		assert.Zero(t, zero.GateWaitDelayMs)
	})
}
