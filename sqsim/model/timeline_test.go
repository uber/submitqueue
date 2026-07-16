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

package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/sqsim/entity"
)

func TestBuildStatusAtUsesLatestVisiblePoint(t *testing.T) {
	timeline := []entity.BuildStatusPoint{
		{AfterMs: 0, Status: entity.BuildAccepted},
		{AfterMs: 1000, Status: entity.BuildRunning},
		{AfterMs: 5000, Status: entity.BuildSucceeded},
	}

	assert.Equal(t, entity.BuildAccepted, BuildStatusAt(timeline, 500*time.Millisecond))
	assert.Equal(t, entity.BuildRunning, BuildStatusAt(timeline, 2*time.Second))
	assert.Equal(t, entity.BuildSucceeded, BuildStatusAt(timeline, 5*time.Second))
}
