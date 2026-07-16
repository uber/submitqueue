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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/sqsim"
	"github.com/uber/submitqueue/sqsim/scenarios"
)

func TestProfileWriteLoadRoundTrip(t *testing.T) {
	scenario, err := scenarios.HappyPath()
	require.NoError(t, err)
	profile, err := Compile("happy-path", scenario)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "profile.json")
	require.NoError(t, Write(path, profile))

	loaded, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, profile, loaded)

	loaded.Scenario.Lands[0].Behavior.BuildRunner.Triggers[0].Outcome.Build.Timeline[0].Status = sqsim.BuildFailed
	assert.Equal(t, sqsim.BuildAccepted, profile.Scenario.Lands[0].Behavior.BuildRunner.Triggers[0].Outcome.Build.Timeline[0].Status)
}

func TestCompileRejectsInvalidName(t *testing.T) {
	scenario, err := scenarios.HappyPath()
	require.NoError(t, err)

	_, err = Compile("not/a/name", scenario)
	require.Error(t, err)
}
