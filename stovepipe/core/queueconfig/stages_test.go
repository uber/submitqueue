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

func testStages() Stages {
	return Stages{
		Process:       Process{MaxConcurrent: 2, GateWaitDelayMs: 11},
		BuildSignal:   BuildSignal{AcceptedPollDelayMs: 12, RunningPollDelayMs: 13},
		BuildAnalysis: BuildAnalysis{PollDelayMs: 14},
		IssueTrack:    IssueTrack{RecheckDelayMs: 15, RecoveryWaitDelayMs: 16},
	}
}

func TestStagesValidateEveryStage(t *testing.T) {
	valid := testStages()
	require.NoError(t, valid.Validate())

	for _, tt := range []struct {
		name       string
		invalidate func(*Stages)
	}{
		{"process", func(c *Stages) { c.Process = Process{} }},
		{"build signal", func(c *Stages) { c.BuildSignal = BuildSignal{} }},
		{"build analysis", func(c *Stages) { c.BuildAnalysis = BuildAnalysis{} }},
		{"issue tracking", func(c *Stages) { c.IssueTrack = IssueTrack{} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.invalidate(&cfg)

			require.Error(t, cfg.Validate())
		})
	}
}
