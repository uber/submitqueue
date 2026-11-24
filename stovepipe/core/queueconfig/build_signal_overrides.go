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

import "time"

// BuildSignalOverrides contains optional configuration for the build-signal stage.
// Nil fields inherit their previous value; explicit values are retained for validation.
type BuildSignalOverrides struct {
	// AcceptedPollDelay overrides the polling interval for accepted builds.
	AcceptedPollDelay *time.Duration `yaml:"acceptedPollDelay"`

	// RunningPollDelay overrides the polling interval for running builds.
	RunningPollDelay *time.Duration `yaml:"runningPollDelay"`
}

// Apply copies base and replaces explicitly supplied settings.
// Durations are truncated to milliseconds. Validate the final resolved configuration.
func (o BuildSignalOverrides) Apply(base BuildSignal) BuildSignal {
	if o.AcceptedPollDelay != nil {
		base.AcceptedPollDelayMs = o.AcceptedPollDelay.Milliseconds()
	}

	if o.RunningPollDelay != nil {
		base.RunningPollDelayMs = o.RunningPollDelay.Milliseconds()
	}

	return base
}

func defaultBuildSignal() BuildSignal {
	return BuildSignal{
		AcceptedPollDelayMs: (5 * time.Second).Milliseconds(),
		RunningPollDelayMs:  (2 * time.Second).Milliseconds(),
	}
}
