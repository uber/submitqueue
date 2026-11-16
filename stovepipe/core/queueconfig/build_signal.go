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

import "fmt"

// BuildSignal contains configuration for the build-signal stage.
type BuildSignal struct {
	// AcceptedPollDelayMs is the positive polling interval for accepted builds, in milliseconds.
	AcceptedPollDelayMs int64 `json:"accepted_poll_delay_ms" yaml:"accepted_poll_delay_ms"`

	// RunningPollDelayMs is the positive polling interval for running builds, in milliseconds.
	RunningPollDelayMs int64 `json:"running_poll_delay_ms" yaml:"running_poll_delay_ms"`
}

// Validate checks the build-signal stage's configuration constraints.
func (c BuildSignal) Validate() error {
	if c.AcceptedPollDelayMs <= 0 {
		return fmt.Errorf("AcceptedPollDelayMs must be positive, got %d", c.AcceptedPollDelayMs)
	}

	if c.RunningPollDelayMs <= 0 {
		return fmt.Errorf("RunningPollDelayMs must be positive, got %d", c.RunningPollDelayMs)
	}

	return nil
}
