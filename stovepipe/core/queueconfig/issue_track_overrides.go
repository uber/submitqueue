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

// IssueTrackOverrides contains optional configuration for the issue-tracking stage.
// Nil fields inherit their previous value; explicit values are retained for validation.
type IssueTrackOverrides struct {
	// RefreshDelay overrides the interval between refreshes.
	RefreshDelay *time.Duration `yaml:"refreshDelay"`

	// RecoveryWaitDelay overrides the wait during recovery.
	RecoveryWaitDelay *time.Duration `yaml:"recoveryWaitDelay"`
}

// Apply copies base and replaces explicitly supplied settings.
// Durations are truncated to milliseconds. Validate the final resolved configuration.
func (o IssueTrackOverrides) Apply(base IssueTrack) IssueTrack {
	if o.RefreshDelay != nil {
		base.RefreshDelayMs = o.RefreshDelay.Milliseconds()
	}

	if o.RecoveryWaitDelay != nil {
		base.RecoveryWaitDelayMs = o.RecoveryWaitDelay.Milliseconds()
	}

	return base
}

func defaultIssueTrack() IssueTrack {
	return IssueTrack{
		RefreshDelayMs:      (30 * time.Second).Milliseconds(),
		RecoveryWaitDelayMs: (30 * time.Second).Milliseconds(),
	}
}
