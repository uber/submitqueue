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

// IssueTrack contains configuration for the issue-tracking stage.
type IssueTrack struct {
	// RecheckDelayMs is the positive delay between issue-tracking checks, in milliseconds.
	RecheckDelayMs int64 `json:"recheck_delay_ms" yaml:"recheck_delay_ms"`

	// RecoveryWaitDelayMs is the positive wait during recovery, in milliseconds.
	RecoveryWaitDelayMs int64 `json:"recovery_wait_delay_ms" yaml:"recovery_wait_delay_ms"`
}

// Validate checks the issue-tracking stage's configuration constraints.
func (c IssueTrack) Validate() error {
	if c.RecheckDelayMs <= 0 {
		return fmt.Errorf("RecheckDelayMs must be positive, got %d", c.RecheckDelayMs)
	}

	if c.RecoveryWaitDelayMs <= 0 {
		return fmt.Errorf("RecoveryWaitDelayMs must be positive, got %d", c.RecoveryWaitDelayMs)
	}

	return nil
}
