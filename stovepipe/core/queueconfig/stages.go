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

// Stages contains configuration for the validation pipeline stages.
type Stages struct {
	// Process contains configuration for the process stage.
	Process Process `json:"process" yaml:"process"`

	// BuildSignal contains configuration for the build-signal stage.
	BuildSignal BuildSignal `json:"build_signal" yaml:"build_signal"`

	// BuildAnalysis contains configuration for the build-analysis stage.
	BuildAnalysis BuildAnalysis `json:"build_analysis" yaml:"build_analysis"`

	// IssueTrack contains configuration for the issue-tracking stage.
	IssueTrack IssueTrack `json:"issue_track" yaml:"issue_track"`
}

// Validate checks each stage after all configuration layers have been applied.
func (c Stages) Validate() error {
	if err := c.Process.Validate(); err != nil {
		return fmt.Errorf("process: %w", err)
	}

	if err := c.BuildSignal.Validate(); err != nil {
		return fmt.Errorf("buildSignal: %w", err)
	}

	if err := c.BuildAnalysis.Validate(); err != nil {
		return fmt.Errorf("buildAnalysis: %w", err)
	}

	if err := c.IssueTrack.Validate(); err != nil {
		return fmt.Errorf("issueTrack: %w", err)
	}

	return nil
}
