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

// StageOverrides contains optional configuration grouped by stage.
// Validate the resolved result after applying shared and per-queue overrides.
type StageOverrides struct {
	// Process contains optional configuration for the process stage.
	Process ProcessOverrides `yaml:"process"`

	// BuildSignal contains optional configuration for the build-signal stage.
	BuildSignal BuildSignalOverrides `yaml:"buildSignal"`

	// BuildAnalysis contains optional configuration for the build-analysis stage.
	BuildAnalysis BuildAnalysisOverrides `yaml:"buildAnalysis"`

	// IssueTrack contains optional configuration for the issue-tracking stage.
	IssueTrack IssueTrackOverrides `yaml:"issueTrack"`
}

// Apply copies base and applies each stage's overrides.
func (o StageOverrides) Apply(base Stages) Stages {
	return Stages{
		Process:       o.Process.Apply(base.Process),
		BuildSignal:   o.BuildSignal.Apply(base.BuildSignal),
		BuildAnalysis: o.BuildAnalysis.Apply(base.BuildAnalysis),
		IssueTrack:    o.IssueTrack.Apply(base.IssueTrack),
	}
}

// DefaultStages returns independent values for standard stage behavior.
// Providers apply explicit shared and per-queue settings over this baseline.
func DefaultStages() Stages {
	return Stages{
		Process:       defaultProcess(),
		BuildSignal:   defaultBuildSignal(),
		BuildAnalysis: defaultBuildAnalysis(),
		IssueTrack:    defaultIssueTrack(),
	}
}
