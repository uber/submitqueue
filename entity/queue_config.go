// Copyright (c) 2025 Uber Technologies, Inc.
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

package entity

// QueueConfig holds the configuration for a single submit queue.
// Each queue maps a VCS repository + target to a processing pipeline.
// A repository can have multiple queues, but each queue has exactly one target.
// Immutable after creation.
type QueueConfig struct {
	// Name uniquely identifies this queue within the system.
	// Referenced by Request.Queue.
	Name string `json:"name" yaml:"name"`

	// VCSType identifies the version control system (e.g., "git", "svn", "perforce").
	// A queue operates on exactly one VCS.
	VCSType string `json:"vcs_type" yaml:"vcs_type"`

	// VCSAddress identifies the repository in the version control system.
	// The format is VCS-specific:
	//   - Git: remote URL (e.g., "git@github.com:uber/submitqueue.git")
	//   - Perforce: depot path (e.g., "//depot/project")
	//   - SVN: repository URL (e.g., "https://svn.example.com/repos/project")
	VCSAddress string `json:"vcs_address" yaml:"vcs_address"`

	// Target is the landing target where changes are merged.
	// The format is VCS-specific:
	//   - Git: branch ref (e.g., "main", "release/v2")
	//   - Perforce: stream or depot path (e.g., "//depot/main/...")
	//   - SVN: repository path (e.g., "trunk/")
	Target string `json:"target" yaml:"target"`

	// BuildRunner identifies the CI pipeline or job that runs builds for this queue.
	// Opaque to the system; meaningful only to the build runner extension implementation.
	// Examples:
	//   - Buildkite: "buildkite.com/uber/submitqueue-ci"
	//   - Jenkins: "jenkins.example.com/job/submitqueue-verify"
	BuildRunner string `json:"build_runner" yaml:"build_runner"`

	// ChangeProvider identifies the change provider implementation for this queue.
	// Opaque to the system; meaningful only to the change provider extension implementation.
	// Examples: "github", "gitlab", "phabricator"
	ChangeProvider string `json:"change_provider" yaml:"change_provider"`

	// MergeChecker identifies the merge checker implementation for this queue.
	// Opaque to the system; meaningful only to the merge checker extension implementation.
	// Examples: "github", "gitlab"
	MergeChecker string `json:"merge_checker" yaml:"merge_checker"`

	// LandProvider identifies the land provider implementation for this queue.
	// Opaque to the system; meaningful only to the land provider extension implementation.
	// Examples: "github", "gitlab"
	LandProvider string `json:"land_provider" yaml:"land_provider"`
}
