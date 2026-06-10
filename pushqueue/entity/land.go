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

// QueueTarget identifies a landing destination in a version control system.
// Defined locally in pushqueue; consolidated into shared entity/ by Chunk 2.
type QueueTarget struct {
	// Name is an optional logical identifier for correlation and config lookup.
	Name string
	// Address is the VCS repository address (remote URL, depot path).
	Address string
	// Target is the landing ref (branch name, stream path).
	Target string
}

// LandStrategy defines the possible landing methods for a code change.
type LandStrategy string

const (
	// LandStrategyUnknown is the unknown strategy. It is set by default
	// when the structure is initialized. It should never be seen in the system.
	LandStrategyUnknown LandStrategy = ""
	// LandStrategyRebase rebases commits onto the target branch before landing.
	LandStrategyRebase LandStrategy = "rebase"
	// LandStrategySquashRebase squashes commits into a single commit before rebasing.
	LandStrategySquashRebase LandStrategy = "squash_rebase"
	// LandStrategyMerge creates a merge commit preserving commit history.
	LandStrategyMerge LandStrategy = "merge"
)

// LandItem represents a single code change to land with its strategy.
type LandItem struct {
	// URIs identifies the change (RFC 3986). Scheme identifies the provider.
	URIs []string
	// Strategy is the landing strategy for this change.
	Strategy LandStrategy
}
