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

// Package landstrategy holds the shared source-control integration strategy
// used across SubmitQueue, runway, and other repo-local domains. It names how a
// change is integrated into the target branch (rebase, squash-rebase, merge,
// promote).
package landstrategy

// Strategy defines the possible source control integration methods.
type Strategy string

const (
	// StrategyUnknown is the unknown strategy. It is set by default when the structure is initialized. It should never be seen in the system and is used for error control.
	StrategyUnknown Strategy = ""
	// StrategyRebase rebases commits onto the target branch before landing.
	StrategyRebase Strategy = "rebase"
	// StrategySquashRebase squashes commits into a single commit before rebasing on top of the target branch.
	StrategySquashRebase Strategy = "squash_rebase"
	// StrategyMerge merges commits into the target branch by creating a separate merge commit, preserving the commit history along with hashes.
	StrategyMerge Strategy = "merge"
	// StrategyPromote integrates the exact revision as-is, with no content transform — it advances the target branch to an already-existing commit rather than producing new revisions. The implementer maps it to its backend (git fast-forward, Mercurial bookmark advance, Subversion/Perforce copy). Used to promote an already-landed/verified commit onto another branch.
	StrategyPromote Strategy = "promote"
)
