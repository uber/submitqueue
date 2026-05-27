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

package changeprovider

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// User represents the author of a change.
type User struct {
	// Name is the display name of the user.
	Name string
	// Email is the email address of the user.
	Email string
}

// ChangedFile represents a single file modification in a change.
type ChangedFile struct {
	// Path is the file path relative to the repository root.
	Path string
	// Patch is the diff patch content for this file.
	Patch string
	// LinesAdded is the number of lines added in this file.
	LinesAdded int
	// LinesDeleted is the number of lines deleted in this file.
	LinesDeleted int
}

// ChangeInfo contains metadata and file changes for a code change.
type ChangeInfo struct {
	// URI is the full change URI for correlation with the input request
	// (e.g., "github://uber/repo/pull/98/c3a4d5e6f7890123456789abcdef0123456789ab" or "phab://D123/xyz789").
	URI string
	// User is the author of the change.
	User User
	// ChangedFiles is the list of files modified in this change. Order is unspecified.
	ChangedFiles []ChangedFile
}

// ChangeProvider fetches change metadata from external systems
// Each implementation is configured for a specific provider (GitHub, GitLab, Phabricator).
type ChangeProvider interface {
	// Get retrieves change information for the provided Change.
	// For a Change with multiple URIs (e.g., stacked PRs), returns one ChangeInfo per URI.
	// Returns a slice of ChangeInfo, one for each change in the stack.
	Get(ctx context.Context, change entity.Change) ([]ChangeInfo, error)
}
