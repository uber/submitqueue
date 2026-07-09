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

// Author represents the author of a change.
type Author struct {
	// Name is the display name of the author.
	Name string `json:"name"`
	// Email is the email address of the author.
	Email string `json:"email"`
}

// ChangedFile represents a single file modification in a change.
type ChangedFile struct {
	// Path is the file path relative to the repository root.
	Path string `json:"path"`
	// LinesAdded is the number of lines added in this file.
	LinesAdded int `json:"lines_added"`
	// LinesDeleted is the number of lines deleted in this file.
	LinesDeleted int `json:"lines_deleted"`
	// LinesModified is the number of lines modified in this file. Some providers
	// (e.g. GitHub) report only additions and deletions and leave this zero.
	LinesModified int `json:"lines_modified"`
}

// TotalLines returns the total number of lines touched in this file.
func (f ChangedFile) TotalLines() int {
	return f.LinesAdded + f.LinesDeleted + f.LinesModified
}

// ChangeDetails holds the provider-supplied facts about a single change (author,
// modified files, line counts). It carries no identity — the owning URI lives on
// ChangeInfo (provider correlation) and ChangeRecord (persisted claim).
type ChangeDetails struct {
	// Author is the author of the change.
	Author Author `json:"author"`
	// ChangedFiles is the list of files modified in this change. Order is unspecified.
	ChangedFiles []ChangedFile `json:"changed_files,omitempty"`
}

// TotalLinesChanged returns the total number of lines touched across all files in the change.
func (d ChangeDetails) TotalLinesChanged() int {
	total := 0
	for _, f := range d.ChangedFiles {
		total += f.TotalLines()
	}
	return total
}

// FileCount returns the number of files touched in the change.
func (d ChangeDetails) FileCount() int {
	return len(d.ChangedFiles)
}

// ChangeInfo maps a change URI to its details. It is the change provider's return
// type: for a Change with multiple URIs (e.g. a stacked PR set), the provider
// returns one ChangeInfo per URI so callers can correlate results to inputs by URI.
type ChangeInfo struct {
	// URI is the full change URI for correlation with the input request
	// (e.g., "github://github.example.com/uber/repo/pull/98/c3a4d5e6f7890123456789abcdef0123456789ab").
	URI string `json:"uri"`
	// Details is the provider-supplied facts for this URI.
	Details ChangeDetails `json:"details"`
}
