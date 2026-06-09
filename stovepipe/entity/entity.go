// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package entity holds Stovepipe-specific domain entities.
package entity

// ChangeInfo represents a new change detected on a VCS remote.
// It is intentionally VCS-agnostic: the URI scheme carries the
// provider identity, mirroring the github:// scheme used in
// SubmitQueue's ChangeInfo.
//
// URI format: "git://<host>/<repo>/<ref>/<new-revision>"
// Example:    "git://github.com/uber/go-code/refs/heads/main/c3a4d5e6f789..."
//
// Fields are immutable after construction.
type ChangeInfo struct {
	// URI is the canonical VCS identifier for this change.
	// Scheme is "git://"; path encodes host, repo, ref, and new revision.
	URI string `json:"uri"`

	// PreviousURI is the URI of the prior revision on the same ref, if known.
	// Empty string if unavailable.
	// Example: "git://github.com/uber/go-code/refs/heads/main/aabbccdd..."
	PreviousURI string `json:"previous_uri,omitempty"`

	// AuthorName is the display name of the person who authored the change.
	AuthorName string `json:"author_name,omitempty"`
	// AuthorEmail is the email address of the author.
	AuthorEmail string `json:"author_email,omitempty"`
}
