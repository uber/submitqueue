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

// Package git parses change IDs that use the git:// URI scheme.
package git

import (
	"fmt"
	"strings"
)

// Scheme is the URI scheme for git-backed change identifiers.
const Scheme = "git"

// changeIDFormat is the expected format for change IDs, included in error messages.
const changeIDFormat = "git://{owner}/{repo}/{branch}/{revision}"

// revisionLength is the length of a git object name used as a revision identifier.
const revisionLength = 40

// ChangeID represents a parsed git:// change identifier.
// Format: git://{owner}/{repo}/{branch}/{revision}
type ChangeID struct {
	// Owner is the organization or owner of the repository.
	Owner string
	// Repo is the repository name.
	Repo string
	// Branch is the branch the change landed on.
	Branch string
	// Revision is the object name that pins the change on the branch.
	Revision string
}

// ParseChangeID parses a raw change ID string into a ChangeID.
func ParseChangeID(raw string) (ChangeID, error) {
	schemeSplit := strings.SplitN(raw, "://", 2)
	if len(schemeSplit) != 2 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: missing '://' separator (expected format: %s)", raw, changeIDFormat)
	}

	if schemeSplit[0] != Scheme {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: scheme must be %q, got %q (expected format: %s)", raw, Scheme, schemeSplit[0], changeIDFormat)
	}

	path := schemeSplit[1]
	segments := strings.Split(path, "/")
	if len(segments) < 4 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: need at least owner/repo/branch/revision, got %d segments (expected format: %s)", raw, len(segments), changeIDFormat)
	}

	revision := segments[len(segments)-1]
	branch := segments[len(segments)-2]
	repo := segments[len(segments)-3]
	ownerSegments := segments[:len(segments)-3]

	if revision == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty revision (expected format: %s)", raw, changeIDFormat)
	}
	if !isFullHexRevision(revision) {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: revision %q must be %d lowercase hex characters (expected format: %s)", raw, revision, revisionLength, changeIDFormat)
	}
	if branch == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty branch (expected format: %s)", raw, changeIDFormat)
	}
	if repo == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty repo (expected format: %s)", raw, changeIDFormat)
	}

	owner := strings.Join(ownerSegments, "/")
	if owner == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty owner (expected format: %s)", raw, changeIDFormat)
	}

	return ChangeID{
		Owner:    owner,
		Repo:     repo,
		Branch:   branch,
		Revision: revision,
	}, nil
}

// String returns the canonical URI representation of the change ID.
func (c ChangeID) String() string {
	return fmt.Sprintf("%s://%s/%s/%s/%s", Scheme, c.Owner, c.Repo, c.Branch, c.Revision)
}

// OwnerRepo returns the "{owner}/{repo}" string.
func (c ChangeID) OwnerRepo() string {
	return fmt.Sprintf("%s/%s", c.Owner, c.Repo)
}

func isFullHexRevision(s string) bool {
	if len(s) != revisionLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
