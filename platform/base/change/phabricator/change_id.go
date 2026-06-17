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

package phabricator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// changeIDFormat is the expected format for change IDs, included in error messages.
const changeIDFormat = "{scheme}://D{revision_id}/{diff_id}"

// scheme is the canonical URI scheme for Phabricator change identifiers.
// Unlike GitHub (which has multiple variants: github / ghe / ghes), Phabricator
// has a single scheme; the host is encoded out-of-band via queue config.
const scheme = "phab"

// revisionPrefix is the literal "D" character that prefixes Phabricator revision
// IDs (e.g., D12345). Mirrors how revisions are referenced everywhere in the
// Phabricator UI and CLI ("arc diff", "arc patch D12345"), so URIs can be
// constructed by trivial substitution rather than reshaping.
const revisionPrefix = "D"

// revisionPattern matches a revision segment: the literal "D" followed by a
// positive integer with no leading zero (matches the strict round-trip form
// produced by String()).
var revisionPattern = regexp.MustCompile(`^D([1-9]\d*)$`)

// diffPattern matches a diff segment: a positive integer with no leading zero
// (matches the strict round-trip form produced by String()).
var diffPattern = regexp.MustCompile(`^[1-9]\d*$`)

// ChangeID represents a parsed Phabricator change identifier.
// Format: phab://D{revision_id}/{diff_id}
//
// Revision and diff are Phabricator's two-level identifier model:
//   - RevisionID names the logical review (stable across updates, e.g., D12345).
//   - DiffID names a specific uploaded patch version of that revision
//     (increments on every "arc diff" update). It pins the change to an exact
//     code state, analogous to GitHub's head commit SHA on a pull request.
type ChangeID struct {
	// Scheme captures the URI scheme. Always "phab" today; kept as a field so
	// the parsed form mirrors entity/change/github.ChangeID and so future variants
	// (e.g., a separate scheme per Phabricator install) are a non-breaking add.
	Scheme string
	// RevisionID is the numeric portion of the Phabricator revision identifier
	// (the digits after the leading "D"). For example, "D12345" parses to 12345.
	RevisionID int
	// DiffID is the numeric Phabricator diff identifier that pins a specific
	// uploaded patch version of the revision.
	DiffID int
}

// ParseChangeID parses a raw change ID string into a ChangeID.
// Expected format: phab://D{revision_id}/{diff_id}
// The parser splits on "://" to separate scheme from path, then the path into
// exactly two segments: the D-prefixed revision and the diff ID.
func ParseChangeID(raw string) (ChangeID, error) {
	schemeSplit := strings.SplitN(raw, "://", 2)
	if len(schemeSplit) != 2 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: missing '://' separator (expected format: %s)", raw, changeIDFormat)
	}

	gotScheme := schemeSplit[0]
	if gotScheme != scheme {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: scheme must be %q, got %q (expected format: %s)", raw, scheme, gotScheme, changeIDFormat)
	}

	path := schemeSplit[1]

	segments := strings.Split(path, "/")
	if len(segments) != 2 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: path must have exactly 2 segments (revision/diff), got %d (expected format: %s)", raw, len(segments), changeIDFormat)
	}

	revisionSegment := segments[0]
	diffSegment := segments[1]

	revisionMatch := revisionPattern.FindStringSubmatch(revisionSegment)
	if revisionMatch == nil {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: revision %q must match D{positive_int} (expected format: %s)", raw, revisionSegment, changeIDFormat)
	}
	revisionID, err := strconv.Atoi(revisionMatch[1])
	if err != nil {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: revision %q overflows int: %w (expected format: %s)", raw, revisionSegment, err, changeIDFormat)
	}

	if !diffPattern.MatchString(diffSegment) {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: diff %q must be a positive integer (expected format: %s)", raw, diffSegment, changeIDFormat)
	}
	diffID, err := strconv.Atoi(diffSegment)
	if err != nil {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: diff %q overflows int: %w (expected format: %s)", raw, diffSegment, err, changeIDFormat)
	}

	return ChangeID{
		Scheme:     gotScheme,
		RevisionID: revisionID,
		DiffID:     diffID,
	}, nil
}

// String returns the string representation of the change ID.
func (c ChangeID) String() string {
	return fmt.Sprintf("%s://%s%d/%d", c.Scheme, revisionPrefix, c.RevisionID, c.DiffID)
}

// Revision returns the Phabricator revision identifier in its canonical
// D-prefixed string form (e.g., "D12345").
func (c ChangeID) Revision() string {
	return fmt.Sprintf("%s%d", revisionPrefix, c.RevisionID)
}
