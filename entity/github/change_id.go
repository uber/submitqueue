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

package github

import (
	"fmt"
	"strconv"
	"strings"
)

// changeIDFormat is the expected format for change IDs, included in error messages.
const changeIDFormat = "{scheme}://{owner}/{repo}/{pr_number}/{head_commit_sha}"

// ChangeID represents a parsed GitHub-family change identifier.
// Covers GitHub.com, GitHub Enterprise (GHE), and GitHub Enterprise Server (GHES)
// since they share the same pull request model.
// Format: {scheme}://{owner}/{repo}/{pr_number}/{head_commit_sha}
type ChangeID struct {
	// Scheme captures the source variant (e.g., "github", "ghe", "ghes").
	Scheme string
	// Org is the organization or owner of the repository.
	Org string
	// Repo is the repository name.
	Repo string
	// PRNumber is the pull request number.
	PRNumber int
	// HeadCommitSHA is the head commit SHA at the time of request creation.
	HeadCommitSHA string
}

// ParseChangeID parses a raw change ID string into a ChangeID.
// Expected format: {scheme}://{owner}/{repo}/{pr_number}/{head_commit_sha}
// The parser works from the end: SHA (last), PR number (second-to-last),
// and everything before is the repo path (split into owner and repo).
func ParseChangeID(raw string) (ChangeID, error) {
	// Split on "://" to get scheme and path
	schemeSplit := strings.SplitN(raw, "://", 2)
	if len(schemeSplit) != 2 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: missing '://' separator (expected format: %s)", raw, changeIDFormat)
	}

	scheme := schemeSplit[0]
	if scheme == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty scheme (expected format: %s)", raw, changeIDFormat)
	}

	path := schemeSplit[1]

	// Split the path into segments and parse from the end.
	segments := strings.Split(path, "/")
	// Need at least 4 segments: {owner}/{repo}/{pr_number}/{sha}
	if len(segments) < 4 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: need at least owner/repo/pr/sha, got %d segments (expected format: %s)", raw, len(segments), changeIDFormat)
	}

	sha := segments[len(segments)-1]
	prStr := segments[len(segments)-2]
	repoSegments := segments[:len(segments)-2]

	if sha == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty head commit SHA (expected format: %s)", raw, changeIDFormat)
	}

	prNumber, err := strconv.Atoi(prStr)
	if err != nil {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: PR number %q is not a valid integer (expected format: %s)", raw, prStr, changeIDFormat)
	}

	// Split repo path: last segment is repo name, everything before is the owner.
	if len(repoSegments) < 2 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: repo path must have at least owner/repo (expected format: %s)", raw, changeIDFormat)
	}

	repo := repoSegments[len(repoSegments)-1]
	org := strings.Join(repoSegments[:len(repoSegments)-1], "/")

	if org == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty owner (expected format: %s)", raw, changeIDFormat)
	}
	if repo == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty repo (expected format: %s)", raw, changeIDFormat)
	}

	return ChangeID{
		Scheme:        scheme,
		Org:           org,
		Repo:          repo,
		PRNumber:      prNumber,
		HeadCommitSHA: sha,
	}, nil
}

// String returns the string representation of the change ID.
func (c ChangeID) String() string {
	return fmt.Sprintf("%s://%s/%s/%d/%s", c.Scheme, c.Org, c.Repo, c.PRNumber, c.HeadCommitSHA)
}

// OwnerRepo returns the "{org}/{repo}" string.
func (c ChangeID) OwnerRepo() string {
	return fmt.Sprintf("%s/%s", c.Org, c.Repo)
}
