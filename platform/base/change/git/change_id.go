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
	"net/url"
	"strings"

	"github.com/uber/submitqueue/platform/base/change/changeutil"
)

// scheme is the canonical URI scheme for git-backed change identifiers.
const scheme = "git"

// refPrefix is the namespace every fully-qualified git ref lives under
// (refs/heads/<branch>, refs/tags/<tag>, ...). Disambiguates between branches and tags
// of the same name.
const refPrefix = "refs/"

// changeIDFormat is the expected format for change IDs, included in error messages.
const changeIDFormat = "git://{remote}/{repo}/{ref}/{commit_sha}"

// shaLength is the length of a git commit SHA.
const shaLength = 40

// ChangeID represents a parsed git:// change identifier.
// Format: git://{remote}/{repo}/{ref}/{commit_sha}
//
// Ref is a fully-qualified, percent-encoded git ref so that branches, tags, and
// ref names containing slashes all fit a single path segment unambiguously.
type ChangeID struct {
	// Scheme captures the URI scheme (always "git" in current implementation).
	Scheme string
	// Remote is the host (or host:port) of the git remote the repository lives
	// on (e.g. "git.example.com" or "git.example.com:9418").
	Remote string
	// Repo is the path to the repository on the remote and may contain slashes
	// (e.g. "uber/monorepo" or "team/group/repo.git").
	Repo string
	// Ref is the fully-qualified git ref the change landed on, decoded from the
	// URI (e.g. "refs/heads/main", "refs/tags/v1.0").
	Ref string
	// CommitSHA is a commit that ref has pointed to at some point in time.
	CommitSHA string
}

// ParseChangeID parses a raw change ID string into a ChangeID.
// Expected format: git://{remote}/{repo}/{ref}/{commit_sha}, where {ref} is a
// fully-qualified, percent-encoded git ref (e.g. "refs%2Fheads%2Fmain").
func ParseChangeID(raw string) (ChangeID, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: %w (expected format: %s)", raw, err, changeIDFormat)
	}
	if u.Scheme != scheme {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: scheme must be %q, got %q (expected format: %s)", raw, scheme, u.Scheme, changeIDFormat)
	}
	if u.Host == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: missing remote (expected format: %s)", raw, changeIDFormat)
	}

	// Split on the escaped path so the percent-encoded ref stays a single
	// segment (url.URL.Path decodes %2F to "/", which would split it apart).
	segments := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	// Need at least 3 segments: {repo}/{ref}/{commit_sha}.
	if len(segments) < 3 {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: need at least repo/ref/sha, got %d path segments (expected format: %s)", raw, len(segments), changeIDFormat)
	}

	sha := segments[len(segments)-1]
	encodedRef := segments[len(segments)-2]
	repo := strings.Join(segments[:len(segments)-2], "/")

	if sha == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty commit SHA (expected format: %s)", raw, changeIDFormat)
	}
	if !changeutil.IsFullHex(sha, shaLength) {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: commit SHA %q must be %d lowercase hex characters (expected format: %s)", raw, sha, shaLength, changeIDFormat)
	}

	ref, err := url.PathUnescape(encodedRef)
	if err != nil {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: ref %q is not valid percent-encoding: %w (expected format: %s)", raw, encodedRef, err, changeIDFormat)
	}
	if !strings.HasPrefix(ref, refPrefix) || ref == refPrefix {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: ref %q must be a fully-qualified git ref (e.g. refs/heads/main, refs/tags/v1.0) (expected format: %s)", raw, ref, changeIDFormat)
	}

	if repo == "" {
		return ChangeID{}, fmt.Errorf("invalid change ID %q: empty repo (expected format: %s)", raw, changeIDFormat)
	}

	return ChangeID{
		Scheme:    u.Scheme,
		Remote:    u.Host,
		Repo:      repo,
		Ref:       ref,
		CommitSHA: sha,
	}, nil
}

// String returns the string representation of the change ID.
func (c ChangeID) String() string {
	return fmt.Sprintf("%s://%s/%s/%s/%s", c.Scheme, c.Remote, c.Repo, url.PathEscape(c.Ref), c.CommitSHA)
}
