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

package git

import (
	"context"
	"fmt"
	"strings"
)

// authorIdent is the person a commit is credited to, as distinct from the
// committer that produced it.
//
// Git records the two separately and the merger keeps them separate: the
// merger is always the committer, because it is what applied the change, while
// the author is carried over from the change itself. A strategy that mints a
// fresh commit would otherwise credit the service for work someone else wrote.
type authorIdent struct {
	// Name is the author's display name, verbatim from the commit it was read
	// from. Empty when the commit did not record one.
	Name string
	// Email is the author's email address, verbatim from the commit it was read
	// from. Empty when the commit did not record one.
	Email string
}

// usable reports whether the identity can be passed to `git commit --author`.
//
// A half-known identity is unusable — git wants a name and an address, and
// substituting a placeholder for the missing half would attribute the commit
// to someone who does not exist, which is worse than not attributing it at
// all. So is one containing an angle bracket or a line break: --author takes a
// single "Name <address>" string that git parses back apart, and such a value
// splits in the wrong place or makes git search the history for a matching
// author and fail the commit outright.
func (a authorIdent) usable() bool {
	if a.Name == "" || a.Email == "" {
		return false
	}
	return !strings.ContainsAny(a.Name+a.Email, "<>\n")
}

// args renders the identity as the `git commit` arguments that credit it. It
// returns nil for an unusable identity, which leaves the configured committer
// identity to stand in for both roles. The identity travels as an argument
// rather than through the environment because a workspace command carries
// arguments only.
func (a authorIdent) args() []string {
	if !a.usable() {
		return nil
	}
	return []string{"--author", a.Name + " <" + a.Email + ">"}
}

// commitAuthor reads the author recorded on a commit.
//
// The commit is the one the change's URI pins, so the attribution follows what
// the request names rather than anything the merger infers. It is already
// present locally: every referenced commit is fetched and verified before a
// step is applied, so this reads from the object store and costs no network.
//
// A commit with no readable author is not an error. It yields an incomplete
// identity, and the caller falls back to the committer identity rather than
// failing a merge over attribution.
func (m *gitMerger) commitAuthor(ctx context.Context, sha string) (authorIdent, error) {
	// NUL-separated: a display name may contain spaces, angle brackets, quotes
	// or anything else a person put in their git config, and none of those can
	// be mistaken for the field separator.
	out, err := m.run(ctx, "show", "--no-patch", "--format=%an%x00%ae", sha)
	if err != nil {
		return authorIdent{}, fmt.Errorf("git show --no-patch --format author %s: %w", sha, err)
	}
	name, email, ok := strings.Cut(strings.TrimRight(string(out), "\n"), "\x00")
	if !ok {
		return authorIdent{}, nil
	}
	return authorIdent{Name: name, Email: email}, nil
}
