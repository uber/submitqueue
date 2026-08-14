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

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	gitchange "github.com/uber/submitqueue/platform/base/change/git"
	"github.com/uber/submitqueue/submitqueue/client"
)

// fakeRemote is the authority in the change URIs this source mints. Nothing
// resolves it — with no repository behind a change, the URI is an identifier
// and not an address.
const fakeRemote = "demo.example.com"

// fakeRepo is the repository segment of those URIs.
const fakeRepo = "demo"

// fakeSource invents changes. It performs no I/O at all: there is no repository
// to create a branch in, so a change is a URI and nothing more.
//
// This is what the default provider submits. It is the fastest way to watch the
// queue work, and the reason the quickstart needs neither a repository nor a
// credential — at the cost of the URIs pointing at nothing, which is only
// sound because the fake change provider echoes back whatever it is handed and
// the noop merger never tries to fetch it.
type fakeSource struct{}

func (fakeSource) baseSHA(_ context.Context, branch string) (string, error) {
	return syntheticSHA("base", branch), nil
}

func (fakeSource) open(_ context.Context, spec changeSpec) (openedChange, error) {
	// Derived from the branch, so a change is distinct from every other change
	// and stable across runs of the same tag — a run can be replayed and the
	// URIs it submits will match.
	headSHA := syntheticSHA("head", spec.branch)
	return openedChange{
		headSHA: headSHA,
		// No files are written anywhere, but the change still says which paths
		// it would have touched, so the conflict analyzer has something to key
		// on. It is the only claim in this mode that is not backed by anything.
		uri: withFiles(gitchange.ChangeID{
			Scheme: "git", Remote: fakeRemote, Repo: fakeRepo,
			Ref: "refs/heads/" + spec.branch, CommitSHA: headSHA,
		}.String(), spec.files),
		// There is no pull request to number and nothing to link to, so the
		// branch name is what identifies the change. An empty URL renders as
		// plain text rather than as a link that goes nowhere.
		cell: client.Cell{Text: spec.branch},
	}, nil
}

// syntheticSHA is a stand-in for a commit SHA: 40 lowercase hex characters,
// which is what a change URI requires and what the gateway validates.
func syntheticSHA(kind, seed string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + seed))
	return hex.EncodeToString(sum[:])[:40]
}
