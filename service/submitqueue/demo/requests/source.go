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
	"net/url"
	"path"

	"github.com/uber/submitqueue/platform/fakemarker"
	"github.com/uber/submitqueue/submitqueue/client"
)

// Provider names, matching the demo provider directories and the Makefile's
// PROVIDER variable.
const (
	providerFake   = "fake"
	providerGit    = "git"
	providerGitHub = "github"
)

// changeSource creates the changes a run submits, in whatever way the provider
// it stands for makes a change exist.
//
// Everything above this interface — how many changes to create, whether they
// stack, when each is enqueued, what the table shows — is the same for all
// three. What differs is only whether a change is a pull request, a branch in a
// repository on disk, or nothing at all but a URI.
type changeSource interface {
	// baseSHA resolves the commit the first change branches from.
	baseSHA(ctx context.Context, branch string) (string, error)

	// open makes one change exist and reports what the run needs to track it.
	open(ctx context.Context, spec changeSpec) (openedChange, error)
}

// changeSpec is one change to create: a branch cut from a parent, carrying
// files. The caller decides what a change is made of, so every provider
// produces the same shape of change.
type changeSpec struct {
	// branch is the ref to create.
	branch string
	// parentBranch is what the change targets — the base branch, or the branch
	// of the change before it in a stack.
	parentBranch string
	// parentSHA is the commit the branch is cut from.
	parentSHA string
	// title describes the change where a provider shows one.
	title string
	// files are committed one at a time, so a change arrives as a range of
	// commits rather than a single edit.
	files []changeFile
	// note reports progress to the run's table. Sources call it for the steps
	// slow enough to be worth watching.
	note func(format string, args ...any)
}

// changeFile is one file a change writes, and the commit that carries it.
type changeFile struct {
	path    string
	body    string
	message string
}

// maxChangeURIBytes is the longest change URI the gateway accepts, because a
// URI is also a storage key.
const maxChangeURIBytes = 255

// withFiles appends to a change URI the paths it touches, for sources whose
// changes no provider can be asked about.
//
// The orchestrator's conflict analyzer keys on the files a change reports, and
// gets them from the change provider. With no provider behind fake and git
// changes, the run that authored them is the only thing that knows — so it says
// so on the URI, and the fake provider reads it back. Without this the analyzer
// sees a change that touches nothing, and a batch that touches nothing conflicts
// with nothing.
//
// One path per directory, not all of them. The demo's analyzer keys on the
// directory, so a second file in a directory already named adds a key that is
// already there — while the URI has a fixed byte budget that a change touching
// eight files would blow straight through. Paths that do not fit are dropped
// rather than truncated: a shortened path is a different directory, which would
// be worse than an unreported one.
func withFiles(base string, files []changeFile) string {
	seen := make(map[string]struct{}, len(files))
	marker := "?" + fakemarker.FilesPrefix

	for _, f := range files {
		dir := path.Dir(f.path)
		if _, ok := seen[dir]; ok {
			continue
		}
		entry := url.QueryEscape(f.path)
		if len(seen) > 0 {
			entry = "," + entry
		}
		if len(base)+len(marker)+len(entry) > maxChangeURIBytes {
			break
		}
		seen[dir] = struct{}{}
		marker += entry
	}

	if len(seen) == 0 {
		return base
	}
	return base + marker
}

// openedChange is what a run needs back about a change that now exists.
type openedChange struct {
	// headSHA is the commit the change's URI pins, and what the next change in
	// a stack is cut from.
	headSHA string
	// uri is the change URI submitted to the gateway.
	uri string
	// cell is what the table shows for this change.
	cell client.Cell
}
