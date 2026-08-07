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
	"fmt"
	"strings"

	entitygit "github.com/uber/submitqueue/platform/base/change/git"
	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/runway/extension/merger"
)

// changeRef is everything the merger needs from a change URI, reduced to a
// form that does not depend on which provider minted it. Adding a provider
// means adding one case to resolveChange, not touching the apply paths.
type changeRef struct {
	// Provider is the URI scheme the change was addressed through ("github",
	// "git"). Every change in one request must agree on it.
	Provider string
	// SHA is the full commit hash the URI pins the change to. This is the
	// commit that gets fetched and applied.
	SHA string
	// Ref is the fully-qualified ref the provider publishes this change's head
	// under (e.g. "refs/pull/12/head"). Used as a fetch fallback and as the
	// staleness comparison point. Empty when the scheme has no such ref, in
	// which case both are skipped.
	Ref string
	// Label is a short human-readable identifier for the change, used in
	// commit messages the merger synthesizes.
	Label string
}

// resolveChange maps a change URI onto the merger's provider-neutral view of
// it. An unrecognized scheme is terminal: no retry produces a parser for it.
func resolveChange(uri string) (changeRef, error) {
	scheme, _, ok := strings.Cut(uri, "://")
	if !ok {
		return changeRef{}, fmt.Errorf("%w: change URI %q has no scheme", merger.ErrInvalidRequest, uri)
	}

	switch scheme {
	case "github":
		cid, err := entitygithub.ParseChangeID(uri)
		if err != nil {
			return changeRef{}, fmt.Errorf("%w: invalid change URI %q: %v", merger.ErrInvalidRequest, uri, err)
		}
		return changeRef{
			Provider: scheme,
			SHA:      cid.HeadCommitSHA,
			// GitHub publishes every PR's head under refs/pull/<n>/head in the
			// base repository, including PRs opened from a fork.
			Ref:   fmt.Sprintf("refs/pull/%d/head", cid.PRNumber),
			Label: fmt.Sprintf("%s#%d", cid.OwnerRepo(), cid.PRNumber),
		}, nil

	case "git":
		cid, err := entitygit.ParseChangeID(uri)
		if err != nil {
			return changeRef{}, fmt.Errorf("%w: invalid change URI %q: %v", merger.ErrInvalidRequest, uri, err)
		}
		// A git:// URI already names its own fully-qualified ref, so the
		// staleness check reads exactly the ref the caller pinned.
		return changeRef{
			Provider: scheme,
			SHA:      cid.CommitSHA,
			Ref:      cid.Ref,
			Label:    fmt.Sprintf("%s@%s", cid.Repo, cid.Ref),
		}, nil

	default:
		return changeRef{}, fmt.Errorf("%w: unsupported change URI scheme %q in %q", merger.ErrInvalidRequest, scheme, uri)
	}
}

// stepChangeRefs flattens the already-resolved changes of every step into one
// list, in application order, for the checks that vet a request as a whole.
func stepChangeRefs(steps []resolvedStep) []changeRef {
	var refs []changeRef
	for _, rs := range steps {
		refs = append(refs, rs.refs...)
	}
	return refs
}
