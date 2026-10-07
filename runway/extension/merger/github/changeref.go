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

package github

import (
	"fmt"

	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/runway/extension/merger"
)

// changeRef is a change URI reduced to the pull request it names.
type changeRef struct {
	// PRNumber is the pull request number in the merger's repository.
	PRNumber int
	// SHA is the head commit the URI pins the pull request to.
	SHA string
	// Label is a short human-readable name, "owner/repo#n".
	Label string
}

// resolveChange parses a change URI and rejects one that does not name a pull
// request in this merger's repository. Both failures are terminal.
func (m *githubMerger) resolveChange(uri string) (changeRef, error) {
	cid, err := entitygithub.ParseChangeID(uri)
	if err != nil {
		return changeRef{}, fmt.Errorf("%w: invalid change URI %q: %v", merger.ErrInvalidRequest, uri, err)
	}
	if cid.Host != m.host || cid.Org != m.owner || cid.Repo != m.repo {
		return changeRef{}, fmt.Errorf("%w: change URI %q is not in %s/%s/%s", merger.ErrInvalidRequest, uri, m.host, m.owner, m.repo)
	}
	return changeRef{
		PRNumber: cid.PRNumber,
		SHA:      cid.HeadCommitSHA,
		Label:    fmt.Sprintf("%s#%d", cid.OwnerRepo(), cid.PRNumber),
	}, nil
}
