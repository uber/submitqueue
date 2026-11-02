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

// Package github resolves the base revision of a queue by asking the GitHub
// REST API for the tip of the queue's target branch, so it needs no clone.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/extension/baserevision"
)

// Target locates the branch whose tip is a queue's base revision.
type Target struct {
	// Owner is the organization or user that owns the repository.
	Owner string

	// Repo is the repository name.
	Repo string

	// Branch is the target branch.
	Branch string
}

// Params are the dependencies of a Resolver.
type Params struct {
	// HTTPClient resolves a relative path against the GitHub API root and
	// carries the credentials, as with the GitHub change provider.
	HTTPClient *http.Client

	// Targets maps each queue to its target branch.
	Targets map[string]Target
}

type resolver struct {
	Params
}

// New returns a Resolver that reads the target branch tip of each queue from
// GitHub as of the call. A queue without a target, or any API failure, is an
// error.
func New(params Params) baserevision.Resolver {
	return resolver{params}
}

func (r resolver) Resolve(ctx context.Context, queue string) (string, error) {
	target, ok := r.Targets[queue]
	if !ok {
		return "", fmt.Errorf("no target branch configured for queue %s", queue)
	}

	path := (&url.URL{Path: fmt.Sprintf("/repos/%s/%s/git/ref/heads/%s", target.Owner, target.Repo, target.Branch)}).EscapedPath()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to read branch %s of %s/%s: %w", target.Branch, target.Owner, target.Repo, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GitHub API request failed: %w", phttp.NewStatusError(resp.StatusCode, body))
	}

	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ref); err != nil {
		return "", fmt.Errorf("failed to decode the branch response: %w", err)
	}
	if !baserevision.IsFullCommitSHA(ref.Object.SHA) {
		return "", fmt.Errorf("GitHub returned %q, not a full commit SHA", ref.Object.SHA)
	}
	return ref.Object.SHA, nil
}
