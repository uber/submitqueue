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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	githubchange "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/submitqueue/client"
)

// githubSource opens changes as real pull requests, entirely over GitHub's REST
// API — so it needs no clone and no git binary, only a token.
type githubSource struct {
	client *githubClient
	// host is the authority in the change URIs this source mints, which for
	// GitHub Enterprise differs from the API root.
	host string
}

func newGitHubSource(cfg config, owner, repo string) *githubSource {
	return &githubSource{
		client: &githubClient{root: cfg.apiRoot, token: cfg.token, owner: owner, repo: repo},
		host:   cfg.host,
	}
}

func (s *githubSource) baseSHA(ctx context.Context, branch string) (string, error) {
	return s.client.branchSHA(ctx, branch)
}

func (s *githubSource) open(ctx context.Context, spec changeSpec) (openedChange, error) {
	spec.note("creating branch %s", spec.branch)
	if err := s.client.createBranch(ctx, spec.branch, spec.parentSHA); err != nil {
		return openedChange{}, fmt.Errorf("create branch %s: %w", spec.branch, err)
	}

	var headSHA string
	for k, file := range spec.files {
		spec.note("committing %s (%d/%d)", file.path, k+1, len(spec.files))
		sha, err := s.client.commitFile(ctx, spec.branch, file.path, file.body, file.message)
		if err != nil {
			return openedChange{}, fmt.Errorf("commit %s to %s: %w", file.path, spec.branch, err)
		}
		headSHA = sha
	}

	spec.note("opening pull request for %s", spec.branch)
	number, url, err := s.client.openPR(ctx, spec.title, spec.branch, spec.parentBranch)
	if err != nil {
		return openedChange{}, fmt.Errorf("open pull request for %s: %w", spec.branch, err)
	}

	return openedChange{
		headSHA: headSHA,
		uri: githubchange.ChangeID{
			Scheme: "github", Host: s.host, Org: s.client.owner, Repo: s.client.repo,
			PRNumber: number, HeadCommitSHA: headSHA,
		}.String(),
		// The pull request number, clickable where the terminal allows it.
		cell: client.Cell{Text: fmt.Sprintf("#%d", number), URL: url},
	}, nil
}

// githubClient is the slice of GitHub's REST API this tool needs: read a
// branch, create a branch, commit a file, open a pull request.
type githubClient struct {
	root  string
	token string
	owner string
	repo  string
}

func (g *githubClient) branchSHA(ctx context.Context, branch string) (string, error) {
	var out struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := g.do(ctx, http.MethodGet, "/git/ref/heads/"+branch, nil, &out); err != nil {
		return "", err
	}
	return out.Object.SHA, nil
}

func (g *githubClient) createBranch(ctx context.Context, branch, fromSHA string) error {
	return g.do(ctx, http.MethodPost, "/git/refs",
		map[string]string{"ref": "refs/heads/" + branch, "sha": fromSHA}, nil)
}

// commitFile writes a file on a branch and returns the resulting commit SHA —
// the commit a change URI pins the pull request to.
func (g *githubClient) commitFile(ctx context.Context, branch, path, content, message string) (string, error) {
	body := map[string]string{
		"message": message,
		"content": base64.StdEncoding.EncodeToString([]byte(content)),
		"branch":  branch,
	}
	var out struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := g.do(ctx, http.MethodPut, "/contents/"+path, body, &out); err != nil {
		return "", err
	}
	return out.Commit.SHA, nil
}

func (g *githubClient) openPR(ctx context.Context, title, head, base string) (int, string, error) {
	body := map[string]string{"title": title, "head": head, "base": base, "body": "Opened by service/submitqueue/demo/requests."}
	var out struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := g.do(ctx, http.MethodPost, "/pulls", body, &out); err != nil {
		return 0, "", err
	}
	return out.Number, out.HTMLURL, nil
}

// do issues one authenticated request against the repository, decoding into out
// when it is non-nil.
func (g *githubClient) do(ctx context.Context, method, path string, body any, out any) error {
	endpoint := fmt.Sprintf("%s/repos/%s/%s%s", g.root, g.owner, g.repo, path)

	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encode request for %s: %w", endpoint, err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request for %s: %w", endpoint, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+g.token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var detail bytes.Buffer
		_, _ = detail.ReadFrom(resp.Body)
		return fmt.Errorf("%s %s returned %s: %s", method, endpoint, resp.Status, strings.TrimSpace(detail.String()))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response from %s: %w", endpoint, err)
	}
	return nil
}
