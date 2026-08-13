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
)

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
	body := map[string]string{"title": title, "head": head, "base": base, "body": "Opened by service/submitqueue/demo/pr."}
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
