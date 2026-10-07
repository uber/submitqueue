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

// Package githubtestrepo gives tests real pull requests on github.com to land.
//
// The GitHub merger and the GitHub change provider talk to GitHub's REST API,
// and only real pull requests show what that API actually does — an in-process
// fake encodes only our reading of the docs. The suites that verify them (the
// merger extension test and TestGitHubLandE2E) share this package to open
// branches and pull requests, link them into stacks, push a new head, read back
// whether GitHub merged them and with which commit, and remove what they opened.
//
// New skips the calling test unless SQ_GITHUB_TOKEN and SQ_GITHUB_TEST_REPO
// ("owner/repo") are set, so runs without the credential never touch GitHub.
// Where the credential is expected — SQ_GITHUB_TEST_REQUIRED=true, which CI sets
// on every run that can read its secrets — a missing one fails the test instead,
// so a deleted secret cannot quietly turn these suites into skips. A token GitHub
// rejects (expired or revoked) always fails. Fixtures are named under sq-it/<run id>/ and closed and deleted when the test
// ends; what a test merges stays on the repository's default branch.
package githubtestrepo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"

	phttp "github.com/uber/submitqueue/platform/http"
)

const (
	// TokenEnv names the variable holding a token with write access to the
	// test repository.
	TokenEnv = "SQ_GITHUB_TOKEN"
	// RepoEnv names the variable holding the test repository, "owner/repo".
	RepoEnv = "SQ_GITHUB_TEST_REPO"
	// RequiredEnv names the variable that, when "true", makes a missing
	// TokenEnv or RepoEnv fail the test rather than skip it.
	RequiredEnv = "SQ_GITHUB_TEST_REQUIRED"

	// Host is the GitHub instance the test repository's change URIs name.
	Host = "github.com"
	// APIBaseURL is the REST API root for Host.
	APIBaseURL = "https://api.github.com"

	apiVersion = "2026-03-10"
)

// TestRepo creates fixtures in the test repository and removes them when the
// test ends.
type TestRepo struct {
	t          testing.TB
	httpClient *http.Client
	token      string
	owner      string
	repo       string
	trunk      string
	runID      string
	branches   []string
	pulls      []int
}

// Pull is a fixture pull request.
type Pull struct {
	// Number is the pull request number.
	Number int
	// Branch is the pull request's head branch.
	Branch string
	// Head is the head commit the pull request was opened at.
	Head string
}

// New returns a TestRepo for the configured repository. When the credential or
// repository is not configured it skips the test, or fails it if RequiredEnv is
// "true".
func New(t testing.TB) *TestRepo {
	t.Helper()
	token, ownerRepo := os.Getenv(TokenEnv), os.Getenv(RepoEnv)
	if token == "" || ownerRepo == "" {
		if os.Getenv(RequiredEnv) == "true" {
			require.FailNowf(t, "GitHub test repository not configured",
				"%s=true but %s or %s is empty; check the CI secret", RequiredEnv, TokenEnv, RepoEnv)
		}
		t.Skipf("set %s and %s=owner/repo to run against github.com", TokenEnv, RepoEnv)
	}
	owner, repo, ok := strings.Cut(ownerRepo, "/")
	require.True(t, ok, "%s must be owner/repo", RepoEnv)

	httpClient, err := phttp.NewClient(APIBaseURL)
	require.NoError(t, err)
	httpClient.Timeout = 30 * time.Second
	httpClient.Transport = &oauth2.Transport{
		Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}),
		Base:   httpClient.Transport,
	}

	r := &TestRepo{
		t:          t,
		httpClient: httpClient,
		token:      token,
		owner:      owner,
		repo:       repo,
		runID:      fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	var repoInfo struct {
		DefaultBranch string `json:"default_branch"`
	}
	r.call(http.MethodGet, r.path(""), nil, &repoInfo)
	r.trunk = repoInfo.DefaultBranch
	t.Cleanup(r.cleanup)
	return r
}

// HTTPClient is an authenticated client rooted at APIBaseURL.
func (r *TestRepo) HTTPClient() *http.Client { return r.httpClient }

// Token is the credential the test repository is reached with.
func (r *TestRepo) Token() string { return r.token }

// Owner is the test repository's owner.
func (r *TestRepo) Owner() string { return r.owner }

// Repo is the test repository's name.
func (r *TestRepo) Repo() string { return r.repo }

// Trunk is the test repository's default branch.
func (r *TestRepo) Trunk() string { return r.trunk }

// RunID uniquely names this test run's fixtures.
func (r *TestRepo) RunID() string { return r.runID }

// URI is the change URI naming p at the head it was opened with.
func (r *TestRepo) URI(p Pull) string {
	return fmt.Sprintf("github://%s/%s/%s/pull/%d/%s", Host, r.owner, r.repo, p.Number, p.Head)
}

// URIs is URI for each pull request, in order.
func (r *TestRepo) URIs(pulls ...Pull) []string {
	uris := make([]string, 0, len(pulls))
	for _, p := range pulls {
		uris = append(uris, r.URI(p))
	}
	return uris
}

// OpenStack opens n pull requests, each adding its own file, the first based on
// the trunk and each later one on the branch below it, and links them into a
// GitHub stack.
func (r *TestRepo) OpenStack(name string, n int) []Pull {
	pulls := r.OpenChain(name, n)
	numbers := make([]int, 0, n)
	for _, p := range pulls {
		numbers = append(numbers, p.Number)
	}
	r.call(http.MethodPost, r.path("/stacks"), map[string]any{"pull_requests": numbers}, nil)
	return pulls
}

// OpenChain opens n pull requests stacked by base branch without creating a
// GitHub stack.
func (r *TestRepo) OpenChain(name string, n int) []Pull {
	base := r.trunk
	pulls := make([]Pull, 0, n)
	for i := 1; i <= n; i++ {
		p := r.OpenPull(fmt.Sprintf("%s-%d", name, i), base)
		pulls = append(pulls, p)
		base = p.Branch
	}
	return pulls
}

// OpenPull opens one pull request against base that adds a file unique to it.
func (r *TestRepo) OpenPull(name, base string) Pull {
	r.t.Helper()
	branch := fmt.Sprintf("sq-it/%s/%s", r.runID, name)
	var baseRef struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	r.call(http.MethodGet, r.path("/git/ref/heads/"+base), nil, &baseRef)
	r.call(http.MethodPost, r.path("/git/refs"), map[string]string{"ref": "refs/heads/" + branch, "sha": baseRef.Object.SHA}, nil)
	r.branches = append(r.branches, branch)

	head := r.CommitFile(branch, r.FilePath(name), name)
	var pr struct {
		Number int `json:"number"`
	}
	r.call(http.MethodPost, r.path("/pulls"), map[string]string{
		"title": fmt.Sprintf("sq integration %s (%s)", name, r.runID),
		"head":  branch,
		"base":  base,
		"body":  "Opened by a SubmitQueue integration test; closed or merged automatically.",
	}, &pr)
	r.pulls = append(r.pulls, pr.Number)
	return Pull{Number: pr.Number, Branch: branch, Head: head}
}

// FilePath is the path of the file a fixture named name adds.
func (r *TestRepo) FilePath(name string) string {
	return fmt.Sprintf("sq-it/%s/%s.txt", r.runID, name)
}

// CommitFile writes a file on branch and returns the new head commit.
func (r *TestRepo) CommitFile(branch, path, content string) string {
	r.t.Helper()
	body := map[string]string{
		"message": "sq integration " + path,
		"content": base64.StdEncoding.EncodeToString([]byte(content + "\n")),
		"branch":  branch,
	}
	var existing struct {
		SHA string `json:"sha"`
	}
	if code, _ := r.send(http.MethodGet, r.path("/contents/"+path+"?ref="+branch), nil, &existing); code == http.StatusOK {
		body["sha"] = existing.SHA
	}
	var out struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	r.call(http.MethodPut, r.path("/contents/"+path), body, &out)
	return out.Commit.SHA
}

// AwaitPullHead waits until GitHub reports head as the pull request's head. A
// push reaches the pull request asynchronously, so a staleness check run before
// that would compare against the old head. Bounded by the test runner's
// timeout.
func (r *TestRepo) AwaitPullHead(number int, head string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var pr struct {
			Head struct {
				SHA string `json:"sha"`
			} `json:"head"`
		}
		r.call(http.MethodGet, r.path(fmt.Sprintf("/pulls/%d", number)), nil, &pr)
		if pr.Head.SHA == head {
			return
		}
		<-ticker.C
	}
}

// PullState is what GitHub reports about a fixture pull request.
type PullState struct {
	// State is "open" or "closed".
	State string `json:"state"`
	// Merged reports whether the pull request was merged.
	Merged bool `json:"merged"`
}

// PullState reads a pull request's current state.
func (r *TestRepo) PullState(number int) PullState {
	var st PullState
	r.call(http.MethodGet, r.path(fmt.Sprintf("/pulls/%d", number)), nil, &st)
	return st
}

// MergeCommit is the commit GitHub's merged event records for a pull request,
// or "" when it has none.
func (r *TestRepo) MergeCommit(number int) string {
	var events []struct {
		Event    string `json:"event"`
		CommitID string `json:"commit_id"`
	}
	r.call(http.MethodGet, r.path(fmt.Sprintf("/issues/%d/events?per_page=100", number)), nil, &events)
	for _, e := range events {
		if e.Event == "merged" {
			return e.CommitID
		}
	}
	return ""
}

// FileOnTrunk reports whether path exists on the trunk.
func (r *TestRepo) FileOnTrunk(path string) bool {
	code, _ := r.send(http.MethodGet, r.path("/contents/"+path+"?ref="+r.trunk), nil, nil)
	return code == http.StatusOK
}

// cleanup closes every fixture pull request still open and deletes every
// fixture branch. Failures are logged, not fatal: a leftover branch in the
// test repository is harmless and must not mask the test's own result.
func (r *TestRepo) cleanup() {
	// Every fixture pull request is closed without first reading its state: a
	// failed read must not stop cleanup, and closing a merged or already closed
	// one is a harmless rejection.
	for _, n := range r.pulls {
		r.cleanupRequest(http.MethodPatch, r.path(fmt.Sprintf("/pulls/%d", n)), map[string]string{"state": "closed"})
	}
	for _, b := range r.branches {
		r.cleanupRequest(http.MethodDelete, r.path("/git/refs/heads/"+b), nil)
	}
}

// cleanupRequest sends one cleanup request and logs, rather than fails on, a
// rejection, so one bad fixture cannot strand the rest. A 422 is GitHub saying
// the pull request is already merged or closed, or the branch already gone.
func (r *TestRepo) cleanupRequest(method, path string, body any) {
	if code, resp := r.send(method, path, body, nil); (code < 200 || code >= 300) && code != http.StatusUnprocessableEntity {
		r.t.Logf("cleanup %s %s: status %d: %s", method, path, code, resp)
	}
}

func (r *TestRepo) path(suffix string) string {
	return "/repos/" + r.owner + "/" + r.repo + suffix
}

// call sends a request and fails the test on a non-2xx answer.
func (r *TestRepo) call(method, path string, body, out any) {
	r.t.Helper()
	code, resp := r.send(method, path, body, out)
	require.True(r.t, code >= 200 && code < 300, "%s %s: status %d: %s", method, path, code, resp)
}

func (r *TestRepo) send(method, path string, body, out any) (int, string) {
	r.t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		require.NoError(r.t, err)
	}
	code, resp, err := phttp.SendRequest(context.Background(), r.httpClient, method, path, payload, func(r *http.Request) {
		r.Header.Set("Accept", "application/vnd.github+json")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-GitHub-Api-Version", apiVersion)
	})
	if err != nil {
		r.t.Logf("%s %s: %v", method, path, err)
		return 0, ""
	}
	if out != nil && code >= 200 && code < 300 && len(resp) > 0 {
		require.NoError(r.t, json.Unmarshal(resp, out), "%s %s", method, path)
	}
	return code, string(resp)
}
