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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/platform/base/change"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

const testHost = "github.com"

func testURI(number int) string {
	return fmt.Sprintf("github://%s/acme/widgets/pull/%d/%s", testHost, number, headSHA)
}

// prBody is a GraphQL response body for one pull request at headSHA.
func prBody(reviewDecision, mergeStateStatus string, contexts ...map[string]any) string {
	pr := map[string]any{
		"headRefOid":       headSHA,
		"reviewDecision":   reviewDecision,
		"mergeStateStatus": mergeStateStatus,
		"commits": map[string]any{"nodes": []any{
			map[string]any{"commit": map[string]any{
				"statusCheckRollup": map[string]any{"contexts": map[string]any{"nodes": contexts}},
			}},
		}},
	}
	body, _ := json.Marshal(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": pr}}})
	return string(body)
}

func checkRun(name, status, conclusion string, required bool) map[string]any {
	return map[string]any{"__typename": "CheckRun", "name": name, "status": status, "conclusion": conclusion, "isRequired": required}
}

type graphqlCall struct {
	method    string
	path      string
	auth      string
	variables map[string]any
}

// fakeGitHub answers GraphQL requests with the body configured for the pull
// request number in the request's variables and records each call.
type fakeGitHub struct {
	mu     sync.Mutex
	status int
	bodies map[int]string
	calls  []graphqlCall
}

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var req graphqlRequest
	_ = json.Unmarshal(raw, &req)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, graphqlCall{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization"), variables: req.Variables})
	if f.status != 0 {
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, "upstream unavailable")
		return
	}
	number, _ := req.Variables["number"].(float64)
	_, _ = io.WriteString(w, f.bodies[int(number)])
}

// authTransport stands in for the credential the wiring layer configures.
type authTransport struct{ base http.RoundTripper }

func (t authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer test-token")
	return t.base.RoundTrip(req)
}

func newTestValidator(t *testing.T, gh *fakeGitHub, scope tally.Scope) validator.Validator {
	t.Helper()
	server := httptest.NewServer(gh)
	t.Cleanup(server.Close)
	client, err := phttp.NewClient(server.URL)
	require.NoError(t, err)
	client.Transport = authTransport{base: client.Transport}

	v, err := New(Params{
		Config:       validator.Config{QueueName: "acme/widgets"},
		HTTPClient:   client,
		Host:         testHost,
		Logger:       zaptest.NewLogger(t).Sugar(),
		MetricsScope: scope,
	})
	require.NoError(t, err)
	return v
}

func request(uris ...string) entity.Request {
	return entity.Request{ID: "acme/widgets/1", Queue: "acme/widgets", Change: change.Change{URIs: uris}}
}

func TestValidate(t *testing.T) {
	approved := prBody("APPROVED", "CLEAN", checkRun("ci", "COMPLETED", "SUCCESS", true))
	failing := prBody("APPROVED", "BLOCKED", checkRun("ci", "COMPLETED", "FAILURE", true))

	tests := []struct {
		name string
		gh   *fakeGitHub
		uris []string
		// wantReason is the rejection's reason; empty means the request passes.
		wantReason rejectionReason
		wantURI    string
		// wantFetched is the pull request numbers read from GitHub, in order.
		wantFetched []int
	}{
		{
			name:        "passing pull request",
			gh:          &fakeGitHub{bodies: map[int]string{1: approved}},
			uris:        []string{testURI(1)},
			wantFetched: []int{1},
		},
		{
			name:        "failing required check rejects",
			gh:          &fakeGitHub{bodies: map[int]string{1: failing}},
			uris:        []string{testURI(1)},
			wantReason:  reasonCheckFailed,
			wantURI:     testURI(1),
			wantFetched: []int{1},
		},
		{
			name:        "stack is read bottom first and rejected at the pull request that fails",
			gh:          &fakeGitHub{bodies: map[int]string{1: approved, 2: failing}},
			uris:        []string{testURI(1), testURI(2)},
			wantReason:  reasonCheckFailed,
			wantURI:     testURI(2),
			wantFetched: []int{1, 2},
		},
		{
			name:        "pull requests above a rejected one are not read",
			gh:          &fakeGitHub{bodies: map[int]string{1: failing, 2: approved}},
			uris:        []string{testURI(1), testURI(2)},
			wantReason:  reasonCheckFailed,
			wantURI:     testURI(1),
			wantFetched: []int{1},
		},
		{
			name:        "URI of another change provider is skipped",
			gh:          &fakeGitHub{bodies: map[int]string{1: approved}},
			uris:        []string{"phab://D123/diff/456", testURI(1)},
			wantFetched: []int{1},
		},
		{
			name:       "URI on another GitHub host is rejected without reading it",
			gh:         &fakeGitHub{},
			uris:       []string{"github://github.example.com/acme/widgets/pull/1/" + headSHA},
			wantReason: reasonHost,
			wantURI:    "github://github.example.com/acme/widgets/pull/1/" + headSHA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newTestValidator(t, tt.gh, tally.NoopScope)

			err := v.Validate(context.Background(), request(tt.uris...))

			var fetched []int
			for _, call := range tt.gh.calls {
				assert.Equal(t, http.MethodPost, call.method)
				assert.Equal(t, "/graphql", call.path)
				assert.Equal(t, "Bearer test-token", call.auth)
				assert.Equal(t, "acme", call.variables["owner"])
				assert.Equal(t, "widgets", call.variables["repo"])
				number, _ := call.variables["number"].(float64)
				fetched = append(fetched, int(number))
			}
			assert.Equal(t, tt.wantFetched, fetched)

			if tt.wantReason == "" {
				assert.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, validator.ErrRejected)
			var rej rejection
			require.True(t, errors.As(err, &rej), "want a rejection, got %v", err)
			assert.Equal(t, tt.wantReason, rej.reason)
			assert.Equal(t, tt.wantURI, rej.uri)
		})
	}
}

func TestValidateReadFailures(t *testing.T) {
	tests := []struct {
		name       string
		gh         *fakeGitHub
		wantStatus int
	}{
		{
			name:       "HTTP status keeps its code for classification",
			gh:         &fakeGitHub{status: http.StatusBadGateway},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "GraphQL error",
			gh: &fakeGitHub{bodies: map[int]string{
				1: `{"data":{"repository":{"pullRequest":null}},"errors":[{"type":"NOT_FOUND","message":"Could not resolve to a PullRequest"}]}`,
			}},
		},
		{
			name: "response without a pull request",
			gh:   &fakeGitHub{bodies: map[int]string{1: `{"data":{"repository":{"pullRequest":null}}}`}},
		},
		{
			name: "undecodable response",
			gh:   &fakeGitHub{bodies: map[int]string{1: `not json`}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newTestValidator(t, tt.gh, tally.NoopScope)

			err := v.Validate(context.Background(), request(testURI(1)))

			require.Error(t, err)
			assert.NotErrorIs(t, err, validator.ErrRejected, "a failure to read GitHub is not a rejection")
			var statusErr *phttp.StatusError
			if tt.wantStatus != 0 {
				require.True(t, errors.As(err, &statusErr))
				assert.Equal(t, tt.wantStatus, statusErr.StatusCode)
			} else {
				assert.False(t, errors.As(err, &statusErr))
			}
		})
	}
}

func TestValidateMetrics(t *testing.T) {
	tests := []struct {
		name           string
		gh             *fakeGitHub
		wantResult     string
		wantRejections map[string]int64
	}{
		{
			name:       "pass",
			gh:         &fakeGitHub{bodies: map[int]string{1: prBody("APPROVED", "CLEAN")}},
			wantResult: "success",
		},
		{
			name:           "rejection is counted by reason and is not an error",
			gh:             &fakeGitHub{bodies: map[int]string{1: prBody("REVIEW_REQUIRED", "BLOCKED")}},
			wantResult:     "success",
			wantRejections: map[string]int64{"not_approved": 1},
		},
		{
			name:       "read failure is an error",
			gh:         &fakeGitHub{status: http.StatusInternalServerError},
			wantResult: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			v := newTestValidator(t, tt.gh, scope)

			_ = v.Validate(context.Background(), request(testURI(1)))

			snapshot := scope.Snapshot()
			results := map[string]bool{}
			for _, h := range snapshot.Histograms() {
				if h.Name() == "github_validator.validate.finish" {
					assert.Equal(t, "acme/widgets", h.Tags()["queue"])
					results[h.Tags()["result"]] = true
				}
			}
			assert.Equal(t, map[string]bool{tt.wantResult: true}, results)

			rejections := map[string]int64{}
			for _, c := range snapshot.Counters() {
				if c.Name() == "github_validator.validate.rejections" {
					assert.Equal(t, "acme/widgets", c.Tags()["queue"])
					rejections[c.Tags()["reason"]] += c.Value()
				}
			}
			if tt.wantRejections == nil {
				assert.Empty(t, rejections)
			} else {
				assert.Equal(t, tt.wantRejections, rejections)
			}
		})
	}
}

func TestNew(t *testing.T) {
	client := &http.Client{}

	tests := []struct {
		name    string
		params  Params
		wantErr bool
	}{
		{
			name:   "complete params",
			params: Params{HTTPClient: client, Host: testHost},
		},
		{
			name:    "missing HTTP client",
			params:  Params{Host: testHost},
			wantErr: true,
		},
		{
			name:    "missing host",
			params:  Params{HTTPClient: client},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params.Logger = zaptest.NewLogger(t).Sugar()
			tt.params.MetricsScope = tally.NoopScope

			v, err := New(tt.params)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, v)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, v)
		})
	}
}
