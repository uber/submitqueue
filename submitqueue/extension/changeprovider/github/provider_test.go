package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/errs"
	httperrs "github.com/uber/submitqueue/platform/errs/http"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// Sample 40-char lowercase hex SHAs used across the test cases.
const (
	shaA   = "abcdef0123456789abcdef0123456789abcdef01"
	shaB   = "0123456789abcdef0123456789abcdef01234567"
	shaXYZ = "1234567890abcdef1234567890abcdef12345678"
	shaOld = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	shaNew = "feedfacefeedfacefeedfacefeedfacefeedface"
)

func newTestProvider(t *testing.T, serverURL string) changeprovider.ChangeProvider {
	t.Helper()
	client, err := phttp.NewClient(serverURL)
	require.NoError(t, err)
	return NewProvider(Params{
		HTTPClient:   client,
		Logger:       zaptest.NewLogger(t).Sugar(),
		MetricsScope: tally.NoopScope,
	})
}

func servePR(t *testing.T, w http.ResponseWriter, data pullRequestData) {
	t.Helper()
	var resp graphqlResponse
	resp.Data.Repository.PullRequest = data
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(resp))
}

func TestProvider_Get(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		uris    []string
		wantErr bool
	}{
		{
			name: "returns result for valid PR",
			handler: func(w http.ResponseWriter, r *http.Request) {
				servePR(t, w, pullRequestData{
					Number:     123,
					HeadRefOid: shaA,
					Author:     authorData{Name: "Test User", Email: "test@example.com"},
					Files: filesData{
						Nodes: []fileNode{
							{Path: "main.go"},
							{Path: "test.go"},
						},
					},
				})
			},
			uris: []string{"github://github.example.com/uber/submitqueue/pull/123/" + shaA},
		},
		{
			name:    "invalid URI returns error",
			uris:    []string{"invalid://uri"},
			wantErr: true,
		},
		{
			name: "inconsistent change set returns error",
			uris: []string{
				"github://github.example.com/uber/submitqueue/pull/123/" + shaA,
				"github://github.example.com/uber/different-repo/pull/456/" + shaB,
			},
			wantErr: true,
		},
		{
			name: "stale PR returns error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				servePR(t, w, pullRequestData{
					Number:     123,
					HeadRefOid: shaNew,
					Files:      filesData{Nodes: []fileNode{{Path: "main.go"}}},
				})
			},
			uris:    []string{"github://github.example.com/uber/submitqueue/pull/123/" + shaOld},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverURL := "http://localhost"
			if tt.handler != nil {
				server := httptest.NewServer(tt.handler)
				defer server.Close()
				serverURL = server.URL
			}

			p := newTestProvider(t, serverURL)
			infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{URIs: tt.uris}})

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, infos, 1)
			assert.Equal(t, tt.uris[0], infos[0].URI)
			assert.Len(t, infos[0].Details.ChangedFiles, 2)
		})
	}
}

func TestProvider_Get_Pagination(t *testing.T) {
	pages := []pullRequestData{
		{
			Number:     456,
			HeadRefOid: shaXYZ,
			Files: filesData{
				PageInfo: pageInfo{EndCursor: "cursor1", HasNextPage: true},
				Nodes:    []fileNode{{Path: "file1.go"}},
			},
		},
		{
			Number:     456,
			HeadRefOid: shaXYZ,
			Files: filesData{
				PageInfo: pageInfo{HasNextPage: false},
				Nodes:    []fileNode{{Path: "file2.go"}},
			},
		},
	}
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servePR(t, w, pages[callCount])
		callCount++
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"github://github.example.com/uber/submitqueue/pull/456/" + shaXYZ},
	}})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount)
	require.Len(t, infos, 1)
	assert.Len(t, infos[0].Details.ChangedFiles, 2)
}

func TestProvider_Get_PaginationRejectsStaleHead(t *testing.T) {
	for _, tt := range []struct {
		name      string
		headSHAs  []string
		wantCalls int
	}{
		{name: "first page", headSHAs: []string{shaNew, shaXYZ}, wantCalls: 1},
		{name: "middle page", headSHAs: []string{shaXYZ, shaNew, shaXYZ}, wantCalls: 2},
		{name: "last page", headSHAs: []string{shaXYZ, shaNew}, wantCalls: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Less(t, callCount, len(tt.headSHAs))
				headSHA := tt.headSHAs[callCount]
				callCount++
				servePR(t, w, pullRequestData{
					Number:     456,
					HeadRefOid: headSHA,
					Files: filesData{
						PageInfo: pageInfo{EndCursor: fmt.Sprintf("cursor%d", callCount), HasNextPage: callCount < len(tt.headSHAs)},
						Nodes:    []fileNode{{Path: fmt.Sprintf("file%d.go", callCount)}},
					},
				})
			}))
			t.Cleanup(server.Close)

			p := newTestProvider(t, server.URL)
			infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
				URIs: []string{"github://github.example.com/uber/submitqueue/pull/456/" + shaXYZ},
			}})

			require.Error(t, err)
			assert.Empty(t, infos)
			assert.Equal(t, tt.wantCalls, callCount)
		})
	}
}

func TestProvider_Get_MultiplePRs(t *testing.T) {
	prData := []pullRequestData{
		{Number: 123, HeadRefOid: shaA, Files: filesData{Nodes: []fileNode{{Path: "file1.go"}}}},
		{Number: 456, HeadRefOid: shaB, Files: filesData{Nodes: []fileNode{{Path: "file2.go"}}}},
	}
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servePR(t, w, prData[callCount])
		callCount++
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{
			"github://github.example.com/uber/submitqueue/pull/123/" + shaA,
			"github://github.example.com/uber/submitqueue/pull/456/" + shaB,
		},
	}})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount)
	require.Len(t, infos, 2)
	assert.Equal(t, "github://github.example.com/uber/submitqueue/pull/123/"+shaA, infos[0].URI)
	assert.Equal(t, "github://github.example.com/uber/submitqueue/pull/456/"+shaB, infos[1].URI)
}

func TestProvider_Get_FetchError_StopsOnFirstFailure(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount == 1 {
			callCount++
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		servePR(t, w, pullRequestData{
			Number:     123,
			HeadRefOid: shaA,
			Files:      filesData{Nodes: []fileNode{{Path: "file1.go"}}},
		})
		callCount++
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	_, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{
			"github://github.example.com/uber/submitqueue/pull/123/" + shaA,
			"github://github.example.com/uber/submitqueue/pull/456/" + shaB,
		},
	}})

	require.Error(t, err)
	assert.Equal(t, 2, callCount)
}

func TestProvider_Get_HTTPStatusError(t *testing.T) {
	processor := errs.NewClassifierProcessor(httperrs.Classifier)
	for _, tt := range []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "service unavailable", status: http.StatusServiceUnavailable, retryable: true},
		{name: "rate limited", status: http.StatusTooManyRequests, retryable: true},
		{name: "bad request", status: http.StatusBadRequest},
		{name: "unauthorized", status: http.StatusUnauthorized},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const body = `{"message":"upstream failure"}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(server.Close)

			p := newTestProvider(t, server.URL)
			_, err := p.Get(context.Background(), entity.Request{Change: change.Change{
				URIs: []string{"github://github.example.com/uber/submitqueue/pull/123/" + shaA},
			}})
			var statusErr *phttp.StatusError
			require.ErrorAs(t, err, &statusErr)
			assert.Equal(t, tt.status, statusErr.StatusCode)
			assert.Equal(t, body, statusErr.Body)

			classified := processor.Process(err)
			assert.Equal(t, tt.retryable, errs.IsRetryable(classified))
			assert.True(t, errs.IsDependencyError(classified))
		})
	}
}
