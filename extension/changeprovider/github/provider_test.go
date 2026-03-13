package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/core/httpclient"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/changeprovider"
)

func newTestProvider(t *testing.T, serverURL string) changeprovider.ChangeProvider {
	t.Helper()
	client, err := httpclient.NewClient(serverURL, "", 30*time.Second)
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
					HeadRefOid: "abc123",
					Author:     authorData{Name: "Test User", Email: "test@example.com"},
					Files: filesData{
						Nodes: []fileNode{
							{Path: "main.go"},
							{Path: "test.go"},
						},
					},
				})
			},
			uris: []string{"github://uber/submitqueue/123/abc123"},
		},
		{
			name:    "invalid URI returns error",
			uris:    []string{"invalid://uri"},
			wantErr: true,
		},
		{
			name: "inconsistent change set returns error",
			uris: []string{
				"github://uber/submitqueue/123/abc123",
				"github://uber/different-repo/456/def456",
			},
			wantErr: true,
		},
		{
			name: "stale PR returns error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				servePR(t, w, pullRequestData{
					Number:     123,
					HeadRefOid: "newsha",
					Files:      filesData{Nodes: []fileNode{{Path: "main.go"}}},
				})
			},
			uris:    []string{"github://uber/submitqueue/123/oldsha"},
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
			infos, err := p.Get(context.Background(), entity.Change{URIs: tt.uris})

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, infos, 1)
			assert.Equal(t, tt.uris[0], infos[0].URI)
			assert.Len(t, infos[0].ChangedFiles, 2)
		})
	}
}

func TestProvider_Get_Pagination(t *testing.T) {
	pages := []pullRequestData{
		{
			Number:     456,
			HeadRefOid: "xyz789",
			Files: filesData{
				PageInfo: pageInfo{EndCursor: "cursor1", HasNextPage: true},
				Nodes:    []fileNode{{Path: "file1.go"}},
			},
		},
		{
			Number:     456,
			HeadRefOid: "xyz789",
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
	infos, err := p.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/456/xyz789"},
	})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount)
	require.Len(t, infos, 1)
	assert.Len(t, infos[0].ChangedFiles, 2)
}

func TestProvider_Get_MultiplePRs(t *testing.T) {
	prData := []pullRequestData{
		{Number: 123, HeadRefOid: "abc123", Files: filesData{Nodes: []fileNode{{Path: "file1.go"}}}},
		{Number: 456, HeadRefOid: "def456", Files: filesData{Nodes: []fileNode{{Path: "file2.go"}}}},
	}
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servePR(t, w, prData[callCount])
		callCount++
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	infos, err := p.Get(context.Background(), entity.Change{
		URIs: []string{
			"github://uber/submitqueue/123/abc123",
			"github://uber/submitqueue/456/def456",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount)
	require.Len(t, infos, 2)
	assert.Equal(t, "github://uber/submitqueue/123/abc123", infos[0].URI)
	assert.Equal(t, "github://uber/submitqueue/456/def456", infos[1].URI)
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
			HeadRefOid: "abc123",
			Files:      filesData{Nodes: []fileNode{{Path: "file1.go"}}},
		})
		callCount++
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	_, err := p.Get(context.Background(), entity.Change{
		URIs: []string{
			"github://uber/submitqueue/123/abc123",
			"github://uber/submitqueue/456/def456",
		},
	})

	require.Error(t, err)
	assert.Equal(t, 2, callCount)
}
