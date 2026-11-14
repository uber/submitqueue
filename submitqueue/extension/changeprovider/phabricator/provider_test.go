package phabricator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/conduit"
	"github.com/uber/submitqueue/platform/errs"
	httperrs "github.com/uber/submitqueue/platform/errs/http"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

func newTestProvider(t *testing.T, serverURL string) changeprovider.ChangeProvider {
	t.Helper()
	httpClient, err := phttp.NewClient(serverURL)
	require.NoError(t, err)
	return NewProvider(Params{
		Conduit:      conduit.NewClient(httpClient, "test-token"),
		Logger:       zaptest.NewLogger(t).Sugar(),
		MetricsScope: tally.NoopScope,
	})
}

func validDiffResponse() map[string]*diffResult {
	return map[string]*diffResult{
		"100": {
			AuthorName:  "Test Author",
			AuthorEmail: "test@example.com",
			Changes: []fileChange{
				{CurrentPath: "main.go", AddLines: "10", DelLines: "3"},
				{CurrentPath: "test.go", AddLines: "20", DelLines: "0"},
			},
		},
	}
}

func TestProvider_Get(t *testing.T) {
	testCases := []struct {
		name    string
		handler http.HandlerFunc
		uris    []string
		wantErr string
	}{
		{
			name: "returns result for valid diff",
			handler: func(w http.ResponseWriter, r *http.Request) {
				serveConduit(t, w, validDiffResponse())
			},
			uris: []string{"phab://phab.example.com/D200/100"},
		},
		{
			name:    "invalid URI returns error",
			uris:    []string{"invalid://uri"},
			wantErr: "failed to parse Phabricator change ID",
		},
		{
			name: "rejected token returns error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"result":null,"error_code":"ERR-INVALID-SESSION","error_info":"Session key is not present."}`))
			},
			uris:    []string{"phab://phab.example.com/D200/100"},
			wantErr: "ERR-INVALID-SESSION",
		},
		{
			name: "HTTP error returns error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			uris:    []string{"phab://phab.example.com/D200/100"},
			wantErr: "unexpected status 500",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serverURL := "http://localhost"
			if tc.handler != nil {
				server := httptest.NewServer(tc.handler)
				defer server.Close()
				serverURL = server.URL
			}

			p := newTestProvider(t, serverURL)
			infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{URIs: tc.uris}})

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, infos, 1)
			assert.Equal(t, "phab://phab.example.com/D200/100", infos[0].URI)
			assert.Equal(t, "Test Author", infos[0].Details.Author.Name)
			assert.Len(t, infos[0].Details.ChangedFiles, 2)
		})
	}
}

func TestProvider_Get_MultipleDiffs(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		serveConduit(t, w, map[string]*diffResult{
			"100": {
				AuthorName:  "Author A",
				AuthorEmail: "a@example.com",
				Changes:     []fileChange{{CurrentPath: "file1.go", AddLines: "5", DelLines: "1"}},
			},
			"101": {
				AuthorName:  "Author B",
				AuthorEmail: "b@example.com",
				Changes:     []fileChange{{CurrentPath: "file2.go", AddLines: "8", DelLines: "2"}},
			},
		})
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"phab://phab.example.com/D200/100", "phab://phab.example.com/D201/101"},
	}})

	require.NoError(t, err)
	assert.Equal(t, 1, callCount)
	require.Len(t, infos, 2)
	assert.Equal(t, "phab://phab.example.com/D200/100", infos[0].URI)
	assert.Equal(t, "phab://phab.example.com/D201/101", infos[1].URI)
}

func TestProvider_Get_ConnectionError(t *testing.T) {
	p := newTestProvider(t, "http://127.0.0.1:0")
	_, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"phab://phab.example.com/D200/100"},
	}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch diffs")
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
			const body = "upstream failure"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(server.Close)

			p := newTestProvider(t, server.URL)
			_, err := p.Get(t.Context(), entity.Request{Change: change.Change{
				URIs: []string{"phab://phab.example.com/D200/100"},
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

func TestProvider_Get_FileDetails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := map[string]*diffResult{
			"100": {
				AuthorName:  "Author",
				AuthorEmail: "author@example.com",
				Changes: []fileChange{
					{CurrentPath: "added.go", AddLines: "52", DelLines: "0"},
					{CurrentPath: "modified.go", AddLines: "10", DelLines: "5"},
					{CurrentPath: "deleted.go", AddLines: "0", DelLines: "30"},
				},
			},
		}
		serveConduit(t, w, result)
	}))
	defer server.Close()

	p := newTestProvider(t, server.URL)
	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{
		URIs: []string{"phab://phab.example.com/D200/100"},
	}})

	require.NoError(t, err)
	require.Len(t, infos, 1)

	files := infos[0].Details.ChangedFiles
	require.Len(t, files, 3)

	assert.Equal(t, "added.go", files[0].Path)
	assert.Equal(t, 52, files[0].LinesAdded)
	assert.Equal(t, 0, files[0].LinesDeleted)
	assert.Equal(t, 0, files[0].LinesModified)

	assert.Equal(t, "modified.go", files[1].Path)
	assert.Equal(t, 10, files[1].LinesAdded)
	assert.Equal(t, 5, files[1].LinesDeleted)
	assert.Equal(t, 0, files[1].LinesModified)

	assert.Equal(t, "deleted.go", files[2].Path)
	assert.Equal(t, 0, files[2].LinesAdded)
	assert.Equal(t, 30, files[2].LinesDeleted)
	assert.Equal(t, 0, files[2].LinesModified)
}

func TestProvider_Get_Batching(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		ids := requestedDiffIDs(t, r)
		result := make(map[string]*diffResult, len(ids))
		for _, id := range ids {
			result[id] = &diffResult{
				AuthorName: "Author " + id,
				Changes:    []fileChange{{CurrentPath: id + ".go", AddLines: "1", DelLines: "0"}},
			}
		}
		serveConduit(t, w, result)
	}))
	defer server.Close()

	uris := make([]string, 25)
	for i := range uris {
		diffID := i + 1
		revisionID := 1000 + i
		uris[i] = fmt.Sprintf("phab://phab.example.com/D%d/%d", revisionID, diffID)
	}

	p := newTestProvider(t, server.URL)
	infos, err := p.Get(context.Background(), entity.Request{Change: change.Change{URIs: uris}})

	require.NoError(t, err)
	assert.Equal(t, 3, callCount)
	assert.Len(t, infos, 25)
}

func TestProvider_Get_BatchingStopsOnError(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ids := requestedDiffIDs(t, r)
		result := make(map[string]*diffResult, len(ids))
		for _, id := range ids {
			result[id] = &diffResult{
				Changes: []fileChange{{CurrentPath: id + ".go", AddLines: "1", DelLines: "0"}},
			}
		}
		serveConduit(t, w, result)
	}))
	defer server.Close()

	uris := make([]string, 25)
	for i := range uris {
		diffID := i + 1
		revisionID := 1000 + i
		uris[i] = fmt.Sprintf("phab://phab.example.com/D%d/%d", revisionID, diffID)
	}

	p := newTestProvider(t, server.URL)
	_, err := p.Get(context.Background(), entity.Request{Change: change.Change{URIs: uris}})

	require.Error(t, err)
	assert.Equal(t, 2, callCount)
}

// serveConduit writes a successful Conduit response envelope.
func serveConduit(t *testing.T, w http.ResponseWriter, result any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"result": result, "error_code": nil, "error_info": nil}))
}

// requestedDiffIDs returns the diff IDs a querydiffs call asked for, as the
// string keys Conduit answers with.
func requestedDiffIDs(t *testing.T, r *http.Request) []string {
	t.Helper()
	var params struct {
		IDs []int `json:"ids"`
	}
	if !assert.NoError(t, r.ParseForm()) || !assert.NoError(t, json.Unmarshal([]byte(r.PostForm.Get("params")), &params)) {
		return nil
	}
	ids := make([]string, len(params.IDs))
	for i, id := range params.IDs {
		ids[i] = strconv.Itoa(id)
	}
	return ids
}
