package phabricator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractHeadSHA(t *testing.T) {
	testCases := []struct {
		name    string
		diff    *diffResult
		want    string
		wantErr string
	}{
		{
			name: "valid local commits",
			diff: &diffResult{
				Properties: properties{
					LocalCommits: map[string]localCommit{
						"abc123def456": {Commit: "abc123def456"},
					},
				},
			},
			want: "abc123def456",
		},
		{
			name: "empty local commits",
			diff: &diffResult{
				Properties: properties{
					LocalCommits: map[string]localCommit{},
				},
			},
			wantErr: "no local commits found",
		},
		{
			name:    "nil local commits",
			diff:    &diffResult{},
			wantErr: "no local commits found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sha, err := extractHeadSHA(tc.diff)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, sha)
		})
	}
}

func TestBuildQueryDiffsRequest(t *testing.T) {
	testCases := []struct {
		name     string
		diffIDs  []int
		apiToken string
		wantAll  []string
	}{
		{
			name:    "single ID",
			diffIDs: []int{42},
			wantAll: []string{"ids%5B%5D=42"},
		},
		{
			name:    "multiple IDs",
			diffIDs: []int{42, 43, 44},
			wantAll: []string{"ids%5B%5D=42", "ids%5B%5D=43", "ids%5B%5D=44"},
		},
		{
			name:     "includes api.token when set",
			diffIDs:  []int{42},
			apiToken: "my-secret",
			wantAll:  []string{"ids%5B%5D=42", "api.token=my-secret"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			form := buildQueryDiffsRequest(tc.diffIDs, tc.apiToken)
			encoded := form.Encode()
			for _, want := range tc.wantAll {
				assert.Contains(t, encoded, want)
			}
		})
	}
}

func TestDoConduitRequest(t *testing.T) {
	var capturedPath string
	var capturedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":{}}`))
	}))
	defer server.Close()

	client := &http.Client{Transport: &testTransport{baseURL: server.URL}}
	form := buildQueryDiffsRequest([]int{100, 200}, "")

	resp, err := doConduitRequest(t.Context(), client, form)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "/api/differential.querydiffs", capturedPath)
	assert.Contains(t, capturedQuery, "ids%5B%5D=100")
	assert.Contains(t, capturedQuery, "ids%5B%5D=200")
}

func TestDoConduitRequest_ConnectionError(t *testing.T) {
	client := &http.Client{Transport: &testTransport{baseURL: "http://127.0.0.1:0"}}
	form := buildQueryDiffsRequest([]int{1}, "")

	_, err := doConduitRequest(t.Context(), client, form)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP request failed")
}

func TestParseConduitResponse(t *testing.T) {
	validResult := map[string]*diffResult{
		"100": {AuthorName: "Alice", Changes: []fileChange{{CurrentPath: "a.go"}}},
		"101": {AuthorName: "Bob", Changes: []fileChange{{CurrentPath: "b.go"}}},
	}

	testCases := []struct {
		name    string
		resp    *http.Response
		diffIDs []int
		wantErr string
	}{
		{
			name:    "success",
			resp:    newConduitHTTPResponse(t, validResult, http.StatusOK),
			diffIDs: []int{100, 101},
		},
		{
			name:    "HTTP error",
			resp:    newPlainHTTPResponse("server error", http.StatusInternalServerError),
			diffIDs: []int{100},
			wantErr: "Conduit API returned status 500",
		},
		{
			name:    "conduit error",
			resp:    newConduitErrorHTTPResponse(t, "ERR-CONDUIT-CORE", "Invalid diff ID"),
			diffIDs: []int{100},
			wantErr: "Conduit error",
		},
		{
			name:    "diff not found in response",
			resp:    newConduitHTTPResponse(t, map[string]*diffResult{"999": {}}, http.StatusOK),
			diffIDs: []int{100},
			wantErr: "diff 100 not found",
		},
		{
			name:    "malformed JSON",
			resp:    newPlainHTTPResponse("{not json", http.StatusOK),
			diffIDs: []int{100},
			wantErr: "failed to decode Conduit response",
		},
		{
			name:    "malformed result payload",
			resp:    newPlainHTTPResponse(`{"result":"not a map"}`, http.StatusOK),
			diffIDs: []int{100},
			wantErr: "failed to decode querydiffs result",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.resp.Body.Close()
			results, err := parseConduitResponse(tc.resp, tc.diffIDs)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Len(t, results, len(tc.diffIDs))
		})
	}
}

// newConduitHTTPResponse builds an http.Response with a successful Conduit envelope.
func newConduitHTTPResponse(t *testing.T, result any, statusCode int) *http.Response {
	t.Helper()
	resultBytes, err := json.Marshal(result)
	require.NoError(t, err)
	envelope := conduitResponse{Result: resultBytes}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
}

// newConduitErrorHTTPResponse builds an http.Response with a Conduit error envelope.
func newConduitErrorHTTPResponse(t *testing.T, code, info string) *http.Response {
	t.Helper()
	envelope := conduitResponse{ErrorCode: &code, ErrorInfo: &info}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
}

// newPlainHTTPResponse builds an http.Response with a plain string body.
func newPlainHTTPResponse(body string, statusCode int) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// serveConduit writes a successful Conduit response envelope to an HTTP handler.
func serveConduit(t *testing.T, w http.ResponseWriter, result any) {
	t.Helper()
	resultBytes, err := json.Marshal(result)
	require.NoError(t, err)
	resp := conduitResponse{Result: resultBytes}
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(resp))
}

// testTransport rewrites request URLs to point at the test server.
type testTransport struct {
	baseURL string
}

func (t *testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = t.baseURL[len("http://"):]
	return http.DefaultTransport.RoundTrip(req)
}
