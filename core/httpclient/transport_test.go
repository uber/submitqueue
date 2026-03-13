package httpclient

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTripFunc is a test helper that implements http.RoundTripper via a function.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestBaseURLTransport_RewritesURL(t *testing.T) {
	tests := []struct {
		name        string
		baseURL     string
		requestPath string
		wantURL     string
	}{
		{
			name:        "relative path resolved against base",
			baseURL:     "https://api.github.com",
			requestPath: "/graphql",
			wantURL:     "https://api.github.com/graphql",
		},
		{
			name:        "enterprise base URL",
			baseURL:     "https://ghe.example.com/api",
			requestPath: "/graphql",
			wantURL:     "https://ghe.example.com/api/graphql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedURL string
			transport := &BaseURLTransport{
				BaseURL: mustParseURL(t, tt.baseURL),
				Next: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					capturedURL = req.URL.String()
					return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
				}),
			}

			req, err := http.NewRequest(http.MethodGet, tt.requestPath, nil)
			require.NoError(t, err)

			_, err = transport.RoundTrip(req)
			require.NoError(t, err)
			assert.Equal(t, tt.wantURL, capturedURL)
		})
	}
}

func TestBaseURLTransport_DoesNotMutateOriginalRequest(t *testing.T) {
	transport := &BaseURLTransport{
		BaseURL: mustParseURL(t, "https://api.github.com"),
		Next: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
	}

	req, err := http.NewRequest(http.MethodGet, "/graphql", nil)
	require.NoError(t, err)
	originalURL := req.URL.String()

	_, err = transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, originalURL, req.URL.String())
}

func TestBearerTransport_AddsAuthHeader(t *testing.T) {
	var capturedHeader string
	transport := &BearerTransport{
		Token: "test-token",
		Next: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			capturedHeader = req.Header.Get("Authorization")
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
	}

	req, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, "Bearer test-token", capturedHeader)
}

func TestBearerTransport_DoesNotMutateOriginalRequest(t *testing.T) {
	transport := &BearerTransport{
		Token: "test-token",
		Next: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
	}

	req, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Empty(t, req.Header.Get("Authorization"))
}

func TestNewClient_InvalidURL(t *testing.T) {
	_, err := NewClient("://invalid", "", 30*time.Second)
	require.Error(t, err)
}

func TestNewClient_SetsTimeout(t *testing.T) {
	client, err := NewClient("https://api.github.com", "", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, client.Timeout)
}

func TestNewClient_AuthHeader(t *testing.T) {
	tests := []struct {
		name           string
		token          string
		wantAuthHeader string
	}{
		{
			name:           "no token, no auth header",
			token:          "",
			wantAuthHeader: "",
		},
		{
			name:           "with token, adds bearer auth header",
			token:          "my-token",
			wantAuthHeader: "Bearer my-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Captured-Auth", r.Header.Get("Authorization"))
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client, err := NewClient(server.URL, tt.token, 30*time.Second)
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAuthHeader, resp.Header.Get("X-Captured-Auth"))
		})
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}
