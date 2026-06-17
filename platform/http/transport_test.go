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

package http

import (
	nethttp "net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTripFunc is a test helper that implements nethttp.RoundTripper via a function.
type roundTripFunc func(*nethttp.Request) (*nethttp.Response, error)

func (f roundTripFunc) RoundTrip(req *nethttp.Request) (*nethttp.Response, error) {
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
				Next: roundTripFunc(func(req *nethttp.Request) (*nethttp.Response, error) {
					capturedURL = req.URL.String()
					return &nethttp.Response{StatusCode: nethttp.StatusOK, Body: nethttp.NoBody}, nil
				}),
			}

			req, err := nethttp.NewRequest(nethttp.MethodGet, tt.requestPath, nil)
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
		Next: roundTripFunc(func(req *nethttp.Request) (*nethttp.Response, error) {
			return &nethttp.Response{StatusCode: nethttp.StatusOK, Body: nethttp.NoBody}, nil
		}),
	}

	req, err := nethttp.NewRequest(nethttp.MethodGet, "/graphql", nil)
	require.NoError(t, err)
	originalURL := req.URL.String()

	_, err = transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, originalURL, req.URL.String())
}

func TestNewClient_InvalidURL(t *testing.T) {
	_, err := NewClient("://invalid")
	require.Error(t, err)
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}
