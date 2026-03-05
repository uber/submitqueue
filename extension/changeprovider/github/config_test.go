package github

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	graphQLURL := "https://api.github.com/graphql"

	client := NewClient(httpClient, graphQLURL)

	assert.Equal(t, httpClient, client.HTTPClient())
	assert.Equal(t, graphQLURL, client.GraphQLURL())
}

func TestNewAuthenticatedClient(t *testing.T) {
	token := "ghp_test123"
	baseURL := "https://api.github.com"
	timeout := 30 * time.Second

	client := NewAuthenticatedClient(token, baseURL, timeout)

	assert.NotNil(t, client.HTTPClient())
	assert.Equal(t, "https://api.github.com/graphql", client.GraphQLURL())
	assert.Equal(t, timeout, client.HTTPClient().Timeout)

	// Verify bearer transport is configured
	transport, ok := client.HTTPClient().Transport.(*bearerTransport)
	assert.True(t, ok, "transport should be bearerTransport")
	assert.Equal(t, token, transport.token)
	assert.Equal(t, http.DefaultTransport, transport.base)
}

func TestNewAuthenticatedClient_EmptyToken(t *testing.T) {
	token := ""
	baseURL := "https://api.github.com"
	timeout := 30 * time.Second

	client := NewAuthenticatedClient(token, baseURL, timeout)

	assert.NotNil(t, client.HTTPClient())
	assert.Equal(t, "https://api.github.com/graphql", client.GraphQLURL())

	// Verify transport is NOT bearerTransport when token is empty
	assert.Equal(t, http.DefaultTransport, client.HTTPClient().Transport)
}

func TestNewAuthenticatedClient_GHES(t *testing.T) {
	token := "ghp_enterprise"
	baseURL := "https://ghe.company.com/api"
	timeout := 15 * time.Second

	client := NewAuthenticatedClient(token, baseURL, timeout)

	assert.NotNil(t, client.HTTPClient())
	assert.Equal(t, "https://ghe.company.com/api/graphql", client.GraphQLURL())
	assert.Equal(t, timeout, client.HTTPClient().Timeout)
}

func TestBearerTransport_AddsAuthHeader(t *testing.T) {
	token := "ghp_test_token"
	mockBase := &mockRoundTripper{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			// Verify the Authorization header was added
			assert.Equal(t, "Bearer ghp_test_token", req.Header.Get("Authorization"))
			return &http.Response{
				StatusCode: http.StatusOK,
			}, nil
		},
	}

	transport := &bearerTransport{
		token: token,
		base:  mockBase,
	}

	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/test", nil)
	assert.NoError(t, err)

	_, err = transport.RoundTrip(req)
	assert.NoError(t, err)
}

func TestBearerTransport_ClonesRequest(t *testing.T) {
	token := "ghp_test_token"
	originalReq, err := http.NewRequest(http.MethodGet, "https://api.github.com/test", nil)
	assert.NoError(t, err)

	mockBase := &mockRoundTripper{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			// Verify the request was cloned (different pointer)
			assert.NotSame(t, originalReq, req, "request should be cloned")
			assert.Equal(t, originalReq.URL.String(), req.URL.String())
			return &http.Response{
				StatusCode: http.StatusOK,
			}, nil
		},
	}

	transport := &bearerTransport{
		token: token,
		base:  mockBase,
	}

	_, err = transport.RoundTrip(originalReq)
	assert.NoError(t, err)

	// Verify original request is unchanged
	assert.Empty(t, originalReq.Header.Get("Authorization"), "original request should not be modified")
}
