package github

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	baseURL := "https://api.github.com"

	client := NewClient(httpClient, baseURL)

	assert.NotNil(t, client)
	assert.Equal(t, httpClient, client.HTTPClient())
	assert.Equal(t, baseURL, client.BaseURL())
}

func TestClient_GraphQLURL(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		expected string
	}{
		{
			name:     "standard github",
			baseURL:  "https://api.github.com",
			expected: "https://api.github.com/graphql",
		},
		{
			name:     "github enterprise",
			baseURL:  "https://ghe.example.com/api",
			expected: "https://ghe.example.com/api/graphql",
		},
		{
			name:     "localhost",
			baseURL:  "http://localhost:8080",
			expected: "http://localhost:8080/graphql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(&http.Client{}, tt.baseURL)
			assert.Equal(t, tt.expected, client.GraphQLURL())
		})
	}
}

func TestClient_BaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
	}{
		{name: "standard github", baseURL: "https://api.github.com"},
		{name: "github enterprise", baseURL: "https://ghe.example.com/api"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(&http.Client{}, tt.baseURL)
			assert.Equal(t, tt.baseURL, client.BaseURL())
		})
	}
}

func TestClient_RESTURL(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		path     string
		expected string
	}{
		{
			name:     "repos endpoint",
			baseURL:  "https://api.github.com",
			path:     "/repos/uber/submitqueue/pulls/123",
			expected: "https://api.github.com/repos/uber/submitqueue/pulls/123",
		},
		{
			name:     "enterprise repos endpoint",
			baseURL:  "https://ghe.example.com/api",
			path:     "/repos/myorg/myrepo/pulls/456",
			expected: "https://ghe.example.com/api/repos/myorg/myrepo/pulls/456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(&http.Client{}, tt.baseURL)
			assert.Equal(t, tt.expected, client.RESTURL(tt.path))
		})
	}
}
