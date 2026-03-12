package github

import (
	"net/http"
)

// Client is a GitHub API client that encapsulates connection details and authentication.
// The client is protocol-agnostic - it provides helpers for both GraphQL and REST endpoints.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new GitHub API client with a pre-configured HTTP client.
// The caller is responsible for configuring authentication in the HTTP client's Transport.
//
// Parameters:
// - httpClient: Configured HTTP client (with auth, timeout, transport, etc.)
// - baseURL: GitHub instance base URL (e.g., "https://api.github.com" or "https://ghe.company.com/api")
func NewClient(httpClient *http.Client, baseURL string) *Client {
	return &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
	}
}

// HTTPClient returns the configured HTTP client.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

// BaseURL returns the configured GitHub base URL.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// GraphQLURL returns the GitHub GraphQL endpoint URL.
// Constructs the URL by appending "/graphql" to the base URL.
func (c *Client) GraphQLURL() string {
	return c.baseURL + "/graphql"
}

// RESTURL constructs a GitHub REST API endpoint URL.
// The path should start with "/" (e.g., "/repos/uber/submitqueue/pulls/123").
func (c *Client) RESTURL(path string) string {
	return c.baseURL + path
}
