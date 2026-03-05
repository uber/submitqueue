package github

import (
	"net/http"
	"time"
)

const (
	// DefaultTimeout is the default HTTP request timeout for GitHub API calls.
	// This applies to the entire request/response cycle.
	DefaultTimeout = 30 * time.Second
)

// Client is a GitHub API client that encapsulates connection details and authentication.
type Client struct {
	httpClient *http.Client
	graphQLURL string
}

// NewClient creates a new GitHub API client with a pre-configured HTTP client.
// The caller is responsible for configuring authentication in the HTTP client.
//
// Parameters:
//   - httpClient: Configured HTTP client (with auth, timeout, transport, etc.)
//   - graphQLURL: GitHub GraphQL endpoint (e.g., "https://api.github.com/graphql")
//
// Example with custom HTTP client:
//
//	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "ghp_xxx"})
//	httpClient := oauth2.NewClient(ctx, tokenSource)
//	client := github.NewClient(httpClient, "https://api.github.com/graphql")
func NewClient(httpClient *http.Client, graphQLURL string) *Client {
	return &Client{
		httpClient: httpClient,
		graphQLURL: graphQLURL,
	}
}

// NewAuthenticatedClient creates a GitHub API client with bearer token authentication.
// This is a convenience helper for simple token-based auth.
//
// Parameters:
//   - token: GitHub personal access token (can be empty for public access)
//   - baseURL: GitHub instance base URL (e.g., "https://api.github.com" or "https://ghe.company.com")
//   - timeout: HTTP request timeout (use DefaultTimeout if unsure)
//
// The GraphQL URL is derived by appending "/graphql" to baseURL.
//
// Example:
//
//	// GitHub.com
//	client := github.NewAuthenticatedClient("ghp_xxx", "https://api.github.com", github.DefaultTimeout)
//
//	// GitHub Enterprise Server
//	client := github.NewAuthenticatedClient("ghp_xxx", "https://ghe.company.com/api", github.DefaultTimeout)
func NewAuthenticatedClient(token string, baseURL string, timeout time.Duration) *Client {
	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: newBearerTransport(token, http.DefaultTransport),
	}

	return &Client{
		httpClient: httpClient,
		graphQLURL: baseURL + "/graphql",
	}
}

// bearerTransport is an http.RoundTripper that adds a Bearer token to requests.
type bearerTransport struct {
	token string
	base  http.RoundTripper
}

// newBearerTransport creates an HTTP transport with bearer token authentication.
// If token is empty, returns the base transport unchanged.
func newBearerTransport(token string, base http.RoundTripper) http.RoundTripper {
	if token == "" {
		return base
	}

	return &bearerTransport{
		token: token,
		base:  base,
	}
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(req)
}

// HTTPClient returns the configured HTTP client.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

// GraphQLURL returns the configured GraphQL endpoint URL.
func (c *Client) GraphQLURL() string {
	return c.graphQLURL
}
