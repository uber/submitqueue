package httpclient

import (
	"net/http"
	"net/url"
	"strings"
	"time"
)

// BaseURLTransport is an http.RoundTripper that rewrites every request URL
// to resolve against a fixed base URL. This allows callers to make requests
// with relative paths (e.g. "/graphql") and have the transport prepend the
// configured base URL transparently.
type BaseURLTransport struct {
	// BaseURL is the API base URL (e.g. "https://api.github.com").
	BaseURL *url.URL
	// Next is the underlying RoundTripper. Defaults to http.DefaultTransport if nil.
	Next http.RoundTripper
}

// RoundTrip rewrites req.URL to resolve against BaseURL, then delegates to Next.
// The base URL path and request path are joined explicitly so that base URLs
// with a path component (e.g. "https://ghe.example.com/api") are handled
// correctly regardless of whether the request path starts with "/".
func (t *BaseURLTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())

	merged := *t.BaseURL
	merged.Path = strings.TrimRight(t.BaseURL.Path, "/") + "/" + strings.TrimLeft(req.URL.Path, "/")
	merged.RawQuery = req.URL.RawQuery
	newReq.URL = &merged

	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(newReq)
}

// BearerTransport is an http.RoundTripper that adds a Bearer token
// Authorization header to every request.
type BearerTransport struct {
	// Token is the bearer token to include in requests.
	Token string
	// Next is the underlying RoundTripper. Defaults to http.DefaultTransport if nil.
	Next http.RoundTripper
}

// RoundTrip adds the Authorization header, then delegates to Next.
func (t *BearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())
	newReq.Header.Set("Authorization", "Bearer "+t.Token)

	next := t.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(newReq)
}

// NewClient builds an *http.Client with BaseURLTransport and optionally
// BearerTransport configured. The transport chain is:
//
//	BearerTransport (if token provided) → BaseURLTransport → DefaultTransport
func NewClient(rawBaseURL, token string, timeout time.Duration) (*http.Client, error) {
	u, err := url.Parse(rawBaseURL)
	if err != nil {
		return nil, err
	}

	var transport http.RoundTripper = &BaseURLTransport{
		BaseURL: u,
		Next:    http.DefaultTransport,
	}
	if token != "" {
		transport = &BearerTransport{Token: token, Next: transport}
	}

	return &http.Client{Transport: transport, Timeout: timeout}, nil
}
