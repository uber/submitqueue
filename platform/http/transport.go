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

// Package http provides shared HTTP client helpers (e.g. base-URL transport) for code that calls external HTTP APIs.
package http

import (
	nethttp "net/http"
	"net/url"
	"strings"
)

// BaseURLTransport is a nethttp.RoundTripper that rewrites every request URL
// to resolve against a fixed base URL. This allows callers to make requests
// with relative paths (e.g. "/graphql") and have the transport prepend the
// configured base URL transparently.
type BaseURLTransport struct {
	// BaseURL is the API base URL (e.g. "https://api.github.com").
	BaseURL *url.URL
	// Next is the underlying RoundTripper. Defaults to nethttp.DefaultTransport if nil.
	Next nethttp.RoundTripper
}

// RoundTrip rewrites req.URL to resolve against BaseURL, then delegates to Next.
// The base URL path and request path are joined explicitly so that base URLs
// with a path component (e.g. "https://ghe.example.com/api") are handled
// correctly regardless of whether the request path starts with "/".
func (t *BaseURLTransport) RoundTrip(req *nethttp.Request) (*nethttp.Response, error) {
	newReq := req.Clone(req.Context())

	merged := *t.BaseURL
	merged.Path = strings.TrimRight(t.BaseURL.Path, "/") + "/" + strings.TrimLeft(req.URL.Path, "/")
	merged.RawQuery = req.URL.RawQuery
	newReq.URL = &merged

	next := t.Next
	if next == nil {
		next = nethttp.DefaultTransport
	}
	return next.RoundTrip(newReq)
}

// NewClient builds an *nethttp.Client with BaseURLTransport configured.
// Callers are responsible for layering additional transports (e.g. auth) and
// setting Timeout on the returned client.
func NewClient(rawBaseURL string) (*nethttp.Client, error) {
	u, err := url.Parse(rawBaseURL)
	if err != nil {
		return nil, err
	}

	return &nethttp.Client{Transport: &BaseURLTransport{
		BaseURL: u,
		Next:    nethttp.DefaultTransport,
	}}, nil
}
