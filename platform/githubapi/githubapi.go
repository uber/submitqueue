// Copyright (c) 2026 Uber Technologies, Inc.
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

// Package githubapi holds the request conventions shared by every component
// that calls the GitHub API directly: the REST headers, pinned to one API
// version, and the GraphQL request and response envelope.
//
// It is not a client. Callers keep their own *http.Client, with the API root
// resolved by platform/http.BaseURLTransport and auth set by a transport, and
// their own typed requests; this package only removes the parts every caller
// would otherwise repeat.
package githubapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	phttp "github.com/uber/submitqueue/platform/http"
)

// Version is the GitHub REST API version every REST request pins.
const Version = "2026-03-10"

// SetRESTHeaders sets the headers of a JSON request to the GitHub REST API. It
// has the signature platform/http.SendRequest takes for its setHeaders.
func SetRESTHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", Version)
}

// GraphQLRequest is the body of a request to the GitHub GraphQL API.
type GraphQLRequest struct {
	// Query is the GraphQL document.
	Query string `json:"query"`
	// Variables binds the query's variables by name.
	Variables map[string]any `json:"variables"`
}

type graphQLResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors graphQLErrors   `json:"errors"`
}

type graphQLError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// graphQLErrors is the errors array of a response GitHub answered with 200.
type graphQLErrors []graphQLError

func (e graphQLErrors) Error() string {
	msgs := make([]string, len(e))
	for i, err := range e {
		msgs[i] = err.Type + ": " + err.Message
	}
	return "GraphQL errors: " + strings.Join(msgs, "; ")
}

// QueryGraphQL posts req to the client's /graphql and decodes the response's
// data into data. A non-200 reply is a *platform/http.StatusError, so error
// classifiers can read its code; a 200 reply that carries errors fails even
// when it also carries data. A null data leaves data untouched.
func QueryGraphQL(ctx context.Context, client *http.Client, req GraphQLRequest, data any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal GraphQL request: %w", err)
	}
	// The path is relative: the client's transport roots it at the API.
	status, respBody, err := phttp.SendRequest(ctx, client, http.MethodPost, "/graphql", body, setGraphQLHeaders)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return phttp.NewStatusError(status, respBody)
	}

	var resp graphQLResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return fmt.Errorf("decode GraphQL response: %w", err)
	}
	if len(resp.Errors) > 0 {
		return resp.Errors
	}
	if len(resp.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(resp.Data, data); err != nil {
		return fmt.Errorf("decode GraphQL data: %w", err)
	}
	return nil
}

func setGraphQLHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
}
