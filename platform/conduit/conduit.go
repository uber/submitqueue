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

// Package conduit calls Phabricator's Conduit API: it encodes a method's
// parameters, authenticates the call and decodes Conduit's result envelope.
//
// It holds one generic Call and no per-method wrappers. Each caller keeps its
// own typed requests and results and decides what a missing object means for
// it, so a method only one caller uses never lands here.
//
// The API token travels inside the JSON parameters, as "__conduit__.token".
// Conduit ignores an "api.token" field once the parameters are JSON, so auth
// cannot be left to a transport the way it is for other HTTP APIs.
package conduit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	phttp "github.com/uber/submitqueue/platform/http"
)

// Error is a failure Conduit reports in its result envelope. Conduit answers
// such failures, including a missing or invalid token (ERR-INVALID-SESSION),
// with HTTP 200, so they never surface as a platform/http.StatusError.
type Error struct {
	// Code is Conduit's error_code, e.g. ERR-INVALID-SESSION.
	Code string
	// Info is Conduit's error_info, a human-readable description.
	Info string
}

func (e *Error) Error() string {
	return fmt.Sprintf("conduit error %s: %s", e.Code, e.Info)
}

// Client calls Conduit methods with one API token. It is safe for concurrent
// use.
type Client struct {
	httpClient *http.Client
	token      string
}

// NewClient wraps httpClient, which must resolve relative paths against the
// Phabricator host (e.g. via platform/http.BaseURLTransport) and carries the
// caller's timeout and connection limits. token authenticates every call.
func NewClient(httpClient *http.Client, token string) *Client {
	return &Client{httpClient: httpClient, token: token}
}

type envelope struct {
	Result    json.RawMessage `json:"result"`
	ErrorCode *string         `json:"error_code"`
	ErrorInfo *string         `json:"error_info"`
}

// Call invokes method with params, which must encode as a JSON object (nil
// sends none), and decodes the result into result unless it is nil. A non-200
// reply is a *platform/http.StatusError and a Conduit error_code is an *Error.
// A null result, or an empty array (PHP's encoding of an empty map), leaves
// result untouched.
func (c *Client) Call(ctx context.Context, method string, params, result any) error {
	body, err := c.encodeParams(params)
	if err != nil {
		return fmt.Errorf("conduit %s: %w", method, err)
	}

	status, respBody, err := phttp.SendRequest(ctx, c.httpClient, http.MethodPost, "/api/"+url.PathEscape(method), body, setFormHeaders)
	if err != nil {
		return fmt.Errorf("conduit %s: %w", method, err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("conduit %s: %w", method, phttp.NewStatusError(status, respBody))
	}

	var env envelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		return fmt.Errorf("conduit %s: decode response: %w", method, err)
	}
	if env.ErrorCode != nil && *env.ErrorCode != "" {
		info := ""
		if env.ErrorInfo != nil {
			info = *env.ErrorInfo
		}
		return fmt.Errorf("conduit %s: %w", method, &Error{Code: *env.ErrorCode, Info: info})
	}
	if result == nil || isEmpty(env.Result) {
		return nil
	}
	if err := json.Unmarshal(env.Result, result); err != nil {
		return fmt.Errorf("conduit %s: decode result: %w", method, err)
	}
	return nil
}

func (c *Client) encodeParams(params any) ([]byte, error) {
	fields := map[string]json.RawMessage{}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("marshal params: %w", err)
		}
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("params must encode as a JSON object: %w", err)
		}
		if fields == nil {
			fields = map[string]json.RawMessage{}
		}
	}
	auth, err := json.Marshal(map[string]string{"token": c.token})
	if err != nil {
		return nil, fmt.Errorf("marshal auth: %w", err)
	}
	fields["__conduit__"] = auth

	encoded, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("marshal params: %w", err)
	}
	return []byte(url.Values{"params": {string(encoded)}, "output": {"json"}}.Encode()), nil
}

func isEmpty(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte("[]"))
}

func setFormHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
}
