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

// Package buildkite provides the HTTP client and Buildkite-specific facts
// (build state vocabulary, the env-var metadata round-trip convention, and
// build number id encoding) shared by every domain's Buildkite-backed
// BuildRunner. It intentionally holds no BuildRunner interface or domain
// entity types — each domain (submitqueue, stovepipe, ...) defines its own
// BuildRunner and its own BuildStatus, and adapts this package's State to it.
// See doc/rfc/stovepipe/steps/build.md's "Alternatives considered for
// sharing the contract" for the rationale.
package buildkite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	phttp "github.com/uber/submitqueue/platform/http"
)

// ErrNotFound is returned when the Buildkite API responds with 404 to a
// request for a resource by ID (e.g. GetBuild on an unknown build number).
var ErrNotFound = errors.New("buildkite: resource not found")

// Client is a thin wrapper around the Buildkite REST endpoints a BuildRunner
// needs: create, get, and cancel a build.
type Client struct {
	httpClient *http.Client
}

// NewClient wraps a pre-configured *http.Client as a Buildkite Client. The
// caller is responsible for the base URL (via platform/http.BaseURLTransport)
// and auth (via an Authorization-header transport).
func NewClient(httpClient *http.Client) *Client {
	return &Client{httpClient: httpClient}
}

// CreateBuildRequest is the payload for POST /builds.
type CreateBuildRequest struct {
	Message string            `json:"message"`
	Env     map[string]string `json:"env"`
}

// BuildResponse is the subset of fields callers need from a Buildkite build
// object.
type BuildResponse struct {
	Number int               `json:"number"`
	State  string            `json:"state"`
	WebURL string            `json:"web_url"`
	Env    map[string]string `json:"env"`
}

// CreateBuild creates a new Buildkite build.
func (c *Client) CreateBuild(ctx context.Context, req CreateBuildRequest) (BuildResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return BuildResponse{}, fmt.Errorf("marshal request: %w", err)
	}
	var build BuildResponse
	if err := c.do(ctx, http.MethodPost, "/builds", body, &build); err != nil {
		return BuildResponse{}, err
	}
	return build, nil
}

// GetBuild fetches a build by its Buildkite build number.
func (c *Client) GetBuild(ctx context.Context, number int) (BuildResponse, error) {
	u := fmt.Sprintf("/builds/%d", number)
	var build BuildResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &build); err != nil {
		return BuildResponse{}, err
	}
	return build, nil
}

// CancelBuild requests cancellation. Returns nil when the build is already
// terminal (HTTP 422) — the Buildkite API uses that status to indicate a
// non-cancellable build, which the BuildRunner contract treats as a no-op.
func (c *Client) CancelBuild(ctx context.Context, number int) error {
	u := fmt.Sprintf("/builds/%d/cancel", number)
	status, respBody, err := phttp.SendRequest(ctx, c.httpClient, http.MethodPut, u, nil, c.setHeaders)
	if err != nil {
		return err
	}

	switch status {
	case http.StatusOK:
		return nil
	case http.StatusUnprocessableEntity:
		// Already terminal — no-op per BuildRunner.Cancel contract.
		return nil
	default:
		return fmt.Errorf("unexpected status %d from cancel: %s", status, respBody)
	}
}

// do sends an HTTP request with the standard Buildkite headers and, on a 2xx
// response, decodes the body into out (when non-nil). A 404 is reported as
// ErrNotFound so callers can distinguish it from other failures.
func (c *Client) do(ctx context.Context, method, rawURL string, body []byte, out any) error {
	status, respBody, err := phttp.SendRequest(ctx, c.httpClient, method, rawURL, body, c.setHeaders)
	if err != nil {
		return err
	}

	if status == http.StatusNotFound {
		return ErrNotFound
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("API returned status %d: %s", status, respBody)
	}

	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
}

// EncodeBuildNumber encodes a Buildkite build number as an opaque build id
// string.
func EncodeBuildNumber(number int) string {
	return strconv.Itoa(number)
}

// ParseBuildNumber is the inverse of EncodeBuildNumber.
func ParseBuildNumber(id string) (int, error) {
	n, err := strconv.Atoi(id)
	if err != nil {
		return 0, fmt.Errorf("invalid build ID %q", id)
	}
	return n, nil
}

// State is Buildkite's own build-state vocabulary, interpreted from the raw
// state string every domain's Buildkite adapter receives on a build object.
// Buildkite's raw states are: creating, scheduled, running, blocked, passed,
// failed, canceling, canceled, skipped, not_run.
type State string

const (
	// StateUnknown is returned for a raw state this package does not
	// recognize. Not terminal — callers should keep polling rather than
	// treat it as a final outcome.
	StateUnknown State = ""
	// StateAccepted means the build has been accepted for execution but has
	// not started running yet (Buildkite: creating, scheduled).
	StateAccepted State = "accepted"
	// StateRunning means the build is currently executing, including while
	// blocked on a manual block step (Buildkite: running, blocked).
	StateRunning State = "running"
	// StateSucceeded means the build completed successfully (Buildkite:
	// passed).
	StateSucceeded State = "succeeded"
	// StateFailed means the build did not produce a passing result
	// (Buildkite: failed, not_run, skipped).
	StateFailed State = "failed"
	// StateCancelled means the build was cancelled, or is in the process of
	// being cancelled (Buildkite: canceling, canceled).
	StateCancelled State = "cancelled"
)

// ParseState maps a raw Buildkite build-state string to a State. An
// unrecognized raw string maps to StateUnknown rather than being assumed
// terminal.
func ParseState(raw string) State {
	switch raw {
	case "creating", "scheduled":
		return StateAccepted
	case "running", "blocked":
		return StateRunning
	case "passed":
		return StateSucceeded
	case "failed", "not_run", "skipped":
		return StateFailed
	case "canceling", "canceled":
		return StateCancelled
	default:
		return StateUnknown
	}
}

// DecodeMetadataEnv recovers a JSON-encoded map[string]string from env[key].
// Buildkite echoes env vars back on the build object, so a caller that seeded
// a JSON-encoded map at Trigger time can recover it here at Status time
// without any local state. Returns an empty non-nil map when the key is
// absent or its value cannot be decoded — a corrupt env var must not fail the
// caller.
func DecodeMetadataEnv(env map[string]string, key string) map[string]string {
	meta := make(map[string]string)
	raw, ok := env[key]
	if !ok || raw == "" {
		return meta
	}
	_ = json.Unmarshal([]byte(raw), &meta)
	return meta
}
