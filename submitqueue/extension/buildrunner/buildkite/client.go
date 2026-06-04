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

package buildkite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// client is a thin wrapper around the Buildkite REST endpoints that BuildRunner
// needs: create, get, list-by-metadata, and cancel a build.
type client struct {
	httpClient *http.Client
}

type createBuildRequest struct {
	Branch   string            `json:"branch"`
	Message  string            `json:"message"`
	Env      map[string]string `json:"env"`
	MetaData map[string]string `json:"meta_data,omitempty"`
}

// buildResponse is the subset of fields the runner needs from a Buildkite
// build object.
type buildResponse struct {
	Number int    `json:"number"`
	State  string `json:"state"`
	WebURL string `json:"web_url"`
}

func (c *client) createBuild(ctx context.Context, org, pipeline string, req createBuildRequest) (buildResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return buildResponse{}, fmt.Errorf("marshal request: %w", err)
	}
	u := fmt.Sprintf("/organizations/%s/pipelines/%s/builds", org, pipeline)
	var build buildResponse
	if err := c.do(ctx, http.MethodPost, u, body, &build); err != nil {
		return buildResponse{}, err
	}
	return build, nil
}

func (c *client) getBuild(ctx context.Context, org, pipeline string, number int) (buildResponse, error) {
	u := fmt.Sprintf("/organizations/%s/pipelines/%s/builds/%d", org, pipeline, number)
	var build buildResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &build); err != nil {
		return buildResponse{}, err
	}
	return build, nil
}

// findBuildByMeta returns the build in the pipeline whose meta_data[key] equals
// value. ok is false when no such build exists yet. This lets Status and Cancel
// recover the Buildkite reference from Buildkite itself (the source of truth)
// when the in-memory cache misses, e.g. after a process restart.
func (c *client) findBuildByMeta(ctx context.Context, org, pipeline, key, value string) (build buildResponse, ok bool, err error) {
	u := fmt.Sprintf("/organizations/%s/pipelines/%s/builds?meta_data[%s]=%s",
		org, pipeline, url.QueryEscape(key), url.QueryEscape(value))
	var builds []buildResponse
	if err := c.do(ctx, http.MethodGet, u, nil, &builds); err != nil {
		return buildResponse{}, false, err
	}
	if len(builds) == 0 {
		return buildResponse{}, false, nil
	}
	return builds[0], true, nil
}

// cancelBuild requests cancellation. Returns nil when the build is already
// terminal (HTTP 422) — the Buildkite API uses that status to indicate a
// non-cancellable build, which the BuildRunner contract treats as a no-op.
func (c *client) cancelBuild(ctx context.Context, org, pipeline string, number int) error {
	u := fmt.Sprintf("/organizations/%s/pipelines/%s/builds/%d/cancel", org, pipeline, number)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnprocessableEntity:
		// Already terminal — no-op per BuildRunner.Cancel contract.
		return nil
	default:
		return fmt.Errorf("unexpected status %d from cancel", resp.StatusCode)
	}
}

// do sends an HTTP request with the standard Buildkite headers and, on a 2xx
// response, decodes the body into out (when non-nil). A 404 is reported as a
// "build not found" error per the BuildRunner contract.
func (c *client) do(ctx context.Context, method, rawURL string, body []byte, out any) error {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("build not found")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("API returned status %d: %s", resp.StatusCode, respBody)
	}

	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

func (c *client) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
}
