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

package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
	gitrepo "github.com/uber/submitqueue/platform/git/repo"
	"github.com/uber/submitqueue/platform/githubactions"
)

func TestPrimaryErrorClassifiers_BuildStatus(t *testing.T) {
	processor := errs.NewClassifierProcessor(primaryErrorClassifiers()...)
	tests := []struct {
		name         string
		status       int
		transportErr error
		retryable    bool
		dependency   bool
	}{
		{name: "rate limited", status: http.StatusTooManyRequests, retryable: true, dependency: true},
		{name: "service unavailable", status: http.StatusServiceUnavailable, retryable: true, dependency: true},
		{name: "bad request", status: http.StatusBadRequest, dependency: true},
		{name: "unauthorized", status: http.StatusUnauthorized, dependency: true},
		{name: "deadline exceeded", transportErr: context.DeadlineExceeded, retryable: true, dependency: true},
		{name: "shutdown", transportErr: context.Canceled, retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := githubactions.NewClient(&http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if tt.transportErr != nil {
						return nil, tt.transportErr
					}
					return &http.Response{
						StatusCode: tt.status,
						Body:       io.NopCloser(strings.NewReader("upstream failure")),
						Request:    req,
					}, nil
				}),
			}, "owner", "repo", "ci.yml")

			_, err := client.GetRun(context.Background(), 42)
			require.Error(t, err)
			out := processor.Process(fmt.Errorf("failed to get build status: %w", err))
			assert.Equal(t, tt.retryable, errs.IsRetryable(out))
			assert.Equal(t, tt.dependency, errs.IsDependencyError(out))
		})
	}
}

func TestPrimaryErrorClassifiers_Storage(t *testing.T) {
	processor := errs.NewClassifierProcessor(primaryErrorClassifiers()...)
	err := processor.Process(fmt.Errorf("load build: %w", driver.ErrBadConn))
	assert.True(t, errs.IsRetryable(err))
	assert.False(t, errs.IsDependencyError(err))
}

func TestPrimaryErrorClassifiers_GitFetch(t *testing.T) {
	processor := errs.NewClassifierProcessor(primaryErrorClassifiers()...)
	for _, tt := range []struct {
		name       string
		diagnostic string
		cancelled  bool
		retryable  bool
		dependency bool
	}{
		{
			name:       "temporary DNS failure",
			diagnostic: "fatal: Could not resolve host: git.example.invalid",
			retryable:  true,
			dependency: true,
		},
		{
			name:       "authentication failure",
			diagnostic: "fatal: Authentication failed for 'https://git.example.invalid/repo'",
			dependency: true,
		},
		{
			name:       "unknown failure",
			diagnostic: "fatal: unexpected failure",
			dependency: true,
		},
		{
			name:      "shutdown",
			cancelled: true,
			retryable: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			git := filepath.Join(dir, "git")
			diagnostic := "'" + strings.ReplaceAll(tt.diagnostic, "'", "'\\''") + "'"
			script := "#!/bin/sh\nprintf '%s\\n' " + diagnostic + " >&2\nexit 128\n"
			require.NoError(t, os.WriteFile(git, []byte(script), 0o755))
			repo, err := gitrepo.NewRepo(gitrepo.RepoConfig{
				Git:       git,
				Path:      dir,
				RemoteURL: "https://git.example.invalid/repo",
				Target:    "main",
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelled {
				cancel()
			}
			err = repo.FetchTarget(ctx)
			require.Error(t, err)
			out := processor.Process(fmt.Errorf("fetch target: %w", err))
			assert.Equal(t, tt.retryable, errs.IsRetryable(out))
			assert.Equal(t, tt.dependency, errs.IsDependencyError(out))
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
