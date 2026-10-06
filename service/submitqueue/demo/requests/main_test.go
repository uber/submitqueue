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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	valid := config{
		provider: providerGitHub, token: "t", repo: "owner/name",
		count: 3, files: 3, concurrency: 5,
	}

	tests := []struct {
		name    string
		mutate  func(*config)
		wantErr bool
	}{
		{name: "a usable configuration", mutate: func(*config) {}},
		{name: "concurrency of one is sequential, not invalid", mutate: func(c *config) { c.concurrency = 1 }},
		{name: "no token", mutate: func(c *config) { c.token = "" }, wantErr: true},
		{name: "no changes to make", mutate: func(c *config) { c.count = 0 }, wantErr: true},
		{name: "no files to write", mutate: func(c *config) { c.files = 0 }, wantErr: true},
		{name: "zero concurrency would never start", mutate: func(c *config) { c.concurrency = 0 }, wantErr: true},
		{name: "negative concurrency", mutate: func(c *config) { c.concurrency = -1 }, wantErr: true},
		{name: "repo without an owner", mutate: func(c *config) { c.repo = "name" }, wantErr: true},

		// The credential and the repository are GitHub's alone. Requiring
		// either of the other two modes to carry them is what would make the
		// quickstart need a token it has no use for.
		{name: "fake needs no token", mutate: func(c *config) {
			c.provider, c.token, c.repo = providerFake, "", ""
		}},
		{name: "git needs no token", mutate: func(c *config) {
			c.provider, c.token, c.repo = providerGit, "", ""
		}},
		{name: "an unknown provider", mutate: func(c *config) { c.provider = "gitlab" }, wantErr: true},
		{name: "no provider at all", mutate: func(c *config) { c.provider = "" }, wantErr: true},

		// Counts are checked for every mode, not just the ones that do I/O.
		{name: "fake with no changes to make", mutate: func(c *config) {
			c.provider, c.token, c.repo, c.count = providerFake, "", "", 0
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			err := cfg.validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestShapeReportsConcurrency(t *testing.T) {
	assert.Contains(t, shape(config{count: 10, concurrency: 5}), "5 at a time")
	assert.NotContains(t, shape(config{count: 10, concurrency: 1}), "at a time",
		"one at a time is just sequential; saying so adds nothing")
	assert.Contains(t, shape(config{count: 10, concurrency: 5, stacked: true}), "stacked",
		"a stack is sequential whatever the limit says")
	assert.Contains(t, shape(config{count: 10, concurrency: 5, land: true, burst: true}), "all enqueued at once")
	assert.NotContains(t, shape(config{count: 10, concurrency: 5, burst: true, land: false}), "at once",
		"burst is a landing decision; with nothing to land there is no burst")
}
