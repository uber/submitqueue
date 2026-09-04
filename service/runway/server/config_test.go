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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	landstrategypb "github.com/uber/submitqueue/api/base/landstrategy/protopb"
)

// writeConfig writes a land config file and returns its path.
func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "land.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestLoadLandConfig_Defaults(t *testing.T) {
	path := writeConfig(t, `
defaults:
  lander: {type: noop}
queues:
  - name: demo
    lander:
      type: git
      remoteUrl: https://example.com/o/r.git
      checkoutPath: /var/checkouts/r
`)

	cfg, err := loadLandConfig(path)
	require.NoError(t, err)

	demo := cfg.Queues[0].Lander
	require.NotNil(t, demo)
	assert.Equal(t, "origin", demo.Remote)
	assert.Equal(t, "main", demo.Target)
	assert.Equal(t, "x-access-token", demo.TokenUser)
	assert.Equal(t, landstrategypb.Strategy_REBASE, demo.strategy())
	require.NotNil(t, demo.CheckStaleness)
	assert.True(t, *demo.CheckStaleness, "staleness checking is on unless turned off")
	assert.False(t, demo.UpdateHeadBranch)
	assert.True(t, cfg.usesGit())
}

func TestLoadLandConfig_ExplicitValuesWin(t *testing.T) {
	path := writeConfig(t, `
defaults:
  lander: {type: noop}
queues:
  - name: demo
    lander:
      type: git
      remoteUrl: https://example.com/o/r.git
      remote: upstream
      target: trunk
      checkoutPath: /var/checkouts/r
      defaultStrategy: SQUASH_REBASE
      checkStaleness: false
      updateHeadBranch: true
      tokenEnv: SOME_TOKEN
      tokenUser: oauth2
`)

	cfg, err := loadLandConfig(path)
	require.NoError(t, err)

	demo := cfg.Queues[0].Lander
	assert.Equal(t, "upstream", demo.Remote)
	assert.Equal(t, "trunk", demo.Target)
	assert.Equal(t, "oauth2", demo.TokenUser)
	assert.Equal(t, landstrategypb.Strategy_SQUASH_REBASE, demo.strategy())
	require.NotNil(t, demo.CheckStaleness)
	assert.False(t, *demo.CheckStaleness)
	assert.True(t, demo.UpdateHeadBranch)
}

func TestLoadLandConfig_NoopOnlyNeedsNoGit(t *testing.T) {
	path := writeConfig(t, `
defaults:
  lander: {type: noop}
queues:
  - name: demo
`)

	cfg, err := loadLandConfig(path)
	require.NoError(t, err)
	assert.False(t, cfg.usesGit(), "a noop-only deployment must not require a git runtime")
}

func TestLoadLandConfig_EmptyFileIsNoop(t *testing.T) {
	cfg, err := loadLandConfig(writeConfig(t, ""))
	require.NoError(t, err)
	assert.Equal(t, landerTypeNoop, cfg.Defaults.Lander.Type)
	assert.False(t, cfg.usesGit())
}

func TestLoadLandConfig_SharedCheckoutForSameTargetIsAllowed(t *testing.T) {
	// Two queues landing on one target must resolve to one lander instance, so
	// sharing a checkout is the correct configuration rather than a mistake.
	path := writeConfig(t, `
defaults:
  lander: {type: noop}
queues:
  - name: a
    lander: {type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r}
  - name: b
    lander: {type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r}
`)

	_, err := loadLandConfig(path)
	require.NoError(t, err)
}

func TestLoadLandConfig_SharedCheckoutToleratesEquivalentSpellings(t *testing.T) {
	// Sharing is judged on the normalized configuration, so a queue that omits
	// a defaulted field and one that states the default explicitly agree. An
	// empty refspec list and an omitted one likewise mean the same thing.
	path := writeConfig(t, `
defaults:
  lander: {type: noop}
queues:
  - name: a
    lander: {type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r}
  - name: b
    lander:
      type: git
      remoteUrl: https://example.com/o/r.git
      checkoutPath: /var/checkouts/r
      remote: origin
      target: main
      checkStaleness: true
      fetchRefspecs: []
`)

	_, err := loadLandConfig(path)
	require.NoError(t, err)
}

func TestLoadLandConfig_RejectsSharedCheckoutWithDivergentLanderFields(t *testing.T) {
	// A shared checkout means a shared lander instance, built from whichever
	// queue reached the builder first. Any field that instance is constructed
	// from must agree, or the second queue silently runs with the first's
	// value — merging with a strategy it never asked for, and saying nothing.
	tests := []struct {
		name string
		b    string
	}{
		{
			name: "default strategy",
			b:    "{type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, defaultStrategy: REBASE}",
		},
		{
			name: "update head branch",
			b:    "{type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, updateHeadBranch: true}",
		},
		{
			name: "staleness checking",
			b:    "{type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, checkStaleness: false}",
		},
		{
			name: "push attempts",
			b:    "{type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, maxPushAttempts: 7}",
		},
		{
			name: "committer identity",
			b:    "{type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, committerEmail: other@example.com}",
		},
		{
			name: "fetch refspecs",
			b:    "{type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, fetchRefspecs: [+refs/pull/*:refs/pull/*]}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, `
defaults:
  lander: {type: noop}
queues:
  - name: a
    lander: {type: git, remoteUrl: https://example.com/o/r.git, checkoutPath: /var/checkouts/r, defaultStrategy: SQUASH_REBASE, committerEmail: a@example.com}
  - name: b
    lander: `+tt.b+`
`)
			_, err := loadLandConfig(path)
			require.Error(t, err)
		})
	}
}

func TestLanderConfig_FirstDifference(t *testing.T) {
	// The differing field is named rather than left to the reader, because
	// "these two queues disagree somewhere" is not enough to act on when a
	// lander has fifteen fields. This is asserted on the returned value rather
	// than on an error string so it stays a test of behavior.
	base := landerConfig{Type: landerTypeGit, RemoteURL: "https://example.com/o/r.git", Remote: "origin", Target: "main"}
	enabled, disabled := true, false

	tests := []struct {
		name        string
		other       landerConfig
		wantDiffers bool
		wantField   string
	}{
		{name: "identical", other: base},
		{
			name:        "target",
			other:       func() landerConfig { c := base; c.Target = "release"; return c }(),
			wantDiffers: true, wantField: "target",
		},
		{
			name:        "default strategy",
			other:       func() landerConfig { c := base; c.DefaultStrategy = "REBASE"; return c }(),
			wantDiffers: true, wantField: "defaultStrategy",
		},
		{
			name:        "optional bool set on one side only",
			other:       func() landerConfig { c := base; c.CheckStaleness = &enabled; return c }(),
			wantDiffers: true, wantField: "checkStaleness",
		},
		{
			name:        "refspecs",
			other:       func() landerConfig { c := base; c.FetchRefspecs = []string{"+refs/pull/*:refs/pull/*"}; return c }(),
			wantDiffers: true, wantField: "fetchRefspecs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff, differs := base.firstDifference(tt.other)
			assert.Equal(t, tt.wantDiffers, differs)
			if !tt.wantDiffers {
				assert.Empty(t, diff)
				return
			}
			assert.True(t, strings.HasPrefix(diff, tt.wantField+" "),
				"the difference should lead with the field name, got %q", diff)
		})
	}

	// Two pointers to equal values are not a difference: queues spelling an
	// optional bool differently must still share a checkout.
	a, b := base, base
	a.CheckStaleness, b.CheckStaleness = &enabled, &enabled
	_, differs := a.firstDifference(b)
	assert.False(t, differs, "distinct pointers to the same value are not a difference")

	b.CheckStaleness = &disabled
	diff, differs := a.firstDifference(b)
	assert.True(t, differs)
	assert.Equal(t, "checkStaleness (true vs false)", diff,
		"an optional bool reads as its value, not as a pointer address")
}

func TestLoadLandConfig_Rejects(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{
			name: "unknown lander type",
			contents: `
defaults:
  lander: {type: magic}
`,
		},
		{
			name: "git without checkout path",
			contents: `
defaults:
  lander: {type: git, remoteUrl: https://example.com/o/r.git}
`,
		},
		{
			name: "token without a remote to authenticate to",
			contents: `
defaults:
  lander: {type: git, checkoutPath: /var/checkouts/r, tokenEnv: SOME_TOKEN}
`,
		},
		{
			name: "unknown default strategy",
			contents: `
defaults:
  lander:
    type: git
    remoteUrl: https://example.com/o/r.git
    checkoutPath: /var/checkouts/r
    defaultStrategy: FAST_FORWARD
`,
		},
		{
			name: "queue without a name",
			contents: `
defaults:
  lander: {type: noop}
queues:
  - lander: {type: noop}
`,
		},
		{
			name: "duplicate queue",
			contents: `
defaults:
  lander: {type: noop}
queues:
  - name: a
    lander: {type: noop}
  - name: a
    lander: {type: noop}
`,
		},
		{
			// Two landers over one working tree would reset and push it out
			// from under each other mid-land.
			name: "same checkout for different targets",
			contents: `
defaults:
  lander: {type: noop}
queues:
  - name: a
    lander: {type: git, remoteUrl: https://example.com/o/r.git, target: main, checkoutPath: /var/checkouts/r}
  - name: b
    lander: {type: git, remoteUrl: https://example.com/o/r.git, target: release, checkoutPath: /var/checkouts/r}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadLandConfig(writeConfig(t, tt.contents))
			require.Error(t, err)
		})
	}
}

func TestLanderConfig_NeedsHTTPCredential(t *testing.T) {
	tests := []struct {
		name string
		cfg  landerConfig
		want bool
	}{
		{
			name: "https remote with token",
			cfg:  landerConfig{RemoteURL: "https://example.com/o/r.git", TokenEnv: "T"},
			want: true,
		},
		{
			name: "https remote without token",
			cfg:  landerConfig{RemoteURL: "https://example.com/o/r.git"},
			want: false,
		},
		{
			// The credential is an HTTP header; over SSH there is nothing to
			// attach it to, and the lander passes the agent socket instead.
			name: "ssh remote",
			cfg:  landerConfig{RemoteURL: "ssh://git@example.com/o/r.git", TokenEnv: "T"},
			want: false,
		},
		{
			name: "local path used by the hermetic e2e",
			cfg:  landerConfig{RemoteURL: "file:///srv/git/sandbox.git", TokenEnv: "T"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.needsHTTPCredential())
		})
	}
}

func TestLanderConfig_Provisions(t *testing.T) {
	// A checkout with no remote named is one something else built; the wiring
	// must use it as it stands rather than trying to create it.
	external := landerConfig{Type: landerTypeGit, CheckoutPath: "/var/checkouts/r"}
	assert.False(t, external.provisions())

	owned := landerConfig{Type: landerTypeGit, CheckoutPath: "/var/checkouts/r", RemoteURL: "https://example.com/o/r.git"}
	assert.True(t, owned.provisions())
}
