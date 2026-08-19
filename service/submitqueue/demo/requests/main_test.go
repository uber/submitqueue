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
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	"github.com/uber/submitqueue/submitqueue/client"
)

func TestChangeFilePath_IsUniquePerFileAcrossChangesAndRuns(t *testing.T) {
	// Uniqueness is the property the whole layout rests on: two changes writing
	// the same path would collide on content, and the run would measure conflict
	// handling instead of throughput.
	seen := make(map[string]string)
	for _, tag := range []string{"0810-1203", "0810-1204"} {
		for change := 1; change <= 20; change++ {
			for file := 1; file <= 8; file++ {
				path := changeFilePath(tag, resolveFolders(tag, 0), change, file)
				owner := fmt.Sprintf("%s/%d/%d", tag, change, file)
				if prev, ok := seen[path]; ok {
					t.Fatalf("path %s produced for both %s and %s", path, prev, owner)
				}
				seen[path] = owner
			}
		}
	}
}

func TestChangeFilePath_PutsEveryFileOfAChangeInOneDirectory(t *testing.T) {
	// The conflict analyzer keys on the directory, so a change spread across
	// several would overlap with everything and report a conflict that says
	// nothing about the change.
	dirs := make(map[string]struct{})
	for file := 1; file <= 8; file++ {
		parts := strings.Split(changeFilePath("0810-1203", 5, 1, file), "/")
		require.Len(t, parts, 3, "demo root, one folder, and the leaf")
		assert.Equal(t, "demo", parts[0])
		assert.Regexp(t, `^\d{2}$`, parts[1])
		dirs[strings.Join(parts[:2], "/")] = struct{}{}
	}
	assert.Len(t, dirs, 1, "one change writes into one directory")
}

// -folders is the dial on what a run demonstrates, so both ends of it have to
// do what they say.
func TestChangeFilePath_FollowsTheFolderCount(t *testing.T) {
	folderOf := func(folders, change int) string {
		parts := strings.Split(changeFilePath("0810-1203", folders, change, 1), "/")
		return strings.Join(parts[:2], "/")
	}

	t.Run("one folder puts every change together", func(t *testing.T) {
		dirs := make(map[string]struct{})
		for change := 1; change <= 10; change++ {
			dirs[folderOf(1, change)] = struct{}{}
		}
		assert.Len(t, dirs, 1, "every change must conflict with every other")
	})

	t.Run("many folders keep changes apart", func(t *testing.T) {
		dirs := make(map[string]struct{})
		for change := 1; change <= 3; change++ {
			dirs[folderOf(64, change)] = struct{}{}
		}
		assert.Len(t, dirs, 3, "three changes in 64 folders should not collide")
	})
}

func TestResolveFolders(t *testing.T) {
	t.Run("honors an explicit count", func(t *testing.T) {
		assert.Equal(t, 1, resolveFolders("0810-1203", 1))
		assert.Equal(t, 42, resolveFolders("0810-1203", 42))
	})

	t.Run("picks within the range when unset", func(t *testing.T) {
		// A demo that cannot be replayed is hard to talk about when something in
		// it goes wrong, and how the changes were spread is part of what
		// happened — so the pick follows the run tag.
		assert.Equal(t, resolveFolders("0810-1203", 0), resolveFolders("0810-1203", 0))

		seen := make(map[int]struct{})
		for minute := range 60 {
			folders := resolveFolders(fmt.Sprintf("0810-12%02d", minute), 0)
			assert.GreaterOrEqual(t, folders, minShardDirs)
			assert.LessOrEqual(t, folders, maxShardDirs)
			seen[folders] = struct{}{}
		}
		assert.Greater(t, len(seen), 1, "runs must not all pick the same number")
	})
}

func TestChangeFileCount(t *testing.T) {
	tests := []struct {
		name string
		min  int
	}{
		{name: "default minimum", min: 3},
		{name: "single file floor", min: 1},
		{name: "non-positive is clamped", min: 0},
		{name: "negative is clamped", min: -5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			floor := tt.min
			if floor < 1 {
				floor = 1
			}
			for change := 1; change <= 50; change++ {
				got := changeFileCount("0810-1203", change, tt.min)
				assert.GreaterOrEqual(t, got, floor)
				assert.LessOrEqual(t, got, floor+3)
			}
		})
	}
}

func TestChangeFileCount_VariesButIsReproducible(t *testing.T) {
	counts := make(map[int]struct{})
	for change := 1; change <= 30; change++ {
		got := changeFileCount("0810-1203", change, 3)
		counts[got] = struct{}{}
		assert.Equal(t, got, changeFileCount("0810-1203", change, 3),
			"replaying a tag must reproduce the run")
	}
	assert.Greater(t, len(counts), 1, "the count should vary across changes, not be constant")
}

func TestRowCount(t *testing.T) {
	// A stack lands as one request however many pull requests it chains, so it
	// gets one row; independent changes get one each.
	assert.Equal(t, 3, rowCount(config{count: 3}), "independent changes are one request each")
	assert.Equal(t, 1, rowCount(config{count: 3, stacked: true}), "a stack is a single request")
}

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

// recordingSource counts how many changes have been created, so a test can
// observe creation relative to enqueuing. It creates through fakeSource, so it
// stands in for any provider — the two-phase orchestration under test is the
// same whichever one is wired.
type recordingSource struct {
	created *int64
}

func (recordingSource) baseSHA(context.Context, string) (string, error) { return "base", nil }

func (r recordingSource) open(ctx context.Context, spec changeSpec) (openedChange, error) {
	atomic.AddInt64(r.created, 1)
	return fakeSource{}.open(ctx, spec)
}

// landerFunc adapts a function to the lander interface.
type landerFunc func(context.Context, string, []string, mergestrategypb.Strategy) (string, error)

func (f landerFunc) Land(ctx context.Context, queue string, uris []string, s mergestrategypb.Strategy) (string, error) {
	return f(ctx, queue, uris, s)
}

// TestCreateBurst_EnqueuesOnlyAfterEveryChangeIsCreated is the property -burst
// exists for: no request reaches the queue until the last change has been
// created, so they all arrive together. It is provider-independent — burst runs
// every source through the same two phases.
func TestCreateBurst_EnqueuesOnlyAfterEveryChangeIsCreated(t *testing.T) {
	const count = 8
	cfg := config{count: count, concurrency: 4, files: 3, folders: 3, land: true, burst: true, queue: "q", prefix: "demo"}

	var created int64
	src := recordingSource{created: &created}

	var mu sync.Mutex
	createdWhenLanded := make([]int64, 0, count)
	lander := landerFunc(func(context.Context, string, []string, mergestrategypb.Strategy) (string, error) {
		mu.Lock()
		createdWhenLanded = append(createdWhenLanded, atomic.LoadInt64(&created))
		mu.Unlock()
		return "sqid", nil
	})

	tracker := client.NewTracker(client.NewRows(count))
	got, err := createBurst(context.Background(), src, lander, cfg,
		mergestrategypb.Strategy_SQUASH_REBASE, "run", "base", tracker)
	require.NoError(t, err)
	require.Len(t, got, count)

	require.Len(t, createdWhenLanded, count, "every change is enqueued exactly once")
	for _, seen := range createdWhenLanded {
		assert.Equal(t, int64(count), seen,
			"burst enqueues nothing until every change has been created")
	}
}
