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

package demo

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeFilePath_IsUniquePerFileAcrossChangesAndRuns(t *testing.T) {
	seen := make(map[string]string)
	for _, tag := range []string{"0810-1203", "0810-1204"} {
		for change := 1; change <= 20; change++ {
			for file := 1; file <= 8; file++ {
				path := changeFilePath(tag, resolveFolders(tag, 0), change, file)
				owner := fmt.Sprintf("%s/%d/%d", tag, change, file)
				_, exists := seen[path]
				require.False(t, exists, "path %s duplicated", path)
				seen[path] = owner
			}
		}
	}
}

func TestChangeFilePath_PutsEveryFileOfAChangeInOneDirectory(t *testing.T) {
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
