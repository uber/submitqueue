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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeFilePath_IsUniquePerFileAcrossChangesAndRuns(t *testing.T) {
	// Uniqueness is the property the whole layout rests on: two changes writing
	// the same path would collide on content, and the run would measure conflict
	// handling instead of throughput.
	seen := make(map[string]string)
	for _, tag := range []string{"0810-1203", "0810-1204"} {
		for change := 1; change <= 20; change++ {
			for file := 1; file <= 8; file++ {
				path := changeFilePath(tag, change, file)
				owner := fmt.Sprintf("%s/%d/%d", tag, change, file)
				if prev, ok := seen[path]; ok {
					t.Fatalf("path %s produced for both %s and %s", path, prev, owner)
				}
				seen[path] = owner
			}
		}
	}
}

func TestChangeFilePath_ShardsUnderTheDemoRoot(t *testing.T) {
	path := changeFilePath("0810-1203", 1, 1)

	parts := strings.Split(path, "/")
	require.Len(t, parts, shardDirs+2, "demo root, %d bucket dirs, and the leaf", shardDirs)
	assert.Equal(t, "demo", parts[0])
	for _, bucket := range parts[1 : len(parts)-1] {
		assert.Len(t, bucket, 2, "each bucket is one hex byte")
		assert.Regexp(t, "^[0-9a-f]{2}$", bucket)
	}
	assert.Equal(t, "0810-1203-1-1.txt", parts[len(parts)-1])
}

func TestChangeFilePath_SpreadsAcrossManyBuckets(t *testing.T) {
	// A layout that puts everything in one directory would satisfy the
	// uniqueness test above while defeating the point of sharding.
	buckets := make(map[string]struct{})
	for change := 1; change <= 20; change++ {
		for file := 1; file <= 4; file++ {
			parts := strings.Split(changeFilePath("0810-1203", change, file), "/")
			buckets[strings.Join(parts[1:len(parts)-1], "/")] = struct{}{}
		}
	}
	assert.Greater(t, len(buckets), 50, "80 files should land in many distinct buckets")
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
