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

package demo

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// Five to ten shards produce both shared-directory and independent changes.
const (
	minShardDirs = 5
	maxShardDirs = 10
)

func resolveFolders(tag string, configured int) int {
	if configured > 0 {
		return configured
	}
	sum := sha256.Sum256([]byte("folders#" + tag))
	return minShardDirs + int(sum[0])%(maxShardDirs-minShardDirs+1)
}

func changeShard(tag string, folders, change int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("shard#%s#%d", tag, change)))
	return fmt.Sprintf("%02d", int(sum[0])%folders)
}

func changeFilePath(tag string, folders, change, file int) string {
	leaf := fmt.Sprintf("%s-%d-%d.txt", tag, change, file)
	return strings.Join([]string{"demo", changeShard(tag, folders, change), leaf}, "/")
}

func changeFileCount(tag string, change, min int) int {
	if min < 1 {
		min = 1
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", tag, change)))
	return min + int(sum[0]%4)
}
