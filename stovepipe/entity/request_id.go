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

package entity

import (
	"fmt"
	"strconv"
	"strings"
)

// CompareRequestID compares ingest order of two request IDs in the same queue.
// Returns -1 if a is older than b, 0 if equal, 1 if a is newer than b.
// IDs must follow the format request/<queue>/<counter>.
func CompareRequestID(queue, a, b string) (int, error) {
	if a == b {
		return 0, nil
	}
	aCounter, err := requestCounter(a, queue)
	if err != nil {
		return 0, err
	}
	bCounter, err := requestCounter(b, queue)
	if err != nil {
		return 0, err
	}
	switch {
	case aCounter < bCounter:
		return -1, nil
	case aCounter > bCounter:
		return 1, nil
	default:
		return 0, nil
	}
}

// requestIDPrefix returns the expected prefix for request ids in queue: "request/<queue>/".
func requestIDPrefix(queue string) string {
	return "request/" + queue + "/"
}

// requestCounter extracts the per-queue counter suffix from a request id.
func requestCounter(id, queue string) (int64, error) {
	prefix := requestIDPrefix(queue)
	if !strings.HasPrefix(id, prefix) {
		return 0, fmt.Errorf("request id %q does not match queue %q", id, queue)
	}
	return strconv.ParseInt(id[len(prefix):], 10, 64)
}
