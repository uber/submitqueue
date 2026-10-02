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

import "github.com/uber/submitqueue/platform/resourceid"

// CompareRequestID compares ingest order of two request IDs in the same queue.
// Returns -1 if a is older than b, 0 if equal, 1 if a is newer than b.
// IDs are canonical decimal strings whose scope is carried separately.
func CompareRequestID(a, b string) (int, error) {
	return resourceid.Compare(a, b)
}
