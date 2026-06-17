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

// Package changeutil holds small helpers shared across the entity/change
// provider subpackages (github, git, ...).
package changeutil

// IsFullHex reports whether s is exactly length lowercase hex characters.
// Providers that pin a change to a commit by SHA (github, git) use this to
// validate the SHA segment of a change URI.
func IsFullHex(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
