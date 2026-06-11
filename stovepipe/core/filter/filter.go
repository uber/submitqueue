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

// Package filter implements a filter for commit events.
package filter

import (
	"strings"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Config controls which VCS URIs are watched.
// WatchedURIPrefixes is a list of URI prefixes to match against ChangeEvent.URI.
// Example: "git://github.com/uber/go-code/refs/heads/main"
// watches all commits on the main branch of uber/go-code.
func ShouldProcess(event entity.ChangeEvent, watchedPrefixes []string) bool {
	for _, prefix := range watchedPrefixes {
		if strings.HasPrefix(event.URI, prefix) {
			return true
		}
	}
	return false
}
