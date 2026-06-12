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

// Package fakemarker holds the shared "sq-fake=<token>" change-URI marker
// convention used by the extension fakes to inject failures from a request
// payload. Each fake recognizes its own tokens (e.g. "build-fail", "push-error");
// this package only locates a token within change URIs so the parsing lives in
// one place instead of being copied into every fake. It is intended for examples
// and tests only, never production.
package fakemarker

import (
	"strings"

	"github.com/uber/submitqueue/entity/change"
)

// Prefix introduces a marker token in a change URI: "sq-fake=<token>".
const Prefix = "sq-fake="

// Token returns the marker token embedded in the first URI that carries one, or
// "" if none do. The token ends at the first "&" or "#" delimiter, so a marker
// may sit among other query parameters or a fragment (e.g.
// "github://o/r/pull/1/a?sq-fake=build-fail&attempt=2").
func Token(uris []string) string {
	for _, u := range uris {
		if i := strings.Index(u, Prefix); i >= 0 {
			rest := u[i+len(Prefix):]
			if j := strings.IndexAny(rest, "&#"); j >= 0 {
				rest = rest[:j]
			}
			return rest
		}
	}
	return ""
}

// TokenInChanges returns the first marker token found across all changes' URIs,
// or "" if none carry one.
func TokenInChanges(changes []change.Change) string {
	for _, c := range changes {
		if tok := Token(c.URIs); tok != "" {
			return tok
		}
	}
	return ""
}
