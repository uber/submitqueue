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

// Package change holds the shared code-change identity used across SubmitQueue,
// Stovepipe, and other repo-local domains. A Change names code to act on via
// provider URIs; the URI parsers live in the github, phabricator, and git subpackages.
package change

// Change represents a code change identified by URIs from a code change provider (e.g., GitHub Pull Request, Phabricator Diff).
// The provider is extracted from the URI scheme. The object is immutable after creation.
type Change struct {
	// URIs identifies the change(s) to land (RFC 3986 compliant): scheme://<host[:port]>/<path>.
	// The scheme identifies the change provider, the authority is the provider instance the
	// change lives on, and the path contains provider-specific resource identifiers.
	//
	// Supported formats:
	//   GitHub PR:         "github://<host[:port]>/<org>/<repo>/pull/<pr>/<head_commit_sha>"
	//   Phabricator Diff:  "phab://<host[:port]>/D<revision>/<diff>"
	//   git ref/commit:    "git://<host[:port]>/<repo>/<ref>/<sha>"
	//   Example:           "github://github.example.com/uber/submitqueue/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"
	//
	// Head/commit SHAs must be the full 40-char lowercase hex form.
	//
	URIs []string `json:"uris"`
}
