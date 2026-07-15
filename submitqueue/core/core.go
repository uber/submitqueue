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

// Package core groups infrastructure and domain logic shared across
// SubmitQueue's own services and pipeline stages — the SubmitQueue-scoped
// analogue of the repo-level platform/. Cross-domain code lives under
// platform/; this package is for plumbing and shared rules private to
// SubmitQueue. Subpackages hold queue topic keys (topickey), request
// lifecycle plumbing (request), changeset resolution (changeset), and
// speculation rules evaluated by multiple stages (speculation).
package core
