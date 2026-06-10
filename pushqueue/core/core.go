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

// Package core groups infrastructure shared across PushQueue's own services
// (gateway and orchestrator) — the PushQueue-scoped analogue of the repo-level
// core/. Cross-domain infrastructure lives in the top-level core/; this package
// is for plumbing private to PushQueue. Subpackages are added here as shared
// needs emerge, mirroring submitqueue/core.
package core
