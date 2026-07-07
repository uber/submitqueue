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

// Package base is the root of shared domain entities used across SubmitQueue,
// Stovepipe, and other repo-local services. Subpackages hold concrete types
// (change identities, queue messages). Service-specific entities live under
// each domain (e.g. submitqueue/entity, stovepipe/entity).
package base
