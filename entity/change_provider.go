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

// ChangeProvider represents a code change from an external provider (e.g., a GitHub pull request or Gerrit changelist)
// along with its associated metadata. The object is immutable after creation.
type ChangeProvider struct {
	// RequestID is the globally unique identifier for the land request. Format: "<queue>/<counter_value>".
	RequestID string
	// ChangeProviderSrc defines the source of the change. For e.g. - Github, Gitlab etc.
	ChangeProviderSrc string
	// ChangeProviderID is the identifier specified by the change provider source. For e.g. - Github PR ID etc.
	ChangeProviderID string
	// Metadata is the interesting data from the change provider that we want to store.
	// This is a freeform JSON object.
	Metadata map[string]string
}
