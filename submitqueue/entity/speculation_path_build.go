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

// SpeculationPathBuild is the path->build mapping: given a speculation path's
// ID, it records the build that path resolved to. This is the forward lookup
// (path->build); the reverse lookup (build->path) is Build.SpeculationPathID.
type SpeculationPathBuild struct {
	// PathID is the speculation path's ID (SpeculationPathInfo.ID). It is the
	// primary key of this mapping and is globally unique — see
	// SpeculationPathInfo.ID for the uniqueness contract — so the mapping
	// needs no additional scoping key.
	PathID string
	// BuildID is the runner-minted build ID (Build.ID) this path resolved to.
	BuildID string
	// BatchID is the batch whose speculation tree contains this path. It
	// makes the row self-describing without parsing PathID's internal format.
	BatchID string
	// CreatedAt is the creation time of this mapping, in milliseconds since
	// epoch.
	CreatedAt int64
	// Version is the version of the object. It is used for optimistic locking:
	// updates are conditional on the persisted version matching the caller's
	// expected version. Versioning starts at 1; version arithmetic is owned by
	// the controller, the store performs a pure conditional write.
	Version int32
}
