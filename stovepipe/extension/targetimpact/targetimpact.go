// Copyright (c) 2026 Uber Technologies, Inc.
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

// Package targetimpact defines the contract that maps validation targets to
// affected projects at a repository revision.
package targetimpact

import "context"

// Mapping associates one validation target with the projects it affects.
// ProjectIDs may be empty when the target affects no deployable project.
type Mapping struct {
	// TargetID is the opaque validation-target identifier supplied to Mapper.
	TargetID string
	// ProjectIDs are the stable project identifiers affected by TargetID.
	ProjectIDs []string
}

// Mapper resolves the affected projects for validation targets at one exact
// repository revision. Target IDs are opaque to Stovepipe: an implementation
// may interpret them as Bazel labels, test names, file paths, or another
// build-system-specific identifier.
type Mapper interface {
	// GetImpactedProjects returns one Mapping for every target ID in targetIDs,
	// in the same order. Callers must supply non-empty, unique target IDs;
	// implementations must return unique, non-empty ProjectIDs within each
	// Mapping. A Mapping with no ProjectIDs is valid; an unresolvable target or
	// revision must cause an error rather than being represented as empty.
	GetImpactedProjects(ctx context.Context, headURI string, targetIDs []string) ([]Mapping, error)
}

// Config carries the queue identity handed to a Factory.
type Config struct {
	// QueueName identifies the queue served by the mapper.
	QueueName string
}

// Factory constructs a Mapper for one queue.
type Factory interface {
	For(cfg Config) (Mapper, error)
}
