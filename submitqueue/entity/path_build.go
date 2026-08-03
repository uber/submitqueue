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

// PathBuild names the build started for one attempt of one speculation path.
//
// It is the reverse of Build's key. A Build is keyed by the identifier the
// runner minted, which is what every stage watching a build already holds; a
// caller starting from a path has no way to derive that identifier, so the link
// is recorded under the coordinates it does hold.
//
// A record is write-once: it is created already naming its build and never
// changes, so an attempt maps to one build for good — a retried path is a new
// attempt under a different key. An absent record means no build is recorded
// for the attempt; it does not promise that none is starting.
type PathBuild struct {
	// Queue is the name of the queue the path's head batch belongs to. It is
	// unique together with PathID and Attempt.
	Queue string
	// PathID is the speculation path, as carried by SpeculationPathEntry.ID,
	// unique within Queue.
	PathID string
	// Attempt is which build attempt for that path this is, starting at 1.
	Attempt int
	// BuildID is the build started for that attempt. Never empty.
	BuildID string
}
