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

package entity

import (
	"fmt"
	"math"
)

// ProjectFactByDegree maps an ordered key to an immutable project fact within a queue.
type ProjectFactByDegree struct {
	// URI is the commit containing the project result.
	URI string
	// Degree is the breakage degree in [0, 1], ordered highest first.
	Degree float64
	// Project identifies the fact and breaks equal-degree ties in ascending bytewise order.
	Project string
}

// NewProjectFactByDegree derives a degree lookup entry from a named project's fact.
// Invalid degrees, empty URIs and whole-repository facts return an error.
func NewProjectFactByDegree(fact ValidationFact) (ProjectFactByDegree, error) {
	if fact.URI == "" || fact.Project == "" || math.IsNaN(fact.Degree) || fact.Degree < DegreeGreen || fact.Degree > DegreeBroken {
		return ProjectFactByDegree{}, fmt.Errorf("project fact degree lookup requires a URI, project and degree in [0, 1]")
	}
	return ProjectFactByDegree{URI: fact.URI, Degree: fact.Degree, Project: fact.Project}, nil
}
