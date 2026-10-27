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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjectFactByDegreePreservesDegree(t *testing.T) {
	for _, degree := range []float64{1, math.Nextafter(1, 0), 0.75, math.Nextafter(0.5, 1), 0.5, math.SmallestNonzeroFloat64, 0, math.Copysign(0, -1)} {
		entry, err := NewProjectFactByDegree(ValidationFact{URI: "commit", Project: "project", Degree: degree})
		require.NoError(t, err)
		assert.Equal(t, ProjectFactByDegree{URI: "commit", Project: "project", Degree: degree}, entry)
	}
}

func TestProjectFactByDegreeRejectsInvalidFacts(t *testing.T) {
	for _, fact := range []ValidationFact{
		{Project: "p"}, {URI: "commit"},
		{URI: "commit", Project: "p", Degree: math.NaN()},
		{URI: "commit", Project: "p", Degree: math.Inf(1)},
		{URI: "commit", Project: "p", Degree: -0.1},
		{URI: "commit", Project: "p", Degree: 1.1},
	} {
		_, err := NewProjectFactByDegree(fact)
		require.Error(t, err)
	}
}
