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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidationFact_IsGreen(t *testing.T) {
	tests := []struct {
		name     string
		degree   float64
		expected bool
	}{
		{name: "green degree is green", degree: DegreeGreen, expected: true},
		{name: "broken degree is not green", degree: DegreeBroken, expected: false},
		{name: "partial breakage is not green", degree: 0.25, expected: false},
		{name: "zero value is green", degree: 0, expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fact := ValidationFact{Degree: tt.degree}
			assert.Equal(t, tt.expected, fact.IsGreen())
		})
	}
}
