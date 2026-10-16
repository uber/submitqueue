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

package tango

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func targetsWithIDs(ids ...uint64) []SignatureTarget {
	targets := make([]SignatureTarget, len(ids))
	for i, id := range ids {
		targets[i] = SignatureTarget{ID: id}
	}
	return targets
}

func TestSharesTarget(t *testing.T) {
	const highBit = uint64(1) << 63

	tests := []struct {
		name string
		a, b []SignatureTarget
		want bool
	}{
		{name: "both empty"},
		{name: "one empty", a: targetsWithIDs(1, 2)},
		{name: "disjoint", a: targetsWithIDs(1, 3, 5), b: targetsWithIDs(2, 4, 6)},
		{name: "same first ID", a: targetsWithIDs(1, 3), b: targetsWithIDs(1, 9), want: true},
		{name: "same last ID", a: targetsWithIDs(1, 7), b: targetsWithIDs(2, 7), want: true},
		{name: "same middle ID", a: targetsWithIDs(1, 4, 9), b: targetsWithIDs(2, 4, 8), want: true},
		{name: "different lengths", a: targetsWithIDs(5), b: targetsWithIDs(1, 2, 3, 4, 5, 6), want: true},
		{name: "IDs above the signed range order as unsigned", a: targetsWithIDs(1, highBit), b: targetsWithIDs(2, highBit+1)},
		{name: "same ID above the signed range", a: targetsWithIDs(1, highBit), b: targetsWithIDs(highBit), want: true},
		{
			name: "distance does not affect the result",
			a:    []SignatureTarget{{ID: 4, Distance: 0}},
			b:    []SignatureTarget{{ID: 4, Distance: 9}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sharesTarget(tt.a, tt.b))
			assert.Equal(t, tt.want, sharesTarget(tt.b, tt.a))
		})
	}
}
