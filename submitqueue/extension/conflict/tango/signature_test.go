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

const (
	legacyLabel   = "//lib/strutil:legacy"
	strutilLabel  = "//lib/strutil:strutil"
	fuzzTestLabel = "//lib/strutil:fuzz_test"
	apiLabel      = "//svc/api:api"
	apiTestLabel  = "//svc/api:api_test"

	legacyID   = uint64(0x735cc8dafbb93f02)
	strutilID  = uint64(0x90afc9166e08f639)
	fuzzTestID = uint64(0xa9becee9e924a370)
	apiID      = uint64(0xbb211c1937bbeab8)
	apiTestID  = uint64(0xc48fa23bb87b90f4)
)

func TestTargetID(t *testing.T) {
	tests := []struct {
		label string
		want  uint64
	}{
		{legacyLabel, legacyID},
		{strutilLabel, strutilID},
		{fuzzTestLabel, fuzzTestID},
		{apiLabel, apiID},
		{apiTestLabel, apiTestID},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			assert.Equal(t, tt.want, targetID(tt.label))
		})
	}
}

func TestToSignatureTargets(t *testing.T) {
	tests := []struct {
		name       string
		targets    []LabelDistance
		wantTarget []SignatureTarget
		wantLabels []hashedLabel
	}{
		{
			name:       "empty input is an empty signature",
			targets:    nil,
			wantTarget: nil,
			wantLabels: nil,
		},
		{
			name:       "single target",
			targets:    []LabelDistance{{Label: apiLabel, Distance: 1}},
			wantTarget: []SignatureTarget{{ID: apiID, Distance: 1}},
			wantLabels: []hashedLabel{{id: apiID, label: apiLabel}},
		},
		{
			name: "response order is sorted by ID",
			targets: []LabelDistance{
				{Label: strutilLabel, Distance: 0},
				{Label: apiLabel, Distance: 1},
				{Label: apiTestLabel, Distance: 2},
				{Label: fuzzTestLabel, Distance: 0},
				{Label: legacyLabel, Distance: 0},
			},
			wantTarget: []SignatureTarget{
				{ID: legacyID, Distance: 0},
				{ID: strutilID, Distance: 0},
				{ID: fuzzTestID, Distance: 0},
				{ID: apiID, Distance: 1},
				{ID: apiTestID, Distance: 2},
			},
			wantLabels: []hashedLabel{
				{id: legacyID, label: legacyLabel},
				{id: strutilID, label: strutilLabel},
				{id: fuzzTestID, label: fuzzTestLabel},
				{id: apiID, label: apiLabel},
				{id: apiTestID, label: apiTestLabel},
			},
		},
		{
			name: "a repeated label keeps the smaller distance",
			targets: []LabelDistance{
				{Label: apiLabel, Distance: 3},
				{Label: apiLabel, Distance: 1},
				{Label: apiLabel, Distance: 2},
			},
			wantTarget: []SignatureTarget{{ID: apiID, Distance: 1}},
			wantLabels: []hashedLabel{{id: apiID, label: apiLabel}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTargets, gotLabels := toSignatureTargets(tt.targets)

			assert.Equal(t, tt.wantTarget, gotTargets)
			assert.Equal(t, tt.wantLabels, gotLabels)
		})
	}
}

func TestToSignatureTargets_LeavesInputUnchanged(t *testing.T) {
	targets := []LabelDistance{
		{Label: apiTestLabel, Distance: 2},
		{Label: legacyLabel, Distance: 0},
		{Label: apiLabel, Distance: 1},
	}
	original := append([]LabelDistance(nil), targets...)

	toSignatureTargets(targets)

	assert.Equal(t, original, targets)
}

func TestMergeHashedTargetsByID(t *testing.T) {
	tests := []struct {
		name       string
		hashed     []hashedTarget
		wantTarget []SignatureTarget
		wantLabels []hashedLabel
	}{
		{
			name: "labels sharing an ID collapse to the smaller distance and both stay in the dictionary",
			hashed: []hashedTarget{
				{hashedLabel: hashedLabel{id: 7, label: "//b:y"}, distance: 2},
				{hashedLabel: hashedLabel{id: 7, label: "//a:x"}, distance: 5},
				{hashedLabel: hashedLabel{id: 3, label: "//c:z"}, distance: 0},
			},
			wantTarget: []SignatureTarget{{ID: 3, Distance: 0}, {ID: 7, Distance: 2}},
			wantLabels: []hashedLabel{
				{id: 3, label: "//c:z"},
				{id: 7, label: "//a:x"},
				{id: 7, label: "//b:y"},
			},
		},
		{
			name: "colliding labels with equal distances keep one signature entry",
			hashed: []hashedTarget{
				{hashedLabel: hashedLabel{id: 9, label: "//b:y"}, distance: 1},
				{hashedLabel: hashedLabel{id: 9, label: "//a:x"}, distance: 1},
			},
			wantTarget: []SignatureTarget{{ID: 9, Distance: 1}},
			wantLabels: []hashedLabel{{id: 9, label: "//a:x"}, {id: 9, label: "//b:y"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTargets, gotLabels := mergeHashedTargetsByID(tt.hashed)

			assert.Equal(t, tt.wantTarget, gotTargets)
			assert.Equal(t, tt.wantLabels, gotLabels)
		})
	}
}
