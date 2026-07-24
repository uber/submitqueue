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

func TestSpeculationPath_ID_Deterministic(t *testing.T) {
	path := SpeculationPath{
		Head: "queueA/batch/3",
		Bets: []DependencyBet{
			{Batch: "queueA/batch/1", Bet: BetIncluded},
			{Batch: "queueA/batch/2", Bet: BetExcluded},
		},
	}

	// Same content produces the same ID across separate value instances.
	same := SpeculationPath{
		Head: "queueA/batch/3",
		Bets: []DependencyBet{
			{Batch: "queueA/batch/1", Bet: BetIncluded},
			{Batch: "queueA/batch/2", Bet: BetExcluded},
		},
	}

	assert.Equal(t, path.ID(), same.ID())
	assert.NotEmpty(t, path.ID())
}

func TestSpeculationPath_ID_Sensitivity(t *testing.T) {
	base := SpeculationPath{
		Head: "queueA/batch/3",
		Bets: []DependencyBet{
			{Batch: "queueA/batch/1", Bet: BetIncluded},
			{Batch: "queueA/batch/2", Bet: BetIncluded},
		},
	}

	tests := []struct {
		name string
		path SpeculationPath
	}{
		{
			name: "different head",
			path: SpeculationPath{
				Head: "queueA/batch/4",
				Bets: base.Bets,
			},
		},
		{
			name: "different bet type",
			path: SpeculationPath{
				Head: base.Head,
				Bets: []DependencyBet{
					{Batch: "queueA/batch/1", Bet: BetIncluded},
					{Batch: "queueA/batch/2", Bet: BetExcluded},
				},
			},
		},
		{
			name: "different dependency",
			path: SpeculationPath{
				Head: base.Head,
				Bets: []DependencyBet{
					{Batch: "queueA/batch/1", Bet: BetIncluded},
					{Batch: "queueA/batch/9", Bet: BetIncluded},
				},
			},
		},
		{
			name: "different bet order",
			path: SpeculationPath{
				Head: base.Head,
				Bets: []DependencyBet{
					{Batch: "queueA/batch/2", Bet: BetIncluded},
					{Batch: "queueA/batch/1", Bet: BetIncluded},
				},
			},
		},
		{
			name: "no bets",
			path: SpeculationPath{
				Head: base.Head,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEqual(t, base.ID(), tt.path.ID())
		})
	}
}

func TestSpeculationPathStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		status   SpeculationPathStatus
		expected bool
	}{
		{name: "passed is terminal", status: SpeculationPathStatusPassed, expected: true},
		{name: "failed is terminal", status: SpeculationPathStatusFailed, expected: true},
		{name: "cancelled is terminal", status: SpeculationPathStatusCancelled, expected: true},
		{name: "pending is not terminal", status: SpeculationPathStatusPending, expected: false},
		{name: "building is not terminal", status: SpeculationPathStatusBuilding, expected: false},
		{name: "cancelling is not terminal", status: SpeculationPathStatusCancelling, expected: false},
		{name: "unknown is not terminal", status: SpeculationPathStatusUnknown, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.IsTerminal())
		})
	}
}

func TestSpeculationPathEntry_IDDerivesFromPath(t *testing.T) {
	path := SpeculationPath{
		Head: "queueA/batch/2",
		Bets: []DependencyBet{{Batch: "queueA/batch/1", Bet: BetIncluded}},
	}

	// A well-formed entry keys itself by its path's content hash.
	entry := SpeculationPathEntry{ID: path.ID(), Path: path}
	assert.Equal(t, entry.Path.ID(), entry.ID)

	// A different path (same head, different bet) is a different entry.
	other := SpeculationPath{
		Head: path.Head,
		Bets: []DependencyBet{{Batch: "queueA/batch/1", Bet: BetExcluded}},
	}
	assert.NotEqual(t, entry.ID, other.ID())
}
