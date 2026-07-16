// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/sqsim/runner"
)

func TestModelRendersRowsSpinnerAndHistory(t *testing.T) {
	startedAt := time.Unix(100, 0)
	m := model{
		cancel:       func() {},
		scenario:     "mixed",
		showDetails:  true,
		width:        100,
		height:       24,
		now:          startedAt.Add(3 * time.Second),
		buildStarted: map[string]time.Time{"l1": startedAt.Add(time.Second)},
		snapshot: runner.Snapshot{
			Scenario:  "mixed",
			StartedAt: startedAt,
			Now:       startedAt.Add(3 * time.Second),
			Requests: []runner.Request{{
				Name:     "l1",
				SQID:     "sqsim/1",
				Status:   "building",
				Expected: "landed",
				History: []runner.HistoryEvent{
					{TimestampMs: startedAt.Add(time.Second).UnixMilli(), Status: "building", Metadata: map[string]string{"controller": "build"}},
				},
			}},
		},
	}

	view := m.View()
	assert.Contains(t, view, "l1")
	assert.Contains(t, view, "building")
	assert.Contains(t, view, "2s")
	assert.Contains(t, view, "History")
	assert.Contains(t, view, "build")

	updated, _ := m.Update(tickMsg(startedAt.Add(4 * time.Second)))
	assert.Contains(t, updated.(model).View(), "3s")
}

func TestModelNavigationAndDetailToggle(t *testing.T) {
	m := model{
		cancel:       func() {},
		showDetails:  true,
		width:        100,
		height:       12,
		buildStarted: make(map[string]time.Time),
		snapshot: runner.Snapshot{
			Requests: []runner.Request{{Name: "l1"}, {Name: "l2"}, {Name: "l3"}},
		},
	}

	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = updated.(model)
	assert.Equal(t, 1, m.selected)

	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	m = updated.(model)
	assert.False(t, m.showDetails)
}

func TestModelShowsLiveSubmissionProgress(t *testing.T) {
	m := model{
		cancel:       func() {},
		width:        100,
		height:       24,
		buildStarted: make(map[string]time.Time),
		snapshot: runner.Snapshot{
			Requests: []runner.Request{
				{Name: "l1", SQID: "sqsim/1", Status: "submitted"},
				{Name: "l2"},
			},
		},
	}

	view := m.View()
	assert.Contains(t, view, "Submitted 1/2")
	assert.Contains(t, view, "waiting")
}

func TestSnapshotObserverKeepsLatestSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshots := make(chan runner.Snapshot, 1)
	observer := snapshotObserver{ctx: ctx, snapshots: snapshots}

	observer.Observe(runner.Snapshot{Scenario: "old"})
	observer.Observe(runner.Snapshot{Scenario: "new"})

	assert.Equal(t, "new", (<-snapshots).Scenario)
}

func TestModelUsesSimpleStatusColors(t *testing.T) {
	m := model{
		cancel:       func() {},
		width:        100,
		height:       24,
		selected:     1,
		buildStarted: make(map[string]time.Time),
		snapshot: runner.Snapshot{
			Requests: []runner.Request{
				{Name: "landed", Status: "landed"},
				{Name: "selected", Status: "building"},
				{Name: "failed", Status: "error"},
				{Name: "waiting"},
			},
		},
	}

	view := m.View()
	assert.Contains(t, view, ansiGreen)
	assert.Contains(t, view, ansiRed)
	assert.Contains(t, view, ansiDimGray)
	assert.Contains(t, view, ansiBoldInverse)
}

func TestHistoryWindowScrollsFromLatest(t *testing.T) {
	start, end := historyWindow(10, 0, 3)
	assert.Equal(t, 7, start)
	assert.Equal(t, 10, end)

	start, end = historyWindow(10, 4, 3)
	assert.Equal(t, 3, start)
	assert.Equal(t, 6, end)
}

func TestStageCellsUseHistory(t *testing.T) {
	cells := stageCells(runner.Request{
		Status: "building",
		History: []runner.HistoryEvent{
			{Status: "validating"},
			{Status: "validated"},
			{Status: "batched"},
			{Status: "scored"},
			{Status: "speculating"},
			{Status: "building"},
		},
	}, "/")
	require.Len(t, cells, 6)
	assert.Equal(t, []string{"ok", "ok", "ok", "ok", "/", "."}, cells[:])
}

func TestTruncate(t *testing.T) {
	assert.Equal(t, "abcdef", truncate("abcdef", 6))
	assert.True(t, strings.HasSuffix(truncate("abcdefgh", 6), "..."))
}
