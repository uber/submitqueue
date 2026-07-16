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

// Package tui renders an interactive SQSim scenario run.
package tui

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/uber/submitqueue/sqsim/runner"
)

const (
	defaultWidth  = 100
	defaultHeight = 24

	ansiReset       = "\x1b[0m"
	ansiDimGray     = "\x1b[2;90m"
	ansiCyan        = "\x1b[36m"
	ansiGreen       = "\x1b[32m"
	ansiYellow      = "\x1b[33m"
	ansiRed         = "\x1b[31m"
	ansiBoldInverse = "\x1b[1;7m"
)

var spinnerFrames = []string{"|", "/", "-", "\\"}

// Execute runs a scenario and sends observations to the supplied observer.
type Execute func(context.Context, runner.Observer) (runner.Report, error)

type resultMsg struct {
	report runner.Report
	err    error
}

type tickMsg time.Time

type snapshotObserver struct {
	ctx       context.Context
	snapshots chan runner.Snapshot
}

func (o snapshotObserver) Observe(snapshot runner.Snapshot) {
	for {
		select {
		case o.snapshots <- snapshot:
			return
		case <-o.ctx.Done():
			return
		default:
		}
		select {
		case <-o.snapshots:
		case <-o.ctx.Done():
			return
		default:
		}
	}
}

type model struct {
	cancel        context.CancelFunc
	scenario      string
	snapshots     <-chan runner.Snapshot
	results       <-chan resultMsg
	snapshot      runner.Snapshot
	report        runner.Report
	runErr        error
	finished      bool
	stopping      bool
	selected      int
	rowOffset     int
	historyOffset int
	showDetails   bool
	width         int
	height        int
	frame         int
	now           time.Time
	buildStarted  map[string]time.Time
}

// Run executes a scenario in an alternate-screen terminal UI.
func Run(ctx context.Context, scenario string, input io.Reader, output io.Writer, execute Execute) (runner.Report, error) {
	if execute == nil {
		return runner.Report{}, fmt.Errorf("execute is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	snapshots := make(chan runner.Snapshot, 1)
	results := make(chan resultMsg, 1)
	observer := snapshotObserver{ctx: runCtx, snapshots: snapshots}
	go func() {
		report, err := execute(runCtx, observer)
		results <- resultMsg{report: report, err: err}
	}()

	initial := model{
		cancel:       cancel,
		scenario:     scenario,
		snapshots:    snapshots,
		results:      results,
		showDetails:  true,
		width:        defaultWidth,
		height:       defaultHeight,
		buildStarted: make(map[string]time.Time),
	}
	program := tea.NewProgram(initial, tea.WithInput(input), tea.WithOutput(output), tea.WithAltScreen())
	final, err := program.Run()
	if err != nil {
		cancel()
		return runner.Report{}, fmt.Errorf("run terminal UI: %w", err)
	}
	finalModel := final.(model)
	if finalModel.runErr != nil {
		return finalModel.report, finalModel.runErr
	}
	if !finalModel.finished {
		return finalModel.report, context.Canceled
	}
	return finalModel.report, nil
}

func (m model) Init() tea.Cmd {
	return tea.Batch(waitSnapshot(m.snapshots), waitResult(m.results), nextTick())
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		m.width = message.Width
		m.height = message.Height
		m.ensureVisible()
	case runner.Snapshot:
		m.snapshot = message
		if m.now.Before(message.Now) {
			m.now = message.Now
		}
		m.trackBuildStarts()
		m.ensureVisible()
		return m, waitSnapshot(m.snapshots)
	case resultMsg:
		m.report = message.report
		m.runErr = message.err
		m.finished = true
		if m.stopping {
			return m, tea.Quit
		}
	case tickMsg:
		m.frame++
		m.now = time.Time(message)
		return m, nextTick()
	case tea.KeyMsg:
		switch message.String() {
		case "ctrl+c", "q":
			if m.finished {
				return m, tea.Quit
			}
			m.stopping = true
			m.cancel()
		case "up", "k":
			if m.selected > 0 {
				m.selected--
				m.historyOffset = 0
				m.ensureVisible()
			}
		case "down", "j":
			if m.selected+1 < len(m.snapshot.Requests) {
				m.selected++
				m.historyOffset = 0
				m.ensureVisible()
			}
		case "pgup":
			m.historyOffset += max(1, m.detailLines()/2)
		case "pgdown":
			m.historyOffset = max(0, m.historyOffset-max(1, m.detailLines()/2))
		case "enter", "tab":
			m.showDetails = !m.showDetails
			m.ensureVisible()
		}
	}
	return m, nil
}

func (m model) View() string {
	var view strings.Builder
	fmt.Fprintf(&view, "SQSim  %s  %s\n", m.scenario, m.runState())
	fmt.Fprintf(&view, "Elapsed %s  Requests %d  Submitted %d/%d\n\n",
		m.elapsed(),
		len(m.snapshot.Requests),
		m.submittedCount(),
		len(m.snapshot.Requests),
	)
	if len(m.snapshot.Requests) == 0 {
		fmt.Fprintf(&view, "  %s Starting fresh local stack\n", spinnerFrames[m.frame%len(spinnerFrames)])
	} else {
		view.WriteString("  REQUEST          VAL  BAT  SCO  SPE  BLD  MRG  STATUS             BUILD\n")
		start, end := m.visibleRows()
		for i := start; i < end; i++ {
			view.WriteString(m.requestRow(i, m.snapshot.Requests[i]))
			view.WriteByte('\n')
		}
		if end < len(m.snapshot.Requests) {
			fmt.Fprintf(&view, "  ... %d more requests\n", len(m.snapshot.Requests)-end)
		}
	}
	if m.showDetails && len(m.snapshot.Requests) > 0 {
		view.WriteString("\n")
		view.WriteString(m.detailView(m.snapshot.Requests[m.selected]))
	}
	view.WriteString("\n")
	if m.finished {
		view.WriteString("j/k select  pgup/pgdn history  enter details  q quit")
	} else if m.stopping {
		view.WriteString("Stopping local stack...")
	} else {
		view.WriteString("j/k select  pgup/pgdn history  enter details  q stop")
	}
	return view.String()
}

func waitSnapshot(snapshots <-chan runner.Snapshot) tea.Cmd {
	return func() tea.Msg {
		return <-snapshots
	}
}

func waitResult(results <-chan resultMsg) tea.Cmd {
	return func() tea.Msg {
		return <-results
	}
}

func nextTick() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(now time.Time) tea.Msg {
		return tickMsg(now)
	})
}

func (m model) runState() string {
	if m.stopping && !m.finished {
		return "stopping"
	}
	if m.runErr != nil {
		return "infrastructure failure"
	}
	if !m.finished {
		return spinnerFrames[m.frame%len(spinnerFrames)] + " running"
	}
	if m.report.Passed {
		return "PASS"
	}
	return "FAIL"
}

func (m model) elapsed() time.Duration {
	if m.snapshot.StartedAt.IsZero() {
		return 0
	}
	return m.currentTime().Sub(m.snapshot.StartedAt).Round(100 * time.Millisecond)
}

func (m model) visibleRows() (int, int) {
	capacity := m.rowCapacity()
	start := min(m.rowOffset, max(0, len(m.snapshot.Requests)-capacity))
	return start, min(len(m.snapshot.Requests), start+capacity)
}

func (m model) rowCapacity() int {
	reserved := 8
	if m.showDetails {
		reserved += m.detailLines()
	}
	return max(1, m.height-reserved)
}

func (m model) detailLines() int {
	return max(5, min(10, m.height/3))
}

func (m *model) ensureVisible() {
	if len(m.snapshot.Requests) == 0 {
		m.selected = 0
		m.rowOffset = 0
		return
	}
	m.selected = min(m.selected, len(m.snapshot.Requests)-1)
	capacity := m.rowCapacity()
	if m.selected < m.rowOffset {
		m.rowOffset = m.selected
	}
	if m.selected >= m.rowOffset+capacity {
		m.rowOffset = m.selected - capacity + 1
	}
}

func (m model) requestRow(index int, request runner.Request) string {
	cursor := " "
	if index == m.selected {
		cursor = ">"
	}
	stages := stageCells(request, spinnerFrames[m.frame%len(spinnerFrames)])
	build := ""
	if request.Status == "building" {
		build = m.buildElapsed(request).String()
	}
	status := request.Status
	if status == "" {
		status = "waiting"
	}
	if index == m.selected {
		return style(fmt.Sprintf("%s %-16s  %3s  %3s  %3s  %3s  %3s  %3s  %-18s %7s",
			cursor,
			truncate(request.Name, 16),
			stages[0],
			stages[1],
			stages[2],
			stages[3],
			stages[4],
			stages[5],
			truncate(status, 18),
			build,
		), ansiBoldInverse)
	}
	for i := range stages {
		stages[i] = styleStage(stages[i])
	}
	return fmt.Sprintf("%s %-16s  %s  %s  %s  %s  %s  %s  %s %7s",
		cursor,
		truncate(request.Name, 16),
		stages[0],
		stages[1],
		stages[2],
		stages[3],
		stages[4],
		stages[5],
		styleStatus(request, fmt.Sprintf("%-18s", truncate(status, 18))),
		build,
	)
}

func (m model) submittedCount() int {
	submitted := 0
	for _, request := range m.snapshot.Requests {
		if request.SQID != "" {
			submitted++
		}
	}
	return submitted
}

func (m model) detailView(request runner.Request) string {
	var detail strings.Builder
	fmt.Fprintf(&detail, "Details  %s  %s  expected=%s\n", request.Name, request.SQID, request.Expected)
	if request.LastError != "" {
		fmt.Fprintf(&detail, "Error    %s\n", truncate(request.LastError, max(20, m.width-9)))
	}
	keys := make([]string, 0, len(request.Metadata))
	for key := range request.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+"="+request.Metadata[key])
		}
		fmt.Fprintf(&detail, "Metadata %s\n", truncate(strings.Join(parts, " "), max(20, m.width-9)))
	}
	detail.WriteString("History\n")
	start, end := historyWindow(len(request.History), m.historyOffset, m.detailLines()-3)
	for _, event := range request.History[start:end] {
		elapsed := time.Duration(0)
		if !m.snapshot.StartedAt.IsZero() && event.TimestampMs > 0 {
			elapsed = time.UnixMilli(event.TimestampMs).Sub(m.snapshot.StartedAt).Round(10 * time.Millisecond)
		}
		controller := event.Metadata["controller"]
		fmt.Fprintf(&detail, "  %8s  %-16s %s\n", elapsed, event.Status, controller)
	}
	if start > 0 {
		fmt.Fprintf(&detail, "  ... %d earlier events\n", start)
	}
	return detail.String()
}

func (m *model) trackBuildStarts() {
	for _, request := range m.snapshot.Requests {
		if request.Status != "building" {
			continue
		}
		if _, ok := m.buildStarted[request.Name]; !ok {
			m.buildStarted[request.Name] = m.snapshot.Now
		}
	}
}

func (m model) buildElapsed(request runner.Request) time.Duration {
	start := m.buildStarted[request.Name]
	for _, event := range request.History {
		if event.Status == "building" && event.TimestampMs > 0 {
			start = time.UnixMilli(event.TimestampMs)
			break
		}
	}
	if start.IsZero() {
		return 0
	}
	return m.currentTime().Sub(start).Round(100 * time.Millisecond)
}

func (m model) currentTime() time.Time {
	if m.finished || m.now.Before(m.snapshot.Now) {
		return m.snapshot.Now
	}
	return m.now
}

func historyWindow(length, offset, capacity int) (int, int) {
	if length == 0 || capacity <= 0 {
		return 0, 0
	}
	end := max(0, length-min(offset, length))
	start := max(0, end-capacity)
	return start, end
}

func truncate(value string, width int) string {
	if width <= 0 || len(value) <= width {
		return value
	}
	if width <= 3 {
		return value[:width]
	}
	return value[:width-3] + "..."
}

func styleStage(value string) string {
	switch value {
	case ".":
		return style(fmt.Sprintf("%3s", value), ansiDimGray)
	case "ok":
		return style(fmt.Sprintf("%3s", value), ansiGreen)
	case "x":
		return style(fmt.Sprintf("%3s", value), ansiRed)
	default:
		return style(fmt.Sprintf("%3s", value), ansiCyan)
	}
}

func styleStatus(request runner.Request, value string) string {
	switch {
	case request.Status == "":
		return style(value, ansiDimGray)
	case request.Status == "landed":
		return style(value, ansiGreen)
	case request.Status == "error" || request.Status == "cancelled":
		return style(value, ansiRed)
	case request.LastError != "":
		return style(value, ansiYellow)
	default:
		return style(value, ansiCyan)
	}
}

func style(value, code string) string {
	return code + value + ansiReset
}
