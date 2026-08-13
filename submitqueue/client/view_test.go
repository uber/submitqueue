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

package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"google.golang.org/grpc"
)

func TestDigest(t *testing.T) {
	tests := []struct {
		name       string
		events     []*pb.HistoryEvent
		wantTrail  []string
		wantStatus string
		wantNote   string
	}{
		{
			name: "no events yet",
		},
		{
			name:       "one event",
			events:     []*pb.HistoryEvent{{Status: "accepted"}},
			wantTrail:  []string{"accepted"},
			wantStatus: "accepted",
		},
		{
			name: "trail keeps the order it was recorded in",
			events: []*pb.HistoryEvent{
				{Status: "accepted"}, {Status: "started"}, {Status: "batched"}, {Status: "landed"},
			},
			wantTrail:  []string{"accepted", "started", "batched", "landed"},
			wantStatus: "landed",
		},
		{
			name: "a status recorded twice in a row is one step",
			events: []*pb.HistoryEvent{
				{Status: "accepted"}, {Status: "started"}, {Status: "started"}, {Status: "batched"},
			},
			wantTrail:  []string{"accepted", "started", "batched"},
			wantStatus: "batched",
		},
		{
			name: "a status revisited later is a step again",
			events: []*pb.HistoryEvent{
				{Status: "speculating"}, {Status: "batched"}, {Status: "speculating"},
			},
			wantTrail:  []string{"speculating", "batched", "speculating"},
			wantStatus: "speculating",
		},
		{
			name: "the error on the latest event is the one shown",
			events: []*pb.HistoryEvent{
				{Status: "started", LastError: "transient"}, {Status: "error", LastError: "merge conflict"},
			},
			wantTrail:  []string{"started", "error"},
			wantStatus: "error",
			wantNote:   "merge conflict",
		},
		{
			name:       "events without a status do not become steps",
			events:     []*pb.HistoryEvent{{Status: ""}, {Status: "accepted"}, {Status: ""}},
			wantTrail:  []string{"accepted"},
			wantStatus: "accepted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trail, status, note := digest(tt.events)
			assert.Equal(t, tt.wantTrail, trail)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, tt.wantNote, note)
		})
	}
}

func TestRowElapsed(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name string
		row  Row
		want string
	}{
		{
			name: "absent before the gateway accepts it",
			row:  Row{},
			want: absent,
		},
		{
			name: "running while in flight",
			row:  Row{Submitted: now.Add(-5 * time.Second)},
			want: "5s",
		},
		{
			name: "frozen once settled, however long ago that was",
			row:  Row{Submitted: now.Add(-90 * time.Second), Settled: now.Add(-60 * time.Second)},
			want: "30s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.row.elapsed())
		})
	}
}

// TestRowElapsedStopsAtSettle pins the behavior the clock exists for: a settled
// row reads the same however much later it is drawn, while an unsettled one
// does not.
func TestRowElapsedStopsAtSettle(t *testing.T) {
	start := time.Now().Add(-time.Minute)
	settled := Row{Submitted: start, Settled: start.Add(10 * time.Second)}
	inFlight := Row{Submitted: start}

	first := settled.elapsed()
	time.Sleep(time.Millisecond)
	assert.Equal(t, first, settled.elapsed())
	assert.NotEqual(t, first, inFlight.elapsed())
}

func TestRowStage(t *testing.T) {
	tests := []struct {
		name string
		row  Row
		want string
	}{
		{
			name: "nothing to report before the request exists",
			row:  Row{},
			want: absent,
		},
		{
			name: "accepted but nothing recorded yet",
			row:  Row{SQID: "demo-queue/17"},
			want: "…",
		},
		{
			name: "the states it passed through",
			row:  Row{SQID: "demo-queue/17", Trail: []string{"accepted", "started", "landed"}},
			want: "accepted → started → landed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.row.stage())
		})
	}
}

func TestChangesCell(t *testing.T) {
	one := []Cell{{Text: "#41", URL: "https://github.com/o/r/pull/41"}}
	two := []Cell{
		{Text: "#41", URL: "https://github.com/o/r/pull/41"},
		{Text: "#421", URL: "https://github.com/o/r/pull/421"},
	}

	t.Run("a terminal gets short clickable labels", func(t *testing.T) {
		r := &renderer{inPlace: true}
		text, visible := r.changesCell(&Row{Cells: two})

		assert.Contains(t, text, "https://github.com/o/r/pull/41")
		assert.Contains(t, text, "\033]8;;")
		// "#41,#421" occupies eight columns however many escape bytes carry it.
		assert.Equal(t, len("#41,#421"), visible)
		assert.Greater(t, len(text), visible, "the escapes should not be counted as width")
	})

	t.Run("a pipe gets the addresses themselves", func(t *testing.T) {
		r := &renderer{inPlace: false}
		text, visible := r.changesCell(&Row{Cells: one})

		assert.Equal(t, "https://github.com/o/r/pull/41", text)
		assert.Equal(t, len(text), visible)
		assert.NotContains(t, text, "\033")
	})

	t.Run("no pull requests yet", func(t *testing.T) {
		r := &renderer{inPlace: true}
		text, visible := r.changesCell(&Row{})

		assert.Equal(t, absent, text)
		assert.Equal(t, 1, visible)
	})
}

// TestPadCountsVisibleWidth guards the alignment trap: a hyperlinked cell is
// mostly escape bytes, so padding by length would push the columns apart.
func TestPadCountsVisibleWidth(t *testing.T) {
	linked := hyperlink("#41", "https://github.com/o/r/pull/41")

	assert.Equal(t, linked+"       ", pad(linked, len("#41"), 10))
	assert.Equal(t, "#41       ", pad("#41", 3, 10))
	assert.Equal(t, "#41", pad("#41", 3, 3), "a cell at the column width is not padded")
	assert.Equal(t, "#41", pad("#41", 3, 2), "a cell wider than the column is left alone")
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{name: "short enough to keep", in: "accepted", n: 20, want: "accepted"},
		{name: "exactly the limit", in: "accepted", n: 8, want: "accepted"},
		{name: "cut with a marker", in: "accepted → started", n: 10, want: "accepted …"},
		{name: "newlines flattened", in: "line\nbreak", n: 20, want: "line break"},
		{name: "no room at all", in: "accepted", n: 0, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, truncate(tt.in, tt.n))
		})
	}
}

// TestTruncateSplitsOnRunes checks that a cut lands between characters. The
// trail is joined with a multi-byte arrow, so cutting by bytes would leave
// mojibake in the middle of the table.
func TestTruncateSplitsOnRunes(t *testing.T) {
	got := truncate("accepted → started → batched", 12)
	assert.True(t, utf8ValidAndCounted(got, 12), "got %q", got)
}

func utf8ValidAndCounted(s string, n int) bool {
	return len([]rune(s)) <= n && strings.ToValidUTF8(s, "?") == s
}

// TestDrawLineAccounting is the invariant the in-place redraw rests on: the
// cursor moves back exactly as far as the previous draw reached. Off by one and
// every subsequent draw leaves a stale row on screen.
func TestDrawLineAccounting(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	rows := []*Row{
		{SQID: "demo-queue/17", Cells: []Cell{{Text: "#41", URL: "https://github.com/o/r/pull/41"}},
			Submitted: time.Now(), Trail: []string{"accepted", "started"}},
		{SQID: "demo-queue/18", Cells: []Cell{{Text: "#42", URL: "https://github.com/o/r/pull/42"}},
			Submitted: time.Now(), Trail: []string{"accepted"}},
		{},
	}

	first := captureStdout(t, func() { r.draw(rows, "watching") })
	emitted := strings.Count(first, "\n")
	assert.Equal(t, emitted, r.lastLines, "the first draw must record how far it reached")
	assert.True(t, strings.HasPrefix(first, "\033[K"), "the first draw has nothing to move back over")

	second := captureStdout(t, func() { r.draw(rows, "still watching") })
	assert.True(t, strings.HasPrefix(second, fmt.Sprintf("\033[%dA", emitted)),
		"the redraw must move back over exactly the %d lines it wrote, got %q", emitted, head(second, 12))
	assert.Equal(t, strings.Count(second, "\n"), r.lastLines)
}

// TestDrawStaysWithinLineWidth checks the other half of the redraw contract: a
// line that wraps occupies two physical rows and desyncs the cursor for good.
func TestDrawStaysWithinLineWidth(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	rows := []*Row{{
		SQID:      "demo-queue/17",
		Submitted: time.Now(),
		Trail:     strings.Split(strings.Repeat("speculating ", 30), " "),
		Note:      strings.Repeat("a very long error message ", 10),
	}}

	out := captureStdout(t, func() { r.draw(rows, strings.Repeat("status ", 40)) })
	for _, line := range strings.Split(out, "\n") {
		line = strings.ReplaceAll(line, "\033[K", "")
		assert.LessOrEqual(t, len([]rune(line)), r.lineWidth(), "line too wide: %q", line)
	}
}

// TestDrawFollowsAResize is the regression for a table that wraps correctly at
// startup and then stops. The width is not fixed for the life of the process: a
// watch runs for minutes, and a window dragged narrower inside them leaves every
// later frame wrapped to a width the window no longer has. The terminal then
// wraps those lines itself — mid-word, ignoring the column alignment — and the
// redraw, which counts the lines it emitted rather than the lines that appeared,
// drifts a little further with every frame.
func TestDrawFollowsAResize(t *testing.T) {
	width := 200
	r := newRenderer()
	r.inPlace = true
	r.width = width
	r.size = func() (int, bool) { return width, true }

	rows := []*Row{{SQID: "demo-queue/72", Submitted: time.Now(), Trail: longTrail}}

	wide := captureStdout(t, func() { r.draw(rows, "watching") })
	for _, line := range strings.Split(wide, "\n") {
		assert.LessOrEqual(t, len([]rune(visible(strings.ReplaceAll(line, "\033[K", "")))), width)
	}

	// The window is dragged in. Nothing tells the process; it has to look.
	width = 94
	narrow := captureStdout(t, func() { r.draw(rows, "watching") })
	require.NotEmpty(t, narrow)
	for _, line := range strings.Split(narrow, "\n") {
		clean := visible(strings.ReplaceAll(line, "\033[K", ""))
		assert.LessOrEqual(t, len([]rune(clean)), width,
			"a line wider than the window is wrapped by the terminal, which desyncs the redraw: %q", clean)
	}
}

// TestDrawKeepsTheLastWidthWhenTheTerminalStopsAnswering guards the other
// direction: a probe that fails once must not snap the table to the fallback
// width, which on a narrow window is wider than the window itself.
func TestDrawKeepsTheLastWidthWhenTheTerminalStopsAnswering(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	r.width = 94
	r.size = func() (int, bool) { return defaultLineWidth, false }

	r.resize()
	assert.Equal(t, 94, r.width, "an unanswered probe leaves the last known width alone")
}

// TestNewRendererNeedsASizeToRedrawInPlace pins the two probes together. Drawing
// in place means wrapping, and wrapping to a guessed width is what puts a line
// past the edge of a narrower window. A terminal that will not report its size
// is therefore rendered as a log, which needs no width at all.
func TestNewRendererNeedsASizeToRedrawInPlace(t *testing.T) {
	// Test stdout is not a sized terminal, which is exactly the case at issue.
	r := newRenderer()
	width, sized := terminalSize()
	require.False(t, sized, "test stdout is not expected to be a sized terminal")
	assert.Equal(t, defaultLineWidth, width)
	assert.False(t, r.inPlace, "without a known width the renderer must not wrap and redraw")
}

func TestDrawPipedSkipsClockOnlyRedraws(t *testing.T) {
	r := newRenderer()
	r.inPlace = false
	rows := []*Row{{SQID: "demo-queue/17", Submitted: time.Now().Add(-5 * time.Second), Trail: []string{"accepted"}}}

	first := captureStdout(t, func() { r.draw(rows, "watching") })
	require.NotEmpty(t, first)
	assert.NotContains(t, first, "\033", "a redirected run must not emit escape codes")

	rows[0].Submitted = time.Now().Add(-30 * time.Second)
	assert.Empty(t, captureStdout(t, func() { r.draw(rows, "watching") }),
		"only the clock moved, so there is nothing new to say")

	rows[0].Trail = append(rows[0].Trail, "started")
	assert.NotEmpty(t, captureStdout(t, func() { r.draw(rows, "watching") }),
		"the request moved, so the table should be reprinted")
}

// TestFitGrowsColumnsOnly checks that a value wider than its header widens the
// column and that a later, narrower table does not pull it back in — a column
// that shrank would make the table jitter as rows fill in.
func TestFitGrowsColumnsOnly(t *testing.T) {
	r := newRenderer()
	wide := []*Row{{SQID: "some-very-long-queue-name/1234"}}
	r.fit(wide)
	grown := r.wRequest
	assert.Equal(t, len("some-very-long-queue-name/1234"), grown)

	r.fit([]*Row{{}})
	assert.Equal(t, grown, r.wRequest)
}

func TestNewRows(t *testing.T) {
	assert.Len(t, NewRows(3), 3, "independent changes are one request each")
	assert.Len(t, NewRows(1), 1, "a stack is a single request")
}

func TestOutcome(t *testing.T) {
	landed := &Row{Status: "landed"}
	failed := &Row{Status: "error"}

	assert.Equal(t, "all 2 request(s) landed", outcome([]*Row{landed, landed}))
	assert.Equal(t, "1 of 2 request(s) did not land", outcome([]*Row{landed, failed}))
}

func TestSummarize(t *testing.T) {
	assert.NoError(t, summarize([]*Row{{SQID: "q/1", Status: "landed"}}))
	assert.Error(t, summarize([]*Row{{SQID: "q/1", Status: "landed"}, {SQID: "q/2", Status: "error"}}))
}

// TestRowLineAlignment is the column contract: on every row the stage begins at
// exactly the same screen column, whatever the cells before it contain. A
// hyperlinked row is the interesting one, since its changes cell is mostly
// escape bytes that occupy no width — measuring those as if they did would both
// shove the column sideways and eat the stage's room to render.
// fakeGateway answers history lookups from a table the test controls. Only the
// one method is reachable; the embedded interface satisfies the rest.
type fakeGateway struct {
	pb.SubmitQueueGatewayClient
	mu     sync.Mutex
	events map[string][]*pb.HistoryEvent
}

func (f *fakeGateway) set(sqid string, statuses ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events == nil {
		f.events = map[string][]*pb.HistoryEvent{}
	}
	events := make([]*pb.HistoryEvent, 0, len(statuses))
	for _, s := range statuses {
		events = append(events, &pb.HistoryEvent{Status: s})
	}
	f.events[sqid] = events
}

func (f *fakeGateway) GetRequestHistoryByID(
	_ context.Context, in *pb.GetRequestHistoryByIDRequest, _ ...grpc.CallOption,
) (*pb.GetRequestHistoryByIDResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &pb.GetRequestHistoryByIDResponse{Events: f.events[in.Sqid]}, nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestTrackerSettlesOnlyWhenSealedAndTerminal covers the condition that ends a
// run. Polling starts while pull requests are still being created, so "nothing
// outstanding" is true before creation has begun — sealing is what separates
// that from actually being finished.
func TestTrackerSettlesOnlyWhenSealedAndTerminal(t *testing.T) {
	tr := NewTracker(NewRows(2))
	tr.r.inPlace = false
	gw := &fakeGateway{}
	ctx := context.Background()

	captureStdout(t, func() {
		tr.Update(func() { tr.rows[0].SQID = "demo-queue/1" })
		tr.Seal()
	})
	assert.False(t, isClosed(tr.settled), "a row that was never enqueued is not settled")

	captureStdout(t, func() { tr.Update(func() { tr.rows[1].SQID = "demo-queue/2" }) })
	gw.set("demo-queue/1", "accepted", "started")
	gw.set("demo-queue/2", "accepted")
	captureStdout(t, func() { tr.refresh(ctx, gw, "demo-queue") })

	assert.Equal(t, []string{"accepted", "started"}, tr.rows[0].Trail)
	assert.False(t, isClosed(tr.settled), "requests still in flight")

	gw.set("demo-queue/1", "accepted", "started", "landed")
	gw.set("demo-queue/2", "accepted", "error")
	captureStdout(t, func() { tr.refresh(ctx, gw, "demo-queue") })

	assert.True(t, isClosed(tr.settled), "every request reached a terminal status")
	assert.True(t, tr.rows[0].Done)
	assert.False(t, tr.rows[0].Settled.IsZero(), "settling stops the clock")
}

// TestTrackerSealBeforeEnqueueDoesNotSettle guards the ordering hazard the seal
// exists for: polling that ran before anything was enqueued must not conclude
// the run just because it found nothing outstanding.
func TestTrackerSealBeforeEnqueueDoesNotSettle(t *testing.T) {
	tr := NewTracker(NewRows(1))
	tr.r.inPlace = false

	captureStdout(t, func() { tr.refresh(context.Background(), &fakeGateway{}, "demo-queue") })
	assert.False(t, isClosed(tr.settled), "nothing has been enqueued yet")
}

// TestTrackerPollsWhileCreating is the behavior the tracker exists for: a run
// that only polled after every pull request was created would show an empty
// trail for the whole creation phase. Here a row enqueued first picks up its
// trail while a later row has not been enqueued at all.
func TestTrackerPollsWhileCreating(t *testing.T) {
	tr := NewTracker(NewRows(3))
	tr.r.inPlace = false
	gw := &fakeGateway{}
	gw.set("demo-queue/1", "accepted", "started", "batched")

	captureStdout(t, func() {
		tr.Update(func() { tr.rows[0].SQID = "demo-queue/1" })
		tr.refresh(context.Background(), gw, "demo-queue")
	})

	assert.Equal(t, "accepted → started → batched", tr.rows[0].stage())
	assert.Equal(t, absent, tr.rows[2].stage(), "a row not yet enqueued has nothing to show")
}

// TestTrackerConcurrentPollAndUpdate exercises the two writers against each
// other so the race detector has something to find. Creation fills in rows from
// one goroutine while polling reads and redraws from another.
func TestTrackerConcurrentPollAndUpdate(t *testing.T) {
	tr := NewTracker(NewRows(8))
	tr.r.inPlace = false
	gw := &fakeGateway{}
	ctx := context.Background()

	captureStdout(t, func() {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := range tr.rows {
				sqid := fmt.Sprintf("demo-queue/%d", i)
				gw.set(sqid, "accepted", "landed")
				i := i
				tr.Update(func() {
					tr.rows[i].Cells = append(tr.rows[i].Cells, Cell{Text: fmt.Sprintf("#%d", 100+i), URL: "https://example.test/pull/1"})
					tr.rows[i].SQID, tr.rows[i].Submitted = sqid, time.Now()
				})
			}
			tr.Seal()
		}()

		go func() {
			defer wg.Done()
			for range 20 {
				tr.refresh(ctx, gw, "demo-queue")
			}
		}()

		wg.Wait()
		tr.refresh(ctx, gw, "demo-queue")
	})

	assert.True(t, isClosed(tr.settled))
	require.NoError(t, tr.Conclude())
}

func TestWrap(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		width int
		want  []string
	}{
		{name: "nothing to wrap", in: "short", width: 20, want: []string{"short"}},
		{
			name:  "breaks on spaces",
			in:    "queue name must not be empty",
			width: 12,
			want:  []string{"queue name", "must not be", "empty"},
		},
		{
			name:  "a token longer than the line is split",
			in:    "aaaaaaaaaa bb",
			width: 4,
			want:  []string{"aaaa", "aaaa", "aa", "bb"},
		},
		{name: "newlines are just whitespace", in: "one\ntwo", width: 20, want: []string{"one two"}},
		{name: "no width", in: "anything", width: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrap(tt.in, tt.width)
			assert.Equal(t, tt.want, got)
			for _, line := range got {
				assert.LessOrEqual(t, len([]rune(line)), tt.width)
			}
		})
	}
}

// TestNoteLinesRenderErrorInFull is the point of wrapping rather than
// truncating: the interesting part of a pipeline error is usually at the end,
// so an ellipsis in the stage column hides exactly what the reader needs.
func TestNoteLinesRenderErrorInFull(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	failed := &Row{
		SQID:      "demo-queue/1",
		Cells:     []Cell{{Text: "#75", URL: "https://github.com/behinddwalls/sq-demo/pull/75"}},
		Submitted: time.Now(),
		Trail:     []string{"accepted", "started", "validated", "batched", "error"},
		Note: `speculator failed for queue demo-queue: score dependency "demo-queue/batch/1": ` +
			`failed to resolve storage for queue "": queue name must not be empty`,
	}
	r.fit([]*Row{failed})

	lines := r.noteLines(failed)
	require.NotEmpty(t, lines)

	var text strings.Builder
	for i, line := range lines {
		assert.LessOrEqual(t, len([]rune(line)), r.lineWidth(), "a wrapped note still has to fit the line")
		trimmed := strings.TrimLeft(line, " ")
		if i == 0 {
			assert.True(t, strings.HasPrefix(trimmed, "↳ "), "the first line is marked")
		}
		text.WriteString(strings.TrimPrefix(strings.TrimPrefix(trimmed, "↳ "), "  "))
		text.WriteString(" ")
	}

	assert.Contains(t, text.String(), "queue name must not be empty",
		"the tail of the error is what says what went wrong; it must survive")

	// The row itself keeps only the trail, so the columns stay aligned.
	assert.NotContains(t, strings.Join(r.rowLines(failed), "\n"), "speculator failed")
	assert.NotContains(t, strings.Join(r.rowLines(failed), "\n"), "…")
}

// TestNoteLinesIndentToStageColumn keeps a wrapped error visually attached to
// its row rather than looking like a new column.
func TestNoteLinesIndentToStageColumn(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	rows := []*Row{{SQID: "demo-queue/1", Submitted: time.Now(), Trail: []string{"error"}, Note: "boom"}}
	r.fit(rows)

	lines := r.noteLines(rows[0])
	require.Len(t, lines, 1)
	assert.Equal(t, strings.Repeat(" ", r.prefixWidth())+"↳ boom", lines[0])
}

func TestRowLineAlignment(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	rows := []*Row{
		{
			SQID:      "demo-queue/17",
			Cells:     []Cell{{Text: "#41", URL: "https://github.com/behinddwalls/sq-demo/pull/41"}},
			Submitted: time.Now(),
			Trail:     []string{"accepted", "started", "batched", "speculating", "landed"},
		},
		{SQID: "demo-queue/1234", Submitted: time.Now(), Trail: []string{"accepted"}},
		{},
	}
	r.fit(rows)

	for _, rw := range rows {
		shown := []rune(visible(r.rowLines(rw)[0]))
		require.GreaterOrEqual(t, len(shown), r.prefixWidth())
		assert.Equal(t, rw.stage(), string(shown[r.prefixWidth():]),
			"the stage should start at column %d and be rendered whole", r.prefixWidth())
	}
}

// visible strips OSC 8 hyperlink sequences, leaving what the terminal draws.
func visible(s string) string {
	for {
		start := strings.Index(s, "\033]8;;")
		if start < 0 {
			return s
		}
		end := strings.Index(s[start:], "\033\\")
		if end < 0 {
			return s
		}
		s = s[:start] + s[start+end+len("\033\\"):]
	}
}

// captureStdout collects what fn writes to stdout. The renderer writes there
// directly, which is the thing under test. The pipe is drained as fn runs, so a
// test that draws more than the pipe buffer holds does not deadlock.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	rd, wr, err := os.Pipe()
	require.NoError(t, err)

	collected := make(chan string, 1)
	go func() {
		out, readErr := io.ReadAll(rd)
		if readErr != nil {
			collected <- ""
			return
		}
		collected <- string(out)
	}()

	original := os.Stdout
	os.Stdout = wr
	defer func() { os.Stdout = original }()

	fn()
	require.NoError(t, wr.Close())
	return <-collected
}

func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// longTrail is the shape that prompted wrapping: every status the pipeline now
// publishes, which no longer fits a default-width line.
var longTrail = []string{
	"accepted", "started", "validating", "validated", "batched",
	"speculating", "speculated", "building", "built", "landing", "landed",
}

func TestRowLinesWrapsRatherThanTruncates(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	rw := &Row{SQID: "demo-queue/1", Submitted: time.Now(), Trail: longTrail}
	r.fit([]*Row{rw})

	lines := r.rowLines(rw)
	require.Greater(t, len(lines), 1, "a trail this long has to wrap")

	joined := strings.Join(lines, " ")
	assert.NotContains(t, joined, "…", "nothing is cut, so there is no ellipsis")
	for _, status := range longTrail {
		assert.Contains(t, joined, status, "every status survives the wrap")
	}
	assert.Contains(t, lines[len(lines)-1], "landed",
		"the end of the trail is where the request is; it must be the part that shows")
}

func TestRowLinesContinuationsAlignUnderTheStage(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	rw := &Row{SQID: "demo-queue/1", Submitted: time.Now(), Trail: longTrail}
	r.fit([]*Row{rw})

	lines := r.rowLines(rw)
	require.Greater(t, len(lines), 1)

	for _, line := range lines[1:] {
		leading := len(line) - len(strings.TrimLeft(line, " "))
		assert.GreaterOrEqual(t, leading, r.prefixWidth(),
			"a continuation sits under the stage column, not under the request id")
	}
}

func TestRowLinesFitTheWidth(t *testing.T) {
	// The redraw moves the cursor back by the number of lines it emitted, so a
	// line wide enough to wrap physically would desync every redraw after it.
	widths := []int{80, 100, 120, 200}
	for _, width := range widths {
		t.Run(fmt.Sprintf("width %d", width), func(t *testing.T) {
			r := newRenderer()
			r.inPlace = true
			r.width = width
			rw := &Row{SQID: "demo-queue/1", Submitted: time.Now(), Trail: longTrail}
			r.fit([]*Row{rw})

			for _, line := range r.rowLines(rw) {
				assert.LessOrEqual(t, len([]rune(visible(line))), width, "line too wide: %q", line)
			}
		})
	}
}

func TestRowLinesUseTheWholeWidthBeforeWrapping(t *testing.T) {
	// The point of asking the terminal how wide it is: a window with room for
	// the whole trail should show it on one line.
	r := newRenderer()
	r.inPlace = true
	r.width = 240
	rw := &Row{SQID: "demo-queue/1", Submitted: time.Now(), Trail: longTrail}
	r.fit([]*Row{rw})

	lines := r.rowLines(rw)
	assert.Len(t, lines, 1, "a wide terminal needs no wrapping")
	assert.Contains(t, lines[0], "landed")
}

func TestRowLinesPipedStayOnOneLine(t *testing.T) {
	// A log has no width to respect and is easier to read and grep unwrapped.
	r := newRenderer()
	r.inPlace = false
	rw := &Row{SQID: "demo-queue/1", Submitted: time.Now(), Trail: longTrail}
	r.fit([]*Row{rw})

	lines := r.rowLines(rw)
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], "landed")
	assert.NotContains(t, lines[0], "…")
}

func TestStageWidthHasAFloor(t *testing.T) {
	r := newRenderer()
	r.inPlace = true
	r.width = 20
	r.wRequest, r.wChanges, r.wElapsed = 40, 40, 40

	assert.Equal(t, minStageWidth, r.stageWidth(),
		"a window too narrow to hold the columns still wraps to something readable")
}

func TestLineWidthFallsBackWhenUnset(t *testing.T) {
	// Tests build renderers directly; a zero width must not collapse the table.
	r := &renderer{inPlace: true}
	assert.Equal(t, defaultLineWidth, r.lineWidth())
}
