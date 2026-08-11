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
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	"golang.org/x/term"
)

const (
	// pollInterval bounds how often the watcher re-reads every request's history.
	pollInterval = 2 * time.Second

	// defaultLineWidth is the width assumed when the terminal will not say how
	// wide it is — piped output, or a terminal that answers no size at all.
	defaultLineWidth = 120

	// absent is what a cell shows before there is anything to put in it.
	absent = "—"

	// minNoteWidth keeps a wrapped error readable even when the columns before
	// it have eaten most of the line.
	minNoteWidth = 40

	// minStageWidth is the narrowest the stage column is allowed to wrap to. A
	// window narrow enough to force this is already unreadable; the floor keeps
	// the wrap from degenerating into one word per line.
	minStageWidth = 24
)

// terminalStatuses are the states a land request settles on. They are keyed off
// the gateway's own vocabulary so this view cannot quietly drift from it.
var terminalStatuses = map[string]bool{
	string(entity.RequestStatusLanded):    true,
	string(entity.RequestStatusError):     true,
	string(entity.RequestStatusCancelled): true,
}

// Cell is one entry in a row's changes column: what to show, and optionally
// where it points.
//
// The column is caller-supplied rather than derived, because what identifies a
// change depends on who is watching. A tool that just created pull requests
// knows their numbers and can link to them; a client watching a queue it did
// not create knows only the change URIs the gateway reports. Both render
// through the same cell.
type Cell struct {
	// Text is what the reader sees on a terminal.
	Text string
	// URL is where the text points. Empty when the caller has no address for
	// it, in which case Text is shown as-is in both modes.
	URL string
}

// Row is one land request and everything shown about it. A row exists from the
// first draw, before the request it will carry has been accepted, so the table
// never changes shape while a run is in progress.
type Row struct {
	// Cells are what the changes column shows for this request, in caller order.
	Cells []Cell

	// SQID is empty until the gateway accepts the request.
	SQID string
	// Submitted is when the gateway accepted it, and starts the elapsed clock.
	Submitted time.Time
	// Settled is when a terminal status was first observed, and stops it.
	Settled time.Time

	// Trail is the ordered set of statuses the gateway recorded for the request.
	Trail  []string
	Status string
	Note   string
	Done   bool
}

// NewRows allocates n empty rows, so the table has its final shape before
// anything has been accepted.
func NewRows(n int) []*Row {
	rows := make([]*Row, n)
	for i := range rows {
		rows[i] = &Row{}
	}
	return rows
}

// elapsed is how long the request has been with the queue: absent until it is
// accepted, running while it is in flight, and frozen once it settles.
func (rw *Row) elapsed() string {
	if rw.Submitted.IsZero() {
		return absent
	}
	end := time.Now()
	if !rw.Settled.IsZero() {
		end = rw.Settled
	}
	return fmt.Sprintf("%ds", int(end.Sub(rw.Submitted).Seconds()))
}

// stage is the path the request has taken, as the gateway recorded it. The
// waiting marker covers the gap between acceptance and the first recorded
// event, so an accepted request is never shown as though nothing happened.
func (rw *Row) stage() string {
	if len(rw.Trail) > 0 {
		return strings.Join(rw.Trail, " → ")
	}
	if rw.SQID != "" {
		return "…"
	}
	return absent
}

// Draw renders the table once and returns. It is what a one-shot listing wants;
// a caller following requests as they move uses a Tracker instead, which owns
// the rows and redraws them.
func Draw(rows []*Row, status string) {
	newRenderer().draw(rows, status)
}

// digest reduces a request's recorded history to the trail worth showing, the
// status it currently holds, and the error the latest event carried. A status
// recorded more than once in a row is one step in the trail, not several.
func digest(events []*pb.HistoryEvent) (trail []string, status, note string) {
	if len(events) == 0 {
		return nil, "", ""
	}
	for _, e := range events {
		if e == nil || e.Status == "" {
			continue
		}
		if len(trail) > 0 && trail[len(trail)-1] == e.Status {
			continue
		}
		trail = append(trail, e.Status)
	}
	if last := events[len(events)-1]; last != nil {
		status, note = last.Status, last.LastError
	}
	if status == "" && len(trail) > 0 {
		status = trail[len(trail)-1]
	}
	return trail, status, note
}

// outcome is the one-line verdict shown under the finished table.
func outcome(rows []*Row) string {
	landed := 0
	for _, rw := range rows {
		if rw.Status == string(entity.RequestStatusLanded) {
			landed++
		}
	}
	if landed == len(rows) {
		return fmt.Sprintf("all %d request(s) landed", len(rows))
	}
	return fmt.Sprintf("%d of %d request(s) did not land", len(rows)-landed, len(rows))
}

// summarize fails the run if anything did not land, so a scripted caller
// notices.
func summarize(rows []*Row) error {
	var failed []string
	for _, rw := range rows {
		if rw.Status != string(entity.RequestStatusLanded) {
			failed = append(failed, fmt.Sprintf("%s=%s", rw.SQID, rw.Status))
		}
	}
	if len(failed) > 0 {
		sort.Strings(failed)
		return fmt.Errorf("%d of %d request(s) did not land: %s", len(failed), len(rows), strings.Join(failed, ", "))
	}
	return nil
}

// renderer draws the status table, redrawing in place on a terminal and
// appending a fresh block otherwise, so piping the output to a file stays
// readable instead of filling with escape codes.
//
// Column widths only ever grow, so a value that turns out to be wider than the
// header does not make the table jitter as rows fill in.
//
// No line the renderer emits may exceed the terminal's width. A line that wraps
// occupies two physical rows, and the in-place redraw moves the cursor back by
// the number of lines it *emitted* — so one wrapped line desyncs every redraw
// after it. Everything wide is therefore wrapped deliberately, into lines the
// renderer counts itself.
type renderer struct {
	inPlace bool

	// width is the terminal's width, re-read before every draw. A watch runs for
	// minutes and a window can be resized inside them, so this is not a property
	// the process can sample once — see resize.
	width int

	// size reports the terminal's width and whether it could be read. Held as a
	// field so a test can drive a resize without a terminal.
	size func() (int, bool)

	wRequest int
	wChanges int
	wElapsed int
	wStage   int

	// lastLines is how many lines the previous draw actually emitted, which is
	// how far the cursor has to move back to overwrite them.
	lastLines int
	drawn     bool

	// lastBody is the signature of the previous table, so piped output can skip
	// a redundant reprint when a step moved but the table did not.
	lastBody string
}

func newRenderer() *renderer {
	width, sized := terminalSize()
	return &renderer{
		// Redrawing in place requires knowing the width to wrap to. A terminal
		// that will not report its size is therefore treated as a log: emitting
		// a guessed width into a narrower window is what produces a physically
		// wrapped line, and the redraw counts the lines it emitted rather than
		// the lines that appeared, so one of those desyncs every frame after it.
		inPlace:  sized,
		width:    width,
		size:     terminalSize,
		wRequest: len("REQUEST"),
		wChanges: len("CHANGES"),
		wElapsed: len("ELAPSED"),
		wStage:   len("STAGE"),
	}
}

// terminalSize is how wide the output is, in columns, and whether that came
// from the terminal rather than from the fallback.
//
// Asking the terminal is what lets a wide window show a long trail in full,
// rather than everyone being held to the narrowest window anyone might have.
// Anything that is not a sized terminal — a pipe, a file, a CI log — falls back
// to a fixed width, since there is no width to discover and a log wants a
// stable one anyway.
func terminalSize() (int, bool) {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		return defaultLineWidth, false
	}
	return w, true
}

// lineWidth is the width to render to. It tolerates a renderer built without
// one — a zero-value renderer in a test — rather than collapsing to nothing.
func (r *renderer) lineWidth() int {
	if r.width <= 0 {
		return defaultLineWidth
	}
	return r.width
}

// resize re-reads the terminal's width before a draw.
//
// The width is not a property of the process: a watch runs for minutes and the
// window can be dragged narrower at any point in them. Sampling once at startup
// means every frame after a resize is wrapped to a width the window no longer
// has, and the terminal wraps those lines itself — mid-word, ignoring the
// column alignment, and without telling the redraw, which then moves the cursor
// back by fewer lines than actually appeared.
//
// Only the width is re-read. Whether output is a terminal at all cannot change
// under a running process, and re-deciding it per frame would let a transient
// probe failure switch rendering modes mid-run.
func (r *renderer) resize() {
	if !r.inPlace || r.size == nil {
		return
	}
	if w, sized := r.size(); sized {
		r.width = w
	}
}

func (r *renderer) draw(rows []*Row, status string) {
	r.resize()
	body := r.body(rows)

	if !r.inPlace {
		sig := signature(rows)
		if sig == r.lastBody {
			// Nothing in the table moved; the step that prompted this draw is a
			// terminal affordance and has no place in a log.
			return
		}
		r.lastBody = sig
		fmt.Println(strings.Join(body, "\n"))
		fmt.Println()
		return
	}

	if r.drawn {
		fmt.Printf("\033[%dA", r.lastLines)
	}
	for _, line := range body {
		fmt.Printf("\033[K%s\n", line)
	}
	fmt.Printf("\033[K\n")
	fmt.Printf("\033[K  ▸ %s\n", truncate(status, r.lineWidth()-4))
	// Every draw emits the body, one blank line, and the status line; moving
	// back by exactly this many lines is what keeps the redraw from drifting.
	r.lastLines = len(body) + 2
	r.drawn = true
}

// body renders the header and one line per row.
func (r *renderer) body(rows []*Row) []string {
	r.fit(rows)

	lines := make([]string, 0, len(rows)+2)
	lines = append(lines,
		fmt.Sprintf("  %-*s  %-*s  %*s  %s",
			r.wRequest, "REQUEST", r.wChanges, "CHANGES", r.wElapsed, "ELAPSED", "STAGE"),
		fmt.Sprintf("  %s  %s  %s  %s",
			rule(r.wRequest), rule(r.wChanges), rule(r.wElapsed), rule(r.wStage)))

	for _, rw := range rows {
		lines = append(lines, r.rowLines(rw)...)
		lines = append(lines, r.noteLines(rw)...)
	}
	return lines
}

// fit grows the columns to hold what the rows now contain. Widths never shrink,
// so the table does not shift under a value that has already been printed — but
// on a terminal the stage column stays inside the line, since its rule would
// otherwise wrap on a long trail and take the redraw with it.
func (r *renderer) fit(rows []*Row) {
	for _, rw := range rows {
		r.wRequest = max(r.wRequest, utf8.RuneCountInString(rw.SQID))
		_, visible := r.changesCell(rw)
		r.wChanges = max(r.wChanges, visible)
		r.wStage = max(r.wStage, utf8.RuneCountInString(rw.stage()))
	}
	if r.inPlace {
		r.wStage = min(r.wStage, r.stageWidth())
	}
}

// prefixWidth is the space every row spends before the stage column.
func (r *renderer) prefixWidth() int {
	return 2 + r.wRequest + 2 + r.wChanges + 2 + r.wElapsed + 2
}

// rowLines renders one row: its columns, and the stage wrapped onto indented
// continuation lines when the trail does not fit the width.
//
// Wrapping rather than cutting is what keeps a long trail readable — the end of
// it is where the request actually is, so a cut there hides the interesting
// part. Continuations align under the stage column so the wrapped text reads as
// one field rather than as new rows.
func (r *renderer) rowLines(rw *Row) []string {
	sqid := rw.SQID
	if sqid == "" {
		sqid = absent
	}
	changes, visible := r.changesCell(rw)

	prefix := fmt.Sprintf("  %-*s  %s  %*s  ",
		r.wRequest, sqid, pad(changes, visible, r.wChanges), r.wElapsed, rw.elapsed())

	tail := rw.stage()
	if !r.inPlace {
		// A log has no width to respect and is easier to read and grep on one
		// line, so it takes the trail whole.
		return []string{prefix + tail}
	}

	indent := r.prefixWidth()
	segments := wrap(tail, r.stageWidth())
	if len(segments) == 0 {
		return []string{prefix + tail}
	}

	lines := make([]string, 0, len(segments))
	lines = append(lines, prefix+segments[0])
	for _, segment := range segments[1:] {
		lines = append(lines, strings.Repeat(" ", indent)+"  "+segment)
	}
	return lines
}

// stageWidth is the room a wrapped stage has. The two columns subtracted are
// the indent a continuation line carries, so every line of a wrapped stage
// fits the same budget as the first.
func (r *renderer) stageWidth() int {
	return max(minStageWidth, r.lineWidth()-r.prefixWidth()-2)
}

// noteLines renders a request's error under its row, wrapped and indented to
// the stage column. An error is the one thing in the table worth reading in
// full — truncating it to the width of a cell hides the part that says what
// went wrong — so it gets as many lines as it needs instead of an ellipsis.
func (r *renderer) noteLines(rw *Row) []string {
	if rw.Note == "" {
		return nil
	}

	indent := r.prefixWidth()
	// A piped run spends most of the line on URLs, so the wrap width is floored
	// rather than allowed to collapse to nothing.
	width := max(minNoteWidth, r.lineWidth()-indent-2)

	wrapped := wrap(rw.Note, width)
	lines := make([]string, 0, len(wrapped))
	for i, text := range wrapped {
		marker := "  "
		if i == 0 {
			marker = "↳ "
		}
		lines = append(lines, strings.Repeat(" ", indent)+marker+text)
	}
	return lines
}

// wrap breaks text into lines no wider than width, splitting on spaces and
// hard-splitting any single token too long to fit on a line of its own.
func wrap(s string, width int) []string {
	if width < 1 {
		return nil
	}

	var lines []string
	current := ""
	flush := func() {
		if current != "" {
			lines = append(lines, current)
			current = ""
		}
	}

	for _, word := range strings.Fields(s) {
		for utf8.RuneCountInString(word) > width {
			flush()
			runes := []rune(word)
			lines = append(lines, string(runes[:width]))
			word = string(runes[width:])
		}
		switch {
		case current == "":
			current = word
		case utf8.RuneCountInString(current)+1+utf8.RuneCountInString(word) <= width:
			current += " " + word
		default:
			flush()
			current = word
		}
	}
	flush()
	return lines
}

// changesCell renders a row's cells and reports the width they occupy on
// screen. The two differ on a terminal, where a hyperlink is mostly escape
// bytes that take up no space.
func (r *renderer) changesCell(rw *Row) (string, int) {
	if len(rw.Cells) == 0 {
		return absent, utf8.RuneCountInString(absent)
	}

	parts := make([]string, 0, len(rw.Cells))
	visible := 0
	for _, c := range rw.Cells {
		if c.URL == "" {
			// Nothing to point at, so the text is all there is in either mode.
			parts = append(parts, c.Text)
			visible += utf8.RuneCountInString(c.Text)
			continue
		}
		if r.inPlace {
			parts = append(parts, hyperlink(c.Text, c.URL))
			visible += utf8.RuneCountInString(c.Text)
			continue
		}
		// Piped output has nothing to click, so the address itself has to be
		// readable — and copyable out of a log.
		parts = append(parts, c.URL)
		visible += utf8.RuneCountInString(c.URL)
	}
	separator := ","
	if !r.inPlace {
		separator = " "
	}
	return strings.Join(parts, separator), visible + len(separator)*(len(parts)-1)
}

// hyperlink wraps text in an OSC 8 escape so terminals that understand it make
// the text clickable, and the rest simply show the text.
func hyperlink(text, url string) string {
	if url == "" {
		return text
	}
	return "\033]8;;" + url + "\033\\" + text + "\033]8;;\033\\"
}

// pad right-pads a cell to a column width using its on-screen width, which is
// not its length whenever it carries escape sequences.
func pad(s string, visible, width int) string {
	if visible >= width {
		return s
	}
	return s + strings.Repeat(" ", width-visible)
}

func rule(n int) string {
	return strings.Repeat("─", n)
}

// signature is what a piped run treats as the table having moved. The elapsed
// clock is left out on purpose: it advances every second, and a log that
// reprinted the table for that alone would say nothing while saying it often.
func signature(rows []*Row) string {
	var b strings.Builder
	for _, rw := range rows {
		fmt.Fprintf(&b, "%s|%s|%s|%s\n", rw.SQID, labelsOf(rw.Cells), rw.stage(), rw.Note)
	}
	return b.String()
}

// labelsOf is the cells' text, for identifying a row without its addresses.
func labelsOf(cells []Cell) string {
	parts := make([]string, 0, len(cells))
	for _, c := range cells {
		parts = append(parts, c.Text)
	}
	return strings.Join(parts, ",")
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if n < 1 {
		return ""
	}
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n-1]) + "…"
}
