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

// Command pr populates a scratch repository with pull requests, enqueues them,
// and watches them move through the pipeline — so the demo stack can be
// exercised repeatedly without opening pull requests by hand.
//
// Nothing is awaited until the end, which is the point. Each pull request is
// enqueued the moment it is created, so the queue is already working on the
// first while the last is still being opened. A queue that only ever holds one
// request in flight never batches, never analyzes a conflict against another
// batch, and never speculates; those behaviors only appear when requests
// overlap. The table watches all of them at once.
//
// Two shapes of change, because the pipeline treats them differently:
//
//   - independent (default): each pull request targets the base branch and is
//     enqueued as its own request, immediately after it is created. This is
//     what puts requests in flight against each other.
//   - stacked (-stacked): each pull request is based on the one before it, and
//     all of them go in as a single request once the chain exists — the
//     atomic-stack path, where the whole set reaches the target in one push.
//
// The table is drawn before the first pull request exists and refreshed for the
// whole run, so there is never a stretch with nothing to look at. Each row is
// one land request and shows the states it has passed through, read from the
// gateway's history API rather than sampled — polling only the current status
// would miss any transition that happens between two ticks, which for a fast
// queue is most of them.
//
// Everything goes through GitHub's REST API rather than a local clone, so the
// tool needs no checkout and no git binary — only GITHUB_TOKEN, the same
// credential the stack itself uses.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	githubchange "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/submitqueue/entity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// pollInterval bounds how often the watcher re-reads every request's history.
	pollInterval = 2 * time.Second

	// maxLineWidth caps a redrawn line. A line that wraps occupies two physical
	// rows, which permanently desyncs the cursor arithmetic the in-place redraw
	// depends on; capping is cheaper than asking the terminal how wide it is.
	maxLineWidth = 120

	// absent is what a cell shows before there is anything to put in it.
	absent = "—"

	// minNoteWidth keeps a wrapped error readable even when the columns before
	// it have eaten most of the line.
	minNoteWidth = 40
)

// terminalStatuses are the states a land request settles on. They are keyed off
// the gateway's own vocabulary so this tool cannot quietly drift from it.
var terminalStatuses = map[string]bool{
	string(entity.RequestStatusLanded):    true,
	string(entity.RequestStatusError):     true,
	string(entity.RequestStatusCancelled): true,
}

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// config is everything the run needs, resolved from flags and the environment.
type config struct {
	repo     string
	base     string
	count    int
	stacked  bool
	prefix   string
	land     bool
	watch    bool
	gateway  string
	queue    string
	strategy string
	token    string
	apiRoot  string
	host     string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.repo, "repo", "behinddwalls/sq-demo", "scratch repository as owner/name")
	flag.StringVar(&c.base, "base", "main", "branch the changes target")
	flag.IntVar(&c.count, "count", 3, "how many pull requests to create")
	flag.BoolVar(&c.stacked, "stacked", false, "chain the pull requests and enqueue them as one stack")
	flag.StringVar(&c.prefix, "prefix", "demo", "branch name prefix")
	flag.BoolVar(&c.land, "land", true, "enqueue each pull request as it is created")
	flag.BoolVar(&c.watch, "watch", true, "watch the requests until they all settle")
	flag.StringVar(&c.gateway, "gateway", "localhost:8081", "gateway address")
	flag.StringVar(&c.queue, "queue", "demo-queue", "queue to land on")
	flag.StringVar(&c.strategy, "strategy", "SQUASH_REBASE", "merge strategy")
	flag.Parse()

	c.token = os.Getenv("GITHUB_TOKEN")
	c.apiRoot = "https://api.github.com"
	c.host = "github.com"
	return c
}

func run(ctx context.Context, cfg config) error {
	if cfg.token == "" {
		return fmt.Errorf("GITHUB_TOKEN is not set; it is the same credential the stack uses")
	}
	if cfg.count < 1 {
		return fmt.Errorf("-count must be at least 1")
	}
	owner, repo, ok := strings.Cut(cfg.repo, "/")
	if !ok || owner == "" || repo == "" {
		return fmt.Errorf("-repo %q must be owner/name", cfg.repo)
	}
	strategy, err := parseStrategy(cfg.strategy)
	if err != nil {
		return err
	}

	gh := &githubClient{root: cfg.apiRoot, token: cfg.token, owner: owner, repo: repo}
	baseSHA, err := gh.branchSHA(ctx, cfg.base)
	if err != nil {
		return fmt.Errorf("read %s: %w", cfg.base, err)
	}

	var client pb.SubmitQueueGatewayClient
	if cfg.land {
		conn, err := grpc.NewClient(cfg.gateway, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to gateway %s: %w", cfg.gateway, err)
		}
		defer conn.Close()
		client = pb.NewSubmitQueueGatewayClient(conn)
	}

	// A run tag keeps repeated invocations from colliding on branch names, and
	// makes it obvious in the repository which changes came from one run.
	tag := time.Now().Format("0102-150405")
	fmt.Printf("Creating %d pull request(s) in %s — %s\n\n", cfg.count, cfg.repo, shape(cfg))

	// Every row is known before anything is created: one per pull request, or a
	// single one for a stack, since the whole chain lands as one request. The
	// table is therefore complete from the first draw and only ever fills in.
	t := newTracker(newRows(cfg))
	t.note("starting")

	// Statuses are read on their own clock, concurrently with creation. A run
	// that only started polling once every pull request existed would show an
	// empty trail for the whole creation phase — which for a large -count is
	// most of the run, and is exactly the stretch worth watching, since the
	// early requests are already moving through the queue by then.
	if cfg.land {
		polling, stop := context.WithCancel(ctx)
		defer stop()
		go t.poll(polling, client, cfg.queue)
	}

	created, err := createAndEnqueue(ctx, gh, client, cfg, strategy, tag, baseSHA, t)
	if err != nil {
		return err
	}
	t.seal()

	if !cfg.land {
		t.note("created %d pull request(s), not enqueued", len(created))
		fmt.Printf("\nEnqueue them with:\n  make land PRS=\"%s\"\n", strings.Join(urlsOf(created), " "))
		return nil
	}
	if !cfg.watch {
		t.note("enqueued, not watching")
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.settled:
	}
	return t.conclude()
}

func shape(cfg config) string {
	if cfg.stacked {
		return "stacked, enqueued as one request once the chain exists"
	}
	return "independent, each enqueued as soon as it is created"
}

// change is one pull request this run created.
type change struct {
	number int
	url    string
	branch string
	// uri is the SubmitQueue change URI pinning the pull request to its head.
	uri string
}

func urlsOf(cs []change) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.url)
	}
	return out
}

// row is one land request and everything shown about it. A row exists from the
// first draw, before the pull request it will carry has been opened, so the
// table never changes shape while the run is in progress.
type row struct {
	// changes are the pull requests the request carries, in caller order. A
	// stacked run puts every change on one row.
	changes []change

	// sqid is empty until the gateway accepts the request.
	sqid string
	// submitted is when the gateway accepted it, and starts the elapsed clock.
	submitted time.Time
	// settled is when a terminal status was first observed, and stops it.
	settled time.Time

	// trail is the ordered set of statuses the gateway recorded for the request.
	trail  []string
	status string
	note   string
	done   bool
}

// newRows allocates the rows the run will fill: one per pull request, or a
// single row for a stack, which lands as one request.
func newRows(cfg config) []*row {
	n := cfg.count
	if cfg.stacked {
		n = 1
	}
	rows := make([]*row, n)
	for i := range rows {
		rows[i] = &row{}
	}
	return rows
}

// elapsed is how long the request has been with the queue: absent until it is
// accepted, running while it is in flight, and frozen once it settles.
func (rw *row) elapsed() string {
	if rw.submitted.IsZero() {
		return absent
	}
	end := time.Now()
	if !rw.settled.IsZero() {
		end = rw.settled
	}
	return fmt.Sprintf("%ds", int(end.Sub(rw.submitted).Seconds()))
}

// stage is the path the request has taken, as the gateway recorded it. The
// waiting marker covers the gap between acceptance and the first recorded
// event, so an accepted request is never shown as though nothing happened.
func (rw *row) stage() string {
	if len(rw.trail) > 0 {
		return strings.Join(rw.trail, " → ")
	}
	if rw.sqid != "" {
		return "…"
	}
	return absent
}

// createAndEnqueue opens the pull requests and puts them on the queue, filling
// in the tracker's rows as it goes and reporting each step beneath the table.
//
// For independent changes the two steps interleave: each pull request is
// enqueued the moment it exists, so the queue is already working on it while
// the next is being opened. Stacked changes cannot interleave — one request
// carries the whole chain, so it can only be submitted once the chain is
// complete.
//
// Each change edits its own file. Independent changes would otherwise collide
// on content and the run would measure conflict handling rather than the
// throughput it is trying to show; a caller wanting a conflict can make one
// deliberately.
func createAndEnqueue(
	ctx context.Context,
	gh *githubClient,
	client pb.SubmitQueueGatewayClient,
	cfg config,
	strategy mergestrategypb.Strategy,
	tag, baseSHA string,
	t *tracker,
) ([]change, error) {
	created := make([]change, 0, cfg.count)

	parentBranch, parentSHA := cfg.base, baseSHA
	for i := 1; i <= cfg.count; i++ {
		// A stack is one request, so every change lands on the single row.
		target := t.rows[0]
		if !cfg.stacked {
			target = t.rows[i-1]
		}

		branch := fmt.Sprintf("%s/%s/%d", cfg.prefix, tag, i)
		t.note("creating branch %s", branch)
		if err := gh.createBranch(ctx, branch, parentSHA); err != nil {
			return nil, fmt.Errorf("create branch %s: %w", branch, err)
		}

		path := fmt.Sprintf("demo/%s-%d.txt", tag, i)
		body := fmt.Sprintf("change %d of run %s\n", i, tag)
		t.note("committing %s", path)
		headSHA, err := gh.commitFile(ctx, branch, path, body, fmt.Sprintf("demo change %d (run %s)", i, tag))
		if err != nil {
			return nil, fmt.Errorf("commit to %s: %w", branch, err)
		}

		t.note("opening pull request for %s", branch)
		number, url, err := gh.openPR(ctx, fmt.Sprintf("demo change %d (run %s)", i, tag), branch, parentBranch)
		if err != nil {
			return nil, fmt.Errorf("open pull request for %s: %w", branch, err)
		}

		c := change{
			number: number, url: url, branch: branch,
			uri: githubchange.ChangeID{
				Scheme: "github", Host: cfg.host, Org: gh.owner, Repo: gh.repo,
				PRNumber: number, HeadCommitSHA: headSHA,
			}.String(),
		}
		created = append(created, c)
		t.update(func() { target.changes = append(target.changes, c) })

		if cfg.stacked {
			// The next change builds on this one, so it sees this change's
			// content and its pull request is based on this branch.
			parentBranch, parentSHA = branch, headSHA
			continue
		}
		if !cfg.land {
			continue
		}
		t.note("enqueuing #%d", number)
		sqid, err := enqueue(ctx, client, cfg, strategy, []change{c})
		if err != nil {
			return nil, err
		}
		t.update(func() { target.sqid, target.submitted = sqid, time.Now() })
	}

	// The stack goes in as one request, which is only possible now that every
	// change in it exists.
	if cfg.stacked && cfg.land {
		t.note("enqueuing the stack")
		sqid, err := enqueue(ctx, client, cfg, strategy, created)
		if err != nil {
			return nil, err
		}
		t.update(func() { t.rows[0].sqid, t.rows[0].submitted = sqid, time.Now() })
	}
	return created, nil
}

// enqueue submits one land request carrying the given changes, in order, and
// returns the identifier the gateway assigned it.
func enqueue(
	ctx context.Context,
	client pb.SubmitQueueGatewayClient,
	cfg config,
	strategy mergestrategypb.Strategy,
	changes []change,
) (string, error) {
	uris := make([]string, 0, len(changes))
	for _, c := range changes {
		uris = append(uris, c.uri)
	}

	resp, err := client.Land(ctx, &pb.LandRequest{
		Queue:    cfg.queue,
		Change:   &changepb.Change{Uris: uris},
		Strategy: strategy,
	})
	if err != nil {
		return "", fmt.Errorf("land %s failed: %w", labelsOf(changes), err)
	}
	return resp.Sqid, nil
}

// labelsOf names the pull requests on a row the way they are shown.
func labelsOf(cs []change) string {
	labels := make([]string, 0, len(cs))
	for _, c := range cs {
		labels = append(labels, fmt.Sprintf("#%d", c.number))
	}
	return strings.Join(labels, ",")
}

// tracker owns the rows for the duration of the run. Two goroutines touch
// them — creation fills in pull requests and identifiers, polling fills in
// statuses — and both draw the same table, so the mutex is what keeps one from
// redrawing halfway through the other's update.
type tracker struct {
	mu     sync.Mutex
	rows   []*row
	r      *renderer
	status string
	// sealed records that every request that will be enqueued has been. Without
	// it, polling would find nothing outstanding before creation had begun and
	// call the run finished.
	sealed bool

	// settled closes once every request has reached a terminal status.
	settled chan struct{}
	once    sync.Once
}

func newTracker(rows []*row) *tracker {
	return &tracker{rows: rows, r: newRenderer(), settled: make(chan struct{})}
}

// note replaces the line under the table and redraws.
func (t *tracker) note(format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = fmt.Sprintf(format, args...)
	t.r.draw(t.rows, t.status)
}

// update applies a change to the rows and redraws with it.
func (t *tracker) update(fn func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fn()
	t.r.draw(t.rows, t.status)
}

// conclude draws the verdict and reports whether everything landed. It reads
// the rows under the lock because a poll may still be applying its last round.
func (t *tracker) conclude() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = outcome(t.rows)
	t.r.draw(t.rows, t.status)
	return summarize(t.rows)
}

// seal declares that nothing further will be enqueued, which is what lets an
// otherwise-finished run conclude.
func (t *tracker) seal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sealed = true
	t.signalLocked()
}

// signalLocked closes settled once there is nothing left to wait for.
func (t *tracker) signalLocked() {
	if !t.sealed {
		return
	}
	for _, rw := range t.rows {
		if rw.sqid == "" || !rw.done {
			return
		}
	}
	t.once.Do(func() { close(t.settled) })
}

// poll re-reads statuses until the run finishes or the context ends.
func (t *tracker) poll(ctx context.Context, client pb.SubmitQueueGatewayClient, queue string) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.settled:
			return
		case <-ticker.C:
		}
		t.refresh(ctx, client, queue)
	}
}

// refresh re-reads every request that has been accepted but has not settled.
//
// The reads happen outside the lock. Holding it across a round of RPCs would
// stall creation behind the network, and creation racing ahead is the whole
// point of enqueuing each pull request the moment it exists.
func (t *tracker) refresh(ctx context.Context, client pb.SubmitQueueGatewayClient, queue string) {
	t.mu.Lock()
	outstanding := make([]*row, 0, len(t.rows))
	for _, rw := range t.rows {
		if rw.sqid != "" && !rw.done {
			outstanding = append(outstanding, rw)
		}
	}
	total := len(t.rows)
	t.mu.Unlock()

	type reading struct {
		rw     *row
		trail  []string
		status string
		note   string
	}
	readings := make([]reading, 0, len(outstanding))
	for _, rw := range outstanding {
		// sqid is written once, before the row becomes outstanding, so reading
		// it here without the lock is safe.
		resp, err := client.GetRequestHistoryByID(ctx, &pb.GetRequestHistoryByIDRequest{Sqid: rw.sqid, Queue: queue})
		if err != nil || resp == nil || len(resp.Events) == 0 {
			// A history that is not readable yet is normal right after Land;
			// the next tick picks it up.
			continue
		}
		trail, status, note := digest(resp.Events)
		readings = append(readings, reading{rw: rw, trail: trail, status: status, note: note})
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	settled := 0
	for _, got := range readings {
		got.rw.trail, got.rw.status, got.rw.note = got.trail, got.status, got.note
		if terminalStatuses[got.status] && !got.rw.done {
			// Stamped from the local clock rather than the event timestamp so
			// the elapsed column is measured end to end against one clock.
			got.rw.done, got.rw.settled = true, time.Now()
		}
	}
	for _, rw := range t.rows {
		if rw.done {
			settled++
		}
	}

	t.status = fmt.Sprintf("%d of %d settled", settled, total)
	t.r.draw(t.rows, t.status)
	t.signalLocked()
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
func outcome(rows []*row) string {
	landed := 0
	for _, rw := range rows {
		if rw.status == string(entity.RequestStatusLanded) {
			landed++
		}
	}
	if landed == len(rows) {
		return fmt.Sprintf("all %d request(s) landed", len(rows))
	}
	return fmt.Sprintf("%d of %d request(s) did not land", len(rows)-landed, len(rows))
}

// summarize fails the run if anything did not land, so a scripted demo notices.
func summarize(rows []*row) error {
	var failed []string
	for _, rw := range rows {
		if rw.status != string(entity.RequestStatusLanded) {
			failed = append(failed, fmt.Sprintf("%s=%s", rw.sqid, rw.status))
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
type renderer struct {
	inPlace bool

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
	info, err := os.Stdout.Stat()
	tty := err == nil && info.Mode()&os.ModeCharDevice != 0
	return &renderer{
		inPlace:  tty,
		wRequest: len("REQUEST"),
		wChanges: len("CHANGES"),
		wElapsed: len("ELAPSED"),
		wStage:   len("STAGE"),
	}
}

func (r *renderer) draw(rows []*row, status string) {
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
	fmt.Printf("\033[K  ▸ %s\n", truncate(status, maxLineWidth-4))
	// Every draw emits the body, one blank line, and the status line; moving
	// back by exactly this many lines is what keeps the redraw from drifting.
	r.lastLines = len(body) + 2
	r.drawn = true
}

// body renders the header and one line per row.
func (r *renderer) body(rows []*row) []string {
	r.fit(rows)

	lines := make([]string, 0, len(rows)+2)
	lines = append(lines,
		fmt.Sprintf("  %-*s  %-*s  %*s  %s",
			r.wRequest, "REQUEST", r.wChanges, "CHANGES", r.wElapsed, "ELAPSED", "STAGE"),
		fmt.Sprintf("  %s  %s  %s  %s",
			rule(r.wRequest), rule(r.wChanges), rule(r.wElapsed), rule(r.wStage)))

	for _, rw := range rows {
		lines = append(lines, r.rowLine(rw))
		lines = append(lines, r.noteLines(rw)...)
	}
	return lines
}

// fit grows the columns to hold what the rows now contain. Widths never shrink,
// so the table does not shift under a value that has already been printed — but
// on a terminal the stage column stays inside the line, since its rule would
// otherwise wrap on a long trail and take the redraw with it.
func (r *renderer) fit(rows []*row) {
	for _, rw := range rows {
		r.wRequest = max(r.wRequest, utf8.RuneCountInString(rw.sqid))
		_, visible := r.changesCell(rw)
		r.wChanges = max(r.wChanges, visible)
		r.wStage = max(r.wStage, utf8.RuneCountInString(rw.stage()))
	}
	if r.inPlace {
		r.wStage = min(r.wStage, max(len("STAGE"), maxLineWidth-r.prefixWidth()))
	}
}

// prefixWidth is the space every row spends before the stage column.
func (r *renderer) prefixWidth() int {
	return 2 + r.wRequest + 2 + r.wChanges + 2 + r.wElapsed + 2
}

func (r *renderer) rowLine(rw *row) string {
	sqid := rw.sqid
	if sqid == "" {
		sqid = absent
	}
	changes, visible := r.changesCell(rw)

	prefix := fmt.Sprintf("  %-*s  %s  %*s  ",
		r.wRequest, sqid, pad(changes, visible, r.wChanges), r.wElapsed, rw.elapsed())

	tail := rw.stage()
	if r.inPlace {
		// Only the tail can overflow, and unlike the changes cell it never holds
		// escape sequences, so it is the one part safe to cut. The budget comes
		// from the column widths rather than the rendered prefix, which counts a
		// hyperlink's escape bytes that take up no space on screen.
		tail = truncate(tail, maxLineWidth-r.prefixWidth())
	}
	return prefix + tail
}

// noteLines renders a request's error under its row, wrapped and indented to
// the stage column. An error is the one thing in the table worth reading in
// full — truncating it to the width of a cell hides the part that says what
// went wrong — so it gets as many lines as it needs instead of an ellipsis.
func (r *renderer) noteLines(rw *row) []string {
	if rw.note == "" {
		return nil
	}

	indent := r.prefixWidth()
	// A piped run spends most of the line on URLs, so the wrap width is floored
	// rather than allowed to collapse to nothing.
	width := max(minNoteWidth, maxLineWidth-indent-2)

	wrapped := wrap(rw.note, width)
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

// changesCell renders the pull requests on a row and reports the width they
// occupy on screen. The two differ on a terminal, where a hyperlink is mostly
// escape bytes that take up no space.
func (r *renderer) changesCell(rw *row) (string, int) {
	if len(rw.changes) == 0 {
		return absent, utf8.RuneCountInString(absent)
	}

	parts := make([]string, 0, len(rw.changes))
	visible := 0
	for _, c := range rw.changes {
		label := fmt.Sprintf("#%d", c.number)
		if r.inPlace {
			parts = append(parts, hyperlink(label, c.url))
			visible += len(label)
			continue
		}
		// Piped output has nothing to click, so the address itself has to be
		// readable — and copyable out of a log.
		parts = append(parts, c.url)
		visible += len(c.url)
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
func signature(rows []*row) string {
	var b strings.Builder
	for _, rw := range rows {
		fmt.Fprintf(&b, "%s|%s|%s|%s\n", rw.sqid, labelsOf(rw.changes), rw.stage(), rw.note)
	}
	return b.String()
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

func parseStrategy(name string) (mergestrategypb.Strategy, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "DEFAULT":
		return mergestrategypb.Strategy_DEFAULT, nil
	case "REBASE":
		return mergestrategypb.Strategy_REBASE, nil
	case "SQUASH_REBASE":
		return mergestrategypb.Strategy_SQUASH_REBASE, nil
	case "MERGE":
		return mergestrategypb.Strategy_MERGE, nil
	case "PROMOTE":
		return mergestrategypb.Strategy_PROMOTE, nil
	default:
		return mergestrategypb.Strategy_DEFAULT, fmt.Errorf("unknown strategy %q", name)
	}
}

// githubClient is the slice of GitHub's REST API this tool needs: read a
// branch, create a branch, commit a file, open a pull request.
type githubClient struct {
	root  string
	token string
	owner string
	repo  string
}

func (g *githubClient) branchSHA(ctx context.Context, branch string) (string, error) {
	var out struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := g.do(ctx, http.MethodGet, "/git/ref/heads/"+branch, nil, &out); err != nil {
		return "", err
	}
	return out.Object.SHA, nil
}

func (g *githubClient) createBranch(ctx context.Context, branch, fromSHA string) error {
	return g.do(ctx, http.MethodPost, "/git/refs",
		map[string]string{"ref": "refs/heads/" + branch, "sha": fromSHA}, nil)
}

// commitFile writes a file on a branch and returns the resulting commit SHA —
// the commit a change URI pins the pull request to.
func (g *githubClient) commitFile(ctx context.Context, branch, path, content, message string) (string, error) {
	body := map[string]string{
		"message": message,
		"content": base64.StdEncoding.EncodeToString([]byte(content)),
		"branch":  branch,
	}
	var out struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := g.do(ctx, http.MethodPut, "/contents/"+path, body, &out); err != nil {
		return "", err
	}
	return out.Commit.SHA, nil
}

func (g *githubClient) openPR(ctx context.Context, title, head, base string) (int, string, error) {
	body := map[string]string{"title": title, "head": head, "base": base, "body": "Opened by service/submitqueue/demo/pr."}
	var out struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := g.do(ctx, http.MethodPost, "/pulls", body, &out); err != nil {
		return 0, "", err
	}
	return out.Number, out.HTMLURL, nil
}

// do issues one authenticated request against the repository, decoding into out
// when it is non-nil.
func (g *githubClient) do(ctx context.Context, method, path string, body any, out any) error {
	endpoint := fmt.Sprintf("%s/repos/%s/%s%s", g.root, g.owner, g.repo, path)

	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encode request for %s: %w", endpoint, err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request for %s: %w", endpoint, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+g.token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var detail bytes.Buffer
		_, _ = detail.ReadFrom(resp.Body)
		return fmt.Errorf("%s %s returned %s: %s", method, endpoint, resp.Status, strings.TrimSpace(detail.String()))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response from %s: %w", endpoint, err)
	}
	return nil
}
