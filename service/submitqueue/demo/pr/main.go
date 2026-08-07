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
// overlap. The status table at the end watches all of them at once.
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
	"time"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	githubchange "github.com/uber/submitqueue/platform/base/change/github"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// pollInterval bounds how often the watcher re-reads every request's status.
const pollInterval = 2 * time.Second

// terminalStatuses are the states a land request settles on, in the gateway's
// customer-facing vocabulary.
var terminalStatuses = map[string]bool{"landed": true, "error": true, "cancelled": true}

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

	created, requests, err := createAndEnqueue(ctx, gh, client, cfg, strategy, tag, baseSHA)
	if err != nil {
		return err
	}

	if !cfg.land {
		fmt.Printf("\nEnqueue them with:\n  make land PRS=\"%s\"\n", strings.Join(urlsOf(created), " "))
		return nil
	}
	if !cfg.watch {
		return nil
	}
	return watch(ctx, client, cfg.queue, requests)
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

// createAndEnqueue opens the pull requests and puts them on the queue.
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
) ([]change, []*request, error) {
	created := make([]change, 0, cfg.count)
	requests := make([]*request, 0, cfg.count)

	parentBranch, parentSHA := cfg.base, baseSHA
	for i := 1; i <= cfg.count; i++ {
		branch := fmt.Sprintf("%s/%s/%d", cfg.prefix, tag, i)
		if err := gh.createBranch(ctx, branch, parentSHA); err != nil {
			return nil, nil, fmt.Errorf("create branch %s: %w", branch, err)
		}

		path := fmt.Sprintf("demo/%s-%d.txt", tag, i)
		body := fmt.Sprintf("change %d of run %s\n", i, tag)
		headSHA, err := gh.commitFile(ctx, branch, path, body, fmt.Sprintf("demo change %d (run %s)", i, tag))
		if err != nil {
			return nil, nil, fmt.Errorf("commit to %s: %w", branch, err)
		}

		number, url, err := gh.openPR(ctx, fmt.Sprintf("demo change %d (run %s)", i, tag), branch, parentBranch)
		if err != nil {
			return nil, nil, fmt.Errorf("open pull request for %s: %w", branch, err)
		}

		c := change{
			number: number, url: url, branch: branch,
			uri: githubchange.ChangeID{
				Scheme: "github", Host: cfg.host, Org: gh.owner, Repo: gh.repo,
				PRNumber: number, HeadCommitSHA: headSHA,
			}.String(),
		}
		created = append(created, c)
		fmt.Printf("  created  #%-5d %s\n", number, url)

		if cfg.stacked {
			// The next change builds on this one, so it sees this change's
			// content and its pull request is based on this branch.
			parentBranch, parentSHA = branch, headSHA
			continue
		}
		if !cfg.land {
			continue
		}
		req, err := enqueue(ctx, client, cfg, strategy, []change{c})
		if err != nil {
			return nil, nil, err
		}
		requests = append(requests, req)
		fmt.Printf("  enqueued %-6s %s\n", req.changes, req.sqid)
	}

	// The stack goes in as one request, which is only possible now that every
	// change in it exists.
	if cfg.stacked && cfg.land {
		req, err := enqueue(ctx, client, cfg, strategy, created)
		if err != nil {
			return nil, nil, err
		}
		requests = append(requests, req)
		fmt.Printf("  enqueued %-6s %s\n", req.changes, req.sqid)
	}
	return created, requests, nil
}

// request is one land request being watched.
type request struct {
	sqid string
	// changes names the pull requests the request carries, for the table.
	changes string
	// submitted is when the gateway accepted it, so elapsed time is per request
	// rather than per run.
	submitted time.Time

	status string
	note   string
	done   bool
}

// enqueue submits one land request carrying the given changes, in order.
func enqueue(
	ctx context.Context,
	client pb.SubmitQueueGatewayClient,
	cfg config,
	strategy mergestrategypb.Strategy,
	changes []change,
) (*request, error) {
	uris := make([]string, 0, len(changes))
	labels := make([]string, 0, len(changes))
	for _, c := range changes {
		uris = append(uris, c.uri)
		labels = append(labels, fmt.Sprintf("#%d", c.number))
	}

	resp, err := client.Land(ctx, &pb.LandRequest{
		Queue:    cfg.queue,
		Change:   &changepb.Change{Uris: uris},
		Strategy: strategy,
	})
	if err != nil {
		return nil, fmt.Errorf("land %s failed: %w", strings.Join(labels, ","), err)
	}
	return &request{
		sqid:      resp.Sqid,
		changes:   strings.Join(labels, ","),
		submitted: time.Now(),
		status:    "submitted",
	}, nil
}

// watch polls every request until they have all settled, redrawing a table as
// statuses change so the pipeline is visible while it runs.
func watch(ctx context.Context, client pb.SubmitQueueGatewayClient, queue string, requests []*request) error {
	if len(requests) == 0 {
		return nil
	}
	fmt.Printf("\n")
	r := newRenderer(len(requests) + 2)
	r.draw(requests)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		changed, pending := refresh(ctx, client, queue, requests)
		if changed || r.inPlace {
			// On a terminal the elapsed column moves every tick, so redraw
			// regardless; piped output only reprints when something happened.
			r.draw(requests)
		}
		if pending == 0 {
			return summarize(requests)
		}
	}
}

// refresh re-reads every unsettled request, reporting whether anything moved
// and how many are still in flight.
func refresh(ctx context.Context, client pb.SubmitQueueGatewayClient, queue string, requests []*request) (changed bool, pending int) {
	for _, req := range requests {
		if req.done {
			continue
		}
		resp, err := client.GetRequestSummaryByID(ctx, &pb.GetRequestSummaryByIDRequest{Sqid: req.sqid, Queue: queue})
		if err != nil || resp.Request == nil {
			// A summary that is not readable yet is normal right after Land;
			// the next tick picks it up.
			pending++
			continue
		}
		if resp.Request.Status != req.status || resp.Request.LastError != req.note {
			changed = true
		}
		req.status = resp.Request.Status
		req.note = resp.Request.LastError
		req.done = terminalStatuses[req.status]
		if !req.done {
			pending++
		}
	}
	return changed, pending
}

// summarize fails the run if anything did not land, so a scripted demo notices.
func summarize(requests []*request) error {
	var failed []string
	for _, req := range requests {
		if req.status != "landed" {
			failed = append(failed, fmt.Sprintf("%s=%s", req.sqid, req.status))
		}
	}
	if len(failed) > 0 {
		sort.Strings(failed)
		return fmt.Errorf("%d of %d request(s) did not land: %s", len(failed), len(requests), strings.Join(failed, ", "))
	}
	fmt.Printf("\nAll %d request(s) landed.\n", len(requests))
	return nil
}

// renderer draws the status table, redrawing in place on a terminal and
// appending a fresh block otherwise, so piping the output to a file stays
// readable instead of filling with escape codes.
type renderer struct {
	lines   int
	inPlace bool
	drawn   bool
}

func newRenderer(lines int) *renderer {
	info, err := os.Stdout.Stat()
	tty := err == nil && info.Mode()&os.ModeCharDevice != 0
	return &renderer{lines: lines, inPlace: tty}
}

func (r *renderer) draw(requests []*request) {
	if r.inPlace && r.drawn {
		// Move back over the previous table and overwrite it in place.
		fmt.Printf("\033[%dA", r.lines)
	}
	r.line(fmt.Sprintf("  %-24s %-10s %-12s %8s", "REQUEST", "CHANGES", "STATUS", "ELAPSED"))
	r.line("  " + strings.Repeat("-", 24) + " " + strings.Repeat("-", 10) + " " + strings.Repeat("-", 12) + " " + strings.Repeat("-", 8))
	for _, req := range requests {
		line := fmt.Sprintf("  %-24s %-10s %-12s %7ds", req.sqid, req.changes, req.status, int(time.Since(req.submitted).Seconds()))
		if req.note != "" {
			line += "  " + truncate(req.note, 60)
		}
		r.line(line)
	}
	r.drawn = true
}

// line writes one table row, clearing whatever the previous draw left on it
// when redrawing in place.
func (r *renderer) line(s string) {
	if r.inPlace {
		fmt.Printf("\033[K%s\n", s)
		return
	}
	fmt.Println(s)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
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
