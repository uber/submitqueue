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

// Command client is a small operator CLI for the SubmitQueue gateway: submit a
// land request, read a request's status, and see what a queue is doing.
//
// It exists so that driving a real queue does not require hand-assembling
// protobuf with grpcurl — in particular, `land -pr` turns a pull request URL
// into the change URI the pipeline wants, so nobody has to paste a 40-character
// commit SHA by hand.
//
// Everything it does beyond parsing flags lives in submitqueue/client, so this
// file stays a thin front end and the same behaviour is available to any other
// tool that wants it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	githubchange "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/submitqueue/client"
)

const usage = `Usage: client [global flags] <command> [command flags]

Commands:
  ping     Check that the gateway is reachable
  land     Submit a change, or an ordered stack of changes, to a queue
  status   Read a request's current status
  list     Show a queue's recent requests as a table
  watch    Follow a queue's requests until they settle

Global flags:
  -addr       gateway address (default "localhost:8081")
  -tls        dial with transport security (default false)
  -token-env  environment variable holding the bearer token (default "SQ_TOKEN")
  -timeout    request timeout, 0 for none (default 10s; watch ignores it)

Examples:
  client ping
  client land -queue my-queue -pr https://github.com/uber/sq-sandbox/pull/7
  client land -queue my-queue -uri github://github.com/uber/r/pull/7/<sha> -strategy SQUASH_REBASE
  client land -queue my-queue -pr <url-of-first> -pr <url-of-second>
  client status -queue my-queue -sqid my-queue/12
  client list -queue my-queue -since 1h
  client watch -queue my-queue
  client -addr sq.example.com:443 -tls list -queue my-queue
`

func main() {
	var opts client.Options
	flag.StringVar(&opts.Addr, "addr", "localhost:8081", "gateway server address")
	flag.BoolVar(&opts.TLS, "tls", false, "dial the gateway with transport security")
	flag.StringVar(&opts.TokenEnv, "token-env", client.DefaultTokenEnv,
		"environment variable holding the bearer token; empty disables authentication")
	timeout := flag.Duration("timeout", 10*time.Second, "request timeout; 0 for none")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	if err := run(opts, *timeout, args[0], args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(opts client.Options, timeout time.Duration, command string, args []string) error {
	sq, err := client.New(opts)
	if err != nil {
		return err
	}
	defer sq.Close()

	// A watch runs until its queue settles or the operator stops it, so it is
	// the one command that must not inherit a per-call deadline.
	if command == "watch" {
		timeout = 0
	}
	ctx, cancel := client.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch command {
	case "ping":
		return runPing(ctx, sq, args)
	case "land":
		return runLand(ctx, sq, args)
	case "status":
		return runStatus(ctx, sq, args)
	case "list":
		return runList(ctx, sq, args)
	case "watch":
		return runWatch(ctx, sq, args)
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", command)
	}
}

func runPing(ctx context.Context, sq *client.Client, args []string) error {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	message := fs.String("message", "", "message to echo back")
	if err := fs.Parse(args); err != nil {
		return err
	}

	resp, err := sq.Ping(ctx, *message)
	if err != nil {
		return err
	}

	fmt.Printf("Message:      %s\n", resp.Message)
	fmt.Printf("Service Name: %s\n", resp.ServiceName)
	fmt.Printf("Timestamp:    %d (%s)\n", resp.Timestamp, time.Unix(resp.Timestamp, 0))
	fmt.Printf("Hostname:     %s\n", resp.Hostname)
	return nil
}

func runLand(ctx context.Context, sq *client.Client, args []string) error {
	fs := flag.NewFlagSet("land", flag.ExitOnError)
	queue := fs.String("queue", "", "queue to land on (required)")
	strategy := fs.String("strategy", "REBASE", "REBASE, SQUASH_REBASE, MERGE, PROMOTE, or DEFAULT")

	// Both are repeatable, and order is significant: several changes in one
	// request are a stack, applied in the order given, each on top of the last.
	var uris repeatable
	var prs repeatable
	fs.Var(&uris, "uri", "change URI; repeat for a stack, in application order")
	fs.Var(&prs, "pr", "pull request URL to resolve into a change URI; repeat for a stack")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *queue == "" {
		return fmt.Errorf("-queue is required")
	}
	if len(uris) == 0 && len(prs) == 0 {
		return fmt.Errorf("at least one -uri or -pr is required")
	}

	resolved, err := resolvePullRequests(ctx, prs)
	if err != nil {
		return err
	}
	// Explicit URIs first, then resolved ones, each in the order given.
	all := append(append([]string{}, uris...), resolved...)

	parsedStrategy, err := client.ParseStrategy(*strategy)
	if err != nil {
		return err
	}

	for i, uri := range all {
		fmt.Printf("Change %d: %s\n", i+1, uri)
	}

	sqid, err := sq.Land(ctx, *queue, all, parsedStrategy)
	if err != nil {
		return err
	}

	fmt.Printf("\nLanded request submitted.\n")
	fmt.Printf("  sqid: %s\n", sqid)
	fmt.Printf("\nFollow it with: client status -queue %s -sqid %s\n", *queue, sqid)
	return nil
}

func runStatus(ctx context.Context, sq *client.Client, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	sqid := fs.String("sqid", "", "request id returned by land (required)")
	// A sqid is only resolvable within its own queue, so the server needs both.
	queue := fs.String("queue", "", "queue the request was landed on (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sqid == "" {
		return fmt.Errorf("-sqid is required")
	}
	if *queue == "" {
		return fmt.Errorf("-queue is required")
	}

	request, err := sq.Summary(ctx, *queue, *sqid)
	if err != nil {
		return err
	}

	fmt.Printf("sqid:   %s\n", request.Sqid)
	fmt.Printf("queue:  %s\n", request.Queue)
	fmt.Printf("status: %s\n", request.Status)
	if request.LastError != "" {
		fmt.Printf("error:  %s\n", request.LastError)
	}
	for i, uri := range request.ChangeUris {
		fmt.Printf("change %d: %s\n", i+1, uri)
	}
	return nil
}

// runList draws a queue's recent requests once and returns.
func runList(ctx context.Context, sq *client.Client, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	queue := fs.String("queue", "", "queue to list (required)")
	since := fs.Duration("since", 0, "only requests received within this window; 0 for all retained history")
	limit := fs.Int("limit", 50, "most requests to show; 0 for every one in the window")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *queue == "" {
		return fmt.Errorf("-queue is required")
	}

	summaries, err := sq.List(ctx, client.ListQuery{Queue: *queue, Since: *since, Limit: *limit})
	if err != nil {
		return err
	}
	if len(summaries) == 0 {
		fmt.Printf("no requests in %s%s\n", *queue, within(*since))
		return nil
	}

	rows := client.RowsFromSummaries(summaries)
	client.Draw(rows, fmt.Sprintf("%d request(s) in %s%s", len(rows), *queue, within(*since)))
	return nil
}

// runWatch follows a queue's requests until every one of them settles.
//
// The set is fixed when the watch starts: a request accepted afterwards is not
// picked up, because a watch that grew as the queue did would never finish, and
// finishing is what makes the command usable from a script.
func runWatch(ctx context.Context, sq *client.Client, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	queue := fs.String("queue", "", "queue to watch (required)")
	since := fs.Duration("since", 15*time.Minute, "how far back to pick requests up from")
	limit := fs.Int("limit", 50, "most requests to watch; 0 for every one in the window")
	var sqids repeatable
	fs.Var(&sqids, "sqid", "watch only this request; repeat for several, and -since is then ignored")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *queue == "" {
		return fmt.Errorf("-queue is required")
	}

	rows, err := watchRows(ctx, sq, *queue, *since, *limit, sqids)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Printf("no requests in %s%s\n", *queue, within(*since))
		return nil
	}

	t := client.NewTracker(rows)
	// Nothing further will join the set, so the tracker can conclude as soon as
	// what it holds has settled.
	t.Seal()
	t.Note("watching %d request(s) in %s", len(rows), *queue)

	// A watch of a busy queue holds more requests than a window does, so it runs
	// as a full-screen view the reader can scroll. Restored before Conclude, so
	// the final table lands in the scrollback rather than disappearing with the
	// screen it was drawn on.
	stop, quit := t.Interact(ctx)
	defer stop()

	go t.Poll(ctx, sq.Gateway(), *queue)

	select {
	case <-ctx.Done():
		stop()
		return ctx.Err()
	case <-quit:
	case <-t.Settled():
	}
	stop()
	return t.Conclude()
}

// watchRows is the set a watch will follow: the named requests, or whatever the
// queue holds in the window.
func watchRows(
	ctx context.Context,
	sq *client.Client,
	queue string,
	since time.Duration,
	limit int,
	sqids []string,
) ([]*client.Row, error) {
	if len(sqids) > 0 {
		rows := make([]*client.Row, 0, len(sqids))
		for _, sqid := range sqids {
			rows = append(rows, &client.Row{SQID: sqid, Submitted: time.Now()})
		}
		return rows, nil
	}

	summaries, err := sq.List(ctx, client.ListQuery{Queue: queue, Since: since, Limit: limit})
	if err != nil {
		return nil, err
	}
	return client.RowsFromSummaries(summaries), nil
}

// within describes a time window for a message, or nothing at all when the
// window is the whole of retained history.
func within(since time.Duration) string {
	if since <= 0 {
		return ""
	}
	return fmt.Sprintf(" in the last %s", since)
}

// repeatable collects a flag given more than once, preserving the order it was
// given in — which for a stack of changes is the order they must be applied.
type repeatable []string

func (r *repeatable) String() string     { return strings.Join(*r, ",") }
func (r *repeatable) Set(v string) error { *r = append(*r, v); return nil }

// resolvePullRequests turns each pull request URL into the change URI the
// pipeline expects, in the order given.
func resolvePullRequests(ctx context.Context, urls []string) ([]string, error) {
	resolved := make([]string, 0, len(urls))
	for _, raw := range urls {
		uri, err := resolvePullRequest(ctx, raw)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, uri)
	}
	return resolved, nil
}

// resolvePullRequest dispatches a change URL to the provider that hosts it.
//
// Only GitHub is implemented. Adding a provider is a case here plus its own
// resolver, mirroring how the lander and the change provider each dispatch on
// the change's provider rather than assuming one.
func resolvePullRequest(ctx context.Context, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid pull request URL %q: %w", raw, err)
	}
	// Every provider this could support is reached over HTTPS, so the host is what
	// distinguishes them rather than the scheme.
	if u.Host == "" {
		return "", fmt.Errorf("pull request URL %q has no host", raw)
	}
	return resolveGitHubPullRequest(ctx, u)
}

// resolveGitHubPullRequest reads a pull request's head commit and builds its
// change URI. Reading a public repository needs no token; GITHUB_TOKEN is used
// when set, which a private repository requires.
func resolveGitHubPullRequest(ctx context.Context, u *url.URL) (string, error) {
	// https://{host}/{owner}/{repo}/pull/{number}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "pull" {
		return "", fmt.Errorf("expected a URL like https://github.com/{owner}/{repo}/pull/{number}, got %q", u)
	}
	owner, repo := parts[0], parts[1]
	number, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", fmt.Errorf("pull request number %q in %q is not a number", parts[3], u)
	}

	sha, err := githubHeadSHA(ctx, githubAPIRoot(u.Host), owner, repo, number)
	if err != nil {
		return "", err
	}

	return githubchange.ChangeID{
		Scheme:        "github",
		Host:          u.Host,
		Org:           owner,
		Repo:          repo,
		PRNumber:      number,
		HeadCommitSHA: sha,
	}.String(), nil
}

// githubAPIRoot maps a GitHub web host to its API root. GitHub Enterprise
// serves its API under the same host at /api/v3; github.com does not.
func githubAPIRoot(host string) string {
	if host == "github.com" || host == "www.github.com" {
		return "https://api.github.com"
	}
	return "https://" + host + "/api/v3"
}

// githubHeadSHA reads the current head commit of a pull request.
func githubHeadSHA(ctx context.Context, apiRoot, owner, repo string, number int) (string, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d", apiRoot, owner, repo, number)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("building request for %s: %w", endpoint, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// A private repository read without a token comes back as 404, which
		// reads as "no such pull request" unless the cause is named.
		hint := ""
		if resp.StatusCode == http.StatusNotFound && os.Getenv("GITHUB_TOKEN") == "" {
			hint = " (set GITHUB_TOKEN if the repository is private)"
		}
		return "", fmt.Errorf("GET %s returned %s%s", endpoint, resp.Status, hint)
	}

	var body struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decoding response from %s: %w", endpoint, err)
	}
	if body.Head.SHA == "" {
		return "", fmt.Errorf("%s returned no head commit", endpoint)
	}
	return body.Head.SHA, nil
}
