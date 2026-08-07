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
// land request, read a request's status, and ping the service.
//
// It exists so that driving a real queue does not require hand-assembling
// protobuf with grpcurl — in particular, `land -pr` turns a pull request URL
// into the change URI the pipeline wants, so nobody has to paste a 40-character
// commit SHA by hand.
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

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const usage = `Usage: client [global flags] <command> [command flags]

Commands:
  ping     Check that the gateway is reachable
  land     Submit a change, or an ordered stack of changes, to a queue
  status   Read a request's current status

Global flags:
  -addr     gateway address (default "localhost:8081")
  -timeout  request timeout (default 10s)

Examples:
  client ping
  client land -queue my-queue -pr https://github.com/uber/sq-sandbox/pull/7
  client land -queue my-queue -uri github://github.com/uber/r/pull/7/<sha> -strategy SQUASH_REBASE
  client land -queue my-queue -pr <url-of-first> -pr <url-of-second>
  client status -queue my-queue -sqid my-queue/12
`

func main() {
	addr := flag.String("addr", "localhost:8081", "gateway server address")
	timeout := flag.Duration("timeout", 10*time.Second, "request timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	if err := run(*addr, *timeout, args[0], args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(addr string, timeout time.Duration, command string, args []string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	client := pb.NewSubmitQueueGatewayClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch command {
	case "ping":
		return runPing(ctx, client, args)
	case "land":
		return runLand(ctx, client, args)
	case "status":
		return runStatus(ctx, client, args)
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", command)
	}
}

func runPing(ctx context.Context, client pb.SubmitQueueGatewayClient, args []string) error {
	fs := flag.NewFlagSet("ping", flag.ExitOnError)
	message := fs.String("message", "", "message to echo back")
	if err := fs.Parse(args); err != nil {
		return err
	}

	resp, err := client.Ping(ctx, &pb.PingRequest{Message: *message})
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	fmt.Printf("Message:      %s\n", resp.Message)
	fmt.Printf("Service Name: %s\n", resp.ServiceName)
	fmt.Printf("Timestamp:    %d (%s)\n", resp.Timestamp, time.Unix(resp.Timestamp, 0))
	fmt.Printf("Hostname:     %s\n", resp.Hostname)
	return nil
}

func runLand(ctx context.Context, client pb.SubmitQueueGatewayClient, args []string) error {
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

	parsedStrategy, err := parseStrategy(*strategy)
	if err != nil {
		return err
	}

	for i, uri := range all {
		fmt.Printf("Change %d: %s\n", i+1, uri)
	}

	resp, err := client.Land(ctx, &pb.LandRequest{
		Queue:    *queue,
		Change:   &changepb.Change{Uris: all},
		Strategy: parsedStrategy,
	})
	if err != nil {
		return fmt.Errorf("land failed: %w", err)
	}

	fmt.Printf("\nLanded request submitted.\n")
	fmt.Printf("  sqid: %s\n", resp.Sqid)
	fmt.Printf("\nFollow it with: client status -queue %s -sqid %s\n", *queue, resp.Sqid)
	return nil
}

func runStatus(ctx context.Context, client pb.SubmitQueueGatewayClient, args []string) error {
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

	resp, err := client.GetRequestSummaryByID(ctx, &pb.GetRequestSummaryByIDRequest{Sqid: *sqid, Queue: *queue})
	if err != nil {
		return fmt.Errorf("status failed: %w", err)
	}
	if resp.Request == nil {
		return fmt.Errorf("no request found for %q", *sqid)
	}

	fmt.Printf("sqid:   %s\n", resp.Request.Sqid)
	fmt.Printf("queue:  %s\n", resp.Request.Queue)
	fmt.Printf("status: %s\n", resp.Request.Status)
	if resp.Request.LastError != "" {
		fmt.Printf("error:  %s\n", resp.Request.LastError)
	}
	for i, uri := range resp.Request.ChangeUris {
		fmt.Printf("change %d: %s\n", i+1, uri)
	}
	return nil
}

// repeatable collects a flag given more than once, preserving the order it was
// given in — which for a stack of changes is the order they must be applied.
type repeatable []string

func (r *repeatable) String() string     { return strings.Join(*r, ",") }
func (r *repeatable) Set(v string) error { *r = append(*r, v); return nil }

// parseStrategy maps the -strategy value onto the wire enum.
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
// resolver, mirroring how the merger and the change provider each dispatch on
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
