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
// Independent pull requests are opened several at a time (-concurrency), since
// each is several round trips to the provider and nothing about them depends on
// the others. A stack cannot be: every change in it is based on the branch
// before it, so the next cannot be cut until the previous head exists.
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
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	githubchange "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/submitqueue/client"
	"golang.org/x/sync/errgroup"
)

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// config is everything the run needs, resolved from flags and the environment.
type config struct {
	repo        string
	base        string
	count       int
	files       int
	concurrency int
	stacked     bool
	prefix      string
	land        bool
	watch       bool
	addr        string
	tls         bool
	tokenEnv    string
	queue       string
	strategy    string
	token       string
	apiRoot     string
	host        string
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.repo, "repo", "behinddwalls/sq-demo", "scratch repository as owner/name")
	flag.StringVar(&c.base, "base", "main", "branch the changes target")
	flag.IntVar(&c.count, "count", 3, "how many pull requests to create")
	flag.IntVar(&c.files, "files", 3, "fewest files each pull request touches; the actual count varies a little above it")
	flag.IntVar(&c.concurrency, "concurrency", 5,
		"how many pull requests to create at once; a stack ignores it, being sequential by nature")
	flag.BoolVar(&c.stacked, "stacked", false, "chain the pull requests and enqueue them as one stack")
	flag.StringVar(&c.prefix, "prefix", "demo", "branch name prefix")
	flag.BoolVar(&c.land, "land", true, "enqueue each pull request as it is created")
	flag.BoolVar(&c.watch, "watch", true, "watch the requests until they all settle")
	flag.StringVar(&c.addr, "addr", "localhost:8081", "gateway address")
	flag.BoolVar(&c.tls, "tls", false, "dial the gateway with transport security")
	flag.StringVar(&c.tokenEnv, "token-env", client.DefaultTokenEnv, "environment variable holding the gateway bearer token")
	flag.StringVar(&c.queue, "queue", "demo-queue", "queue to land on")
	flag.StringVar(&c.strategy, "strategy", "SQUASH_REBASE", "merge strategy")
	flag.Parse()

	c.token = os.Getenv("GITHUB_TOKEN")
	c.apiRoot = "https://api.github.com"
	c.host = "github.com"
	return c
}

func run(ctx context.Context, cfg config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	owner, repo, _ := strings.Cut(cfg.repo, "/")
	if owner == "" || repo == "" {
		return fmt.Errorf("-repo %q must be owner/name", cfg.repo)
	}
	strategy, err := client.ParseStrategy(cfg.strategy)
	if err != nil {
		return err
	}

	gh := &githubClient{root: cfg.apiRoot, token: cfg.token, owner: owner, repo: repo}
	baseSHA, err := gh.branchSHA(ctx, cfg.base)
	if err != nil {
		return fmt.Errorf("read %s: %w", cfg.base, err)
	}

	var sq *client.Client
	if cfg.land {
		sq, err = client.New(client.Options{Addr: cfg.addr, TLS: cfg.tls, TokenEnv: cfg.tokenEnv})
		if err != nil {
			return err
		}
		defer sq.Close()
	}

	// A run tag keeps repeated invocations from colliding on branch names, and
	// makes it obvious in the repository which changes came from one run.
	tag := time.Now().Format("0102-150405")
	fmt.Printf("Creating %d pull request(s) in %s — %s\n\n", cfg.count, cfg.repo, shape(cfg))

	// Every row is known before anything is created: one per pull request, or a
	// single one for a stack, since the whole chain lands as one request. The
	// table is therefore complete from the first draw and only ever fills in.
	t := client.NewTracker(client.NewRows(rowCount(cfg)))
	t.Note("starting")

	// Statuses are read on their own clock, concurrently with creation. A run
	// that only started polling once every pull request existed would show an
	// empty trail for the whole creation phase — which for a large -count is
	// most of the run, and is exactly the stretch worth watching, since the
	// early requests are already moving through the queue by then.
	if cfg.land {
		polling, stop := context.WithCancel(ctx)
		defer stop()
		go t.Poll(polling, sq.Gateway(), cfg.queue)
	}

	created, err := createAndEnqueue(ctx, gh, sq, cfg, strategy, tag, baseSHA, t)
	if err != nil {
		return err
	}
	t.Seal()

	if !cfg.land {
		t.Note("created %d pull request(s), not enqueued", len(created))
		fmt.Printf("\nEnqueue them with:\n  make land PRS=\"%s\"\n", strings.Join(urlsOf(created), " "))
		return nil
	}
	if !cfg.watch {
		t.Note("enqueued, not watching")
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.Settled():
	}
	return t.Conclude()
}

// rowCount is how many rows the table needs: one per pull request, or a single
// one for a stack, which lands as one request however many changes it carries.
func rowCount(cfg config) int {
	if cfg.stacked {
		return 1
	}
	return cfg.count
}

func shape(cfg config) string {
	if cfg.stacked {
		return "stacked, enqueued as one request once the chain exists"
	}
	if cfg.concurrency > 1 {
		return fmt.Sprintf("independent, %d at a time, each enqueued as soon as it is created", cfg.concurrency)
	}
	return "independent, each enqueued as soon as it is created"
}

// validate rejects a configuration the run cannot proceed with.
func (c config) validate() error {
	if c.token == "" {
		return fmt.Errorf("GITHUB_TOKEN is not set; it is the same credential the stack uses")
	}
	if c.count < 1 {
		return fmt.Errorf("-count must be at least 1")
	}
	if c.concurrency < 1 {
		return fmt.Errorf("-concurrency must be at least 1")
	}
	if c.files < 1 {
		return fmt.Errorf("-files must be at least 1")
	}
	if _, _, ok := strings.Cut(c.repo, "/"); !ok {
		return fmt.Errorf("-repo %q must be owner/name", c.repo)
	}
	return nil
}

// change is one pull request this run created.
type change struct {
	number int
	url    string
	branch string
	// headSHA is the commit the pull request now points at, which the next
	// change in a stack branches from.
	headSHA string
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

// shardDirs is how many nested bucket directories a path carries under the demo
// root. Two levels of 256 buckets spread a run's files widely enough that no
// directory becomes a dumping ground, while staying shallow enough to read in a
// diff.
const shardDirs = 2

// changeFilePath returns the repository path for one file of one change.
//
// The leaf name carries the run tag, the change index and the file index, which
// is what makes it unique: no two files in a run, and no two runs against the
// same repository, can ever name the same path. That uniqueness is load-bearing
// — see createAndEnqueue.
//
// The directories are the leading bytes of the leaf's SHA-256, so files land in
// buckets that are uniform without any coordination and stable across runs. Two
// unrelated changes sharing a bucket is expected and harmless: the bucket is
// only a directory, and it is the leaf that has to be distinct.
func changeFilePath(tag string, change, file int) string {
	leaf := fmt.Sprintf("%s-%d-%d.txt", tag, change, file)
	sum := sha256.Sum256([]byte(leaf))

	parts := make([]string, 0, shardDirs+2)
	parts = append(parts, "demo")
	for i := 0; i < shardDirs; i++ {
		parts = append(parts, fmt.Sprintf("%02x", sum[i]))
	}
	parts = append(parts, leaf)
	return strings.Join(parts, "/")
}

// changeFileCount returns how many files a change touches: at least min, varied
// a little so a run does not produce a row of identically shaped pull requests.
//
// The variation is derived from the run tag and the change index rather than
// from a clock or a global source of randomness, so replaying a tag reproduces
// the same run. A demo that cannot be reproduced is hard to talk about when
// something in it goes wrong.
func changeFileCount(tag string, change, min int) int {
	if min < 1 {
		min = 1
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", tag, change)))
	return min + int(sum[0]%4)
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
// Every file a change writes is its own, at a path no other change uses.
// Independent changes would otherwise collide on content and the run would
// measure conflict handling rather than the throughput it is trying to show; a
// caller wanting a conflict can make one deliberately. Each change spreads
// several files across the sharded tree, so it arrives as a multi-file, multi-
// commit pull request rather than a single-line edit — which is both closer to
// a real change and enough to exercise replaying a range of commits.
func createAndEnqueue(
	ctx context.Context,
	gh *githubClient,
	sq *client.Client,
	cfg config,
	strategy mergestrategypb.Strategy,
	tag, baseSHA string,
	t *client.Tracker,
) ([]change, error) {
	if cfg.stacked {
		return createStack(ctx, gh, sq, cfg, strategy, tag, baseSHA, t)
	}
	return createIndependent(ctx, gh, sq, cfg, strategy, tag, baseSHA, t)
}

// createIndependent opens the pull requests concurrently, up to the configured
// limit, enqueuing each the moment it exists.
//
// Independent changes have nothing to say to each other: each branches from the
// same base and writes files no other change touches, so the only reason to
// create them one at a time was that the loop did. Creating a pull request is
// several round trips to the provider — a branch, a commit per file, the pull
// request itself — and doing that serially is most of what a large run spends
// its time on. It also delays the overlap the demo exists to show, since the
// queue cannot work on requests that have not been submitted yet.
//
// The limit is there because the provider is a shared service with its own
// opinion about burst rates, and because the point is to feed the queue, not to
// find out how fast a repository can be hammered.
func createIndependent(
	ctx context.Context,
	gh *githubClient,
	sq *client.Client,
	cfg config,
	strategy mergestrategypb.Strategy,
	tag, baseSHA string,
	t *client.Tracker,
) ([]change, error) {
	rows := t.Rows()
	// Indexed rather than appended: the workers finish in whatever order the
	// provider answers them, and the caller still wants the run's own order.
	created := make([]change, cfg.count)

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(cfg.concurrency)

	for i := 1; i <= cfg.count; i++ {
		group.Go(func() error {
			c, err := createOne(groupCtx, gh, cfg, tag, baseSHA, cfg.base, i, t, rows[i-1])
			if err != nil {
				return err
			}
			created[i-1] = c

			if !cfg.land {
				return nil
			}
			t.Note("enqueuing #%d", c.number)
			sqid, err := sq.Land(groupCtx, cfg.queue, urisOf([]change{c}), strategy)
			if err != nil {
				return err
			}
			t.Update(func() { rows[i-1].SQID, rows[i-1].Submitted = sqid, time.Now() })
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}
	return created, nil
}

// createStack opens the pull requests one after another, each based on the one
// before it, and submits the whole chain as a single request.
//
// This one cannot be parallelized, and not for want of trying: a change is
// based on the branch of the change before it and must see its content, so the
// next branch cannot be cut until the previous head exists.
func createStack(
	ctx context.Context,
	gh *githubClient,
	sq *client.Client,
	cfg config,
	strategy mergestrategypb.Strategy,
	tag, baseSHA string,
	t *client.Tracker,
) ([]change, error) {
	rows := t.Rows()
	created := make([]change, 0, cfg.count)

	parentBranch, parentSHA := cfg.base, baseSHA
	for i := 1; i <= cfg.count; i++ {
		// A stack is one request, so every change lands on the single row.
		c, err := createOne(ctx, gh, cfg, tag, parentSHA, parentBranch, i, t, rows[0])
		if err != nil {
			return nil, err
		}
		created = append(created, c)
		parentBranch, parentSHA = c.branch, c.headSHA
	}

	// The stack goes in as one request, which is only possible now that every
	// change in it exists.
	if cfg.land {
		t.Note("enqueuing the stack")
		sqid, err := sq.Land(ctx, cfg.queue, urisOf(created), strategy)
		if err != nil {
			return nil, err
		}
		t.Update(func() { rows[0].SQID, rows[0].Submitted = sqid, time.Now() })
	}
	return created, nil
}

// createOne cuts a branch from parentSHA, writes the change's files to it, and
// opens a pull request against parentBranch, recording it on the given row.
func createOne(
	ctx context.Context,
	gh *githubClient,
	cfg config,
	tag, parentSHA, parentBranch string,
	i int,
	t *client.Tracker,
	target *client.Row,
) (change, error) {
	branch := fmt.Sprintf("%s/%s/%d", cfg.prefix, tag, i)
	t.Note("creating branch %s", branch)
	if err := gh.createBranch(ctx, branch, parentSHA); err != nil {
		return change{}, fmt.Errorf("create branch %s: %w", branch, err)
	}

	// Each file is its own commit, so the pull request arrives as a range of
	// commits rather than a single edit. The last one is the head the change
	// URI pins.
	var headSHA string
	fileCount := changeFileCount(tag, i, cfg.files)
	for k := 1; k <= fileCount; k++ {
		path := changeFilePath(tag, i, k)
		body := fmt.Sprintf("change %d of run %s\nfile %d of %d\n", i, tag, k, fileCount)
		t.Note("committing %s (%d/%d)", path, k, fileCount)

		message := fmt.Sprintf("demo change %d (run %s): file %d of %d", i, tag, k, fileCount)
		sha, err := gh.commitFile(ctx, branch, path, body, message)
		if err != nil {
			return change{}, fmt.Errorf("commit %s to %s: %w", path, branch, err)
		}
		headSHA = sha
	}

	t.Note("opening pull request for %s", branch)
	number, url, err := gh.openPR(ctx, fmt.Sprintf("demo change %d (run %s)", i, tag), branch, parentBranch)
	if err != nil {
		return change{}, fmt.Errorf("open pull request for %s: %w", branch, err)
	}

	c := change{
		number: number, url: url, branch: branch, headSHA: headSHA,
		uri: githubchange.ChangeID{
			Scheme: "github", Host: cfg.host, Org: gh.owner, Repo: gh.repo,
			PRNumber: number, HeadCommitSHA: headSHA,
		}.String(),
	}
	// The cell is what the table shows for this change: the pull request
	// number, clickable where the terminal allows it.
	cell := client.Cell{Text: fmt.Sprintf("#%d", number), URL: url}
	t.Update(func() { target.Cells = append(target.Cells, cell) })
	return c, nil
}

// urisOf is the change URIs the run pinned, in caller order.
func urisOf(cs []change) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.uri)
	}
	return out
}
