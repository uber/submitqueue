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

// Command requests creates changes, enqueues each as a land request, and
// watches them move through the pipeline — so the demo stack can be exercised
// repeatedly without authoring changes by hand.
//
// Nothing is awaited until the end, which is the point. Each change is enqueued
// the moment it is created, so the queue is already working on the first while
// the last is still being made. A queue that only ever holds one request in
// flight never batches, never analyzes a conflict against another batch, and
// never speculates; those behaviors only appear when requests overlap. The
// table watches all of them at once.
//
// Independent changes are created several at a time (-concurrency), since
// nothing about them depends on the others — which matters most against a
// provider, where each is several round trips. A stack cannot be: every change
// in it is based on the branch before it, so the next cannot be cut until the
// previous head exists.
//
// Two shapes of change, because the pipeline treats them differently:
//
//   - independent (default): each change targets the base branch and is
//     enqueued as its own request, immediately after it is created. This is
//     what puts requests in flight against each other.
//   - stacked (-stacked): each change is based on the one before it, and all of
//     them go in as a single request once the chain exists — the atomic-stack
//     path, where the whole set reaches the target in one push.
//
// The table is drawn before the first change exists and refreshed for the whole
// run, so there is never a stretch with nothing to look at. Each row is one land
// request and shows the states it has passed through, read from the gateway's
// history API rather than sampled — polling only the current status would miss
// any transition that happens between two ticks, which for a fast queue is most
// of them.
//
// How a change is made depends on -provider, matching the stack the run is
// pointed at:
//
//   - fake (default): a change is a URI and nothing else. No repository, no
//     credential, no I/O — the fastest way to put traffic through the queue.
//   - git: a branch pushed to the sandbox repository the stack merges into.
//     Real commits, still no credential.
//   - github: a real pull request over the REST API, which needs no clone and
//     no git binary, only GITHUB_TOKEN — the same credential the stack uses.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	"github.com/uber/submitqueue/platform/gitexec"
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
	provider    string
	repo        string
	sandboxDir  string
	git         string
	base        string
	count       int
	folders     int
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
	flag.StringVar(&c.provider, "provider", providerFake, "how changes are created: fake, git, or github")
	flag.StringVar(&c.repo, "repo", "behinddwalls/sq-demo", "github: scratch repository as owner/name")
	flag.StringVar(&c.sandboxDir, "sandbox-dir", "/tmp/sq-sandbox", "git: directory holding the sandbox repository")
	flag.StringVar(&c.git, "git", "", "git: path to the git binary; defaults to GIT_EXECUTABLE, then PATH")
	flag.StringVar(&c.base, "base", "main", "branch the changes target")
	flag.IntVar(&c.count, "count", 3, "how many changes to create")
	flag.IntVar(&c.folders, "folders", 0,
		"how many folders to spread the changes across; 0 picks one per run. Changes sharing a folder are batched in order, changes in different folders go out together")
	flag.IntVar(&c.files, "files", 3,
		"fewest files each change touches; the actual count varies a little above it. Ignored by -provider fake, which writes none")
	flag.IntVar(&c.concurrency, "concurrency", 5,
		"how many changes to create at once; a stack ignores it, being sequential by nature, and -provider git serializes its git commands")
	flag.BoolVar(&c.stacked, "stacked", false, "chain the changes and enqueue them as one stack")
	flag.StringVar(&c.prefix, "prefix", "demo", "branch name prefix")
	flag.BoolVar(&c.land, "land", true, "enqueue each change as it is created")
	flag.BoolVar(&c.watch, "watch", true, "watch the requests until they all settle")
	flag.StringVar(&c.addr, "addr", "localhost:8081", "gateway address")
	flag.BoolVar(&c.tls, "tls", false, "dial the gateway with transport security")
	flag.StringVar(&c.tokenEnv, "token-env", client.DefaultTokenEnv, "environment variable holding the gateway bearer token")
	flag.StringVar(&c.queue, "queue", "demo-queue", "queue to land on")
	flag.StringVar(&c.strategy, "strategy", "SQUASH_REBASE", "merge strategy")
	flag.Parse()

	// Only the GitHub source reads a credential; the other two must not fail,
	// or even appear to depend on one, when none is set.
	if c.provider == providerGitHub {
		c.token = os.Getenv("GITHUB_TOKEN")
		c.apiRoot = "https://api.github.com"
		c.host = "github.com"
	}
	return c
}

func run(ctx context.Context, cfg config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	strategy, err := client.ParseStrategy(cfg.strategy)
	if err != nil {
		return err
	}

	src, cleanup, err := newChangeSource(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	baseSHA, err := src.baseSHA(ctx, cfg.base)
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
	// Resolved once, so every change in the run is dealt into the same tree and
	// the number can be reported rather than inferred from the paths.
	cfg.folders = resolveFolders(tag, cfg.folders)
	fmt.Printf("Creating %d change(s) across %d folder(s) via %s — %s\n\n",
		cfg.count, cfg.folders, target(cfg), shape(cfg))

	// Every row is known before anything is created: one per change, or a
	// single one for a stack, since the whole chain lands as one request. The
	// table is therefore complete from the first draw and only ever fills in.
	t := client.NewTracker(client.NewRows(rowCount(cfg)))
	t.Note("starting")

	// Statuses are read on their own clock, concurrently with creation. A run
	// that only started polling once every change existed would show an
	// empty trail for the whole creation phase — which for a large -count is
	// most of the run, and is exactly the stretch worth watching, since the
	// early requests are already moving through the queue by then.
	if cfg.land {
		polling, stop := context.WithCancel(ctx)
		defer stop()
		go t.Poll(polling, sq.Gateway(), cfg.queue)
	}

	created, err := createAndEnqueue(ctx, src, sq, cfg, strategy, tag, baseSHA, t)
	if err != nil {
		return err
	}
	t.Seal()

	if !cfg.land {
		t.Note("created %d change(s), not enqueued", len(created))
		fmt.Printf("\nEnqueue them with:\n  %s\n", enqueueHint(created))
		return nil
	}
	if !cfg.watch {
		t.Note("enqueued, not watching")
		return nil
	}

	// A large run has more changes than a window has lines, so the wait happens
	// in a full-screen view the reader can scroll. Restored before Conclude, so
	// the final table lands in the scrollback and not on a screen that is about
	// to be handed back.
	stop, quit := t.Interact(ctx)
	defer stop()

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

// rowCount is how many rows the table needs: one per change, or a single
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
	switch c.provider {
	case providerFake, providerGit:
	case providerGitHub:
		if c.token == "" {
			return fmt.Errorf("GITHUB_TOKEN is not set; it is the same credential the stack uses")
		}
		if _, _, ok := strings.Cut(c.repo, "/"); !ok {
			return fmt.Errorf("-repo %q must be owner/name", c.repo)
		}
	default:
		return fmt.Errorf("-provider %q must be one of %s, %s, or %s",
			c.provider, providerFake, providerGit, providerGitHub)
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
	if c.folders < 0 {
		return fmt.Errorf("-folders cannot be negative; 0 picks one per run")
	}
	return nil
}

// newChangeSource builds the source for the configured provider, and a cleanup
// to run when the run ends.
func newChangeSource(ctx context.Context, cfg config) (changeSource, func(), error) {
	switch cfg.provider {
	case providerFake:
		return fakeSource{}, func() {}, nil

	case providerGit:
		git, err := gitexec.Resolve(cfg.git)
		if err != nil {
			return nil, nil, err
		}
		src, err := newGitSource(ctx, git, filepath.Join(cfg.sandboxDir, sandboxRepo+".git"), sandboxRepo)
		if err != nil {
			return nil, nil, err
		}
		return src, src.close, nil

	case providerGitHub:
		owner, repo, _ := strings.Cut(cfg.repo, "/")
		return newGitHubSource(cfg, owner, repo), func() {}, nil
	}
	// validate has already rejected anything else.
	return nil, nil, fmt.Errorf("unknown provider %q", cfg.provider)
}

// sandboxRepo is the repository the git provider's sandbox holds, matching what
// tool/gitsandbox creates and what demo/provider/git/merge.yaml merges into.
const sandboxRepo = "sandbox"

// target describes where the run is creating changes, for the opening line.
func target(cfg config) string {
	switch cfg.provider {
	case providerGit:
		return fmt.Sprintf("git (%s)", filepath.Join(cfg.sandboxDir, sandboxRepo+".git"))
	case providerGitHub:
		return fmt.Sprintf("github (%s)", cfg.repo)
	default:
		return "fake changes (no repository)"
	}
}

// change is one change this run created.
type change struct {
	// label identifies the change in the table: a pull request number where
	// there is one, and the branch otherwise.
	label string
	// url is where the change can be opened, empty when it lives nowhere a
	// browser can reach.
	url    string
	branch string
	// headSHA is the commit the change now points at, which the next change in
	// a stack branches from.
	headSHA string
	// uri is the SubmitQueue change URI pinning the change to its head.
	uri string
}

// enqueueHint is the command that lands what a -land=false run created.
//
// Pull request URLs where there are any, since that is what a reader recognizes
// and can open; change URIs otherwise, which is the only handle a branch in a
// local repository has.
func enqueueHint(cs []change) string {
	if len(cs) > 0 && cs[0].url != "" {
		return fmt.Sprintf("make land PRS=%q", strings.Join(urlsOf(cs), " "))
	}
	return fmt.Sprintf("make land URIS=%q", strings.Join(urisOf(cs), " "))
}

func urlsOf(cs []change) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.url)
	}
	return out
}

// How many directories a run spreads its changes across, as a range rather than
// a number: each run picks one, so repeated runs against the same queue do not
// all collide in the same shape.
//
// This range is the whole dial on what a run demonstrates. The default analyzer
// keys on the directory, so a large number means nothing ever collides and a
// run shows only the parallel case, while one means everything serializes and
// it shows only the other. Five to ten is wide enough to look like a real tree
// and small enough that a run of a handful of changes still puts two of them in
// the same place.
//
// It used to be two nested levels of 256, keyed on each file's own name, which
// put a twelve-file run's odds of any collision at roughly one in a thousand
// and scattered a single change across as many directories as it had files.
const (
	minShardDirs = 5
	maxShardDirs = 10
)

// resolveFolders is how many directories a run spreads its changes across:
// what -folders asked for, or a number picked from the range above when it
// asked for nothing.
//
// Worth setting deliberately when the run is meant to show one thing. -folders 1
// puts every change in the same place, so the queue serializes the lot and each
// change speculates on the one before it; a number well above -count keeps them
// apart, so they all go out together.
func resolveFolders(tag string, configured int) int {
	if configured > 0 {
		return configured
	}
	sum := sha256.Sum256([]byte("folders#" + tag))
	return minShardDirs + int(sum[0])%(maxShardDirs-minShardDirs+1)
}

// changeShard is the directory one change writes into, as a fixed-width number
// so a run's collisions are visible at a glance in a table or a diff.
//
// Keyed on the change rather than on each file, so a change occupies exactly
// one directory: a change spread over several would overlap with everything and
// report a conflict that says nothing about it. Derived from the run tag, so
// replaying a tag reproduces the same collisions.
func changeShard(tag string, folders, change int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("shard#%s#%d", tag, change)))
	return fmt.Sprintf("%02d", int(sum[0])%folders)
}

// changeFilePath returns the repository path for one file of one change.
//
// The leaf name carries the run tag, the change index and the file index, which
// is what makes it unique: no two files in a run, and no two runs against the
// same repository, can ever name the same path. That uniqueness is load-bearing
// — see createAndEnqueue — and it is the leaf that carries it, not the
// directory. Two changes deliberately share a directory (see shardDirs) so the
// conflict analyzer has something to find, and they still never write the same
// file, so a shared directory serializes them rather than making them collide.
func changeFilePath(tag string, folders, change, file int) string {
	leaf := fmt.Sprintf("%s-%d-%d.txt", tag, change, file)
	return strings.Join([]string{"demo", changeShard(tag, folders, change), leaf}, "/")
}

// changeFileCount returns how many files a change touches: at least min, varied
// a little so a run does not produce a row of identically shaped changes.
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

// createAndEnqueue creates the changes and puts them on the queue, filling
// in the tracker's rows as it goes and reporting each step beneath the table.
//
// For independent changes the two steps interleave: each change is
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
// commit change rather than a single-line edit — which is both closer to
// a real change and enough to exercise replaying a range of commits.
func createAndEnqueue(
	ctx context.Context,
	src changeSource,
	sq *client.Client,
	cfg config,
	strategy mergestrategypb.Strategy,
	tag, baseSHA string,
	t *client.Tracker,
) ([]change, error) {
	if cfg.stacked {
		return createStack(ctx, src, sq, cfg, strategy, tag, baseSHA, t)
	}
	return createIndependent(ctx, src, sq, cfg, strategy, tag, baseSHA, t)
}

// createIndependent creates the changes concurrently, up to the configured
// limit, enqueuing each the moment it exists.
//
// Independent changes have nothing to say to each other: each branches from the
// same base and writes files no other change touches, so the only reason to
// create them one at a time was that the loop did. Creating a change is
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
	src changeSource,
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
			c, err := createOne(groupCtx, src, cfg, tag, baseSHA, cfg.base, i, t, rows[i-1])
			if err != nil {
				return err
			}
			created[i-1] = c

			if !cfg.land {
				return nil
			}
			t.Note("enqueuing %s", c.label)
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

// createStack creates the changes one after another, each based on the one
// before it, and submits the whole chain as a single request.
//
// This one cannot be parallelized, and not for want of trying: a change is
// based on the branch of the change before it and must see its content, so the
// next branch cannot be cut until the previous head exists.
func createStack(
	ctx context.Context,
	src changeSource,
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
		c, err := createOne(ctx, src, cfg, tag, parentSHA, parentBranch, i, t, rows[0])
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

// createOne describes one change — its branch, and the files it writes — hands
// it to the source to be made real, and records it on the given row.
//
// What a change is made of is decided here rather than by the source, so the
// three providers put the same shape of change through the queue and differ
// only in how it comes to exist.
func createOne(
	ctx context.Context,
	src changeSource,
	cfg config,
	tag, parentSHA, parentBranch string,
	i int,
	t *client.Tracker,
	target *client.Row,
) (change, error) {
	branch := fmt.Sprintf("%s/%s/%d", cfg.prefix, tag, i)

	// Each file is its own commit, so a change arrives as a range of commits
	// rather than a single edit.
	fileCount := changeFileCount(tag, i, cfg.files)
	files := make([]changeFile, 0, fileCount)
	for k := 1; k <= fileCount; k++ {
		files = append(files, changeFile{
			path:    changeFilePath(tag, cfg.folders, i, k),
			body:    fmt.Sprintf("change %d of run %s\nfile %d of %d\n", i, tag, k, fileCount),
			message: fmt.Sprintf("demo change %d (run %s): file %d of %d", i, tag, k, fileCount),
		})
	}

	opened, err := src.open(ctx, changeSpec{
		branch:       branch,
		parentBranch: parentBranch,
		parentSHA:    parentSHA,
		title:        fmt.Sprintf("demo change %d (run %s)", i, tag),
		files:        files,
		note:         t.Note,
	})
	if err != nil {
		return change{}, err
	}

	t.Update(func() { target.Cells = append(target.Cells, opened.cell) })
	return change{
		label:   opened.cell.Text,
		url:     opened.cell.URL,
		branch:  branch,
		headSHA: opened.headSHA,
		uri:     opened.uri,
	}, nil
}

// urisOf is the change URIs the run pinned, in caller order.
func urisOf(cs []change) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.uri)
	}
	return out
}
