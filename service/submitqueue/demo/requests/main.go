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

// Command requests supplies OSS providers and transport to the shared demo runner.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	gitexec "github.com/uber/submitqueue/platform/git/exec"
	"github.com/uber/submitqueue/service/submitqueue/demo"
	"github.com/uber/submitqueue/submitqueue/client"
)

func main() {
	cfg := parseFlags()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
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
	burst       bool
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
		"git/github: how many folders to spread the changes across; 0 picks one per run. Changes sharing a folder are batched in order, changes in different folders go out together")
	flag.IntVar(&c.files, "files", 3,
		"fewest files each change touches; the actual count varies a little above it. Ignored by -provider fake, which synthesizes its own paths")
	flag.IntVar(&c.concurrency, "concurrency", 5,
		"how many changes to create at once; a stack ignores it, being sequential by nature, and -provider git serializes its git commands")
	flag.BoolVar(&c.stacked, "stacked", false, "chain the changes and enqueue them as one stack")
	flag.BoolVar(&c.burst, "burst", false,
		"create every change first, then enqueue them all at once instead of as each is created; independent changes only")
	flag.StringVar(&c.prefix, "prefix", "demo", "branch name prefix")
	flag.BoolVar(&c.land, "land", true, "enqueue each change as it is created")
	flag.BoolVar(&c.watch, "watch", true, "watch the requests until they all settle")
	flag.StringVar(&c.addr, "addr", "localhost:8081", "gateway address")
	flag.BoolVar(&c.tls, "tls", false, "dial the gateway with transport security")
	flag.StringVar(&c.tokenEnv, "token-env", client.DefaultTokenEnv, "environment variable holding the gateway bearer token")
	flag.StringVar(&c.queue, "queue", "demo-queue", "queue to land on")
	flag.StringVar(&c.strategy, "strategy", "SQUASH_REBASE", "land strategy")
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
	var gateway demo.Gateway
	if cfg.land {
		sq, err := client.New(client.Options{Addr: cfg.addr, TLS: cfg.tls, TokenEnv: cfg.tokenEnv})
		if err != nil {
			return err
		}
		defer sq.Close()
		gateway = sq
	}
	fmt.Printf("Creating %d change(s) via %s — %s\n\n", cfg.count, target(cfg), shape(cfg))
	result, err := demo.Run(ctx, demo.Options{
		Count: cfg.count, Folders: cfg.folders, Files: cfg.files, Concurrency: cfg.concurrency,
		Stacked: cfg.stacked, Burst: cfg.burst, Prefix: cfg.prefix, Land: cfg.land, Watch: cfg.watch,
		Queue: cfg.queue, Strategy: strategy,
	}, demo.Dependencies{Source: pinnedSource{source: src, branch: cfg.base, sha: baseSHA}, Gateway: gateway})
	hinted := false
	if !cfg.land && (!cfg.stacked || err == nil) {
		var pinned []demo.Change
		for _, change := range result.Changes {
			if change.URI != "" && change.HeadSHA != "" {
				pinned = append(pinned, change)
			}
		}
		if len(pinned) > 0 {
			fmt.Printf("\nEnqueue them with:\n  %s\n", enqueueHint(pinned))
			hinted = true
		}
	}
	if err != nil {
		printArtifacts(result, hinted)
	}
	return err
}

func printArtifacts(result demo.RunResult, pinnedHinted bool) {
	for _, change := range result.Changes {
		if pinnedHinted && change.URI != "" && change.HeadSHA != "" {
			continue
		}
		fmt.Printf("change: %s %s\n", change.Label, change.URI)
	}
	for _, request := range result.Requests {
		fmt.Printf("request: %s (last observed: %s)\n", request.ID, request.Status)
	}
}

func shape(cfg config) string {
	if cfg.stacked {
		return "stacked, enqueued as one request once the chain exists"
	}
	if cfg.burst && cfg.land {
		return fmt.Sprintf("independent, created %d at a time, then all enqueued at once", cfg.concurrency)
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

func enqueueHint(changes []demo.Change) string {
	values := make([]string, len(changes))
	for i, c := range changes {
		if c.URL != "" {
			values[i] = c.URL
		} else {
			values[i] = c.URI
		}
	}
	flag := "URIS"
	if len(changes) > 0 && changes[0].URL != "" {
		flag = "PRS"
	}
	return fmt.Sprintf("make land %s=%q", flag, strings.Join(values, " "))
}
