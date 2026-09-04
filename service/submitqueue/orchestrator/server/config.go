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

package main

import (
	"fmt"
	"os"
	"time"

	yamlv3 "gopkg.in/yaml.v3"
)

// Change provider types selectable from configuration.
const (
	changeProviderTypeFake        = "fake"
	changeProviderTypeGit         = "git"
	changeProviderTypeGitHub      = "github"
	changeProviderTypePhabricator = "phabricator"
	changeProviderTypeRouting     = "routing"
)

// Build runner types selectable from configuration.
const (
	buildRunnerTypeFake          = "fake"
	buildRunnerTypeGitHubActions = "githubactions"
	buildRunnerTypeBuildkite     = "buildkite"
)

// Conflict analyzer types selectable from configuration.
const (
	analyzerTypeAll         = "all"
	analyzerTypeNone        = "none"
	analyzerTypePathOverlap = "pathoverlap"
)

// Granularities a pathoverlap analyzer compares paths at.
const (
	pathOverlapByFile      = "file"
	pathOverlapByDirectory = "directory"
)

// Scorer types selectable from configuration.
const (
	scorerTypeHeuristic = "heuristic"
	scorerTypeComposite = "composite"
)

// maxBucket is the open upper bound of a size bucket, and defaultFlatScore the
// score a queue with no stated opinion gives every batch.
const (
	maxBucket        = 1<<31 - 1
	defaultFlatScore = 0.5
)

// Ways a composite scorer combines its components.
const combineAvg = "avg"

// defaultBuildBudget is how many builds a queue may have occupying CI at once
// when it states no budget of its own. Four is enough for speculation to be
// visible — a queue that can only build one path never speculates — while
// staying well inside what a modest CI pool absorbs.
const defaultBuildBudget = 4

// Defaults for the provider integrations, matching each vendor's convention.
const (
	// defaultGitRemote and defaultGitTokenUser match git's own convention and
	// the username a forge expects a token to be presented under.
	defaultGitRemote      = "origin"
	defaultGitTokenUser   = "x-access-token"
	defaultGitHubTokenEnv = "GITHUB_TOKEN"
	defaultGitHubBaseURL  = "https://api.github.com"
	defaultPhabTokenEnv   = "PHAB_API_TOKEN"
	defaultHTTPTimeout    = 30 * time.Second
)

// profilesConfig selects, per queue, which implementation of each extension the
// orchestrator wires.
//
// It carries no secret: every integration names the *environment variable*
// holding its credential, so the file stays committable and a deployment
// rotates a token without editing it. `type` is an open string rather than a
// closed set in the schema, so supporting a new provider is a new value here and
// a new implementation behind it — not a change to the shape of this file.
type profilesConfig struct {
	// Defaults applies to any queue without an override for a given extension.
	Defaults queueProfileConfig `yaml:"defaults"`
	// Queues holds per-queue overrides.
	Queues []namedQueueProfileConfig `yaml:"queues"`
}

// namedQueueProfileConfig is one queue's overrides. Each extension is
// independently optional: an absent block inherits that one extension from the
// defaults, so a queue that differs only in its analyzer says only that.
type namedQueueProfileConfig struct {
	Name           string                `yaml:"name"`
	ChangeProvider *changeProviderConfig `yaml:"changeProvider"`
	BuildRunner    *buildRunnerConfig    `yaml:"buildRunner"`
	Analyzer       *analyzerConfig       `yaml:"analyzer"`
	Scorer         *scorerConfig         `yaml:"scorer"`
	Speculator     *speculatorConfig     `yaml:"speculator"`
}

// queueProfileConfig is the full set of extensions a queue resolves to.
type queueProfileConfig struct {
	ChangeProvider changeProviderConfig `yaml:"changeProvider"`
	BuildRunner    buildRunnerConfig    `yaml:"buildRunner"`
	Analyzer       analyzerConfig       `yaml:"analyzer"`
	Scorer         scorerConfig         `yaml:"scorer"`
	Speculator     speculatorConfig     `yaml:"speculator"`
}

// changeProviderConfig selects how change metadata is fetched. The github and
// phabricator blocks are shared by the single-provider types and by routing, which
// dispatches between them on the change URI's scheme.
type changeProviderConfig struct {
	Type        string                `yaml:"type"`
	Git         *gitProviderConfig    `yaml:"git"`
	GitHub      *githubProviderConfig `yaml:"github"`
	Phabricator *phabProviderConfig   `yaml:"phabricator"`
}

// gitProviderConfig configures the git change provider, which derives change
// metadata from a repository rather than asking a service for it.
//
// It mirrors Runway's lander block deliberately: each service keeps its own
// copy of a queue's repository and says for itself where that copy fetches
// from, so the two are configured independently even when they name the same
// remote.
type gitProviderConfig struct {
	// RemoteURL is where this service's copy fetches from. A URL or a local
	// path; a bind-mounted bare repository and a remote host differ only here.
	RemoteURL string `yaml:"remoteUrl"`
	// Remote is the name the copy records RemoteURL under.
	Remote string `yaml:"remote"`
	// Target is the branch a change's first commit is measured against.
	Target string `yaml:"target"`
	// RepoPath is where this service keeps its copy. It belongs to this service
	// alone — another service reading the same remote keeps its own.
	RepoPath string `yaml:"repoPath"`
	// TokenEnv names the environment variable holding a credential for an
	// http(s) remote. Empty means the remote needs none, which is the case for a
	// local path and for SSH served by the host's own configuration.
	TokenEnv string `yaml:"tokenEnv"`
	// TokenUser is the username the credential is presented under.
	TokenUser string `yaml:"tokenUser"`
}

// githubProviderConfig configures the GitHub change provider.
type githubProviderConfig struct {
	// TokenEnv names the environment variable holding the API token.
	TokenEnv string `yaml:"tokenEnv"`
	// BaseURL is the GitHub API root, for GitHub Enterprise.
	BaseURL string `yaml:"baseUrl"`
	// Timeout bounds each API call, as a Go duration ("30s").
	Timeout string `yaml:"timeout"`
}

// phabProviderConfig configures the Phabricator change provider.
type phabProviderConfig struct {
	TokenEnv string `yaml:"tokenEnv"`
	// Endpoint is the Conduit API root.
	Endpoint string `yaml:"endpoint"`
	Timeout  string `yaml:"timeout"`
}

// buildRunnerConfig selects how a batch's build is triggered and polled.
type buildRunnerConfig struct {
	Type string `yaml:"type"`
	// TokenEnv names the environment variable holding the API token.
	TokenEnv string `yaml:"tokenEnv"`
	// BaseURL is the API root. For githubactions it defaults to the public
	// GitHub API; for buildkite it is the pipeline's builds endpoint and is
	// required, since a Buildkite client is bound to one pipeline by its URL.
	BaseURL string `yaml:"baseUrl"`
	Timeout string `yaml:"timeout"`
	// Owner, Repo, and Workflow identify the workflow to dispatch
	// (githubactions only). Ref is the branch the workflow is read from.
	Owner    string `yaml:"owner"`
	Repo     string `yaml:"repo"`
	Workflow string `yaml:"workflow"`
	Ref      string `yaml:"ref"`
	// ExtraInputs are passed to every dispatch alongside the reserved sq_*
	// inputs (githubactions only).
	ExtraInputs map[string]string `yaml:"extraInputs"`
}

// analyzerConfig selects how conflicts between concurrent batches are detected.
type analyzerConfig struct {
	Type string `yaml:"type"`
	// By is the granularity a pathoverlap analyzer compares paths at: "file"
	// conflicts only batches changing the same file, "directory" coarsens that
	// to a shared parent directory. Defaults to "file"; ignored by other types.
	By string `yaml:"by"`
	// FailAlways makes every analysis return an error, for exercising the
	// analyzer failure path. Test-only.
	FailAlways bool `yaml:"failAlways"`
}

// scorerConfig selects how a queue ranks candidate speculation paths. There is
// no scoring stage: the scorer feeds the queue's speculator, which is composed
// from it rather than configured separately.
type scorerConfig struct {
	Type string `yaml:"type"`
	// Buckets map a batch's total lines changed onto a score (heuristic only).
	Buckets []bucketConfig `yaml:"buckets"`
	// Components are the scorers a composite combines, keyed by name.
	Components map[string]scorerConfig `yaml:"components"`
	// Combine names how a composite reduces its components. Defaults to "avg".
	Combine string `yaml:"combine"`
}

// bucketConfig is one band of a heuristic scorer: batches whose size falls in
// [Min, Max] score Score.
type bucketConfig struct {
	Min   int     `yaml:"min"`
	Max   int     `yaml:"max"`
	Score float64 `yaml:"score"`
}

// speculatorConfig tunes how much CI a queue's speculation may occupy. It has no
// `type`: there is one speculator, composed from the queue's scorer, and what
// varies between queues is what it is allowed to spend.
type speculatorConfig struct {
	// BuildBudget caps how many builds this queue may have occupying CI at once,
	// counted across every in-flight batch rather than per batch. Absent or 0
	// takes defaultBuildBudget; must not be negative.
	BuildBudget int `yaml:"buildBudget"`
}

// loadProfilesConfig reads and validates the profiles configuration at path.
func loadProfilesConfig(path string) (profilesConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return profilesConfig{}, fmt.Errorf("failed to read profiles config %q: %w", path, err)
	}

	var cfg profilesConfig
	if err := yamlv3.Unmarshal(data, &cfg); err != nil {
		return profilesConfig{}, fmt.Errorf("failed to parse profiles config %q: %w", path, err)
	}
	if err := cfg.normalizeAndValidate(); err != nil {
		return profilesConfig{}, fmt.Errorf("invalid profiles config %q: %w", path, err)
	}
	return cfg, nil
}

// normalizeAndValidate applies defaults and rejects a configuration that could
// not produce working extensions.
func (c *profilesConfig) normalizeAndValidate() error {
	if err := c.Defaults.normalizeAndValidate("defaults"); err != nil {
		return err
	}

	seen := make(map[string]bool, len(c.Queues))
	for i := range c.Queues {
		q := &c.Queues[i]
		if q.Name == "" {
			return fmt.Errorf("queue entry %d has an empty name", i)
		}
		if seen[q.Name] {
			return fmt.Errorf("queue %q appears more than once", q.Name)
		}
		seen[q.Name] = true

		where := fmt.Sprintf("queue %q", q.Name)
		if q.ChangeProvider != nil {
			if err := q.ChangeProvider.normalizeAndValidate(where); err != nil {
				return err
			}
		}
		if q.BuildRunner != nil {
			if err := q.BuildRunner.normalizeAndValidate(where); err != nil {
				return err
			}
		}
		if q.Analyzer != nil {
			if err := q.Analyzer.normalizeAndValidate(where); err != nil {
				return err
			}
		}
		if q.Scorer != nil {
			if err := q.Scorer.normalizeAndValidate(where); err != nil {
				return err
			}
		}
		if q.Speculator != nil {
			if err := q.Speculator.normalizeAndValidate(where); err != nil {
				return err
			}
		}
	}
	return c.validateGitRepoPaths()
}

// validateGitRepoPaths rejects two queues keeping their copies in one directory
// while disagreeing about what that copy is.
//
// A shared path is legitimate — two queues on the same repository should share
// one copy, and sharing it is what makes them share its lock. Sharing it with a
// different remote or target is not: whichever queue was built first silently
// decides what the other one reads.
func (c profilesConfig) validateGitRepoPaths() error {
	type owner struct {
		queue string
		cfg   gitProviderConfig
	}
	byPath := make(map[string]owner)

	for _, q := range c.Queues {
		resolved := c.resolve(q)
		if resolved.ChangeProvider.Type != changeProviderTypeGit || resolved.ChangeProvider.Git == nil {
			continue
		}
		git := *resolved.ChangeProvider.Git
		previous, seen := byPath[git.RepoPath]
		if !seen {
			byPath[git.RepoPath] = owner{queue: q.Name, cfg: git}
			continue
		}
		if previous.cfg.RemoteURL != git.RemoteURL || previous.cfg.Target != git.Target {
			return fmt.Errorf(
				"queues %q and %q share repoPath %q but read different repositories (%s@%s vs %s@%s); give each its own path",
				previous.queue, q.Name, git.RepoPath,
				previous.cfg.RemoteURL, previous.cfg.Target, git.RemoteURL, git.Target)
		}
	}
	return nil
}

// resolve returns the full profile for a queue: its own overrides where it has
// them, the defaults everywhere else.
func (c profilesConfig) resolve(q namedQueueProfileConfig) queueProfileConfig {
	profile := c.Defaults
	if q.ChangeProvider != nil {
		profile.ChangeProvider = *q.ChangeProvider
	}
	if q.BuildRunner != nil {
		profile.BuildRunner = *q.BuildRunner
	}
	if q.Analyzer != nil {
		profile.Analyzer = *q.Analyzer
	}
	if q.Scorer != nil {
		profile.Scorer = *q.Scorer
	}
	if q.Speculator != nil {
		profile.Speculator = *q.Speculator
	}
	return profile
}

func (p *queueProfileConfig) normalizeAndValidate(where string) error {
	if err := p.ChangeProvider.normalizeAndValidate(where); err != nil {
		return err
	}
	if err := p.BuildRunner.normalizeAndValidate(where); err != nil {
		return err
	}
	if err := p.Analyzer.normalizeAndValidate(where); err != nil {
		return err
	}
	if err := p.Scorer.normalizeAndValidate(where); err != nil {
		return err
	}
	return p.Speculator.normalizeAndValidate(where)
}

func (c *changeProviderConfig) normalizeAndValidate(where string) error {
	if c.Type == "" {
		c.Type = changeProviderTypeFake
	}
	switch c.Type {
	case changeProviderTypeFake:
		return nil
	case changeProviderTypeGit:
		return c.ensureGit(where)
	case changeProviderTypeGitHub:
		c.ensureGitHub()
	case changeProviderTypePhabricator:
		if err := c.ensurePhabricator(where); err != nil {
			return err
		}
	case changeProviderTypeRouting:
		// Routing dispatches on the URI scheme, so it needs at least one provider
		// to dispatch to; which ones is up to the deployment.
		if c.GitHub == nil && c.Phabricator == nil {
			return fmt.Errorf("%s: routing change provider needs a github or phabricator block", where)
		}
		if c.GitHub != nil {
			c.ensureGitHub()
		}
		if c.Phabricator != nil {
			if err := c.ensurePhabricator(where); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s: unknown change provider type %q", where, c.Type)
	}
	return nil
}

// ensureGit defaults and checks the git block. Three values have no sensible
// default and are required: there is no public git remote to fall back on, no
// universal trunk name, and nowhere obvious to keep a copy.
func (c *changeProviderConfig) ensureGit(where string) error {
	if c.Git == nil {
		c.Git = &gitProviderConfig{}
	}
	if c.Git.Remote == "" {
		c.Git.Remote = defaultGitRemote
	}
	if c.Git.TokenUser == "" {
		c.Git.TokenUser = defaultGitTokenUser
	}
	if c.Git.RemoteURL == "" {
		return fmt.Errorf("%s: git change provider requires remoteUrl", where)
	}
	if c.Git.Target == "" {
		return fmt.Errorf("%s: git change provider requires target", where)
	}
	if c.Git.RepoPath == "" {
		return fmt.Errorf("%s: git change provider requires repoPath", where)
	}
	return nil
}

func (c *changeProviderConfig) ensureGitHub() {
	if c.GitHub == nil {
		c.GitHub = &githubProviderConfig{}
	}
	if c.GitHub.TokenEnv == "" {
		c.GitHub.TokenEnv = defaultGitHubTokenEnv
	}
	if c.GitHub.BaseURL == "" {
		c.GitHub.BaseURL = defaultGitHubBaseURL
	}
}

func (c *changeProviderConfig) ensurePhabricator(where string) error {
	if c.Phabricator == nil {
		c.Phabricator = &phabProviderConfig{}
	}
	if c.Phabricator.TokenEnv == "" {
		c.Phabricator.TokenEnv = defaultPhabTokenEnv
	}
	if c.Phabricator.Endpoint == "" {
		return fmt.Errorf("%s: phabricator change provider requires an endpoint", where)
	}
	return nil
}

func (b *buildRunnerConfig) normalizeAndValidate(where string) error {
	if b.Type == "" {
		b.Type = buildRunnerTypeFake
	}
	switch b.Type {
	case buildRunnerTypeFake:
		return nil
	case buildRunnerTypeGitHubActions:
		if b.Owner == "" || b.Repo == "" || b.Workflow == "" {
			return fmt.Errorf("%s: githubactions build runner requires owner, repo, and workflow", where)
		}
		if b.TokenEnv == "" {
			b.TokenEnv = defaultGitHubTokenEnv
		}
		if b.BaseURL == "" {
			b.BaseURL = defaultGitHubBaseURL
		}
	case buildRunnerTypeBuildkite:
		// A Buildkite client is bound to one pipeline by its base URL, so there
		// is no meaningful default to fall back on.
		if b.BaseURL == "" {
			return fmt.Errorf("%s: buildkite build runner requires baseUrl (the pipeline's builds endpoint)", where)
		}
		if b.TokenEnv == "" {
			return fmt.Errorf("%s: buildkite build runner requires tokenEnv", where)
		}
	default:
		return fmt.Errorf("%s: unknown build runner type %q", where, b.Type)
	}
	return nil
}

func (a *analyzerConfig) normalizeAndValidate(where string) error {
	if a.Type == "" {
		a.Type = analyzerTypeAll
	}
	switch a.Type {
	case analyzerTypeAll, analyzerTypeNone:
		return nil
	case analyzerTypePathOverlap:
		if a.By == "" {
			a.By = pathOverlapByFile
		}
		if a.By != pathOverlapByFile && a.By != pathOverlapByDirectory {
			return fmt.Errorf("%s: unknown pathoverlap granularity %q", where, a.By)
		}
		return nil
	default:
		return fmt.Errorf("%s: unknown analyzer type %q", where, a.Type)
	}
}

// normalizeAndValidate applies defaults and rejects a scorer that could not be
// built. An empty block is a flat heuristic: every batch scores the same, which
// is the neutral choice for a queue with no opinion about ordering.
func (s *scorerConfig) normalizeAndValidate(where string) error {
	if s.Type == "" {
		s.Type = scorerTypeHeuristic
	}
	switch s.Type {
	case scorerTypeHeuristic:
		if len(s.Buckets) == 0 {
			s.Buckets = []bucketConfig{{Min: 0, Max: maxBucket, Score: defaultFlatScore}}
		}
		for _, b := range s.Buckets {
			if b.Min > b.Max {
				return fmt.Errorf("%s: scorer bucket min %d exceeds max %d", where, b.Min, b.Max)
			}
			if b.Score < 0 || b.Score > 1 {
				return fmt.Errorf("%s: scorer bucket score %v is outside [0,1]", where, b.Score)
			}
		}
	case scorerTypeComposite:
		if len(s.Components) == 0 {
			return fmt.Errorf("%s: composite scorer needs at least one component", where)
		}
		for name, component := range s.Components {
			if err := component.normalizeAndValidate(fmt.Sprintf("%s component %q", where, name)); err != nil {
				return err
			}
			s.Components[name] = component
		}
		if s.Combine == "" {
			s.Combine = combineAvg
		}
		if s.Combine != combineAvg {
			return fmt.Errorf("%s: unknown composite combine %q", where, s.Combine)
		}
	default:
		return fmt.Errorf("%s: unknown scorer type %q", where, s.Type)
	}
	return nil
}

func (s *speculatorConfig) normalizeAndValidate(where string) error {
	// A negative budget is rejected rather than clamped: sticky would compute no
	// free slots from it, so the queue would batch and then never build anything,
	// which looks like a stuck queue rather than a misconfigured one.
	if s.BuildBudget < 0 {
		return fmt.Errorf("%s: build budget %d is negative", where, s.BuildBudget)
	}
	if s.BuildBudget == 0 {
		s.BuildBudget = defaultBuildBudget
	}
	return nil
}

// timeoutOr parses a Go duration string, falling back when it is empty or
// unparseable — a bad value should not stop the service from starting.
func timeoutOr(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return d
}

// tokenFrom reads the credential the named environment variable holds,
// reporting whether it was both set and non-empty.
func tokenFrom(env string) (string, bool) {
	if env == "" {
		return "", false
	}
	v, ok := os.LookupEnv(env)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}
