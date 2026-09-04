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
	"reflect"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"

	landstrategypb "github.com/uber/submitqueue/api/base/landstrategy/protopb"
)

// Lander types selectable from configuration.
const (
	landerTypeNoop = "noop"
	landerTypeGit  = "git"
)

// landConfig is the runway land configuration file: which lander each queue
// gets, and how its land target is reached.
//
// It carries no secret. A git target names the *environment variable* holding
// its credential (tokenEnv), so the file stays committable and a deployment
// swaps the credential without editing it.
type landConfig struct {
	// Defaults applies to any queue without its own entry.
	Defaults queueLandConfig `yaml:"defaults"`
	// Queues holds per-queue overrides, keyed by queue name.
	Queues []namedQueueLandConfig `yaml:"queues"`
}

// namedQueueLandConfig is one queue's entry.
type namedQueueLandConfig struct {
	// Name is the queue this entry configures.
	Name string `yaml:"name"`
	// Lander replaces the default lander wholesale when present. Overriding is
	// per-block rather than per-field: a half-inherited git target, where the
	// remote comes from one place and the branch from another, is harder to
	// read than one stated outright.
	Lander *landerConfig `yaml:"lander"`
}

// queueLandConfig is the set of extensions a queue resolves to.
type queueLandConfig struct {
	// Lander performs the landability check and the committing land.
	Lander landerConfig `yaml:"lander"`
}

// landerConfig selects a lander implementation and configures it. Fields other
// than Type apply only to the git lander.
type landerConfig struct {
	// Type selects the implementation: "noop" or "git".
	Type string `yaml:"type"`
	// RemoteURL is the repository the lander clones and pushes to. When empty
	// the checkout is taken as provisioned by something else and used as it
	// stands — which is how an externally-managed working tree, or one mounted
	// into the container ready to go, is described.
	RemoteURL string `yaml:"remoteUrl"`
	// Remote is the git remote name for that URL. Defaults to "origin".
	Remote string `yaml:"remote"`
	// Target is the branch land operations apply to. Defaults to "main".
	Target string `yaml:"target"`
	// CheckoutPath is the working tree the lander owns. It is provisioned at
	// startup if absent, and must not be shared by two different targets.
	CheckoutPath string `yaml:"checkoutPath"`
	// DefaultStrategy resolves a step whose strategy is DEFAULT. Defaults to
	// REBASE.
	DefaultStrategy string `yaml:"defaultStrategy"`
	// CheckStaleness verifies each change still points at the commit its URI
	// names before applying it. Defaults to true.
	CheckStaleness *bool `yaml:"checkStaleness"`
	// UpdateHeadBranch moves each change's head branch to the commit it landed
	// as, so the provider marks it merged. Defaults to false.
	UpdateHeadBranch bool `yaml:"updateHeadBranch"`
	// AllowUnrelatedHistories lets a MERGE step integrate a change sharing no
	// ancestry with the target. Defaults to false.
	AllowUnrelatedHistories bool `yaml:"allowUnrelatedHistories"`
	// FetchRefspecs are extra refspecs fetched on every cycle, for a remote
	// that will not serve an unadvertised object by SHA.
	FetchRefspecs []string `yaml:"fetchRefspecs"`
	// MaxPushAttempts caps the reset/apply/push retry loop under contention.
	// Zero uses the lander's own default.
	MaxPushAttempts int `yaml:"maxPushAttempts"`
	// CommitterName and CommitterEmail identify commits the lander creates.
	CommitterName  string `yaml:"committerName"`
	CommitterEmail string `yaml:"committerEmail"`
	// TokenEnv names the environment variable holding the credential for an
	// HTTPS remote — never the credential itself. Empty means the remote needs
	// no token, which is the case for a local path and for SSH (where the
	// lander passes the agent socket through instead).
	TokenEnv string `yaml:"tokenEnv"`
	// TokenUser is the username paired with the token in basic auth. Providers
	// each have their own convention; defaults to "x-access-token".
	TokenUser string `yaml:"tokenUser"`
}

// loadLandConfig reads and validates the land configuration at path.
func loadLandConfig(path string) (landConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return landConfig{}, fmt.Errorf("failed to read land config %q: %w", path, err)
	}

	var cfg landConfig
	if err := yamlv3.Unmarshal(data, &cfg); err != nil {
		return landConfig{}, fmt.Errorf("failed to parse land config %q: %w", path, err)
	}

	if err := cfg.normalizeAndValidate(); err != nil {
		return landConfig{}, fmt.Errorf("invalid land config %q: %w", path, err)
	}
	return cfg, nil
}

// usesGit reports whether any configured queue resolves to the git lander, and
// so whether the process needs a git runtime at all.
func (c landConfig) usesGit() bool {
	if c.Defaults.Lander.Type == landerTypeGit {
		return true
	}
	for _, q := range c.Queues {
		if q.Lander != nil && q.Lander.Type == landerTypeGit {
			return true
		}
	}
	return false
}

// normalizeAndValidate applies defaults and rejects a configuration that could
// not produce working landers.
func (c *landConfig) normalizeAndValidate() error {
	if err := c.Defaults.Lander.normalizeAndValidate("defaults"); err != nil {
		return err
	}

	seen := make(map[string]bool, len(c.Queues))
	// Two queues may legitimately share one land target, but two *different*
	// targets sharing a working tree would have each lander reset and push the
	// other's work mid-flight. Only the second is an error, so the check is on
	// conflicting settings rather than on reuse.
	byCheckout := make(map[string]landerConfig)
	if c.Defaults.Lander.Type == landerTypeGit {
		byCheckout[c.Defaults.Lander.CheckoutPath] = c.Defaults.Lander
	}

	for i := range c.Queues {
		q := &c.Queues[i]
		if q.Name == "" {
			return fmt.Errorf("queue entry %d has an empty name", i)
		}
		if seen[q.Name] {
			return fmt.Errorf("queue %q appears more than once", q.Name)
		}
		seen[q.Name] = true

		if q.Lander == nil {
			continue
		}
		where := fmt.Sprintf("queue %q", q.Name)
		if err := q.Lander.normalizeAndValidate(where); err != nil {
			return err
		}
		if q.Lander.Type != landerTypeGit {
			continue
		}
		if prior, ok := byCheckout[q.Lander.CheckoutPath]; ok {
			if diff, differs := prior.firstDifference(*q.Lander); differs {
				return fmt.Errorf(
					"%s reuses checkout %q but differs from an earlier queue in %s; queues sharing a checkout share one lander instance, so give each configuration its own checkout",
					where, q.Lander.CheckoutPath, diff,
				)
			}
		}
		byCheckout[q.Lander.CheckoutPath] = *q.Lander
	}
	return nil
}

// normalizeAndValidate fills in defaults and checks the fields the selected
// type requires. where names the config location for error messages.
func (m *landerConfig) normalizeAndValidate(where string) error {
	if m.Type == "" {
		m.Type = landerTypeNoop
	}
	switch m.Type {
	case landerTypeNoop:
		return nil
	case landerTypeGit:
	default:
		return fmt.Errorf("%s: unknown lander type %q (want %q or %q)", where, m.Type, landerTypeNoop, landerTypeGit)
	}

	if m.CheckoutPath == "" {
		return fmt.Errorf("%s: git lander requires checkoutPath", where)
	}
	if m.TokenEnv != "" && m.RemoteURL == "" {
		return fmt.Errorf("%s: tokenEnv is set but remoteUrl is not, so there is no remote to authenticate to", where)
	}
	if m.Remote == "" {
		m.Remote = "origin"
	}
	if m.Target == "" {
		m.Target = "main"
	}
	if m.TokenUser == "" {
		m.TokenUser = "x-access-token"
	}
	if m.CheckStaleness == nil {
		enabled := true
		m.CheckStaleness = &enabled
	}
	// An explicit empty list and an omitted one both mean "no extra refspecs".
	// Collapsing them keeps two queues that say it differently from reading as
	// a difference when checkouts are compared.
	if len(m.FetchRefspecs) == 0 {
		m.FetchRefspecs = nil
	}
	if _, err := parseStrategy(m.DefaultStrategy); err != nil {
		return fmt.Errorf("%s: %w", where, err)
	}
	return nil
}

// firstDifference reports the first field in which two git landers disagree,
// rendered for an error message, and whether there was one.
//
// Every field is compared, not only the ones naming the target. Queues sharing
// a checkout share a single lander instance, built from whichever queue reached
// the builder first, so any field that instance is constructed from has to
// agree — a queue asking for SQUASH_REBASE and silently landing with REBASE
// writes the wrong history and reports nothing.
//
// The comparison is reflective rather than field by field so that a field added
// to landerConfig later is covered without anyone remembering to extend this.
// That is the failure mode worth designing against: the old three-field
// comparison was correct when the lander consumed three fields, and quietly
// stopped being correct as it grew to consume eleven.
func (m landerConfig) firstDifference(other landerConfig) (string, bool) {
	mine, theirs := reflect.ValueOf(m), reflect.ValueOf(other)
	t := mine.Type()
	for i := 0; i < t.NumField(); i++ {
		a, b := mine.Field(i), theirs.Field(i)
		if reflect.DeepEqual(a.Interface(), b.Interface()) {
			continue
		}
		name := t.Field(i).Tag.Get("yaml")
		if name == "" {
			name = t.Field(i).Name
		}
		return fmt.Sprintf("%s (%s vs %s)", name, renderLanderField(a), renderLanderField(b)), true
	}
	return "", false
}

// renderLanderField formats one field for an error message, following a pointer
// so an optional bool reads as its value rather than as an address.
func renderLanderField(v reflect.Value) string {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return "unset"
		}
		v = v.Elem()
	}
	return fmt.Sprintf("%v", v.Interface())
}

// provisions reports whether this target names a remote to build its checkout
// from, as opposed to relying on one that already exists.
func (m landerConfig) provisions() bool {
	return m.Type == landerTypeGit && m.RemoteURL != ""
}

// strategy returns the configured default strategy, already validated.
func (m landerConfig) strategy() landstrategypb.Strategy {
	s, _ := parseStrategy(m.DefaultStrategy)
	return s
}

// token reads the credential this target's TokenEnv names. Reporting whether
// the variable was set (rather than just returning "") keeps "no credential
// configured" distinct from "configured but empty", which is almost always a
// deployment mistake worth failing on.
func (m landerConfig) token() (string, bool) {
	if m.TokenEnv == "" {
		return "", false
	}
	return os.LookupEnv(m.TokenEnv)
}

// needsHTTPCredential reports whether this target is reached over HTTP(S) and
// so can carry a token. A local path or an SSH remote authenticates by other
// means, and writing an Authorization header for one would be inert at best.
func (m landerConfig) needsHTTPCredential() bool {
	if m.TokenEnv == "" {
		return false
	}
	lower := strings.ToLower(m.RemoteURL)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}
