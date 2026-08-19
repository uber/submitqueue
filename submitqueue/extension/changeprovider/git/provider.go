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

// Package git provides a changeprovider.ChangeProvider that reads change
// metadata out of a git repository, for a remote that offers no API to ask.
//
// Where the GitHub and Phabricator providers query a service that already knows
// what a change contains, this one derives it: it keeps its own copy of the
// remote and computes each change's files, line counts and author from the
// commits themselves. That makes a plain git remote — an internal host, a
// mirror, a bare repository on disk — a first-class source of change metadata
// with no service in front of it.
//
// # What a change is measured against
//
// A git:// change URI names a commit and the ref it lives on, and nothing else.
// Unlike a pull request it carries no base, so the baseline has to be derived,
// and for a stack it cannot be the target branch: a stack's changes are cut one
// from the next, so measuring each against the target would report the second
// change as containing the first as well. Each change is therefore measured
// from where it diverged from the change before it, and only the first from the
// target. Callers get per-change numbers that sum, which is what any consumer
// aggregating over a batch depends on.
package git

import (
	"context"
	"fmt"
	"sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	changegit "github.com/uber/submitqueue/platform/base/change/git"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

const opName = "git_changeprovider"

// Repository is the local copy this provider reads through. It is the git
// plumbing the provider needs and nothing more: fetching, commit resolution,
// merge bases, and a lock the provider holds across a read so a shared copy's
// object set stays consistent. platform/git/repo.Repo satisfies it.
type Repository interface {
	sync.Locker
	FetchTarget(ctx context.Context) error
	EnsureCommit(ctx context.Context, sha, ref string) error
	MergeBase(ctx context.Context, a, b string) (string, error)
	// RunRaw returns git's stdout untrimmed, since the diff and author formats
	// this provider reads are NUL-delimited with a meaningful trailing byte.
	RunRaw(ctx context.Context, args ...string) (string, error)
	Remote() string
	Target() string
}

// Params carries what a provider needs. The Repository is built once per
// repository and shared by every queue reading it.
type Params struct {
	Config       changeprovider.Config
	Repo         Repository
	Logger       *zap.SugaredLogger
	MetricsScope tally.Scope
}

// provider reads change metadata from a local copy of a git remote.
type provider struct {
	cfg          changeprovider.Config
	repo         Repository
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// New returns a changeprovider.ChangeProvider reading from repo.
func New(params Params) changeprovider.ChangeProvider {
	return &provider{
		cfg:          params.Config,
		repo:         params.Repo,
		logger:       params.Logger.Named(opName),
		metricsScope: params.MetricsScope.SubScope(opName),
	}
}

// Get returns one ChangeInfo per URI, in the order the URIs were given.
//
// The order is load-bearing: it is the stack order, and each change after the
// first is measured from the one before it.
func (p *provider) Get(ctx context.Context, request entity.Request) (_ []entity.ChangeInfo, retErr error) {
	op := coremetrics.Begin(p.metricsScope, "get", coremetrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	uris := request.Change.URIs
	infos := make([]entity.ChangeInfo, 0, len(uris))

	p.repo.Lock()
	defer p.repo.Unlock()

	if err := p.repo.FetchTarget(ctx); err != nil {
		coremetrics.NamedCounter(p.metricsScope, "get", "fetch_errors", 1)
		return nil, fmt.Errorf("failed to update target branch %s: %w", p.repo.Target(), err)
	}

	previous := ""
	for _, uri := range uris {
		id, err := changegit.ParseChangeID(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse change URI: %w", err)
		}
		if err := p.repo.EnsureCommit(ctx, id.CommitSHA, id.Ref); err != nil {
			coremetrics.NamedCounter(p.metricsScope, "get", "commit_unavailable", 1)
			return nil, err
		}

		// The first change stands on the target; each one after it stands on the
		// change before it.
		against := p.repo.Remote() + "/" + p.repo.Target()
		if previous != "" {
			against = previous
		}

		details, err := p.describe(ctx, against, id.CommitSHA)
		if err != nil {
			return nil, fmt.Errorf("failed to describe change %s: %w", uri, err)
		}

		infos = append(infos, entity.ChangeInfo{URI: uri, Details: details})
		previous = id.CommitSHA
	}
	return infos, nil
}

// describe reports what sha changed relative to where it diverged from against.
func (p *provider) describe(ctx context.Context, against, sha string) (entity.ChangeDetails, error) {
	base, err := p.repo.MergeBase(ctx, against, sha)
	if err != nil {
		return entity.ChangeDetails{}, err
	}

	// -M so a rename reads as one moved file rather than a whole file deleted
	// and another added; the scrubbed environment leaves git's own default off.
	raw, err := p.repo.RunRaw(ctx, "diff", "--numstat", "-M", "-z", base, sha)
	if err != nil {
		return entity.ChangeDetails{}, err
	}
	files, err := parseNumstat(raw)
	if err != nil {
		return entity.ChangeDetails{}, err
	}

	author, err := p.author(ctx, sha)
	if err != nil {
		return entity.ChangeDetails{}, err
	}
	return entity.ChangeDetails{Author: author, ChangedFiles: files}, nil
}

// author reads the commit's author, NUL-separated because a display name can
// contain anything a friendlier separator would collide with.
func (p *provider) author(ctx context.Context, sha string) (entity.Author, error) {
	out, err := p.repo.RunRaw(ctx, "show", "--no-patch", "--format=%an%x00%ae", sha)
	if err != nil {
		return entity.Author{}, err
	}
	name, email, found := cut(out)
	if !found {
		return entity.Author{}, fmt.Errorf("unreadable author for commit %s", sha)
	}
	return entity.Author{Name: name, Email: email}, nil
}

// cut splits the author format's two fields, trimming the newline git appends.
func cut(out string) (name, email string, found bool) {
	for i := 0; i < len(out); i++ {
		if out[i] == 0 {
			return out[:i], trimNewline(out[i+1:]), true
		}
	}
	return "", "", false
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}
