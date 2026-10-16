// Copyright (c) 2026 Uber Technologies, Inc.
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

// Package github provides a validator.Validator that rejects a request whose
// GitHub pull requests the repository's own rules would not let merge yet: a
// review is missing or requests changes, a required check failed or is still
// running, or GitHub otherwise reports merging as blocked. It reads that state
// from GitHub's GraphQL API, so the rules are whatever branch protection or
// rulesets the repository configures; nothing is restated in SubmitQueue
// configuration. See README.md for what it leaves to other stages.
package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/validator"
)

// Params holds the dependencies for the GitHub validator.
type Params struct {
	// Config is the per-queue identity handed to the Factory that built this
	// validator.
	Config validator.Config
	// HTTPClient roots relative paths at the GitHub API (for example
	// platform/http.NewClient("https://api.github.com")) and authenticates
	// them. The caller owns the base URL and the credential.
	HTTPClient *http.Client
	// Host is the GitHub instance change URIs must name (e.g. "github.com"):
	// the one HTTPClient talks to.
	Host string
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

type githubValidator struct {
	cfg          validator.Config
	httpClient   *http.Client
	host         string
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// New returns a validator bound to the queue named in params.Config. It errors
// when HTTPClient or Host is missing.
func New(params Params) (validator.Validator, error) {
	if params.HTTPClient == nil {
		return nil, errors.New("github validator: HTTPClient is required")
	}
	if params.Host == "" {
		return nil, errors.New("github validator: Host is required")
	}
	return &githubValidator{
		cfg:          params.Config,
		httpClient:   params.HTTPClient,
		host:         params.Host,
		logger:       params.Logger.Named("github_validator"),
		metricsScope: params.MetricsScope.SubScope("github_validator").Tagged(map[string]string{"queue": params.Config.QueueName}),
	}, nil
}

// Validate checks the request's GitHub pull requests bottom first and returns
// the first rule one breaks. A failure to read GitHub is returned wrapped,
// keeping an HTTP status code in the chain for platform/errs/http. URIs that
// are not GitHub change IDs are skipped, so this validator composes with
// validators for other change providers.
func (v *githubValidator) Validate(ctx context.Context, request entity.Request) error {
	op := coremetrics.Begin(v.metricsScope, "validate", coremetrics.LongLatencyBuckets)
	rej, rejected, err := v.findRejection(ctx, request)
	// A rejection is an expected outcome, so only a failure to read GitHub
	// counts against the operation's result.
	op.Complete(err)
	if err != nil {
		return err
	}
	if !rejected {
		return nil
	}

	coremetrics.NamedCounter(v.metricsScope, "validate", "rejections", 1, coremetrics.NewTag("reason", string(rej.reason)))
	v.logger.Infow("rejecting request",
		"queue", v.cfg.QueueName,
		"request_id", request.ID,
		"uri", rej.uri,
		"reason", string(rej.reason),
		"detail", rej.detail,
	)
	return rej
}

func (v *githubValidator) findRejection(ctx context.Context, request entity.Request) (rejection, bool, error) {
	for _, uri := range request.Change.URIs {
		cid, err := entitygithub.ParseChangeID(uri)
		if err != nil {
			v.logger.Debugw("skipping non-GitHub URI", "uri", uri)
			continue
		}
		if cid.Host != v.host {
			return rejection{uri: cid.String(), reason: reasonHost, detail: fmt.Sprintf("names host %q, not %q", cid.Host, v.host)}, true, nil
		}

		pr, err := fetchPullRequestState(ctx, v.httpClient, cid)
		if err != nil {
			return rejection{}, false, fmt.Errorf("read pull request %s: %w", uri, err)
		}
		if rej, rejected := evaluatePullRequest(cid, pr); rejected {
			return rej, true, nil
		}
	}
	return rejection{}, false, nil
}
