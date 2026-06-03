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

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	entitygithub "github.com/uber/submitqueue/submitqueue/entity/github"
	"github.com/uber/submitqueue/submitqueue/extension/mergechecker"
	"go.uber.org/zap"
)

// Params holds the dependencies for the GitHub MergeChecker.
type Params struct {
	// HTTPClient is a pre-configured HTTP client. The caller is responsible for
	// configuring the base URL (via BaseURLTransport) and auth (via a transport layer).
	HTTPClient *http.Client
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

// mergeChecker implements the mergechecker.MergeChecker interface using the GitHub GraphQL API.
type mergeChecker struct {
	httpClient   *http.Client
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// Verify mergeChecker implements mergechecker.MergeChecker at compile time.
var _ mergechecker.MergeChecker = (*mergeChecker)(nil)

// NewMergeChecker creates a new GitHub MergeChecker.
func NewMergeChecker(params Params) mergechecker.MergeChecker {
	return &mergeChecker{
		httpClient:   params.HTTPClient,
		logger:       params.Logger.Named("github_mergechecker"),
		metricsScope: params.MetricsScope.SubScope("github_mergechecker"),
	}
}

// Check assesses whether a change can merge cleanly using the GitHub GraphQL API.
func (c *mergeChecker) Check(ctx context.Context, change entity.Change) (result mergechecker.Result, retErr error) {
	const opName = "check"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	// Parse all change IDs
	// TODO: classify parse errors as user errors (non-retryable) vs system errors.
	changeIDs := make([]entitygithub.ChangeID, 0, len(change.URIs))
	for _, rawID := range change.URIs {
		cid, err := entitygithub.ParseChangeID(rawID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "parse_errors", 1)
			return result, fmt.Errorf("failed to parse change ID %q: %w", rawID, err)
		}
		changeIDs = append(changeIDs, cid)
	}

	// Fetch PR info from GitHub GraphQL API
	prInfoMap, err := c.fetchPRInfo(ctx, changeIDs)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "graphql_errors", 1)
		return result, fmt.Errorf("failed to fetch PR info: %w", err)
	}

	// Validate PR mergeability
	mergeable, reason, err := validatePRs(changeIDs, prInfoMap)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "validation_errors", 1)
		return result, err
	}

	if !mergeable {
		metrics.NamedCounter(c.metricsScope, opName, "not_mergeable", 1)
		c.logger.Infow("change not mergeable",
			"reason", reason,
			"change_uris", change.URIs,
		)
	} else {
		metrics.NamedCounter(c.metricsScope, opName, "mergeable", 1)
	}

	result.Mergeable = mergeable
	result.Reason = reason
	return result, nil
}

// fetchPRInfo executes a batched GraphQL query to fetch PR info for all change IDs.
func (c *mergeChecker) fetchPRInfo(ctx context.Context, changeIDs []entitygithub.ChangeID) (map[int]PRInfo, error) {
	query := buildGraphQLQuery(changeIDs)

	reqBody, err := json.Marshal(graphQLRequest{Query: query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "/graphql", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("graphql request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read graphql response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("graphql request returned status %d: %s", resp.StatusCode, string(body))
	}

	return parseGraphQLResponse(body, changeIDs)
}
