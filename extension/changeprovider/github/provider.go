package github

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entity"
	entitygithub "github.com/uber/submitqueue/entity/github"
	"github.com/uber/submitqueue/extension/changeprovider"
)

// provider implements the ChangeProvider interface for GitHub.
type provider struct {
	client  *Client
	logger  *zap.SugaredLogger
	metrics tally.Scope
}

// NewProvider creates a new GitHub ChangeProvider.
// The caller is responsible for providing a fully-configured Client with authentication.
// Use NewAuthenticatedClient helper to create a client with bearer token auth.
//
// Parameters:
//   - client: Pre-configured GitHub API client (encapsulates HTTP client and GraphQL URL)
//   - logger: Structured logger
//   - metrics: Metrics scope
func NewProvider(
	client *Client,
	logger *zap.SugaredLogger,
	metrics tally.Scope,
) changeprovider.ChangeProvider {
	return &provider{
		client:  client,
		logger:  logger.Named("github_changeprovider"),
		metrics: metrics.SubScope("github_changeprovider"),
	}
}

// Get retrieves change information from GitHub for the provided Change.
// Returns one ChangeInfo per URI (one per PR in stacked changes).
// TODO add error codes for user errors (non-retryable) vs system errors.
func (p *provider) Get(ctx context.Context, change entity.Change) ([]changeprovider.ChangeInfo, error) {
	p.metrics.Counter("get_change_info_started").Inc(1)
	startTime := time.Now()
	defer func() {
		p.metrics.Timer("get_change_info_latency").Record(time.Since(startTime))
	}()

	// Parse all change IDs
	changeIDs := make([]entitygithub.ChangeID, 0, len(change.URIs))
	for _, uri := range change.URIs {
		parsed, err := entitygithub.ParseChangeID(uri)
		if err != nil {
			p.metrics.Counter("get_change_info_errors").Inc(1)
			return nil, fmt.Errorf("failed to parse GitHub change ID %q: %w", uri, err)
		}
		changeIDs = append(changeIDs, parsed)
	}

	p.logger.Debugw("fetching PR data from GitHub",
		"pr_count", len(changeIDs),
		"uris", change.URIs,
	)

	// Validate stacked changes are consistent (same provider, org, and repo)
	org, repo, err := validateChangeConsistency(changeIDs)
	if err != nil {
		return nil, err
	}

	// Fetch each PR and build ChangeInfo for each
	changeInfos, fetchErrors, failedPRs := p.fetchAllPRs(ctx, changeIDs)

	// Return partial results if any PRs failed
	if len(fetchErrors) > 0 {
		p.logger.Errorw("failed to fetch some PRs",
			"total_prs", len(changeIDs),
			"failed_count", len(fetchErrors),
			"failed_prs", failedPRs,
			"succeeded_count", len(changeInfos),
		)
		return changeInfos, fmt.Errorf("failed to fetch %d of %d PRs (failed: %v): %v",
			len(fetchErrors), len(changeIDs), failedPRs, fetchErrors)
	}

	p.logger.Debugw("successfully fetched PR data",
		"pr_count", len(changeIDs),
	)

	p.metrics.Tagged(map[string]string{
		"org":  org,
		"repo": repo,
	}).Counter("get_success").Inc(1)

	return changeInfos, nil
}

// fetchAllPRs fetches and validates all PRs in the stack, handling partial failures.
// Returns the successfully fetched ChangeInfos, any errors encountered, and the list of failed PR numbers.
func (p *provider) fetchAllPRs(
	ctx context.Context,
	changeIDs []entitygithub.ChangeID,
) ([]changeprovider.ChangeInfo, []error, []int) {
	changeInfos := make([]changeprovider.ChangeInfo, 0, len(changeIDs))
	var fetchErrors []error
	var failedPRs []int

	for _, cid := range changeIDs {
		prData, err := p.fetchPullRequest(ctx, cid)
		if err != nil {
			p.logger.Errorw("failed to fetch PR from GitHub",
				"org", cid.Org,
				"repo", cid.Repo,
				"pr", cid.PRNumber,
				"error", err,
			)
			p.metrics.Tagged(map[string]string{
				"org":        cid.Org,
				"repo":       cid.Repo,
				"error_type": "fetch_pr",
			}).Counter("get_errors").Inc(1)
			fetchErrors = append(fetchErrors, fmt.Errorf("PR #%d: %w", cid.PRNumber, err))
			failedPRs = append(failedPRs, cid.PRNumber)
			continue // Continue to next PR
		}

		// Validate PR hasn't changed since submission
		if err := validatePRStaleness(cid, prData); err != nil {
			fetchErrors = append(fetchErrors, err)
			failedPRs = append(failedPRs, cid.PRNumber)
			continue // Continue to next PR
		}

		// Convert to ChangeInfo
		changeInfo := convertToChangeInfo(cid, prData)
		changeInfos = append(changeInfos, changeInfo)

		p.logger.Debugw("fetched PR data",
			"org", cid.Org,
			"repo", cid.Repo,
			"pr", cid.PRNumber,
			"files_count", len(changeInfo.ChangedFiles),
			"head_sha", prData.HeadRefOid,
		)
	}

	return changeInfos, fetchErrors, failedPRs
}

// fetchPullRequest makes GraphQL request(s) to fetch PR data, handling pagination.
func (p *provider) fetchPullRequest(ctx context.Context, parsed entitygithub.ChangeID) (*pullRequestData, error) {
	var allFiles []fileNode
	var prData pullRequestData
	cursor := ""

	for {
		data, err := p.fetchPullRequestPage(ctx, parsed.Org, parsed.Repo, parsed.PRNumber, cursor)
		if err != nil {
			return nil, err
		}

		if cursor == "" {
			prData = *data
		}

		allFiles = append(allFiles, data.Files.Nodes...)

		if !data.Files.PageInfo.HasNextPage {
			break
		}
		cursor = data.Files.PageInfo.EndCursor
	}

	prData.Files.Nodes = allFiles
	return &prData, nil
}

// fetchPullRequestPage fetches a single page of PR data.
func (p *provider) fetchPullRequestPage(ctx context.Context, org, repo string, prNumber int, cursor string) (*pullRequestData, error) {
	reqBody := buildGraphQLRequest(org, repo, prNumber, cursor)

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}

	resp, err := doGraphQLRequest(ctx, bodyBytes, p.client, org, repo, p.metrics)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return parseGraphQLResponse(resp, org, repo, prNumber, p.logger, p.metrics)
}

