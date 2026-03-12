package github

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	coremetrics "github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	entitygithub "github.com/uber/submitqueue/entity/github"
	"github.com/uber/submitqueue/extension/changeprovider"
)

// Params holds the dependencies for the GitHub ChangeProvider.
type Params struct {
	// Client is a pre-configured GitHub API client (encapsulates HTTP client and GraphQL URL).
	// Auth is the caller's responsibility via HTTP transport/round-tripper.
	Client *Client
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

// provider implements the ChangeProvider interface for GitHub.
type provider struct {
	client       *Client
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// NewProvider creates a new GitHub ChangeProvider.
func NewProvider(params Params) changeprovider.ChangeProvider {
	return &provider{
		client:       params.Client,
		logger:       params.Logger.Named("github_changeprovider"),
		metricsScope: params.MetricsScope.SubScope("github_changeprovider"),
	}
}

// Get retrieves change information from GitHub for the provided Change.
// Returns one ChangeInfo per URI (one per PR in stacked changes).
func (p *provider) Get(ctx context.Context, change entity.Change) (_ []changeprovider.ChangeInfo, retErr error) {
	op := coremetrics.Begin(p.metricsScope, "get")
	defer func() { op.Complete(retErr) }()

	// Parse all change IDs
	changeIDs := make([]entitygithub.ChangeID, 0, len(change.URIs))
	for _, uri := range change.URIs {
		parsed, err := entitygithub.ParseChangeID(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GitHub change ID %q: %w", uri, err)
		}
		changeIDs = append(changeIDs, parsed)
	}

	p.logger.Debugw("fetching PR data from GitHub",
		"pr_count", len(changeIDs),
		"uris", change.URIs,
	)

	// Validate stacked changes are consistent (same provider, org, and repo)
	if err := validateChangeConsistency(changeIDs); err != nil {
		return nil, err
	}

	// Fetch each PR and build ChangeInfo for each
	changeInfos, err := p.fetchAllPRs(ctx, changeIDs)
	if err != nil {
		return nil, err
	}

	p.logger.Debugw("successfully fetched PR data",
		"pr_count", len(changeIDs),
	)

	return changeInfos, nil
}

// fetchAllPRs fetches and validates all PRs in the stack, returning on the first error.
func (p *provider) fetchAllPRs(
	ctx context.Context,
	changeIDs []entitygithub.ChangeID,
) ([]changeprovider.ChangeInfo, error) {
	changeInfos := make([]changeprovider.ChangeInfo, 0, len(changeIDs))

	for _, cid := range changeIDs {
		prData, err := p.fetchPullRequest(ctx, cid)
		if err != nil {
			coremetrics.NamedCounter(p.metricsScope, "fetch_pr", "errors", 1,
				coremetrics.NewTag("org", cid.Org),
				coremetrics.NewTag("repo", cid.Repo),
			)
			return nil, fmt.Errorf("failed to fetch PR #%d: %w", cid.PRNumber, err)
		}

		if err := validatePRStaleness(cid, prData); err != nil {
			return nil, err
		}

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

	return changeInfos, nil
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

	resp, err := doGraphQLRequest(ctx, bodyBytes, p.client)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return parseGraphQLResponse(resp)
}
