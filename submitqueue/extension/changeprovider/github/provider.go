package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	"github.com/uber/submitqueue/platform/githubapi"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// Params holds the dependencies for the GitHub ChangeProvider.
type Params struct {
	// Config is the per-queue identity handed to the Factory that built
	// this provider.
	Config changeprovider.Config
	// HTTPClient is a pre-configured HTTP client. The caller is responsible for
	// configuring the base URL (via BaseURLTransport) and auth (via a transport layer).
	HTTPClient *http.Client
	// Host is the GitHub instance change URIs must name (e.g. "github.com"):
	// the one HTTPClient talks to.
	Host string
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

// provider implements the ChangeProvider interface for GitHub.
type provider struct {
	// cfg is the per-queue identity this provider was built for.
	cfg          changeprovider.Config
	httpClient   *http.Client
	host         string
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// NewProvider creates a new GitHub ChangeProvider. It errors when Host is
// missing.
func NewProvider(params Params) (changeprovider.ChangeProvider, error) {
	if params.Host == "" {
		return nil, errors.New("github change provider: Host is required")
	}
	return &provider{
		cfg:          params.Config,
		httpClient:   params.HTTPClient,
		host:         params.Host,
		logger:       params.Logger.Named("github_changeprovider"),
		metricsScope: params.MetricsScope.SubScope("github_changeprovider"),
	}, nil
}

// Get retrieves change information from GitHub for the request's change.
// Returns one ChangeInfo per URI (one per PR in stacked changes).
func (p *provider) Get(ctx context.Context, request entity.Request) (_ []entity.ChangeInfo, retErr error) {
	op := coremetrics.Begin(p.metricsScope, "get", coremetrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	change := request.Change

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

	// Checked before any API call: HTTPClient would read a PR on another host
	// from this one.
	if err := validateChangeConsistency(changeIDs, p.host); err != nil {
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
) ([]entity.ChangeInfo, error) {
	changeInfos := make([]entity.ChangeInfo, 0, len(changeIDs))

	for _, cid := range changeIDs {
		prData, err := p.fetchPullRequest(ctx, cid)
		if err != nil {
			coremetrics.NamedCounter(p.metricsScope, "fetch_pr", "errors", 1,
				coremetrics.NewTag("org", cid.Org),
				coremetrics.NewTag("repo", cid.Repo),
			)
			return nil, fmt.Errorf("failed to fetch PR #%d: %w", cid.PRNumber, err)
		}

		changeInfo := convertToChangeInfo(cid, prData)
		changeInfos = append(changeInfos, changeInfo)

		p.logger.Debugw("fetched PR data",
			"org", cid.Org,
			"repo", cid.Repo,
			"pr", cid.PRNumber,
			"files_count", len(changeInfo.Details.ChangedFiles),
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

		if err := validatePRStaleness(parsed, data); err != nil {
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
	var data pullRequestQueryData
	if err := githubapi.QueryGraphQL(ctx, p.httpClient, buildGraphQLRequest(org, repo, prNumber, cursor), &data); err != nil {
		return nil, fmt.Errorf("GitHub GraphQL query failed: %w", err)
	}
	return &data.Repository.PullRequest, nil
}
