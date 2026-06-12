package phabricator

import (
	"context"
	"fmt"
	"net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	coremetrics "github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	entityphab "github.com/uber/submitqueue/submitqueue/entity/phabricator"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// queryDiffsBatchSize is the maximum number of diff IDs sent in a single
// differential.querydiffs request.
const queryDiffsBatchSize = 10

// Params holds the dependencies for the Phabricator ChangeProvider.
type Params struct {
	// HTTPClient is a pre-configured HTTP client. The caller is responsible for
	// configuring the base URL (e.g. via httpclient.NewClient) and authentication
	// (e.g. via oauth2.Transport for Bearer tokens) via transport layers.
	HTTPClient *http.Client
	// APIToken is the Conduit API token, sent as the api.token form parameter.
	APIToken string
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

type provider struct {
	httpClient   *http.Client
	apiToken     string
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// NewProvider creates a new Phabricator ChangeProvider.
func NewProvider(params Params) changeprovider.ChangeProvider {
	return &provider{
		httpClient:   params.HTTPClient,
		apiToken:     params.APIToken,
		logger:       params.Logger.Named("phabricator_changeprovider"),
		metricsScope: params.MetricsScope.SubScope("phabricator_changeprovider"),
	}
}

// Get retrieves change information from Phabricator for the request's change.
// Returns one ChangeInfo per URI.
func (p *provider) Get(ctx context.Context, request entity.Request) (_ []entity.ChangeInfo, retErr error) {
	op := coremetrics.Begin(p.metricsScope, "get")
	defer func() { op.Complete(retErr) }()

	change := request.Change

	changeIDs := make([]entityphab.ChangeID, 0, len(change.URIs))
	for _, uri := range change.URIs {
		parsed, err := entityphab.ParseChangeID(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Phabricator change ID %q: %w", uri, err)
		}
		changeIDs = append(changeIDs, parsed)
	}

	p.logger.Debugw("fetching diff data from Phabricator",
		"diff_count", len(changeIDs),
		"uris", change.URIs,
	)

	if err := validateChangeConsistency(changeIDs); err != nil {
		return nil, err
	}

	diffs, err := p.fetchAllDiffs(ctx, changeIDs)
	if err != nil {
		return nil, err
	}

	changeInfos := make([]entity.ChangeInfo, 0, len(changeIDs))
	for _, cid := range changeIDs {
		diff := diffs[cid.DiffID]

		if err := validateDiffResponse(cid.DiffID, diff); err != nil {
			return nil, err
		}

		changeInfo := convertToChangeInfo(cid, diff)
		changeInfos = append(changeInfos, changeInfo)

		p.logger.Debugw("fetched diff data",
			"revision", cid.Revision(),
			"diff_id", cid.DiffID,
			"files_count", len(changeInfo.Details.ChangedFiles),
		)
	}

	p.logger.Debugw("successfully fetched diff data",
		"diff_count", len(changeIDs),
	)

	return changeInfos, nil
}

// fetchAllDiffs fetches diff data for all change IDs in batches of queryDiffsBatchSize.
func (p *provider) fetchAllDiffs(ctx context.Context, changeIDs []entityphab.ChangeID) (map[int]*diffResult, error) {
	diffIDs := make([]int, 0, len(changeIDs))
	for _, cid := range changeIDs {
		diffIDs = append(diffIDs, cid.DiffID)
	}

	results := make(map[int]*diffResult, len(diffIDs))

	for start := 0; start < len(diffIDs); start += queryDiffsBatchSize {
		end := start + queryDiffsBatchSize
		if end > len(diffIDs) {
			end = len(diffIDs)
		}

		batch, err := p.fetchDiffBatch(ctx, diffIDs[start:end])
		if err != nil {
			return nil, fmt.Errorf("failed to fetch diffs: %w", err)
		}
		for id, diff := range batch {
			results[id] = diff
		}
	}

	return results, nil
}

// fetchDiffBatch fetches a single batch of diffs from Phabricator.
func (p *provider) fetchDiffBatch(ctx context.Context, diffIDs []int) (map[int]*diffResult, error) {
	form := buildQueryDiffsRequest(diffIDs, p.apiToken)

	resp, err := doConduitRequest(ctx, p.httpClient, form)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return parseConduitResponse(resp, diffIDs)
}
