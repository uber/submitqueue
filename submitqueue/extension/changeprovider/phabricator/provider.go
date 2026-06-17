package phabricator

import (
	"context"
	"fmt"
	"net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	changephab "github.com/uber/submitqueue/platform/base/change/phabricator"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// queryDiffsBatchSize is the maximum number of diff IDs sent in a single
// differential.querydiffs request.
const queryDiffsBatchSize = 10

// Params holds the dependencies for the Phabricator ChangeProvider.
type Params struct {
	// HTTPClient is a pre-configured HTTP client. The caller is responsible for
	// configuring the base URL (e.g. via httpclient.NewClient) and authentication
	// (e.g. via a RoundTripper that injects credentials) via transport layers.
	HTTPClient *http.Client
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

type provider struct {
	httpClient   *http.Client
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// NewProvider creates a new Phabricator ChangeProvider.
func NewProvider(params Params) changeprovider.ChangeProvider {
	return &provider{
		httpClient:   params.HTTPClient,
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

	changeIDs := make([]changephab.ChangeID, 0, len(change.URIs))
	for _, uri := range change.URIs {
		parsed, err := changephab.ParseChangeID(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Phabricator change ID %q: %w", uri, err)
		}
		changeIDs = append(changeIDs, parsed)
	}

	p.logger.Debugw("fetching diff data from Phabricator",
		"diff_count", len(changeIDs),
		"uris", change.URIs,
	)

	diffs, err := p.fetchAllDiffs(ctx, changeIDs)
	if err != nil {
		return nil, err
	}

	changeInfos := make([]entity.ChangeInfo, 0, len(changeIDs))
	for _, cid := range changeIDs {
		diff := diffs[cid.DiffID]
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
func (p *provider) fetchAllDiffs(ctx context.Context, changeIDs []changephab.ChangeID) (map[int]*diffResult, error) {
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
	form := buildQueryDiffsRequest(diffIDs)

	resp, err := doConduitRequest(ctx, p.httpClient, form)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return parseConduitResponse(resp, diffIDs)
}
