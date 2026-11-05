package phabricator

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	changephab "github.com/uber/submitqueue/platform/base/change/phabricator"
	"github.com/uber/submitqueue/platform/conduit"
	coremetrics "github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/changeprovider"
)

// queryDiffsBatchSize is the maximum number of diff IDs sent in a single
// differential.querydiffs request.
const queryDiffsBatchSize = 10

// Params holds the dependencies for the Phabricator ChangeProvider.
type Params struct {
	// Config is the per-queue identity handed to the Factory that built
	// this provider.
	Config changeprovider.Config
	// Conduit calls the Phabricator instance the queue's diffs live on.
	Conduit *conduit.Client
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

type provider struct {
	// cfg is the per-queue identity this provider was built for.
	cfg          changeprovider.Config
	conduit      *conduit.Client
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// NewProvider creates a new Phabricator ChangeProvider.
func NewProvider(params Params) changeprovider.ChangeProvider {
	return &provider{
		cfg:          params.Config,
		conduit:      params.Conduit,
		logger:       params.Logger.Named("phabricator_changeprovider"),
		metricsScope: params.MetricsScope.SubScope("phabricator_changeprovider"),
	}
}

// Get retrieves change information from Phabricator for the request's change.
// Returns one ChangeInfo per URI.
func (p *provider) Get(ctx context.Context, request entity.Request) (_ []entity.ChangeInfo, retErr error) {
	op := coremetrics.Begin(p.metricsScope, "get", coremetrics.LongLatencyBuckets)
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

		batch, err := queryDiffs(ctx, p.conduit, diffIDs[start:end])
		if err != nil {
			return nil, fmt.Errorf("failed to fetch diffs: %w", err)
		}
		for id, diff := range batch {
			results[id] = diff
		}
	}

	return results, nil
}
