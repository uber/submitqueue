package github

import (
	"context"
	"fmt"
	"net/http"

	"github.com/uber-go/tally/v4"
	entitygithub "github.com/uber/submitqueue/entity/github"
	"github.com/uber/submitqueue/extension/landprovider"
	"go.uber.org/zap"
)

// Params holds the dependencies for the GitHub LandProvider.
type Params struct {
	// HTTPClient is a pre-configured HTTP client with auth (bearer token, GitHub App JWT, etc.).
	// Auth is the caller's responsibility via HTTP transport/round-tripper.
	HTTPClient *http.Client
	// APIURL is the GitHub REST API base URL
	// (e.g., "https://api.github.com" or "https://ghe.example.com/api/v3").
	APIURL string
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// MetricsScope is the metrics scope for instrumentation.
	MetricsScope tally.Scope
}

// landProvider implements the landprovider.LandProvider interface using the GitHub REST API.
//
// Limitation: this implementation supports landing exactly one PR per call.
// Landing multiple PRs in a single call is not idempotent — if PR N merges
// successfully but PR N+1 fails, a retry would fail on the already-merged PR.
// Callers needing multi-PR landing should use an implementation that provides
// atomic or idempotent batch semantics.
type landProvider struct {
	httpClient   *http.Client
	apiURL       string
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
}

// Verify landProvider implements landprovider.LandProvider at compile time.
var _ landprovider.LandProvider = (*landProvider)(nil)

// NewLandProvider creates a new GitHub LandProvider.
func NewLandProvider(params Params) landprovider.LandProvider {
	return &landProvider{
		httpClient:   params.HTTPClient,
		apiURL:       params.APIURL,
		logger:       params.Logger.Named("github_landprovider"),
		metricsScope: params.MetricsScope.SubScope("github_landprovider"),
	}
}

// Land merges a single PR into the target branch using the GitHub merge PR REST API.
// Returns an error if entries contain more than one PR, since merging multiple PRs
// is not idempotent — a partial failure leaves already-merged PRs in a state that
// cannot be retried.
func (l *landProvider) Land(ctx context.Context, queue string, entries []landprovider.LandEntry) error {
	l.metricsScope.Counter("land_started").Inc(1)

	if err := validateSinglePR(entries); err != nil {
		l.metricsScope.Counter("validation_errors").Inc(1)
		return err
	}

	entry := entries[0]
	uri := entry.Change.URIs[0]

	cid, err := entitygithub.ParseChangeID(uri)
	if err != nil {
		l.metricsScope.Counter("parse_errors").Inc(1)
		return fmt.Errorf("failed to parse change ID %q: %w", uri, err)
	}

	if err := l.mergePR(ctx, cid, entry.Strategy); err != nil {
		if landprovider.IsLandRejected(err) {
			l.metricsScope.Counter("land_rejected").Inc(1)
		} else {
			l.metricsScope.Counter("api_errors").Inc(1)
		}
		return err
	}

	l.metricsScope.Counter("land_succeeded").Inc(1)
	return nil
}
