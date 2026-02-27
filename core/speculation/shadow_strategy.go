package speculation

import (
	"context"
	"math"
	"sync"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity"
	"go.uber.org/zap"
)

// ShadowStrategy runs a primary strategy and one or more secondary strategies
// concurrently. It always returns the primary strategy's result but logs
// differences between the primary and secondary results for comparison.
// This enables A/B testing of strategies without affecting production.
type ShadowStrategy struct {
	// logger is used for logging differences between strategies.
	logger *zap.SugaredLogger
	// metricsScope is used for emitting comparison metrics.
	metricsScope tally.Scope
	// primary is the strategy whose result is returned.
	primary Strategy
	// secondaries are strategies run for comparison only.
	secondaries []Strategy
}

// Verify ShadowStrategy implements Strategy at compile time.
var _ Strategy = (*ShadowStrategy)(nil)

// NewShadowStrategy creates a new ShadowStrategy that runs the primary strategy
// and all secondaries concurrently, returning the primary result.
func NewShadowStrategy(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	primary Strategy,
	secondaries ...Strategy,
) *ShadowStrategy {
	return &ShadowStrategy{
		logger:       logger.Named("shadow_strategy"),
		metricsScope: scope.SubScope("shadow_strategy"),
		primary:      primary,
		secondaries:  secondaries,
	}
}

// shadowResult holds the result from a strategy execution.
type shadowResult struct {
	tree entity.SpeculationTree
	err  error
}

// Generate runs the primary and all secondary strategies concurrently.
// It returns the primary strategy's result and logs comparison metrics.
func (s *ShadowStrategy) Generate(ctx context.Context, batchID string, dependencyIDs []string) (entity.SpeculationTree, error) {
	var (
		primaryResult shadowResult
		wg            sync.WaitGroup
	)

	// Run primary strategy.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tree, err := s.primary.Generate(ctx, batchID, dependencyIDs)
		primaryResult = shadowResult{tree: tree, err: err}
	}()

	// Run secondaries concurrently.
	secondaryResults := make([]shadowResult, len(s.secondaries))
	for i, secondary := range s.secondaries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tree, err := secondary.Generate(ctx, batchID, dependencyIDs)
			secondaryResults[i] = shadowResult{tree: tree, err: err}
		}()
	}

	wg.Wait()

	// Log comparisons with secondaries.
	for i, sr := range secondaryResults {
		s.compareResults(batchID, i, primaryResult, sr)
	}

	return primaryResult.tree, primaryResult.err
}

// compareResults logs differences between the primary and a secondary result.
func (s *ShadowStrategy) compareResults(batchID string, secondaryIndex int, primary, secondary shadowResult) {
	if secondary.err != nil {
		s.logger.Warnw("secondary strategy failed",
			"batch_id", batchID,
			"secondary_index", secondaryIndex,
			"error", secondary.err,
		)
		s.metricsScope.Counter("secondary_errors").Inc(1)
		return
	}

	if primary.err != nil {
		// Primary failed, nothing meaningful to compare.
		return
	}

	primaryCount := len(primary.tree.Speculations)
	secondaryCount := len(secondary.tree.Speculations)

	s.metricsScope.Gauge("path_count_diff").Update(
		math.Abs(float64(primaryCount - secondaryCount)),
	)

	// Check if the top path (first speculation) agrees between strategies.
	topPathAgrees := false
	if primaryCount > 0 && secondaryCount > 0 {
		pTop := primary.tree.Speculations[0].Path
		sTop := secondary.tree.Speculations[0].Path
		topPathAgrees = pathsEqual(pTop, sTop)
	}

	if topPathAgrees {
		s.metricsScope.Counter("top_path_agrees").Inc(1)
	} else {
		s.metricsScope.Counter("top_path_disagrees").Inc(1)
	}

	s.logger.Debugw("shadow strategy comparison",
		"batch_id", batchID,
		"secondary_index", secondaryIndex,
		"primary_path_count", primaryCount,
		"secondary_path_count", secondaryCount,
		"top_path_agrees", topPathAgrees,
	)
}

// pathsEqual checks if two speculation paths are equivalent.
func pathsEqual(a, b entity.SpeculationPath) bool {
	if a.Head != b.Head {
		return false
	}
	if len(a.Base) != len(b.Base) {
		return false
	}
	for i := range a.Base {
		if a.Base[i] != b.Base[i] {
			return false
		}
	}
	return true
}
