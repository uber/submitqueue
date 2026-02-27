package speculation

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/entity"
)

// ProbabilityFunc provides success probabilities for dependency batch IDs.
// Implementations may use historical data, ML models, or static heuristics.
// The returned map must have an entry for every input ID.
type ProbabilityFunc func(ctx context.Context, dependencyIDs []string) (map[string]float64, error)

// TopKStrategy generates the top-K highest-probability speculation paths.
// It uses an optional ProbabilityFunc to obtain success probabilities for
// each dependency. If no function is provided, defaults to 0.5 for all.
type TopKStrategy struct {
	// probFn provides per-dependency success probabilities. May be nil.
	probFn ProbabilityFunc
	// k is the number of top paths to generate.
	k int
}

// Verify TopKStrategy implements Strategy at compile time.
var _ Strategy = (*TopKStrategy)(nil)

// NewTopKStrategy creates a new TopKStrategy that generates at most k
// speculation paths. If probFn is nil, all dependencies default to 0.5.
// If k <= 0, DefaultK is used.
func NewTopKStrategy(probFn ProbabilityFunc, k int) *TopKStrategy {
	return &TopKStrategy{
		probFn: probFn,
		k:      k,
	}
}

// Generate produces a speculation tree by obtaining dependency probabilities
// and selecting the top-K paths. If the probability function fails, it falls
// back to default probabilities of 0.5 for all dependencies.
func (s *TopKStrategy) Generate(ctx context.Context, batchID string, dependencyIDs []string) (entity.SpeculationTree, error) {
	probabilities := defaultProbabilities(dependencyIDs)

	if s.probFn != nil && len(dependencyIDs) > 0 {
		scored, err := s.probFn(ctx, dependencyIDs)
		if err != nil {
			// Fall back to default probabilities on failure.
			// This is a soft error — speculation continues with uniform priors.
		} else {
			probabilities = scored
		}
	}

	tree, err := GenerateTopK(batchID, dependencyIDs, probabilities, s.k)
	if err != nil {
		return entity.SpeculationTree{}, fmt.Errorf("top-k generation failed: %w", err)
	}

	return tree, nil
}

// defaultProbabilities returns a map with 0.5 probability for all IDs.
func defaultProbabilities(ids []string) map[string]float64 {
	probs := make(map[string]float64, len(ids))
	for _, id := range ids {
		probs[id] = 0.5
	}
	return probs
}
