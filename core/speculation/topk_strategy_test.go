package speculation

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopKStrategy_Generate_NoDependencies(t *testing.T) {
	strategy := NewTopKStrategy(nil, DefaultK)

	tree, err := strategy.Generate(context.Background(), "batch/1", nil)
	require.NoError(t, err)

	assert.Equal(t, "batch/1", tree.BatchID)
	assert.Len(t, tree.Speculations, 1)
	assert.Equal(t, "batch/1", tree.Speculations[0].Path.Head)
	assert.Empty(t, tree.Speculations[0].Path.Base)
}

func TestTopKStrategy_Generate_WithProbabilityFunc(t *testing.T) {
	probFn := func(_ context.Context, ids []string) (map[string]float64, error) {
		probs := map[string]float64{"batch/0": 0.9, "batch/1": 0.8}
		return probs, nil
	}

	strategy := NewTopKStrategy(probFn, DefaultK)

	deps := []string{"batch/0", "batch/1"}
	tree, err := strategy.Generate(context.Background(), "batch/2", deps)
	require.NoError(t, err)

	assert.Equal(t, "batch/2", tree.BatchID)
	assert.NotEmpty(t, tree.Speculations)
	// With 2 deps and k=32, we get all 4 paths (2^2).
	assert.Len(t, tree.Speculations, 4)
}

func TestTopKStrategy_Generate_NilProbFnUsesDefaults(t *testing.T) {
	strategy := NewTopKStrategy(nil, DefaultK)

	deps := []string{"batch/0"}
	tree, err := strategy.Generate(context.Background(), "batch/1", deps)
	require.NoError(t, err)

	assert.Equal(t, "batch/1", tree.BatchID)
	// With 1 dep, we get 2 paths (2^1).
	assert.Len(t, tree.Speculations, 2)
}

func TestTopKStrategy_Generate_ProbFnFailureFallback(t *testing.T) {
	probFn := func(_ context.Context, ids []string) (map[string]float64, error) {
		return nil, fmt.Errorf("scorer unavailable")
	}

	strategy := NewTopKStrategy(probFn, DefaultK)

	deps := []string{"batch/0"}
	// Should succeed despite probability function failure (falls back to 0.5).
	tree, err := strategy.Generate(context.Background(), "batch/1", deps)
	require.NoError(t, err)

	assert.Equal(t, "batch/1", tree.BatchID)
	assert.Len(t, tree.Speculations, 2)
}

func TestTopKStrategy_Generate_CustomK(t *testing.T) {
	probFn := func(_ context.Context, ids []string) (map[string]float64, error) {
		probs := map[string]float64{"batch/0": 0.9, "batch/1": 0.8, "batch/2": 0.7}
		return probs, nil
	}

	strategy := NewTopKStrategy(probFn, 2)

	deps := []string{"batch/0", "batch/1", "batch/2"}
	tree, err := strategy.Generate(context.Background(), "batch/3", deps)
	require.NoError(t, err)

	assert.Equal(t, "batch/3", tree.BatchID)
	assert.Len(t, tree.Speculations, 2)
}
