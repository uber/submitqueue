package speculation

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity"
)

func TestGenerateTree(t *testing.T) {
	tests := []struct {
		name          string
		currentID     string
		dependencyIDs []string
		wantPaths     []entity.SpeculationPath
	}{
		{
			name:          "no dependencies produces single path",
			currentID:     "B1",
			dependencyIDs: nil,
			wantPaths: []entity.SpeculationPath{
				{Base: []string{}, Head: "B1"},
			},
		},
		{
			name:          "one dependency produces two paths",
			currentID:     "B2",
			dependencyIDs: []string{"B1"},
			wantPaths: []entity.SpeculationPath{
				{Base: []string{"B1"}, Head: "B2"}, // optimistic: B1 succeeded
				{Base: []string{}, Head: "B2"},       // pessimistic: B1 failed
			},
		},
		{
			name:          "two dependencies produces four paths",
			currentID:     "B3",
			dependencyIDs: []string{"B1", "B2"},
			wantPaths: []entity.SpeculationPath{
				{Base: []string{"B1", "B2"}, Head: "B3"}, // both succeeded
				{Base: []string{"B1"}, Head: "B3"},        // only B1 succeeded
				{Base: []string{"B2"}, Head: "B3"},        // only B2 succeeded
				{Base: []string{}, Head: "B3"},              // all failed
			},
		},
		{
			name:          "three dependencies produces eight paths matching ERD example",
			currentID:     "B4",
			dependencyIDs: []string{"B1", "B2", "B3"},
			wantPaths: []entity.SpeculationPath{
				{Base: []string{"B1", "B2", "B3"}, Head: "B4"}, // all succeeded
				{Base: []string{"B1", "B2"}, Head: "B4"},        // B1, B2 succeeded
				{Base: []string{"B1", "B3"}, Head: "B4"},        // B1, B3 succeeded
				{Base: []string{"B2", "B3"}, Head: "B4"},        // B2, B3 succeeded
				{Base: []string{"B1"}, Head: "B4"},               // only B1 succeeded
				{Base: []string{"B2"}, Head: "B4"},               // only B2 succeeded
				{Base: []string{"B3"}, Head: "B4"},               // only B3 succeeded
				{Base: []string{}, Head: "B4"},                     // all failed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := GenerateTree(tt.currentID, tt.dependencyIDs)
			require.NoError(t, err)

			assert.Equal(t, tt.currentID, tree.BatchID)
			require.Len(t, tree.Speculations, len(tt.wantPaths))

			for i, spec := range tree.Speculations {
				assert.Equal(t, tt.wantPaths[i], spec.Path, "path at index %d", i)
			}
		})
	}
}

func TestGenerateTree_Ordering(t *testing.T) {
	tree, err := GenerateTree("B5", []string{"B1", "B2", "B3", "B4"})
	require.NoError(t, err)

	require.Len(t, tree.Speculations, 16)

	// Verify ordering: base length must be non-increasing (most optimistic first).
	for i := 1; i < len(tree.Speculations); i++ {
		assert.GreaterOrEqual(t, len(tree.Speculations[i-1].Path.Base), len(tree.Speculations[i].Path.Base),
			"path at index %d should have >= base length than path at index %d", i-1, i)
	}
}

func TestGenerateTree_AllActionsSchedule(t *testing.T) {
	tree, err := GenerateTree("B3", []string{"B1", "B2"})
	require.NoError(t, err)

	for i, spec := range tree.Speculations {
		assert.Equal(t, entity.SpeculationPathActionSchedule, spec.Action,
			"action at index %d", i)
	}
}

func TestGenerateTree_AllScoresZero(t *testing.T) {
	tree, err := GenerateTree("B3", []string{"B1", "B2"})
	require.NoError(t, err)

	for i, spec := range tree.Speculations {
		assert.Equal(t, float32(0), spec.Score, "score at index %d", i)
	}
}

func TestGenerateTree_InputImmutability(t *testing.T) {
	deps := []string{"B1", "B2", "B3"}
	original := make([]string, len(deps))
	copy(original, deps)

	_, err := GenerateTree("B4", deps)
	require.NoError(t, err)

	assert.Equal(t, original, deps, "input dependency slice should not be mutated")
}

func TestGenerateTree_EmptyDependencySlice(t *testing.T) {
	tree, err := GenerateTree("B1", []string{})
	require.NoError(t, err)

	require.Len(t, tree.Speculations, 1)
	assert.Equal(t, entity.SpeculationPath{Base: []string{}, Head: "B1"}, tree.Speculations[0].Path)
}

func TestGenerateTree_HeadAlwaysCurrentID(t *testing.T) {
	tree, err := GenerateTree("B3", []string{"B1", "B2"})
	require.NoError(t, err)

	for i, spec := range tree.Speculations {
		assert.Equal(t, "B3", spec.Path.Head, "head at index %d", i)
	}
}

func TestGenerateTree_ExceedsMaxDependencies(t *testing.T) {
	deps := make([]string, MaxDependencies+1)
	for i := range deps {
		deps[i] = fmt.Sprintf("B%d", i+1)
	}

	_, err := GenerateTree("current", deps)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestGenerateTree_AtMaxDependencies(t *testing.T) {
	deps := make([]string, MaxDependencies)
	for i := range deps {
		deps[i] = fmt.Sprintf("B%d", i+1)
	}

	tree, err := GenerateTree("current", deps)

	require.NoError(t, err)
	assert.Len(t, tree.Speculations, 1<<MaxDependencies)
}

func TestExhaustiveStrategy_Generate_NoDependencies(t *testing.T) {
	strategy := NewExhaustiveStrategy(0) // 0 → uses MaxDependencies default

	tree, err := strategy.Generate(context.Background(), "batch/1", nil)
	require.NoError(t, err)

	assert.Equal(t, "batch/1", tree.BatchID)
	assert.Len(t, tree.Speculations, 1)
	assert.Equal(t, "batch/1", tree.Speculations[0].Path.Head)
}

func TestExhaustiveStrategy_Generate_WithDependencies(t *testing.T) {
	strategy := NewExhaustiveStrategy(0)

	deps := []string{"batch/0", "batch/1", "batch/2"}
	tree, err := strategy.Generate(context.Background(), "batch/3", deps)
	require.NoError(t, err)

	assert.Equal(t, "batch/3", tree.BatchID)
	// 2^3 = 8 paths.
	assert.Len(t, tree.Speculations, 8)
}

func TestExhaustiveStrategy_Generate_TooManyDependencies(t *testing.T) {
	strategy := NewExhaustiveStrategy(0)

	deps := make([]string, MaxDependencies+1)
	for i := range deps {
		deps[i] = "batch/" + string(rune('a'+i))
	}

	_, err := strategy.Generate(context.Background(), "batch/x", deps)
	assert.Error(t, err)
}

func TestExhaustiveStrategy_Generate_CustomMaxDeps(t *testing.T) {
	strategy := NewExhaustiveStrategy(3)

	// 3 deps should work.
	deps := []string{"batch/0", "batch/1", "batch/2"}
	tree, err := strategy.Generate(context.Background(), "batch/3", deps)
	require.NoError(t, err)
	assert.Len(t, tree.Speculations, 8)

	// 4 deps should fail with custom limit of 3.
	deps = []string{"batch/0", "batch/1", "batch/2", "batch/3"}
	_, err = strategy.Generate(context.Background(), "batch/4", deps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds configured maximum 3")
}

func TestExhaustiveStrategy_Generate_MaxDepsClamped(t *testing.T) {
	// Exceeding MaxDependencies should be clamped.
	strategy := NewExhaustiveStrategy(MaxDependencies + 5)

	deps := make([]string, MaxDependencies+1)
	for i := range deps {
		deps[i] = fmt.Sprintf("batch/%d", i)
	}

	_, err := strategy.Generate(context.Background(), "batch/x", deps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("exceeds configured maximum %d", MaxDependencies))
}
