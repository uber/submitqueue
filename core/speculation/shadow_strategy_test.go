package speculation

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity"
	"go.uber.org/zap/zaptest"
)

// stubStrategy is a test Strategy that returns a fixed result.
type stubStrategy struct {
	tree entity.SpeculationTree
	err  error
}

func (s *stubStrategy) Generate(_ context.Context, _ string, _ []string) (entity.SpeculationTree, error) {
	return s.tree, s.err
}

func TestShadowStrategy_ReturnsPrimaryResult(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("test", nil)

	primaryTree := entity.SpeculationTree{
		BatchID: "batch/1",
		Speculations: []entity.SpeculationInfo{
			{
				Path:   entity.SpeculationPath{Base: []string{"batch/0"}, Head: "batch/1"},
				Action: entity.SpeculationPathActionSchedule,
				Score:  0.9,
			},
		},
	}

	secondaryTree := entity.SpeculationTree{
		BatchID: "batch/1",
		Speculations: []entity.SpeculationInfo{
			{
				Path:   entity.SpeculationPath{Base: []string{}, Head: "batch/1"},
				Action: entity.SpeculationPathActionSchedule,
				Score:  1.0,
			},
		},
	}

	primary := &stubStrategy{tree: primaryTree}
	secondary := &stubStrategy{tree: secondaryTree}

	shadow := NewShadowStrategy(logger, scope, primary, secondary)

	tree, err := shadow.Generate(context.Background(), "batch/1", []string{"batch/0"})
	require.NoError(t, err)

	// Should return the primary's result.
	assert.Equal(t, primaryTree, tree)
}

func TestShadowStrategy_PrimaryError(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("test", nil)

	primary := &stubStrategy{err: fmt.Errorf("primary failed")}
	secondary := &stubStrategy{tree: entity.SpeculationTree{BatchID: "batch/1"}}

	shadow := NewShadowStrategy(logger, scope, primary, secondary)

	_, err := shadow.Generate(context.Background(), "batch/1", nil)
	assert.Error(t, err)
}

func TestShadowStrategy_SecondaryError(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("test", nil)

	primaryTree := entity.SpeculationTree{
		BatchID: "batch/1",
		Speculations: []entity.SpeculationInfo{
			{
				Path:   entity.SpeculationPath{Base: []string{}, Head: "batch/1"},
				Action: entity.SpeculationPathActionSchedule,
				Score:  1.0,
			},
		},
	}

	primary := &stubStrategy{tree: primaryTree}
	secondary := &stubStrategy{err: fmt.Errorf("secondary failed")}

	shadow := NewShadowStrategy(logger, scope, primary, secondary)

	tree, err := shadow.Generate(context.Background(), "batch/1", nil)
	require.NoError(t, err)

	// Should still return the primary's result.
	assert.Equal(t, primaryTree, tree)
}

func TestShadowStrategy_TopPathAgreement(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("test", nil)

	sameTree := entity.SpeculationTree{
		BatchID: "batch/1",
		Speculations: []entity.SpeculationInfo{
			{
				Path:   entity.SpeculationPath{Base: []string{"batch/0"}, Head: "batch/1"},
				Action: entity.SpeculationPathActionSchedule,
				Score:  0.9,
			},
		},
	}

	primary := &stubStrategy{tree: sameTree}
	secondary := &stubStrategy{tree: sameTree}

	shadow := NewShadowStrategy(logger, scope, primary, secondary)

	tree, err := shadow.Generate(context.Background(), "batch/1", []string{"batch/0"})
	require.NoError(t, err)
	assert.Equal(t, sameTree, tree)

	// Verify the top_path_agrees counter was incremented.
	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	agreeCounter, ok := counters["test.shadow_strategy.top_path_agrees+"]
	require.True(t, ok)
	assert.Equal(t, int64(1), agreeCounter.Value())
}

func TestShadowStrategy_MultipleSecondaries(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NewTestScope("test", nil)

	primaryTree := entity.SpeculationTree{
		BatchID: "batch/1",
		Speculations: []entity.SpeculationInfo{
			{
				Path:   entity.SpeculationPath{Base: []string{}, Head: "batch/1"},
				Action: entity.SpeculationPathActionSchedule,
				Score:  1.0,
			},
		},
	}

	primary := &stubStrategy{tree: primaryTree}
	sec1 := &stubStrategy{tree: primaryTree}
	sec2 := &stubStrategy{err: fmt.Errorf("sec2 failed")}

	shadow := NewShadowStrategy(logger, scope, primary, sec1, sec2)

	tree, err := shadow.Generate(context.Background(), "batch/1", nil)
	require.NoError(t, err)
	assert.Equal(t, primaryTree, tree)
}
