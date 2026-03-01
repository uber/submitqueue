package composite

import (
	"context"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/scorer"
)

// ReduceFunc combines named scores into a single score.
type ReduceFunc func(map[string]float64) float64

// Min returns the minimum value from scores.
func Min(scores map[string]float64) float64 {
	var min float64
	first := true
	for _, v := range scores {
		if first || v < min {
			min = v
			first = false
		}
	}
	return min
}

// Max returns the maximum value from scores.
func Max(scores map[string]float64) float64 {
	var max float64
	first := true
	for _, v := range scores {
		if first || v > max {
			max = v
			first = false
		}
	}
	return max
}

// Avg returns the arithmetic mean of scores.
func Avg(scores map[string]float64) float64 {
	var sum float64
	for _, v := range scores {
		sum += v
	}
	return sum / float64(len(scores))
}

// compositeScorer combines multiple named scorers into a single score using a reduce function.
type compositeScorer struct {
	// scorers maps scorer names to their implementations.
	scorers map[string]scorer.Scorer
	// reduce combines named scores into a single value.
	reduce ReduceFunc
}

// New creates a composite Scorer that evaluates all named child scorers and combines
// their results using the given reduce function.
// Panics if scorers is empty or reduce is nil.
func New(scorers map[string]scorer.Scorer, reduce ReduceFunc) scorer.Scorer {
	if len(scorers) == 0 {
		panic("composite.New: scorers must not be empty")
	}
	if reduce == nil {
		panic("composite.New: reduce must not be nil")
	}
	return &compositeScorer{
		scorers: scorers,
		reduce:  reduce,
	}
}

// Score evaluates all child scorers and combines their results using the reduce function.
// If any child scorer returns an error, that error is returned immediately.
func (c *compositeScorer) Score(ctx context.Context, change entity.Change) (float64, error) {
	scores := make(map[string]float64, len(c.scorers))
	for name, s := range c.scorers {
		score, err := s.Score(ctx, change)
		if err != nil {
			return 0, err
		}
		scores[name] = score
	}
	return c.reduce(scores), nil
}
