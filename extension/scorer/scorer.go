package scorer

//go:generate mockgen -source=scorer.go -destination=mock/scorer.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// Scorer computes a success probability score for a change based on its characteristics.
type Scorer interface {
	// Score returns a probability between 0.0 and 1.0 indicating the likelihood
	// of a successful land for the given change.
	Score(ctx context.Context, change entity.Change) (float64, error)
}
