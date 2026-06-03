# Scorer

Vendor-agnostic interface for computing success probability scores for code changes.

## Interface

### Scorer

Computes a success probability for a given change.

```go
type Scorer interface {
    Score(ctx context.Context, change entity.Change) (float64, error)
}
```

- **change**: A `entity.Change` identifying the code change to score.
- **Score**: Returns a probability between 0.0 and 1.0 indicating the likelihood of a successful land. Returns an error if scoring fails.

## Implementations

### Heuristic

Scores a change by extracting a numeric value via a `ValueFunc` and matching it against ordered buckets. Each bucket maps a `[Min, Max]` range to a probability.

```go
s := heuristic.New(
    []heuristic.Bucket{
        {Min: 0, Max: 5, Score: 0.95},
        {Min: 6, Max: 20, Score: 0.75},
        {Min: 21, Max: 100, Score: 0.5},
    },
    func(ctx context.Context, change entity.Change) (int, error) {
        // resolve the change into a numeric metric
        return filesChanged, nil
    },
)

score, err := s.Score(ctx, change)
```

### Composite

Combines multiple named scorers into a single score using a reduce function. The reduce function receives a `map[string]float64` mapping scorer names to their scores, enabling domain-aware aggregation.

Built-in reduce functions: `Min`, `Max`, `Avg`.

```go
s := composite.New(
    map[string]scorer.Scorer{
        "files": fileScorer,
        "deps":  depScorer,
    },
    composite.Min,
)

score, err := s.Score(ctx, change)
```

## Implementing a Backend

1. Create `extension/scorer/{backend}/` directory
2. Implement the `Scorer` interface
3. Accept `entity.Change` and resolve it into whatever data the implementation needs
