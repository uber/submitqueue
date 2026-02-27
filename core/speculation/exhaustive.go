package speculation

import (
	"context"
	"fmt"
	"math/bits"
	"sort"

	"github.com/uber/submitqueue/entity"
)

// MaxDependencies is the maximum number of predecessor dependencies allowed
// when generating a speculation tree.
//
// Speculation produces 2^N paths (the power set of predecessors). Growth is
// exponential, so a cap is required to prevent memory exhaustion:
//
//	N=10  →      1,024 paths  (~100 KB)
//	N=15  →     32,768 paths  (~3 MB)
//	N=20  →  1,048,576 paths  (~100 MB)
//	N=25  → 33,554,432 paths  (~3 GB)   — OOM on most machines
//
// The current implementation uses bitmask iteration (one int per subset),
// which limits N to 62 on 64-bit systems before the bit shift overflows.
// An iterative or recursive approach would remove the bitmask ceiling, but
// the exponential memory growth remains the binding constraint regardless
// of algorithm — 2^N paths must all be held in memory.
//
// In practice, a submit queue batch rarely has more than a handful of
// predecessors. A limit of 10 (1,024 paths) is generous for real workloads.
const MaxDependencies = 10

// GenerateTree takes the current batch ID and its ordered dependencies (sorted by
// arrival time), and generates a SpeculationTree for the current batch containing
// all possible speculation paths (power set of predecessors).
//
// Each path represents a possible future: a subset of predecessors that succeed
// (forming the base) with the current batch as the head being tested.
//
// For N dependencies, this produces 2^N speculation paths.
// Paths are sorted most-optimistic first (most predecessors included) to
// least-optimistic (fewest predecessors included).
// All paths are initialized with Action = SpeculationPathActionSchedule.
//
// Returns an error if len(dependencyIDs) exceeds MaxDependencies.
func GenerateTree(currentID string, dependencyIDs []string) (entity.SpeculationTree, error) {
	if len(dependencyIDs) > MaxDependencies {
		return entity.SpeculationTree{}, fmt.Errorf(
			"dependency count %d exceeds maximum %d", len(dependencyIDs), MaxDependencies,
		)
	}

	// Defensive copy to avoid mutation of caller's slice.
	deps := make([]string, len(dependencyIDs))
	copy(deps, dependencyIDs)

	n := len(deps)

	// We enumerate every subset of dependencies using bitmask iteration.
	// An N-bit integer has 2^N possible values (0 to 2^N-1), and each value
	// represents a unique subset: bit i being set means dependency i is
	// included in the base (assumed to have succeeded).
	//
	// Example with deps = [B1, B2, B3]:
	//   mask=0 (000) → base=[]              — all predecessors failed
	//   mask=1 (001) → base=[B1]            — only B1 succeeded
	//   mask=2 (010) → base=[B2]            — only B2 succeeded
	//   mask=3 (011) → base=[B1, B2]        — B1 and B2 succeeded
	//   mask=4 (100) → base=[B3]            — only B3 succeeded
	//   mask=5 (101) → base=[B1, B3]        — B1 and B3 succeeded
	//   mask=6 (110) → base=[B2, B3]        — B2 and B3 succeeded
	//   mask=7 (111) → base=[B1, B2, B3]    — all predecessors succeeded
	//
	// Because we iterate bit positions low-to-high (i=0,1,...,N-1), included
	// dependencies are appended in their original arrival order.
	totalPaths := 1 << n // 2^N
	speculations := make([]entity.SpeculationInfo, 0, totalPaths)

	for mask := range totalPaths {
		// bits.OnesCount gives the number of set bits, which equals the
		// number of dependencies included in this subset.
		base := make([]string, 0, bits.OnesCount(uint(mask)))
		for i := range n {
			if mask&(1<<i) != 0 {
				base = append(base, deps[i])
			}
		}

		speculations = append(speculations, entity.SpeculationInfo{
			Path: entity.SpeculationPath{
				Base: base,
				Head: currentID,
			},
			Action: entity.SpeculationPathActionSchedule,
			Score:  0,
		})
	}

	// Sort most-optimistic first: more predecessors included = higher optimism.
	sort.SliceStable(speculations, func(i, j int) bool {
		return len(speculations[i].Path.Base) > len(speculations[j].Path.Base)
	})

	return entity.SpeculationTree{
		BatchID:      currentID,
		Speculations: speculations,
	}, nil
}

// ExhaustiveStrategy generates all possible speculation paths (2^N) by
// enumerating the full power set of dependencies. No scorer is needed
// because every path is generated regardless of probability.
//
// Primarily useful for:
//   - Testing and verification: compare top-K results against the full solution space.
//   - Small dependency counts: when N is small enough that 2^N is acceptable.
//   - Debugging: inspect every possible path without scorer influence.
//
// For production workloads with larger dependency counts, prefer TopKStrategy
// which generates only the K highest-probability paths efficiently.
type ExhaustiveStrategy struct {
	// maxDeps is the maximum number of dependencies allowed. Must be <= MaxDependencies.
	maxDeps int
}

// Verify ExhaustiveStrategy implements Strategy at compile time.
var _ Strategy = (*ExhaustiveStrategy)(nil)

// NewExhaustiveStrategy creates a new ExhaustiveStrategy.
//
// maxDeps controls the maximum number of dependencies allowed (and thus the
// maximum number of paths generated: 2^maxDeps). If maxDeps is <= 0 or exceeds
// MaxDependencies, MaxDependencies is used. Callers should choose a limit
// appropriate for their memory budget:
//
//	N=5   →       32 paths
//	N=8   →      256 paths
//	N=10  →    1,024 paths  (~100 KB)  — MaxDependencies default
func NewExhaustiveStrategy(maxDeps int) *ExhaustiveStrategy {
	if maxDeps <= 0 || maxDeps > MaxDependencies {
		maxDeps = MaxDependencies
	}
	return &ExhaustiveStrategy{maxDeps: maxDeps}
}

// Generate produces a speculation tree containing all 2^N paths for the
// given dependencies. Returns an error if the dependency count exceeds
// the configured maximum.
func (s *ExhaustiveStrategy) Generate(_ context.Context, batchID string, dependencyIDs []string) (entity.SpeculationTree, error) {
	if len(dependencyIDs) > s.maxDeps {
		return entity.SpeculationTree{}, fmt.Errorf(
			"exhaustive generation failed: dependency count %d exceeds configured maximum %d",
			len(dependencyIDs), s.maxDeps,
		)
	}

	tree, err := GenerateTree(batchID, dependencyIDs)
	if err != nil {
		return entity.SpeculationTree{}, fmt.Errorf("exhaustive generation failed: %w", err)
	}

	return tree, nil
}
