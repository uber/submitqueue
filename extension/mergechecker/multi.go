package mergechecker

import (
	"context"
	"fmt"
	"strings"

	"github.com/uber/submitqueue/entity"
)

// multiChecker dispatches mergeability checks to scheme-specific checkers
// based on the URI scheme of the first change URI. Each scheme
// (e.g., "github", "ghe", "ghes") maps to a checker configured for that host.
type multiChecker struct {
	// checkers maps URI scheme values to their corresponding MergeChecker.
	checkers map[string]MergeChecker
}

// NewMultiChecker creates a MergeChecker that routes mergeability checks
// to scheme-specific checkers. The map keys correspond to URI schemes
// (e.g., "github", "ghe") extracted from the first change URI.
func NewMultiChecker(checkers map[string]MergeChecker) MergeChecker {
	return &multiChecker{checkers: checkers}
}

// Check dispatches the mergeability check to the checker registered for
// the change URI scheme.
func (m *multiChecker) Check(ctx context.Context, queue string, change entity.Change) (Result, error) {
	if len(change.URIs) == 0 {
		return Result{}, fmt.Errorf("no change URIs provided")
	}

	scheme, _, ok := strings.Cut(change.URIs[0], "://")
	if !ok || scheme == "" {
		return Result{}, fmt.Errorf("invalid change URI %q: missing scheme", change.URIs[0])
	}

	checker, ok := m.checkers[scheme]
	if !ok {
		return Result{}, fmt.Errorf("no mergeability checker configured for scheme %q", scheme)
	}
	return checker.Check(ctx, queue, change)
}
