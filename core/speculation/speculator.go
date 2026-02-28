package speculation

import (
	"strings"

	"github.com/uber/submitqueue/entity"
)

// SpeculateInput is a snapshot of the connected set loaded by the controller
// from stores. Contains all data the speculator needs to compute desired state.
// The controller builds this by:
//  1. Receiving a BatchID from the speculate queue
//  2. Loading the batch from BatchStore to get its queue
//  3. Loading all active batches for that queue via BatchStore.GetByQueueAndStates()
//  4. Loading speculation trees via SpeculationTreeStore.Get() for each batch
//  5. Loading builds via BuildStore for those batches
//  6. Loading dependents via BatchDependentStore.Get() for each batch
type SpeculateInput struct {
	// Batches contains all batches in the connected set, keyed by ID.
	// Provides State (for action computation) and Dependencies (for knowing
	// what each batch depends on).
	Batches map[string]entity.Batch
	// Trees contains the current speculation trees for each batch, keyed by batch ID.
	// The speculator reads Actions from these and computes desired Actions.
	Trees map[string]entity.SpeculationTree
	// Builds contains all builds for batches in the set. Each build has a BatchID,
	// SpeculationPath, and Status. Used by CanLand to check if all paths have
	// passing builds.
	Builds []entity.Build
	// Dependents is the reverse dependency map (batchID -> its dependent batch IDs).
	// Used by ConnectedSet to traverse the graph in both directions.
	Dependents map[string][]string
}

// ActionTransition records a single path action change produced by the speculator.
// The controller uses these to decide what to publish downstream:
//   - schedule -> cancel: publish cancel to build topic (stop wasting CI)
//   - cancel -> schedule: publish schedule to build topic (re-speculation)
type ActionTransition struct {
	// BatchID is which batch's tree this transition belongs to.
	BatchID string
	// Path is the specific path (base + head) whose action changed.
	Path entity.SpeculationPath
	// Score is the path's score (passed through to the build controller for prioritization).
	Score float32
	// FromAction is the previous action.
	FromAction entity.SpeculationPathAction
	// ToAction is the new desired action.
	ToAction entity.SpeculationPathAction
}

// SpeculateResult is the complete output of speculation. The controller consumes
// this to:
//  1. Persist UpdatedTrees back to SpeculationTreeStore (only trees that changed)
//  2. Publish each ActionTransition to the build topic as a schedule/cancel action
//  3. Publish each batch in ReadyToLand to the merge topic
type SpeculateResult struct {
	// UpdatedTrees contains trees with modified actions (only includes trees that changed).
	UpdatedTrees map[string]entity.SpeculationTree
	// Transitions contains all action changes across all trees.
	Transitions []ActionTransition
	// ReadyToLand contains batch IDs where all scheduled paths have passing builds.
	ReadyToLand []string
}

// Speculate is the main entry point. It iterates all trees in the input,
// computes the desired action for each path, records transitions, and checks
// CanLand for each batch.
func Speculate(input SpeculateInput) SpeculateResult {
	result := SpeculateResult{
		UpdatedTrees: make(map[string]entity.SpeculationTree),
	}

	// Build a map of batch states for quick lookup.
	batchStates := make(map[string]entity.BatchState, len(input.Batches))
	for id, batch := range input.Batches {
		batchStates[id] = batch.State
	}

	// Extract dependency IDs for each batch.
	batchDeps := make(map[string][]string, len(input.Batches))
	for id, batch := range input.Batches {
		batchDeps[id] = DependencyBatchIDs(batch.Dependencies)
	}

	// Index builds by batch ID for CanLand checks.
	buildsByBatch := make(map[string][]entity.Build)
	for _, build := range input.Builds {
		buildsByBatch[build.BatchID] = append(buildsByBatch[build.BatchID], build)
	}

	for batchID, tree := range input.Trees {
		depIDs := batchDeps[batchID]
		treeChanged := false
		updatedSpecs := make([]entity.SpeculationInfo, len(tree.Speculations))
		copy(updatedSpecs, tree.Speculations)

		for i, spec := range updatedSpecs {
			desired := ComputeDesiredAction(spec.Path, depIDs, batchStates)
			if desired != spec.Action {
				result.Transitions = append(result.Transitions, ActionTransition{
					BatchID:    batchID,
					Path:       spec.Path,
					Score:      spec.Score,
					FromAction: spec.Action,
					ToAction:   desired,
				})
				updatedSpecs[i].Action = desired
				treeChanged = true
			}
		}

		currentTree := tree
		if treeChanged {
			currentTree = entity.SpeculationTree{
				BatchID:      batchID,
				Speculations: updatedSpecs,
			}
			result.UpdatedTrees[batchID] = currentTree
		}

		// Check CanLand regardless of tree changes (build status may have changed).
		if CanLand(currentTree, buildsByBatch[batchID]) {
			result.ReadyToLand = append(result.ReadyToLand, batchID)
		}
	}

	return result
}

// ComputeDesiredAction computes the desired action for a speculation path based
// on the states of the batch's dependencies. Two checks:
//  1. Deps IN base: if failed/cancelled -> cancel (path needs them but they can't succeed)
//  2. Deps NOT in base: if succeeded -> cancel (path excluded them but they merged)
//  3. Otherwise -> schedule
func ComputeDesiredAction(path entity.SpeculationPath, dependencyIDs []string, batchStates map[string]entity.BatchState) entity.SpeculationPathAction {
	baseSet := make(map[string]bool, len(path.Base))
	for _, id := range path.Base {
		baseSet[id] = true
	}

	for _, depID := range dependencyIDs {
		state := batchStates[depID]
		inBase := baseSet[depID]

		if inBase {
			// Path assumes this dep succeeds. If the dep failed or was
			// cancelled, this path is impossible.
			if state == entity.BatchStateFailed || state == entity.BatchStateCancelled {
				return entity.SpeculationPathActionCancel
			}
		} else {
			// Path assumes this dep does NOT succeed. If the dep actually
			// succeeded (merged), this path is invalid.
			if state == entity.BatchStateSucceeded {
				return entity.SpeculationPathActionCancel
			}
		}
	}

	return entity.SpeculationPathActionSchedule
}

// CanLand returns true if every scheduled path in the tree has at least one
// passing build. Cancelled paths don't block landing.
func CanLand(tree entity.SpeculationTree, builds []entity.Build) bool {
	// Build a set of paths that have passing builds.
	passingPaths := make(map[string]bool, len(builds))
	for _, build := range builds {
		if build.Status == entity.BuildStatusPassed {
			passingPaths[specPathKey(build.SpeculationPath)] = true
		}
	}

	for _, spec := range tree.Speculations {
		if spec.Action != entity.SpeculationPathActionSchedule {
			continue
		}
		if !passingPaths[specPathKey(spec.Path)] {
			return false
		}
	}

	return true
}

// ConnectedSet performs BFS from the given batchID through both dependency and
// dependent edges, returning all reachable batch IDs.
func ConnectedSet(batchID string, deps map[string][]string, dependents map[string][]string) []string {
	visited := make(map[string]bool)
	queue := []string{batchID}
	visited[batchID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, id := range deps[current] {
			if !visited[id] {
				visited[id] = true
				queue = append(queue, id)
			}
		}

		for _, id := range dependents[current] {
			if !visited[id] {
				visited[id] = true
				queue = append(queue, id)
			}
		}
	}

	result := make([]string, 0, len(visited))
	for id := range visited {
		result = append(result, id)
	}
	return result
}

// DependencyBatchIDs extracts batch IDs from the Dependencies field of a Batch.
func DependencyBatchIDs(deps []map[string]interface{}) []string {
	ids := make([]string, 0, len(deps))
	for _, dep := range deps {
		if id, ok := dep["ID"].(string); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// specPathKey creates a string key for a SpeculationPath for use in map lookups.
func specPathKey(path entity.SpeculationPath) string {
	return strings.Join(path.Base, ",") + ">" + path.Head
}
