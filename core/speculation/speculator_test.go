package speculation

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity"
)

func TestComputeDesiredAction(t *testing.T) {
	tests := []struct {
		name          string
		path          entity.SpeculationPath
		dependencyIDs []string
		batchStates   map[string]entity.BatchState
		want          entity.SpeculationPathAction
	}{
		{
			name: "no dependencies",
			path: entity.SpeculationPath{
				Base: nil,
				Head: "B1",
			},
			dependencyIDs: nil,
			batchStates:   map[string]entity.BatchState{},
			want:          entity.SpeculationPathActionSchedule,
		},
		{
			name: "all deps in-flight",
			path: entity.SpeculationPath{
				Base: []string{"B1"},
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateSpeculating,
			},
			want: entity.SpeculationPathActionSchedule,
		},
		{
			name: "dep in base failed",
			path: entity.SpeculationPath{
				Base: []string{"B1"},
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateFailed,
			},
			want: entity.SpeculationPathActionCancel,
		},
		{
			name: "dep in base cancelled",
			path: entity.SpeculationPath{
				Base: []string{"B1"},
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateCancelled,
			},
			want: entity.SpeculationPathActionCancel,
		},
		{
			name: "dep in base succeeded (confirmed)",
			path: entity.SpeculationPath{
				Base: []string{"B1"},
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateSucceeded,
			},
			want: entity.SpeculationPathActionSchedule,
		},
		{
			name: "dep NOT in base succeeded (collapse)",
			path: entity.SpeculationPath{
				Base: nil,
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateSucceeded,
			},
			want: entity.SpeculationPathActionCancel,
		},
		{
			name: "dep NOT in base failed (correctly excluded)",
			path: entity.SpeculationPath{
				Base: nil,
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateFailed,
			},
			want: entity.SpeculationPathActionSchedule,
		},
		{
			name: "mixed states - one dep in base failed cancels",
			path: entity.SpeculationPath{
				Base: []string{"B1", "B2"},
				Head: "B3",
			},
			dependencyIDs: []string{"B1", "B2"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateSpeculating,
				"B2": entity.BatchStateFailed,
			},
			want: entity.SpeculationPathActionCancel,
		},
		{
			name: "mixed states - all in-flight schedules",
			path: entity.SpeculationPath{
				Base: []string{"B1"},
				Head: "B3",
			},
			dependencyIDs: []string{"B1", "B2"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateSpeculating,
				"B2": entity.BatchStateCreated,
			},
			want: entity.SpeculationPathActionSchedule,
		},
		{
			name: "re-speculation - dep reverts to in-flight",
			path: entity.SpeculationPath{
				Base: nil,
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateSpeculating,
			},
			want: entity.SpeculationPathActionSchedule,
		},
		{
			name: "dep in base with created state",
			path: entity.SpeculationPath{
				Base: []string{"B1"},
				Head: "B2",
			},
			dependencyIDs: []string{"B1"},
			batchStates: map[string]entity.BatchState{
				"B1": entity.BatchStateCreated,
			},
			want: entity.SpeculationPathActionSchedule,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeDesiredAction(tt.path, tt.dependencyIDs, tt.batchStates)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCanLand(t *testing.T) {
	tests := []struct {
		name   string
		tree   entity.SpeculationTree
		builds []entity.Build
		want   bool
	}{
		{
			name: "all scheduled paths passed",
			tree: entity.SpeculationTree{
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B1"}, Action: entity.SpeculationPathActionSchedule},
					{Path: entity.SpeculationPath{Base: []string{"B0"}, Head: "B1"}, Action: entity.SpeculationPathActionSchedule},
				},
			},
			builds: []entity.Build{
				{BatchID: "B1", SpeculationPath: entity.SpeculationPath{Base: nil, Head: "B1"}, Status: entity.BuildStatusPassed},
				{BatchID: "B1", SpeculationPath: entity.SpeculationPath{Base: []string{"B0"}, Head: "B1"}, Status: entity.BuildStatusPassed},
			},
			want: true,
		},
		{
			name: "some paths still pending",
			tree: entity.SpeculationTree{
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B1"}, Action: entity.SpeculationPathActionSchedule},
					{Path: entity.SpeculationPath{Base: []string{"B0"}, Head: "B1"}, Action: entity.SpeculationPathActionSchedule},
				},
			},
			builds: []entity.Build{
				{BatchID: "B1", SpeculationPath: entity.SpeculationPath{Base: nil, Head: "B1"}, Status: entity.BuildStatusPassed},
				{BatchID: "B1", SpeculationPath: entity.SpeculationPath{Base: []string{"B0"}, Head: "B1"}, Status: entity.BuildStatusRunning},
			},
			want: false,
		},
		{
			name: "cancelled paths ignored",
			tree: entity.SpeculationTree{
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B1"}, Action: entity.SpeculationPathActionSchedule},
					{Path: entity.SpeculationPath{Base: []string{"B0"}, Head: "B1"}, Action: entity.SpeculationPathActionCancel},
				},
			},
			builds: []entity.Build{
				{BatchID: "B1", SpeculationPath: entity.SpeculationPath{Base: nil, Head: "B1"}, Status: entity.BuildStatusPassed},
			},
			want: true,
		},
		{
			name: "no scheduled paths (degenerate true)",
			tree: entity.SpeculationTree{
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B1"}, Action: entity.SpeculationPathActionCancel},
				},
			},
			builds: nil,
			want:   true,
		},
		{
			name: "no builds at all for scheduled path",
			tree: entity.SpeculationTree{
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B1"}, Action: entity.SpeculationPathActionSchedule},
				},
			},
			builds: nil,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanLand(tt.tree, tt.builds)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConnectedSet(t *testing.T) {
	tests := []struct {
		name       string
		batchID    string
		deps       map[string][]string
		dependents map[string][]string
		want       []string
	}{
		{
			name:       "single node",
			batchID:    "B1",
			deps:       map[string][]string{},
			dependents: map[string][]string{},
			want:       []string{"B1"},
		},
		{
			name:    "linear chain",
			batchID: "B1",
			deps: map[string][]string{
				"B2": {"B1"},
				"B3": {"B2"},
			},
			dependents: map[string][]string{
				"B1": {"B2"},
				"B2": {"B3"},
			},
			want: []string{"B1", "B2", "B3"},
		},
		{
			name:    "diamond graph",
			batchID: "B1",
			deps: map[string][]string{
				"B2": {"B1"},
				"B3": {"B1"},
				"B4": {"B2", "B3"},
			},
			dependents: map[string][]string{
				"B1": {"B2", "B3"},
				"B2": {"B4"},
				"B3": {"B4"},
			},
			want: []string{"B1", "B2", "B3", "B4"},
		},
		{
			name:    "disconnected subgraph not reached",
			batchID: "B1",
			deps: map[string][]string{
				"B2": {"B1"},
				"B4": {"B3"},
			},
			dependents: map[string][]string{
				"B1": {"B2"},
				"B3": {"B4"},
			},
			want: []string{"B1", "B2"},
		},
		{
			name:    "start from middle of chain",
			batchID: "B2",
			deps: map[string][]string{
				"B2": {"B1"},
				"B3": {"B2"},
			},
			dependents: map[string][]string{
				"B1": {"B2"},
				"B2": {"B3"},
			},
			want: []string{"B1", "B2", "B3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConnectedSet(tt.batchID, tt.deps, tt.dependents)
			sort.Strings(got)
			sort.Strings(tt.want)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDependencyBatchIDs(t *testing.T) {
	tests := []struct {
		name string
		deps []map[string]interface{}
		want []string
	}{
		{
			name: "extracts IDs",
			deps: []map[string]interface{}{
				{"ID": "B1"},
				{"ID": "B2"},
			},
			want: []string{"B1", "B2"},
		},
		{
			name: "nil deps",
			deps: nil,
			want: []string{},
		},
		{
			name: "missing ID key",
			deps: []map[string]interface{}{
				{"other": "value"},
			},
			want: []string{},
		},
		{
			name: "non-string ID",
			deps: []map[string]interface{}{
				{"ID": 123},
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DependencyBatchIDs(tt.deps)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSpeculate_SingleBatchNoDeps(t *testing.T) {
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateSpeculating},
		},
		Trees: map[string]entity.SpeculationTree{
			"B1": {
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{
						Path:   entity.SpeculationPath{Base: nil, Head: "B1"},
						Action: entity.SpeculationPathActionSchedule,
						Score:  0.9,
					},
				},
			},
		},
		Builds: []entity.Build{
			{BatchID: "B1", SpeculationPath: entity.SpeculationPath{Base: nil, Head: "B1"}, Status: entity.BuildStatusPassed},
		},
	}

	result := Speculate(input)

	assert.Empty(t, result.UpdatedTrees, "no tree changes expected")
	assert.Empty(t, result.Transitions)
	require.Len(t, result.ReadyToLand, 1)
	assert.Equal(t, "B1", result.ReadyToLand[0])
}

func TestSpeculate_TwoBatchesWithDependency(t *testing.T) {
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateSpeculating},
			"B2": {
				ID:    "B2",
				State: entity.BatchStateSpeculating,
				Dependencies: []map[string]interface{}{
					{"ID": "B1"},
				},
			},
		},
		Trees: map[string]entity.SpeculationTree{
			"B1": {
				BatchID: "B1",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B1"}, Action: entity.SpeculationPathActionSchedule, Score: 0.8},
				},
			},
			"B2": {
				BatchID: "B2",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.9},
					{Path: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.3},
				},
			},
		},
	}

	result := Speculate(input)

	// Both batches are in-flight, no action changes expected.
	assert.Empty(t, result.Transitions)
	assert.Empty(t, result.UpdatedTrees)
}

func TestSpeculate_MergeSuccessCollapse(t *testing.T) {
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateSucceeded},
			"B2": {
				ID:    "B2",
				State: entity.BatchStateSpeculating,
				Dependencies: []map[string]interface{}{
					{"ID": "B1"},
				},
			},
		},
		Trees: map[string]entity.SpeculationTree{
			"B2": {
				BatchID: "B2",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.9},
					{Path: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.3},
				},
			},
		},
	}

	result := Speculate(input)

	// Path [B2] should be cancelled (B1 succeeded but path excluded it).
	require.Len(t, result.Transitions, 1)
	assert.Equal(t, "B2", result.Transitions[0].BatchID)
	assert.Equal(t, entity.SpeculationPathActionSchedule, result.Transitions[0].FromAction)
	assert.Equal(t, entity.SpeculationPathActionCancel, result.Transitions[0].ToAction)
	assert.Nil(t, result.Transitions[0].Path.Base)

	require.Contains(t, result.UpdatedTrees, "B2")
}

func TestSpeculate_BuildFailurePrune(t *testing.T) {
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateFailed},
			"B2": {
				ID:    "B2",
				State: entity.BatchStateSpeculating,
				Dependencies: []map[string]interface{}{
					{"ID": "B1"},
				},
			},
		},
		Trees: map[string]entity.SpeculationTree{
			"B2": {
				BatchID: "B2",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.9},
					{Path: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.3},
				},
			},
		},
	}

	result := Speculate(input)

	// Path [B1, B2] should be cancelled (B1 failed but path included it).
	require.Len(t, result.Transitions, 1)
	assert.Equal(t, "B2", result.Transitions[0].BatchID)
	assert.Equal(t, entity.SpeculationPathActionCancel, result.Transitions[0].ToAction)
	assert.Equal(t, []string{"B1"}, result.Transitions[0].Path.Base)
}

func TestSpeculate_MergeFailureReSpeculate(t *testing.T) {
	// B2's path [B2] was previously cancelled (assumed B1 would merge).
	// B1's merge failed, so B1 reverts to in-flight.
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateSpeculating},
			"B2": {
				ID:    "B2",
				State: entity.BatchStateSpeculating,
				Dependencies: []map[string]interface{}{
					{"ID": "B1"},
				},
			},
		},
		Trees: map[string]entity.SpeculationTree{
			"B2": {
				BatchID: "B2",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B2"}, Action: entity.SpeculationPathActionCancel, Score: 0.9},
					{Path: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.3},
				},
			},
		},
	}

	result := Speculate(input)

	// Path [B2] should be re-scheduled (B1 back to in-flight).
	require.Len(t, result.Transitions, 1)
	assert.Equal(t, entity.SpeculationPathActionCancel, result.Transitions[0].FromAction)
	assert.Equal(t, entity.SpeculationPathActionSchedule, result.Transitions[0].ToAction)
}

func TestSpeculate_OutOfOrderLanding(t *testing.T) {
	// B2 depends on B1. Both of B2's paths have passing builds.
	// B2 can land before B1.
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateSpeculating},
			"B2": {
				ID:    "B2",
				State: entity.BatchStateSpeculating,
				Dependencies: []map[string]interface{}{
					{"ID": "B1"},
				},
			},
		},
		Trees: map[string]entity.SpeculationTree{
			"B2": {
				BatchID: "B2",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.9},
					{Path: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.3},
				},
			},
		},
		Builds: []entity.Build{
			{BatchID: "B2", SpeculationPath: entity.SpeculationPath{Base: nil, Head: "B2"}, Status: entity.BuildStatusPassed},
			{BatchID: "B2", SpeculationPath: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Status: entity.BuildStatusPassed},
		},
	}

	result := Speculate(input)

	assert.Empty(t, result.Transitions)
	require.Len(t, result.ReadyToLand, 1)
	assert.Equal(t, "B2", result.ReadyToLand[0])
}

func TestSpeculate_PartialPassNotReady(t *testing.T) {
	// Only one of B2's two scheduled paths has a passing build.
	input := SpeculateInput{
		Batches: map[string]entity.Batch{
			"B1": {ID: "B1", State: entity.BatchStateSpeculating},
			"B2": {
				ID:    "B2",
				State: entity.BatchStateSpeculating,
				Dependencies: []map[string]interface{}{
					{"ID": "B1"},
				},
			},
		},
		Trees: map[string]entity.SpeculationTree{
			"B2": {
				BatchID: "B2",
				Speculations: []entity.SpeculationInfo{
					{Path: entity.SpeculationPath{Base: nil, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.9},
					{Path: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Action: entity.SpeculationPathActionSchedule, Score: 0.3},
				},
			},
		},
		Builds: []entity.Build{
			{BatchID: "B2", SpeculationPath: entity.SpeculationPath{Base: nil, Head: "B2"}, Status: entity.BuildStatusPassed},
			{BatchID: "B2", SpeculationPath: entity.SpeculationPath{Base: []string{"B1"}, Head: "B2"}, Status: entity.BuildStatusRunning},
		},
	}

	result := Speculate(input)

	assert.Empty(t, result.ReadyToLand)
}
