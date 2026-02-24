package inmemory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity"
	entitybuild "github.com/uber/submitqueue/entity/build"
	"github.com/uber/submitqueue/extension/build"
)

// TestInMemoryBuildManager_ScheduleBuild_Success tests successful build scheduling.
func TestInMemoryBuildManager_ScheduleBuild_Success(t *testing.T) {
	mgr := NewInMemoryBuildManager(Params{
		BuildDelay: 50 * time.Millisecond,
	})

	testBatch := entity.Batch{
		ID:       "queue-1/batch/1",
		Queue:    "queue-1",
		Contains: []string{"queue-1/1", "queue-1/2"},
		State:    entity.BatchStateUnknown,
		Version:  1,
	}

	buildID, err := mgr.ScheduleBuild(
		context.Background(),
		"abc123def456",
		[]entity.BatchDependent{},
		testBatch,
		"https://github.com/uber/submitqueue",
		"main",
		"test-pipeline",
		"queue-1/1",
		map[string]string{"TEST_VAR": "value"},
		"Test build",
	)

	require.NoError(t, err)
	provider, id, err := entitybuild.ParseBuildID(buildID)
	require.NoError(t, err)
	assert.Equal(t, "mock", provider)
	assert.NotEmpty(t, id)
	assert.Contains(t, buildID.String(), "mock://")
}

// TestInMemoryBuildManager_ScheduleBuild_ValidationErrors tests parameter validation.
func TestInMemoryBuildManager_ScheduleBuild_ValidationErrors(t *testing.T) {
	mgr := NewInMemoryBuildManager(Params{})

	validBatch := entity.Batch{
		ID:       "queue-1/batch/1",
		Queue:    "queue-1",
		Contains: []string{"queue-1/1"},
		State:    entity.BatchStateUnknown,
		Version:  1,
	}

	testCases := []struct {
		name       string
		baseSHA    string
		batch      entity.Batch
		repoURL    string
		branch     string
		pipelineID string
		sqid       string
		wantErr    string
	}{
		{
			name:       "missing baseSHA",
			baseSHA:    "",
			batch:      validBatch,
			repoURL:    "https://github.com/uber/submitqueue",
			branch:     "main",
			pipelineID: "test",
			sqid:       "queue-1/1",
			wantErr:    "baseSHA is required",
		},
		{
			name:    "missing batchToBeTest.ID",
			baseSHA: "abc123",
			batch: entity.Batch{
				ID: "",
			},
			repoURL:    "https://github.com/uber/submitqueue",
			branch:     "main",
			pipelineID: "test",
			sqid:       "queue-1/1",
			wantErr:    "batchToBeTest.ID is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mgr.ScheduleBuild(
				context.Background(),
				tc.baseSHA,
				[]entity.BatchDependent{},
				tc.batch,
				tc.repoURL,
				tc.branch,
				tc.pipelineID,
				tc.sqid,
				nil,
				"",
			)

			require.Error(t, err)
			assert.True(t, build.IsInvalidRequest(err), "expected ErrInvalidRequest")
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestInMemoryBuildManager_Poll_BuildLifecycle tests polling a build through its lifecycle.
func TestInMemoryBuildManager_Poll_BuildLifecycle(t *testing.T) {
	stateChanges := make(chan entitybuild.BuildState, 10)

	mgr := NewInMemoryBuildManager(Params{
		BuildDelay: 100 * time.Millisecond,
		OnStateChange: func(buildID string, state entitybuild.BuildState) {
			stateChanges <- state
		},
	})

	testBatch := entity.Batch{
		ID:       "queue-1/batch/1",
		Queue:    "queue-1",
		Contains: []string{"queue-1/1"},
		State:    entity.BatchStateUnknown,
		Version:  1,
	}

	buildID, err := mgr.ScheduleBuild(
		context.Background(),
		"abc123def456",
		[]entity.BatchDependent{},
		testBatch,
		"https://github.com/uber/submitqueue",
		"main",
		"test-pipeline",
		"queue-1/1",
		nil,
		"",
	)
	require.NoError(t, err)

	// Immediately poll - should be queued
	status, err := mgr.Poll(context.Background(), buildID)
	require.NoError(t, err)
	assert.Equal(t, entitybuild.BuildStateQueued, status.State)
	assert.Greater(t, status.QueuedAt, int64(0))

	// Wait for running state
	state := <-stateChanges
	assert.Equal(t, entitybuild.BuildStateRunning, state)

	// Wait for terminal state
	state = <-stateChanges
	assert.Equal(t, entitybuild.BuildStatePassed, state)

	// Poll for final status
	status, err = mgr.Poll(context.Background(), buildID)
	require.NoError(t, err)
	assert.Equal(t, entitybuild.BuildStatePassed, status.State)
	assert.True(t, status.State.IsTerminal())
	assert.Greater(t, status.FinishedAt, int64(0))
}

// TestInMemoryBuildManager_Poll_BuildNotFound tests polling a non-existent build.
func TestInMemoryBuildManager_Poll_BuildNotFound(t *testing.T) {
	mgr := NewInMemoryBuildManager(Params{})

	_, err := mgr.Poll(context.Background(), entitybuild.NewBuildID("mock", "99999"))

	require.Error(t, err)
	assert.True(t, build.IsBuildNotFound(err))
}

// TestInMemoryBuildManager_Close tests closing the manager.
func TestInMemoryBuildManager_Close(t *testing.T) {
	mgr := NewInMemoryBuildManager(Params{BuildDelay: 200 * time.Millisecond})

	testBatch := entity.Batch{
		ID:       "queue-1/batch/1",
		Queue:    "queue-1",
		Contains: []string{"queue-1/1"},
		State:    entity.BatchStateUnknown,
		Version:  1,
	}

	buildID, err := mgr.ScheduleBuild(
		context.Background(),
		"abc123",
		[]entity.BatchDependent{},
		testBatch,
		"https://github.com/uber/submitqueue",
		"main",
		"test",
		"queue-1/1",
		nil,
		"",
	)
	require.NoError(t, err)

	// Close the manager
	err = mgr.Close()
	require.NoError(t, err)

	// Verify operations fail after close
	_, err = mgr.Poll(context.Background(), buildID)
	require.Error(t, err)

	// Close should be idempotent
	err = mgr.Close()
	require.NoError(t, err)
}

// TestInMemoryBuildManager_ConcurrentOperations tests thread safety.
func TestInMemoryBuildManager_ConcurrentOperations(t *testing.T) {
	mgr := NewInMemoryBuildManager(Params{BuildDelay: 100 * time.Millisecond})

	numBuilds := 10
	buildIDs := make([]entitybuild.BuildID, numBuilds)
	errors := make([]error, numBuilds)

	var wg sync.WaitGroup
	for i := 0; i < numBuilds; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			testBatch := entity.Batch{
				ID:       "queue-1/batch/1",
				Queue:    "queue-1",
				Contains: []string{"queue-1/1"},
				State:    entity.BatchStateUnknown,
				Version:  1,
			}

			buildID, err := mgr.ScheduleBuild(
				context.Background(),
				"concurrent-test",
				[]entity.BatchDependent{},
				testBatch,
				"https://github.com/uber/submitqueue",
				"main",
				"test",
				"queue-1/1",
				nil,
				"",
			)
			buildIDs[index] = buildID
			errors[index] = err
		}(i)
	}

	wg.Wait()

	// Verify all builds were scheduled successfully
	for i := 0; i < numBuilds; i++ {
		require.NoError(t, errors[i])
		assert.NotEmpty(t, buildIDs[i].String())
	}

	// Verify all build IDs are unique
	seenIDs := make(map[string]bool)
	for _, buildID := range buildIDs {
		assert.False(t, seenIDs[buildID.String()])
		seenIDs[buildID.String()] = true
	}
}

// TestInMemoryBuildManager_MetadataNeverNil verifies the guarantee that Metadata is never nil.
func TestInMemoryBuildManager_MetadataNeverNil(t *testing.T) {
	mgr := NewInMemoryBuildManager(Params{BuildDelay: 50 * time.Millisecond})

	testBatch := entity.Batch{
		ID:       "queue-1/batch/1",
		Queue:    "queue-1",
		Contains: []string{"queue-1/1"},
		State:    entity.BatchStateUnknown,
		Version:  1,
	}

	// Schedule build with nil env (optional parameter)
	buildID, err := mgr.ScheduleBuild(
		context.Background(),
		"abc123",
		[]entity.BatchDependent{},
		testBatch,
		"https://github.com/uber/submitqueue",
		"main",
		"test",
		"queue-1/1",
		nil, // nil env to test edge case
		"",
	)
	require.NoError(t, err)

	// Poll immediately - Metadata should never be nil
	status, err := mgr.Poll(context.Background(), buildID)
	require.NoError(t, err)
	assert.NotNil(t, status.Metadata, "Metadata must never be nil")

	// Verify we can safely iterate without nil check
	count := 0
	for range status.Metadata {
		count++
	}
	assert.GreaterOrEqual(t, count, 0, "Should be able to iterate over Metadata")

	// Wait for build to complete
	time.Sleep(100 * time.Millisecond)

	// Poll again - Metadata should still not be nil in terminal state
	status, err = mgr.Poll(context.Background(), buildID)
	require.NoError(t, err)
	assert.NotNil(t, status.Metadata, "Metadata must never be nil, even in terminal state")
}
