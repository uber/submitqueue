package inmemory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/uber/submitqueue/entity"
	entitybuild "github.com/uber/submitqueue/entity/build"
	"github.com/uber/submitqueue/extension/build"
)

// inMemoryBuildManager is an in-memory implementation of build.BuildManager for testing.
// It simulates asynchronous build execution without making any external API calls.
//
// This is a functional test helper that simulates real CI behavior with goroutines,
// state transitions, and deterministic build outcomes based on batch IDs.
type inMemoryBuildManager struct {
	// mu protects all fields below for thread-safe concurrent access
	mu sync.RWMutex

	// builds stores all builds by their ID
	// Key: build ID string, Value: pointer to buildData
	builds map[string]*buildData

	// nextID is the counter for generating sequential build IDs
	nextID int

	// buildDelay is how long simulated builds take to complete
	// Default is 100ms for fast tests
	buildDelay time.Duration

	// onStateChange is an optional callback invoked when a build changes state
	// This is called while holding the lock, so callbacks should be fast
	// Used for deterministic testing without time.Sleep
	onStateChange func(buildID string, state entitybuild.BuildState)

	// closed tracks whether this manager has been closed
	// After closing, all methods return errors
	closed bool
}

// buildData represents the internal state of a mock build
type buildData struct {
	// baseSHA is the starting point (main branch SHA)
	baseSHA string

	// speculatedBatchesToBeApplied are base batches applied on main
	speculatedBatchesToBeApplied []entity.BatchDependent

	// batchToBeTest is the batch being tested
	batchToBeTest entity.Batch

	// Other build parameters
	repoURL    string
	branch     string
	pipelineID string
	sqid       string
	env        map[string]string
	message    string

	// status is the current BuildStatus
	// This is updated by the background goroutine as the build progresses
	status entitybuild.BuildStatus

	// cancel is called to stop the build execution goroutine
	// nil if the build has already finished
	cancel context.CancelFunc
}

// Params contains configuration options for creating an in-memory BuildManager.
type Params struct {
	// BuildDelay specifies how long simulated builds take to complete.
	// If zero, defaults to 100ms.
	// Set to a smaller value for faster tests, or larger for testing timeouts.
	BuildDelay time.Duration

	// OnStateChange is an optional callback invoked when a build changes state.
	// This is called while holding the manager's lock, so callbacks must be fast and non-blocking.
	// Used for deterministic testing without time.Sleep - tests can wait on channels until specific states.
	// The callback receives the build ID string and the new state.
	OnStateChange func(buildID string, state entitybuild.BuildState)
}

// NewInMemoryBuildManager creates a new in-memory BuildManager for testing.
// All builds are stored in memory and simulated with goroutines.
//
// The implementation supports deterministic test behavior:
//   - Batch IDs containing "fail" will result in failed builds
//   - Batch IDs containing "block" will result in blocked builds
//   - All other batches will result in passed builds
//
// This is a functional test helper that simulates real CI behavior with
// async execution, state transitions, and configurable build delays.
func NewInMemoryBuildManager(params Params) build.BuildManager {
	// Set default build delay if not specified
	delay := params.BuildDelay
	if delay == 0 {
		// Default to 100ms for reasonably fast tests
		delay = 100 * time.Millisecond
	}

	return &inMemoryBuildManager{
		builds:        make(map[string]*buildData),
		nextID:        1,
		buildDelay:    delay,
		onStateChange: params.OnStateChange,
		closed:        false,
	}
}

// ScheduleBuild creates a new in-memory build and starts a goroutine to simulate its execution.
func (m *inMemoryBuildManager) ScheduleBuild(
	ctx context.Context,
	baseSHA string,
	speculatedBatchesToBeApplied []entity.BatchDependent,
	batchToBeTest entity.Batch,
	repoURL string,
	branch string,
	pipelineID string,
	sqid string,
	env map[string]string,
	message string,
) (entitybuild.BuildID, error) {
	// Validate the parameters first before doing any work
	if err := m.validateParams(baseSHA, batchToBeTest, repoURL, branch, pipelineID, sqid); err != nil {
		return "", err
	}

	// Lock for reading/writing build state
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if manager has been closed
	if m.closed {
		return "", fmt.Errorf("manager is closed")
	}

	// Generate a unique build ID
	// Format: "mock://1", "mock://2", etc.
	buildIDStr := fmt.Sprintf("%d", m.nextID)
	m.nextID++

	// Create the BuildID using the standard format
	buildID := entitybuild.NewBuildID("mock", buildIDStr)

	// Record the current time for timestamps (Unix milliseconds)
	now := time.Now().UnixMilli()

	// Generate build message from batches
	buildMessage := m.generateBuildMessage(speculatedBatchesToBeApplied, batchToBeTest, message)

	// Enrich environment variables with batch metadata
	enrichedEnv := m.enrichEnvironment(baseSHA, speculatedBatchesToBeApplied, batchToBeTest, env)

	// Create initial build status in "queued" state
	status := entitybuild.BuildStatus{
		ID:         buildID,
		State:      entitybuild.BuildStateQueued,
		QueuedAt:   now,
		StartedAt:  0, // Not started yet
		FinishedAt: 0, // Not finished yet
		WebURL:     fmt.Sprintf("https://mock-ci.example.com/builds/%s", buildIDStr),
		LogsURL:    fmt.Sprintf("https://mock-ci.example.com/builds/%s/logs", buildIDStr),
		Metadata: map[string]string{
			"base_sha":     baseSHA,
			"base_batches": formatBatchDependentIDs(speculatedBatchesToBeApplied),
			"test_batch":   batchToBeTest.ID,
			"repo":         repoURL,
			"branch":       branch,
			"sqid":         sqid,
		},
	}

	// Create a cancellable context for the build execution goroutine
	buildCtx, cancel := context.WithCancel(context.Background())

	// Store the build data
	m.builds[buildIDStr] = &buildData{
		baseSHA:                      baseSHA,
		speculatedBatchesToBeApplied: speculatedBatchesToBeApplied,
		batchToBeTest:                batchToBeTest,
		repoURL:                      repoURL,
		branch:                       branch,
		pipelineID:                   pipelineID,
		sqid:                         sqid,
		env:                          enrichedEnv,
		message:                      buildMessage,
		status:                       status,
		cancel:                       cancel,
	}

	// Start a goroutine to simulate async build execution
	go m.executeBuild(buildCtx, buildIDStr)

	return buildID, nil
}

// executeBuild simulates the async execution of a build in a background goroutine.
// It transitions through states: queued -> running -> passed/failed/cancelled
func (m *inMemoryBuildManager) executeBuild(ctx context.Context, buildIDStr string) {
	// Wait a small delay to simulate queueing time
	// Use 10% of total build delay for queue time
	queueDelay := m.buildDelay / 10
	select {
	case <-time.After(queueDelay):
		// Queue time elapsed, proceed to running state
	case <-ctx.Done():
		// Build was cancelled while queued
		m.updateBuildState(buildIDStr, entitybuild.BuildStateCancelled, "")
		return
	}

	// Transition to running state
	m.mu.Lock()
	data, exists := m.builds[buildIDStr]
	if exists {
		// Record when the build started executing
		data.status.State = entitybuild.BuildStateRunning
		data.status.StartedAt = time.Now().UnixMilli()
		// Notify listeners that state changed
		if m.onStateChange != nil {
			m.onStateChange(buildIDStr, entitybuild.BuildStateRunning)
		}
	}
	m.mu.Unlock()

	// Simulate build execution for the remaining delay
	executionDelay := m.buildDelay - queueDelay
	select {
	case <-time.After(executionDelay):
		// Build execution completed, determine final state
	case <-ctx.Done():
		// Build was cancelled while running
		m.updateBuildState(buildIDStr, entitybuild.BuildStateCancelled, "")
		return
	}

	// Get batch ID for deterministic testing
	m.mu.RLock()
	data, exists = m.builds[buildIDStr]
	var batchID string
	if exists {
		batchID = data.batchToBeTest.ID
	}
	m.mu.RUnlock()

	// Determine final state based on batch ID for deterministic testing
	var finalState entitybuild.BuildState
	var errorMsg string

	// Check batch ID for test markers using strings.Contains
	// This allows tests to control build outcomes deterministically
	if strings.Contains(batchID, "fail") {
		// Batch ID contains "fail" -> build fails
		finalState = entitybuild.BuildStateFailed
		errorMsg = "Build failed: tests did not pass"
	} else if strings.Contains(batchID, "block") {
		// Batch ID contains "block" -> build is blocked waiting for approval
		finalState = entitybuild.BuildStateBlocked
		errorMsg = ""
	} else {
		// Default case -> build passes
		finalState = entitybuild.BuildStatePassed
		errorMsg = ""
	}

	// Update to final state
	m.updateBuildState(buildIDStr, finalState, errorMsg)
}

// updateBuildState updates a build's state and finished timestamp atomically.
func (m *inMemoryBuildManager) updateBuildState(buildIDStr string, state entitybuild.BuildState, errorMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find the build data
	data, exists := m.builds[buildIDStr]
	if !exists {
		// Build was deleted, nothing to update
		return
	}

	// Update the state
	data.status.State = state
	data.status.ErrorMessage = errorMsg

	// If this is a terminal state, record the finish time and clear cancel function
	if state.IsTerminal() {
		data.status.FinishedAt = time.Now().UnixMilli()
		// Clear the cancel function since build is done
		// This is safe because we hold the lock during both read and write
		data.cancel = nil
	}

	// Notify listeners that state changed
	// This is called while holding the lock, so callbacks must be fast
	if m.onStateChange != nil {
		m.onStateChange(buildIDStr, state)
	}
}

// Poll retrieves the current status of a build.
func (m *inMemoryBuildManager) Poll(ctx context.Context, id entitybuild.BuildID) (entitybuild.BuildStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if manager has been closed
	if m.closed {
		return entitybuild.BuildStatus{}, fmt.Errorf("manager is closed")
	}

	// Parse the BuildID to extract provider and ID
	provider, idStr, err := entitybuild.ParseBuildID(id)
	if err != nil {
		return entitybuild.BuildStatus{}, build.WrapInvalidRequest(err)
	}

	// Verify this is a mock build
	if provider != "mock" {
		return entitybuild.BuildStatus{}, build.WrapInvalidRequest(
			fmt.Errorf("provider mismatch: expected 'mock', got '%s'", provider),
		)
	}

	// Look up the build
	data, exists := m.builds[idStr]
	if !exists {
		// Build not found
		return entitybuild.BuildStatus{}, build.WrapBuildNotFound(
			fmt.Errorf("build %s does not exist", idStr),
		)
	}

	// Return a copy of the current status
	// Copy the status to avoid race conditions if caller modifies it
	status := data.status

	// Deep copy the metadata map
	// Metadata is guaranteed to be non-nil, so we always create a copy
	status.Metadata = make(map[string]string, len(data.status.Metadata))
	for k, v := range data.status.Metadata {
		status.Metadata[k] = v
	}

	return status, nil
}

// CancelBuild requests cancellation of a queued or running build.
func (m *inMemoryBuildManager) CancelBuild(ctx context.Context, id entitybuild.BuildID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if manager has been closed
	if m.closed {
		return fmt.Errorf("manager is closed")
	}

	// Parse the BuildID to extract provider and ID
	provider, idStr, err := entitybuild.ParseBuildID(id)
	if err != nil {
		return build.WrapInvalidRequest(err)
	}

	// Verify this is a mock build
	if provider != "mock" {
		return build.WrapInvalidRequest(
			fmt.Errorf("provider mismatch: expected 'mock', got '%s'", provider),
		)
	}

	// Look up the build
	data, exists := m.builds[idStr]
	if !exists {
		// Build not found
		return build.WrapBuildNotFound(
			fmt.Errorf("build %s does not exist", idStr),
		)
	}

	// Check if build is still cancellable
	// Can only cancel if build is not in a terminal state
	if data.status.State.IsTerminal() {
		return build.WrapBuildNotCancellable(
			fmt.Errorf("build %s is already in terminal state: %s", idStr, data.status.State),
		)
	}

	// Cancel the build execution goroutine if it's still running
	// Check for nil to handle race where build just finished
	// This is safe because we hold the lock during both read and call
	if data.cancel != nil {
		// Calling cancel() will cause the executeBuild goroutine to exit
		// and update the state to BuildStateCancelled via updateBuildState
		data.cancel()
	}

	return nil
}

// Close shuts down the in-memory build manager.
// After Close is called, all operations will return errors.
func (m *inMemoryBuildManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		// Already closed, this is idempotent
		return nil
	}

	// Mark as closed
	m.closed = true

	// Cancel all running builds
	for _, data := range m.builds {
		if data.cancel != nil {
			data.cancel()
		}
	}

	return nil
}

// validateParams checks that all required parameters are present.
func (m *inMemoryBuildManager) validateParams(
	baseSHA string,
	batchToBeTest entity.Batch,
	repoURL string,
	branch string,
	pipelineID string,
	sqid string,
) error {
	// Check baseSHA is not empty
	if baseSHA == "" {
		return build.WrapInvalidRequest(fmt.Errorf("baseSHA is required"))
	}

	// Check batchToBeTest.ID is not empty
	if batchToBeTest.ID == "" {
		return build.WrapInvalidRequest(fmt.Errorf("batchToBeTest.ID is required"))
	}

	// Check repoURL is not empty
	if repoURL == "" {
		return build.WrapInvalidRequest(fmt.Errorf("repoURL is required"))
	}

	// Check branch is not empty
	if branch == "" {
		return build.WrapInvalidRequest(fmt.Errorf("branch is required"))
	}

	// Check pipelineID is not empty
	if pipelineID == "" {
		return build.WrapInvalidRequest(fmt.Errorf("pipelineID is required"))
	}

	// Check sqid is not empty
	if sqid == "" {
		return build.WrapInvalidRequest(fmt.Errorf("sqid is required"))
	}

	// Note: speculatedBatchesToBeApplied can be empty (single batch test)
	// env and message are optional

	// All required parameters are present
	return nil
}

// generateBuildMessage creates a descriptive build message from batch information.
func (m *inMemoryBuildManager) generateBuildMessage(
	baseBatches []entity.BatchDependent,
	testBatch entity.Batch,
	userMessage string,
) string {
	var parts []string

	// Start with the test batch
	if len(baseBatches) == 0 {
		// No base batches, simple message
		parts = append(parts, fmt.Sprintf("Testing batch %s", testBatch.ID))
	} else {
		// Include base batches in message
		baseBatchIDs := formatBatchDependentIDs(baseBatches)
		parts = append(parts, fmt.Sprintf("Testing batch %s on top of %s", testBatch.ID, baseBatchIDs))
	}

	// Append user message if provided
	if userMessage != "" {
		parts = append(parts, userMessage)
	}

	return strings.Join(parts, " - ")
}

// enrichEnvironment adds batch metadata to environment variables.
func (m *inMemoryBuildManager) enrichEnvironment(
	baseSHA string,
	baseBatches []entity.BatchDependent,
	testBatch entity.Batch,
	userEnv map[string]string,
) map[string]string {
	// Copy user env and add batch metadata
	env := make(map[string]string, len(userEnv)+3)
	for k, v := range userEnv {
		env[k] = v
	}

	// Add batch metadata as environment variables
	env["SQ_BASE_SHA"] = baseSHA
	env["SQ_BASE_BATCHES"] = formatBatchDependentIDs(baseBatches)
	env["SQ_TEST_BATCH"] = testBatch.ID

	return env
}

// formatBatchDependentIDs converts a slice of batch dependents to comma-separated IDs.
func formatBatchDependentIDs(batches []entity.BatchDependent) string {
	if len(batches) == 0 {
		return ""
	}
	ids := make([]string, len(batches))
	for i, batch := range batches {
		ids[i] = batch.BatchID
	}
	return strings.Join(ids, ",")
}

