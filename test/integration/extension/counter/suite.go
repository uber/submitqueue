package counter

import (
	"context"
	"sync"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/submitqueue/extension/counter"
	"github.com/uber/submitqueue/test/testutil"
)

// CounterContractSuite defines the contract tests for the counter.Counter interface.
// All counter implementations must pass these tests.
// Implementation-specific tests should embed this suite and call SetCounter().
type CounterContractSuite struct {
	suite.Suite
	ctx     context.Context
	counter counter.Counter
	log     *testutil.TestLogger
}

// SetContext sets the context for tests
func (s *CounterContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetCounter is called by implementation tests to provide the concrete counter instance
func (s *CounterContractSuite) SetCounter(c counter.Counter) {
	s.counter = c
}

// SetLogger sets the logger for tests
func (s *CounterContractSuite) SetLogger(log *testutil.TestLogger) {
	s.log = log
}

// TestCounter_Next tests getting the next sequence number
func (s *CounterContractSuite) TestCounter_Next() {
	t := s.T()
	ctx := s.ctx

	domain := "test-counter-next"

	// Get first sequence number
	seq1, err := s.counter.Next(ctx, domain)
	require.NoError(t, err, "failed to get next sequence")
	assert.Greater(t, seq1, int64(0), "sequence should be positive")

	// Get next sequence number
	seq2, err := s.counter.Next(ctx, domain)
	require.NoError(t, err, "failed to get next sequence")
	assert.Equal(t, seq1+1, seq2, "sequence should increment by 1")

	// Get another
	seq3, err := s.counter.Next(ctx, domain)
	require.NoError(t, err)
	assert.Equal(t, seq2+1, seq3, "sequence should continue incrementing")

	s.log.Logf("Next test passed: %d → %d → %d", seq1, seq2, seq3)
}

// TestCounter_MultipleDomains tests independent counters
func (s *CounterContractSuite) TestCounter_MultipleDomains() {
	t := s.T()
	ctx := s.ctx

	domain1 := "test-counter-1"
	domain2 := "test-counter-2"

	// Get sequences from both domains
	seq1a, err := s.counter.Next(ctx, domain1)
	require.NoError(t, err)

	seq2a, err := s.counter.Next(ctx, domain2)
	require.NoError(t, err)

	seq1b, err := s.counter.Next(ctx, domain1)
	require.NoError(t, err)

	seq2b, err := s.counter.Next(ctx, domain2)
	require.NoError(t, err)

	// Each domain should increment independently
	assert.Equal(t, seq1a+1, seq1b, "domain1 should increment")
	assert.Equal(t, seq2a+1, seq2b, "domain2 should increment")

	s.log.Logf("Multiple domains test passed: domain1=%d→%d, domain2=%d→%d",
		seq1a, seq1b, seq2a, seq2b)
}

// TestCounter_Concurrency tests concurrent access to the same counter
func (s *CounterContractSuite) TestCounter_Concurrency() {
	t := s.T()
	ctx := s.ctx

	domain := "test-counter-concurrent"
	numGoroutines := 10
	numIterations := 10
	totalExpected := numGoroutines * numIterations

	// Collect all sequence numbers
	sequences := make([]int64, 0, totalExpected)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Launch concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				seq, err := s.counter.Next(ctx, domain)
				require.NoError(t, err)

				mu.Lock()
				sequences = append(sequences, seq)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Verify we got the expected number of sequences
	assert.Len(t, sequences, totalExpected, "should have all sequences")

	// Verify all sequences are unique (no duplicates due to race conditions)
	seqMap := make(map[int64]bool)
	for _, seq := range sequences {
		assert.False(t, seqMap[seq], "sequence %d should be unique", seq)
		seqMap[seq] = true
	}

	s.log.Logf("Concurrency test passed: %d goroutines generated %d unique sequences",
		numGoroutines, len(sequences))
}

// TestCounter_Atomicity tests that counter increments are atomic
func (s *CounterContractSuite) TestCounter_Atomicity() {
	t := s.T()
	ctx := s.ctx

	domain := "test-counter-atomic"

	// Get initial value
	initial, err := s.counter.Next(ctx, domain)
	require.NoError(t, err)

	// Launch two goroutines that both try to get next at the same time
	var seq1, seq2 int64
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		seq1, _ = s.counter.Next(ctx, domain)
	}()
	go func() {
		defer wg.Done()
		seq2, _ = s.counter.Next(ctx, domain)
	}()

	wg.Wait()

	// Both should get different values
	assert.NotEqual(t, seq1, seq2, "concurrent Next calls should return different values")

	// Both should be greater than initial
	assert.Greater(t, seq1, initial)
	assert.Greater(t, seq2, initial)

	// Both results should be initial+1 and initial+2 (in any order)
	// Verify we got exactly those two values
	results := []int64{seq1, seq2}
	expected := []int64{initial + 1, initial + 2}

	assert.Contains(t, results, expected[0], "should have initial+1")
	assert.Contains(t, results, expected[1], "should have initial+2")

	s.log.Logf("Atomicity test passed: initial=%d, concurrent results=%d and %d",
		initial, seq1, seq2)
}
