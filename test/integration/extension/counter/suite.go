// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package counter

import (
	"context"
	"sort"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/submitqueue/platform/extension/counter"
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
}

// TestCounter_Concurrency tests concurrent access to the same counter
func (s *CounterContractSuite) TestCounter_Concurrency() {
	t := s.T()
	ctx := s.ctx

	domain := "test-counter-concurrent"
	numGoroutines := 10
	numIterations := 10
	totalExpected := numGoroutines * numIterations

	// Collect results via channel
	results := make(chan int64, totalExpected)

	// Launch concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < numIterations; j++ {
				seq, err := s.counter.Next(ctx, domain)
				require.NoError(t, err)
				results <- seq
			}
		}()
	}

	// Collect all sequences
	sequences := make([]int64, 0, totalExpected)
	for i := 0; i < totalExpected; i++ {
		sequences = append(sequences, <-results)
	}

	// Sort and verify contiguous range
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	assert.Len(t, sequences, totalExpected, "should have all sequences")
	for i := 1; i < len(sequences); i++ {
		assert.Equal(t, sequences[i-1]+1, sequences[i],
			"sequences should be contiguous at index %d: got %d and %d", i, sequences[i-1], sequences[i])
	}
}
