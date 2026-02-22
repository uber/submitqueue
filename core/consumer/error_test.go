package consumer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonRetryableError(t *testing.T) {
	cause := fmt.Errorf("bad payload")
	err := NewNonRetryableError(cause)

	assert.Equal(t, "non-retryable: bad payload", err.Error())
	assert.True(t, IsNonRetryable(err))
}

func TestNonRetryableError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("deserialization failed")
	err := NewNonRetryableError(cause)

	unwrapped := errors.Unwrap(err)
	require.NotNil(t, unwrapped)
	assert.Equal(t, cause, unwrapped)
}

func TestIsNonRetryable_Wrapped(t *testing.T) {
	cause := fmt.Errorf("bad json")
	nonRetryable := NewNonRetryableError(cause)
	wrapped := fmt.Errorf("controller error: %w", nonRetryable)

	assert.True(t, IsNonRetryable(wrapped))
}

func TestIsNonRetryable_RegularError(t *testing.T) {
	err := fmt.Errorf("temporary failure")

	assert.False(t, IsNonRetryable(err))
}

func TestIsNonRetryable_Nil(t *testing.T) {
	assert.False(t, IsNonRetryable(nil))
}
