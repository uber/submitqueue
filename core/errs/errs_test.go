package errs

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnwrap(t *testing.T) {
	cause := errors.New("root cause")

	t.Run("user error", func(t *testing.T) {
		err := NewUserError(cause)
		assert.Equal(t, cause, errors.Unwrap(err))
		assert.True(t, errors.Is(err, cause))
	})

	t.Run("infra error", func(t *testing.T) {
		err := NewRetryableError(cause)
		assert.Equal(t, cause, errors.Unwrap(err))
		assert.True(t, errors.Is(err, cause))
	})
}

func TestIsUserError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("generic"),
			want: false,
		},
		{
			name: "user error",
			err:  NewUserError(errors.New("bad input")),
			want: true,
		},
		{
			name: "retryable user error",
			err:  NewRetryableUserError(errors.New("rate limited")),
			want: true,
		},
		{
			name: "retryable infra error",
			err:  NewRetryableError(errors.New("db down")),
			want: false,
		},
		{
			name: "wrapped user error",
			err:  fmt.Errorf("handler failed: %w", NewUserError(errors.New("bad input"))),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsUserError(tt.err))
		})
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "generic error is not retryable",
			err:  errors.New("something failed"),
			want: false,
		},
		{
			name: "non-retryable user error",
			err:  NewUserError(errors.New("bad input")),
			want: false,
		},
		{
			name: "retryable user error",
			err:  NewRetryableUserError(errors.New("rate limited")),
			want: true,
		},
		{
			name: "retryable infra error",
			err:  NewRetryableError(errors.New("connection reset")),
			want: true,
		},
		{
			name: "wrapped retryable infra error",
			err:  fmt.Errorf("handler: %w", NewRetryableError(errors.New("timeout"))),
			want: true,
		},
		{
			name: "wrapped non-retryable user error",
			err:  fmt.Errorf("handler: %w", NewUserError(errors.New("invalid"))),
			want: false,
		},
		{
			name: "non-retryable dependency infra error",
			err:  NewDependencyError(errors.New("upstream 503")),
			want: false,
		},
		{
			name: "retryable dependency infra error",
			err:  NewRetryableDependencyError(errors.New("upstream timeout")),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsRetryable(tt.err))
		})
	}
}

func TestIsDependencyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("generic"),
			want: false,
		},
		{
			name: "user error",
			err:  NewUserError(errors.New("bad input")),
			want: false,
		},
		{
			name: "retryable infra error without dependency",
			err:  NewRetryableError(errors.New("disk full")),
			want: false,
		},
		{
			name: "non-retryable dependency infra error",
			err:  NewDependencyError(errors.New("upstream 503")),
			want: true,
		},
		{
			name: "retryable dependency infra error",
			err:  NewRetryableDependencyError(errors.New("upstream timeout")),
			want: true,
		},
		{
			name: "wrapped dependency infra error",
			err:  fmt.Errorf("handler: %w", NewDependencyError(errors.New("upstream 503"))),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsDependencyError(tt.err))
		})
	}
}

func TestErrorsAs(t *testing.T) {
	t.Run("extract user error from chain", func(t *testing.T) {
		cause := errors.New("invalid email")
		wrapped := fmt.Errorf("validation: %w", NewRetryableUserError(cause))

		var ue *userError
		require.True(t, errors.As(wrapped, &ue))
		assert.True(t, ue.retryable)
		assert.True(t, errors.Is(ue, cause))
	})

	t.Run("extract infra error from chain", func(t *testing.T) {
		cause := errors.New("connection refused")
		wrapped := fmt.Errorf("rpc: %w", NewRetryableError(cause))

		var ie *infraError
		require.True(t, errors.As(wrapped, &ie))
		assert.True(t, ie.retryable)
		assert.True(t, errors.Is(ie, cause))
	})

	t.Run("extract enclosed custom type through user error", func(t *testing.T) {
		cause := &testCauseError{code: 42}
		wrapped := fmt.Errorf("handler: %w", NewUserError(cause))

		var extracted *testCauseError
		require.True(t, errors.As(wrapped, &extracted))
		assert.Equal(t, 42, extracted.code)
	})

	t.Run("extract enclosed custom type through infra error", func(t *testing.T) {
		cause := &testCauseError{code: 503}
		wrapped := fmt.Errorf("handler: %w", NewRetryableError(cause))

		var extracted *testCauseError
		require.True(t, errors.As(wrapped, &extracted))
		assert.Equal(t, 503, extracted.code)
	})
}

func TestErrorsIs(t *testing.T) {
	t.Run("match user error by framework type", func(t *testing.T) {
		err := NewUserError(errors.New("bad input"))
		wrapped := fmt.Errorf("handler: %w", err)

		assert.True(t, errors.Is(wrapped, &userError{}))
		assert.False(t, errors.Is(wrapped, &infraError{}))
	})

	t.Run("match infra error by framework type", func(t *testing.T) {
		err := NewRetryableError(errors.New("timeout"))
		wrapped := fmt.Errorf("handler: %w", err)

		assert.True(t, errors.Is(wrapped, &infraError{}))
		assert.False(t, errors.Is(wrapped, &userError{}))
	})

	t.Run("match enclosed cause through user error", func(t *testing.T) {
		cause := errors.New("root cause")
		err := NewUserError(cause)
		wrapped := fmt.Errorf("handler: %w", err)

		assert.True(t, errors.Is(wrapped, cause))
	})

	t.Run("match enclosed cause through infra error", func(t *testing.T) {
		cause := errors.New("root cause")
		err := NewRetryableError(cause)
		wrapped := fmt.Errorf("handler: %w", err)

		assert.True(t, errors.Is(wrapped, cause))
	})

	t.Run("match both framework type and cause in same chain", func(t *testing.T) {
		cause := errors.New("db unavailable")
		err := NewRetryableError(cause)
		wrapped := fmt.Errorf("handler: %w", err)

		assert.True(t, errors.Is(wrapped, &infraError{}), "should match framework type")
		assert.True(t, errors.Is(wrapped, cause), "should match enclosed cause")
	})

	t.Run("generic error does not match framework types", func(t *testing.T) {
		err := errors.New("generic")

		assert.False(t, errors.Is(err, &userError{}))
		assert.False(t, errors.Is(err, &infraError{}))
	})
}

// testCauseError is a custom error type used to verify that errors.As can
// extract the enclosed cause type through framework error wrappers.
type testCauseError struct {
	code int
}

func (e *testCauseError) Error() string {
	return fmt.Sprintf("cause error: code %d", e.code)
}
