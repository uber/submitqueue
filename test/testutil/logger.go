package testutil

import (
	"testing"
	"time"
)

// TestLogger is a simple test-aware logger that records elapsed time between logs.
type TestLogger struct {
	t    *testing.T // The testing object to report logs to.
	last time.Time  // Timestamp of the last log, for elapsed calculation.
}

// NewTestLogger creates a TestLogger for the current test.
func NewTestLogger(t *testing.T) *TestLogger {
	t.Helper()
	return &TestLogger{t: t}
}

// Logf prints a formatted log message with timestamp and elapsed time since last log.
func (l *TestLogger) Logf(format string, args ...any) {
	l.t.Helper()
	now := time.Now()
	delta := ""
	if !l.last.IsZero() {
		delta = " +" + now.Sub(l.last).Truncate(time.Millisecond).String()
	}
	l.last = now
	l.t.Logf("[%s%s] "+format, append([]any{now.Format(time.RFC3339Nano), delta}, args...)...)
}
