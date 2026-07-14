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

package testutil

import (
	"sync"
	"testing"
	"time"
)

// TestLogger is a simple test-aware logger that records elapsed time between logs.
// Safe for concurrent use — harness goroutines (e.g. queue observers) may log
// while the test goroutine does.
type TestLogger struct {
	t    *testing.T // The testing object to report logs to.
	mu   sync.Mutex // Guards last.
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
	l.mu.Lock()
	delta := ""
	if !l.last.IsZero() {
		delta = " +" + now.Sub(l.last).Truncate(time.Millisecond).String()
	}
	l.last = now
	l.mu.Unlock()
	l.t.Logf("[%s%s] "+format, append([]any{now.Format(time.RFC3339Nano), delta}, args...)...)
}
