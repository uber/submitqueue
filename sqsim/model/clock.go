// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"context"
	"time"
)

// Clock supplies wall time and cancellable waits to modeled operations.
type Clock interface {
	// Now returns the current wall time.
	Now() time.Time
	// Wait blocks for the duration or until the context is cancelled.
	Wait(ctx context.Context, duration time.Duration) error
}

// RealClock uses the process wall clock.
type RealClock struct{}

// Now returns the current wall time.
func (RealClock) Now() time.Time {
	return time.Now()
}

// Wait blocks for the duration or until the context is cancelled.
func (RealClock) Wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
