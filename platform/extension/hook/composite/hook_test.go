// Copyright (c) 2026 Uber Technologies, Inc.
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

package composite

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/extension/hook"
)

// recordingHook records the events it saw and fails with a fixed error.
type recordingHook struct {
	name string
	err  error
	seen []string
}

var _ hook.Hook = (*recordingHook)(nil)

func (h *recordingHook) Handle(_ context.Context, event *basehook.HookEvent) error {
	h.seen = append(h.seen, event.GetId())
	return h.err
}

func (h *recordingHook) Name() string { return h.name }

func event() *basehook.HookEvent {
	return &basehook.HookEvent{Id: "submitqueue/batch.failed/batch-778/4", Source: "submitqueue", Type: "batch.failed"}
}

func TestHandle(t *testing.T) {
	t.Run("no children", func(t *testing.T) {
		require.NoError(t, New().Handle(context.Background(), event()))
	})

	t.Run("every child sees the event", func(t *testing.T) {
		first := &recordingHook{name: "first"}
		second := &recordingHook{name: "second"}

		require.NoError(t, New(first, second).Handle(context.Background(), event()))
		assert.Equal(t, []string{event().GetId()}, first.seen)
		assert.Equal(t, []string{event().GetId()}, second.seen)
	})

	t.Run("a failing child does not stop the others", func(t *testing.T) {
		boom := errors.New("boom")
		failing := &recordingHook{name: "failing", err: boom}
		healthy := &recordingHook{name: "healthy"}

		err := New(failing, healthy).Handle(context.Background(), event())

		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, []string{event().GetId()}, healthy.seen, "the healthy child runs after the failing one")
	})

	t.Run("every failure survives the join", func(t *testing.T) {
		first := errors.New("first failure")
		second := errors.New("second failure")

		err := New(
			&recordingHook{name: "first", err: first},
			&recordingHook{name: "second", err: second},
		).Handle(context.Background(), event())

		require.Error(t, err)
		assert.ErrorIs(t, err, first)
		assert.ErrorIs(t, err, second)
	})
}
