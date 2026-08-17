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

package hook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	hookext "github.com/uber/submitqueue/platform/extension/hook"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	testTopicKey = "hook"
	testGroup    = "submitqueue-hook"
)

// stubHook records the events it was handed and returns a fixed error.
type stubHook struct {
	name string
	err  error
	seen []*basehook.HookEvent
}

func newStubHook(name string, err error) *stubHook {
	return &stubHook{name: name, err: err}
}

func (h *stubHook) Handle(_ context.Context, event *basehook.HookEvent) error {
	h.seen = append(h.seen, event)
	return h.err
}

func (h *stubHook) Name() string { return h.name }

// gateHook announces that it started and then waits, so a test can require two
// hooks to be in flight at once.
type gateHook struct {
	name    string
	started chan struct{}
	release chan struct{}
}

func (h *gateHook) Handle(context.Context, *basehook.HookEvent) error {
	close(h.started)
	<-h.release
	return nil
}

func (h *gateHook) Name() string { return h.name }

// stubHooks is a hookext.Hooks whose resolution is the function itself.
type stubHooks func(event *basehook.HookEvent) []hookext.Hook

func (f stubHooks) For(event *basehook.HookEvent) []hookext.Hook { return f(event) }

// fixedHooks resolves every event to the same hooks.
func fixedHooks(hooks ...hookext.Hook) stubHooks {
	return func(*basehook.HookEvent) []hookext.Hook { return hooks }
}

func validEvent(t *testing.T) *basehook.HookEvent {
	t.Helper()
	payload, err := structpb.NewStruct(map[string]any{"batch_id": "batch-778"})
	require.NoError(t, err)
	return &basehook.HookEvent{
		Id:          "submitqueue/batch.failed/batch-778/4",
		Source:      "submitqueue",
		Type:        "batch.failed",
		TimestampMs: 1722800012345,
		Version:     4,
		Payload:     payload,
	}
}

func hookPayload(t *testing.T, event *basehook.HookEvent) []byte {
	t.Helper()
	b, err := basehook.Marshal(event)
	require.NoError(t, err)
	return b
}

func hookEventDelivery(ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage("msg-1", payload, "batch-778", nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func newController(hooks hookext.Hooks) *Controller {
	return NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), hooks, testTopicKey, testGroup)
}

func TestControllerProcess(t *testing.T) {
	t.Run("hands a well-formed event to the resolved hook", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newStubHook("stub", nil)
		event := validEvent(t)

		require.NoError(t, newController(fixedHooks(h)).Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, event))))

		require.Len(t, h.seen, 1)
		assert.Equal(t, event.GetId(), h.seen[0].GetId())
		assert.Equal(t, event.GetVersion(), h.seen[0].GetVersion())
		assert.Equal(t, "batch-778", h.seen[0].GetPayload().GetFields()["batch_id"].GetStringValue())
	})

	t.Run("an unversioned event reaches the hook unchanged", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newStubHook("stub", nil)
		event := validEvent(t)
		event.Version = 0

		require.NoError(t, newController(fixedHooks(h)).Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, event))))

		require.Len(t, h.seen, 1)
		assert.Zero(t, h.seen[0].GetVersion())
	})

	t.Run("a hook failure fails the delivery", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		boom := errors.New("boom")
		h := newStubHook("stub", boom)

		err := newController(fixedHooks(h)).Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, validEvent(t))))

		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
	})

	t.Run("an event no hook resolves to is acked", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		require.NoError(t, newController(fixedHooks()).Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, validEvent(t)))))
	})
}

// One failing hook must not keep the others from seeing the event, and the
// error must say which hooks failed so a mixed outcome is attributable.
func TestControllerRunsEveryResolvedHook(t *testing.T) {
	ctrl := gomock.NewController(t)
	exportBoom := errors.New("export boom")
	commentBoom := errors.New("comment boom")
	export := newStubHook("warehouse", exportBoom)
	comment := newStubHook("code-host", commentBoom)
	audit := newStubHook("audit", nil)

	err := newController(fixedHooks(export, comment, audit)).
		Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, validEvent(t))))

	require.Error(t, err)
	assert.ErrorIs(t, err, exportBoom)
	assert.ErrorIs(t, err, commentBoom)
	for _, h := range []*stubHook{export, comment, audit} {
		assert.Len(t, h.seen, 1, "hook %s should have seen the event", h.Name())
	}
}

// Each hook blocks until the other has started, so a serial dispatch cannot
// finish this event and the test hangs until the runner kills it.
func TestControllerRunsHooksConcurrently(t *testing.T) {
	ctrl := gomock.NewController(t)
	release := make(chan struct{})
	first := &gateHook{name: "first", started: make(chan struct{}), release: release}
	second := &gateHook{name: "second", started: make(chan struct{}), release: release}

	go func() {
		<-first.started
		<-second.started
		close(release)
	}()

	require.NoError(t, newController(fixedHooks(first, second)).
		Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, validEvent(t)))))
}

func TestControllerRunsOnlyTheHooksResolvedForTheEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	batchHook := newStubHook("batch", nil)
	requestHook := newStubHook("request", nil)
	hooks := stubHooks(func(event *basehook.HookEvent) []hookext.Hook {
		if event.GetType() == "batch.failed" {
			return []hookext.Hook{batchHook}
		}
		return []hookext.Hook{requestHook}
	})

	require.NoError(t, newController(hooks).Process(context.Background(), hookEventDelivery(ctrl, hookPayload(t, validEvent(t)))))

	assert.Len(t, batchHook.seen, 1)
	assert.Empty(t, requestHook.seen)
}

// A malformed event must fail rather than ack: dead-lettering is what keeps the
// loss visible, and no hook should see an event the contract rejects.
func TestControllerRejectsMalformedEvents(t *testing.T) {
	valid := validEvent(t)

	cases := map[string][]byte{
		"not json":      []byte("{definitely not json"),
		"empty payload": {},
		"no id":         hookPayload(t, &basehook.HookEvent{Source: valid.Source, Type: valid.Type}),
		"no source":     hookPayload(t, &basehook.HookEvent{Id: valid.Id, Type: valid.Type}),
		"no type":       hookPayload(t, &basehook.HookEvent{Id: valid.Id, Source: valid.Source}),
	}

	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newStubHook("stub", nil)

			require.Error(t, newController(fixedHooks(h)).Process(context.Background(), hookEventDelivery(ctrl, payload)))
			assert.Empty(t, h.seen, "a malformed event must never reach the hook")
		})
	}
}

// An event carrying a field this build does not know about must still dispatch:
// producers add fields without waiting for consumers.
func TestControllerToleratesUnknownFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newStubHook("stub", nil)
	payload := []byte(`{"id":"a/b/c/1","source":"a","type":"b","field_from_the_future":7}`)

	require.NoError(t, newController(fixedHooks(h)).Process(context.Background(), hookEventDelivery(ctrl, payload)))
	require.Len(t, h.seen, 1)
}
