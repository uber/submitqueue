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

package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/stovepipe/controller"
	"github.com/uber/submitqueue/stovepipe/controller/dlq"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/zap/zaptest"
)

type fakeRequestHistoryController struct {
	getByID  func(context.Context, entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error)
	getByURI func(context.Context, entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error)
}

var _ controller.RequestHistoryController = (*fakeRequestHistoryController)(nil)

func (f *fakeRequestHistoryController) GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error) {
	return f.getByID(ctx, req)
}

func (f *fakeRequestHistoryController) GetRequestHistoryByURI(ctx context.Context, req entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error) {
	return f.getByURI(ctx, req)
}

func TestGetRequestHistoryByID(t *testing.T) {
	controllerErr := errors.New("controller failed")
	logs := []entity.RequestLog{
		{ID: "occurrence/1", State: entity.RequestStateAccepted, TimestampMs: 1000},
		{ID: "occurrence/2", Event: entity.RequestEventBuildTriggered, TimestampMs: 2000},
	}
	tests := []struct {
		name     string
		logs     []entity.RequestLog
		err      error
		wantLogs int
	}{
		{name: "maps successful result", logs: logs, wantLogs: 2},
		{name: "maps empty result", logs: nil, wantLogs: 0},
		{name: "returns controller error unchanged", err: controllerErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotReq entity.GetRequestHistoryByIDRequest
			fake := &fakeRequestHistoryController{
				getByID: func(_ context.Context, req entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error) {
					gotReq = req
					return tt.logs, tt.err
				},
			}
			srv := &StovepipeServer{requestHistoryController: fake}

			resp, err := srv.GetRequestHistoryByID(context.Background(), &pb.GetRequestHistoryByIDRequest{
				Queue: "monorepo/main", RequestId: "request/1",
			})

			assert.Equal(t, entity.GetRequestHistoryByIDRequest{Queue: "monorepo/main", ID: "request/1"}, gotReq)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.Events, tt.wantLogs)
			if tt.wantLogs > 0 {
				assert.Equal(t, "accepted", resp.Events[0].GetRequestState())
				assert.Equal(t, "build_triggered", resp.Events[1].GetEvent())
			}
		})
	}
}

func TestGetRequestHistoryByURI(t *testing.T) {
	controllerErr := errors.New("controller failed")
	histories := []entity.RequestHistory{{
		RequestID: "request/1",
		Events:    []entity.RequestLog{{ID: "occurrence/1", State: entity.RequestStateAccepted}},
	}}
	tests := []struct {
		name          string
		histories     []entity.RequestHistory
		err           error
		wantHistories int
	}{
		{name: "maps successful result", histories: histories, wantHistories: 1},
		{name: "maps empty result", histories: nil, wantHistories: 0},
		{name: "returns controller error unchanged", err: controllerErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotReq entity.GetRequestHistoryByURIRequest
			fake := &fakeRequestHistoryController{
				getByURI: func(_ context.Context, req entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error) {
					gotReq = req
					return tt.histories, tt.err
				},
			}
			srv := &StovepipeServer{requestHistoryController: fake}

			resp, err := srv.GetRequestHistoryByURI(context.Background(), &pb.GetRequestHistoryByURIRequest{
				Queue: "monorepo/main", Uri: "git://monorepo/abc",
			})

			assert.Equal(t, entity.GetRequestHistoryByURIRequest{Queue: "monorepo/main", URI: "git://monorepo/abc"}, gotReq)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.Histories, tt.wantHistories)
			if tt.wantHistories > 0 {
				assert.Equal(t, "request/1", resp.Histories[0].RequestId)
				require.Len(t, resp.Histories[0].Events, 1)
				assert.Equal(t, "accepted", resp.Histories[0].Events[0].GetRequestState())
			}
		})
	}
}

// recordingConsumer captures what the host registers instead of subscribing.
type recordingConsumer struct {
	controllers []consumer.Controller
}

func (c *recordingConsumer) Register(controller consumer.Controller) error {
	c.controllers = append(c.controllers, controller)
	return nil
}

func (c *recordingConsumer) Start(context.Context) error { return nil }

func (c *recordingConsumer) Stop(int64) error { return nil }

// registeredControllers runs the host's registration exactly as run() does and
// returns the registry it registers against, the primary controllers, and the
// DLQ controllers.
func registeredControllers(t *testing.T) (consumer.TopicRegistry, []consumer.Controller, []consumer.Controller) {
	t.Helper()

	registry, err := newTopicRegistry(nil, "subscriber")
	require.NoError(t, err)

	logger := zaptest.NewLogger(t).Sugar()
	store := storageFactory{}
	primary := &recordingConsumer{}
	deadLetter := &recordingConsumer{}

	_, err = registerPrimaryControllers(primary, logger, tally.NoopScope, store, registry,
		fakeSourceControlFactory{}, fakeBuildRunnerFactory{}, hookResolver{})
	require.NoError(t, err)

	_, err = registerDLQControllers(deadLetter, logger, tally.NoopScope, store, registry,
		fakeSourceControlFactory{})
	require.NoError(t, err)

	return registry, primary.controllers, deadLetter.controllers
}

func topicKeys(controllers []consumer.Controller) []consumer.TopicKey {
	keys := make([]consumer.TopicKey, 0, len(controllers))
	for _, c := range controllers {
		keys = append(keys, c.TopicKey())
	}
	return keys
}

func TestEveryRegisteredControllerResolvesInTheTopicRegistry(t *testing.T) {
	registry, primary, deadLetter := registeredControllers(t)

	for _, c := range append(primary, deadLetter...) {
		t.Run(c.Name(), func(t *testing.T) {
			_, ok := registry.TopicName(c.TopicKey())
			assert.True(t, ok, "no topic name registered for the key the controller subscribes to")

			_, ok = registry.SubscriptionConfig(c.TopicKey(), c.ConsumerGroup())
			assert.True(t, ok, "no subscription registered for the controller's consumer group")
		})
	}
}

func TestHookStage(t *testing.T) {
	registry, primary, deadLetter := registeredControllers(t)

	t.Run("hook events are consumed", func(t *testing.T) {
		assert.Contains(t, topicKeys(primary), basehook.TopicKeyHook)
	})

	t.Run("hook events that exhaust their retries are consumed", func(t *testing.T) {
		assert.Contains(t, topicKeys(deadLetter), dlq.TopicKey(basehook.TopicKeyHook))
	})

	// The key is shared across domains, so an unqualified name would collide with
	// another domain's hook topic on a queue backend the two share.
	t.Run("the hook topics are named for this domain", func(t *testing.T) {
		for _, key := range []consumer.TopicKey{basehook.TopicKeyHook, dlq.TopicKey(basehook.TopicKeyHook)} {
			name, ok := registry.TopicName(key)
			require.True(t, ok)
			assert.True(t, strings.HasPrefix(name, "stovepipe-"), "topic %q is not domain-qualified", name)
		}
	})
}
