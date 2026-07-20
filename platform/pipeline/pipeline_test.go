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

package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// testDeps is a minimal Deps type for testing.
type testDeps struct {
	logger *zap.SugaredLogger
}

// fakeController satisfies consumer.Controller for testing.
type fakeController struct {
	key   consumer.TopicKey
	group string
}

func (f *fakeController) Process(_ context.Context, _ consumer.Delivery) error { return nil }
func (f *fakeController) Name() string                                         { return string(f.key) }
func (f *fakeController) TopicKey() consumer.TopicKey                          { return f.key }
func (f *fakeController) ConsumerGroup() string                                { return f.group }

func newTestLogger() *zap.SugaredLogger {
	l, _ := zap.NewDevelopment()
	return l.Sugar()
}

func TestConstruct_SingleStage_NoDLQ(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start", group: "orchestrator-start"}, nil
			},
		},
	}

	comp, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages)
	require.NoError(t, err)
	assert.NotNil(t, comp)
}

func TestConstruct_WithDLQ(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start", group: "orchestrator-start"}, nil
			},
			DLQ: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start_dlq", group: "orchestrator-start-dlq"}, nil
			},
		},
	}

	comp, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages)
	require.NoError(t, err)
	assert.NotNil(t, comp)
}

func TestConstruct_MultipleStages(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start", group: "orchestrator-start"}, nil
			},
		},
		{
			Key:           "validate",
			Name:          "validate",
			ConsumerGroup: "orchestrator-validate",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "validate", group: "orchestrator-validate"}, nil
			},
			DLQ: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "validate_dlq", group: "orchestrator-validate-dlq"}, nil
			},
		},
	}

	comp, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages)
	require.NoError(t, err)
	assert.NotNil(t, comp)
}

func TestConstruct_EmptyStages_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)

	deps := testDeps{logger: newTestLogger()}
	_, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one stage is required")
}

func TestConstruct_ControllerCreationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return nil, fmt.Errorf("missing dependency")
			},
		},
	}

	_, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage start")
	assert.Contains(t, err.Error(), "missing dependency")
}

func TestConstruct_DLQControllerCreationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start", group: "orchestrator-start"}, nil
			},
			DLQ: func(d testDeps) (consumer.Controller, error) {
				return nil, fmt.Errorf("dlq dependency missing")
			},
		},
	}

	_, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage start dlq")
	assert.Contains(t, err.Error(), "dlq dependency missing")
}

func TestConstruct_WithPublishOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start", group: "orchestrator-start"}, nil
			},
		},
	}

	comp, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages,
		PublishOnly(
			PublishOnlyTopic{Key: "log", Name: "log"},
			PublishOnlyTopic{Key: "merge-request", Name: "merge-request"},
		),
	)
	require.NoError(t, err)
	assert.NotNil(t, comp)
}

func TestConstruct_WithTopicNameOverrides(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)
	q.EXPECT().Subscriber().Return(mqmock.NewMockSubscriber(ctrl)).AnyTimes()
	q.EXPECT().Publisher().Return(mqmock.NewMockPublisher(ctrl)).AnyTimes()

	deps := testDeps{logger: newTestLogger()}
	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New: func(d testDeps) (consumer.Controller, error) {
				return &fakeController{key: "start", group: "orchestrator-start"}, nil
			},
		},
	}

	comp, err := Construct(deps.logger, tally.NoopScope, q, "test-sub", deps, stages,
		TopicNames(map[consumer.TopicKey]string{
			"start": "custom-start-topic",
		}),
	)
	require.NoError(t, err)
	assert.NotNil(t, comp)
}

func TestResolveTopicName(t *testing.T) {
	tests := []struct {
		name      string
		key       consumer.TopicKey
		defaultN  string
		overrides map[consumer.TopicKey]string
		want      string
	}{
		{
			name:     "no overrides",
			key:      "start",
			defaultN: "start",
			want:     "start",
		},
		{
			name:      "nil overrides",
			key:       "start",
			defaultN:  "start",
			overrides: nil,
			want:      "start",
		},
		{
			name:      "key not in overrides",
			key:       "start",
			defaultN:  "start",
			overrides: map[consumer.TopicKey]string{"other": "other-name"},
			want:      "start",
		},
		{
			name:      "key in overrides",
			key:       "start",
			defaultN:  "start",
			overrides: map[consumer.TopicKey]string{"start": "custom-start"},
			want:      "custom-start",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveTopicName(tt.key, tt.defaultN, tt.overrides)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDLQTopicKey(t *testing.T) {
	assert.Equal(t, consumer.TopicKey("start_dlq"), dlqTopicKey("start"))
	assert.Equal(t, consumer.TopicKey("validate_dlq"), dlqTopicKey("validate"))
}

func TestBuildTopicConfigs(t *testing.T) {
	ctrl := gomock.NewController(t)
	q := mqmock.NewMockQueue(ctrl)

	stages := []Stage[testDeps]{
		{
			Key:           "start",
			Name:          "start",
			ConsumerGroup: "orchestrator-start",
			New:           func(d testDeps) (consumer.Controller, error) { return nil, nil },
			DLQ:           func(d testDeps) (consumer.Controller, error) { return nil, nil },
		},
		{
			Key:           "validate",
			Name:          "validate",
			ConsumerGroup: "orchestrator-validate",
			New:           func(d testDeps) (consumer.Controller, error) { return nil, nil },
			// No DLQ for this stage.
		},
	}

	o := &options{
		publishOnly: []PublishOnlyTopic{
			{Key: "log", Name: "log"},
		},
	}

	configs, err := buildTopicConfigs(q, "test-sub", stages, o)
	require.NoError(t, err)

	// Expected: start (primary + DLQ) + validate (primary only) + log (publish-only) = 4
	assert.Len(t, configs, 4)

	// Verify primary stage config.
	assert.Equal(t, consumer.TopicKey("start"), configs[0].Key)
	assert.Equal(t, "start", configs[0].Name)
	assert.Equal(t, "orchestrator-start", configs[0].Subscription.ConsumerGroup)

	// Verify DLQ config derived from primary.
	assert.Equal(t, consumer.TopicKey("start_dlq"), configs[1].Key)
	assert.Equal(t, "start_dlq", configs[1].Name)
	assert.Equal(t, "orchestrator-start-dlq", configs[1].Subscription.ConsumerGroup)

	// Verify DLQ subscription has DLQ disabled (no cascade).
	expected := extqueue.DLQSubscriptionConfig("test-sub", "orchestrator-start-dlq")
	assert.Equal(t, expected.DLQ.Enabled, configs[1].Subscription.DLQ.Enabled)
	assert.Equal(t, expected.Retry.MaxAttempts, configs[1].Subscription.Retry.MaxAttempts)

	// Verify validate stage (no DLQ).
	assert.Equal(t, consumer.TopicKey("validate"), configs[2].Key)

	// Verify publish-only topic.
	assert.Equal(t, consumer.TopicKey("log"), configs[3].Key)
	assert.Equal(t, "log", configs[3].Name)
	assert.Equal(t, "", configs[3].Subscription.ConsumerGroup)
}
