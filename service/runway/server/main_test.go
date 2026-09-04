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
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumergatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	gitexec "github.com/uber/submitqueue/platform/git/exec"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testGitTopicKey consumer.TopicKey = "git-test"
	testGitGroup                      = "git-test-group"
)

type errorController struct {
	err error
}

func (c errorController) Process(context.Context, consumer.Delivery) error {
	return c.err
}

func (errorController) Name() string {
	return "git-test"
}

func (errorController) TopicKey() consumer.TopicKey {
	return testGitTopicKey
}

func (errorController) ConsumerGroup() string {
	return testGitGroup
}

func gitExitError(t *testing.T) error {
	t.Helper()
	err := exec.Command(os.Args[0], "-test.run=[").Run()
	require.Error(t, err)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	return exitErr
}

func TestPrimaryConsumer_GitFailureDisposition(t *testing.T) {
	exitErr := gitExitError(t)
	tests := []struct {
		name        string
		controller  error
		wantOutcome string
	}{
		{
			name:        "temporary remote fetch failure is nacked for retry",
			controller:  gitexec.NewCommandError("fetch", "remote temporarily unavailable", exitErr),
			wantOutcome: "nack",
		},
		{
			name:        "unknown error is rejected to dead letter",
			controller:  errors.New("unknown failure"),
			wantOutcome: "reject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			deliveryChannel := make(chan extqueue.Delivery, 1)
			subscriber := queuemock.NewMockSubscriber(ctrl)
			subscriber.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(deliveryChannel, nil)
			queue := queuemock.NewMockQueue(ctrl)
			queue.EXPECT().Subscriber().Return(subscriber)

			registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{
				Key:   testGitTopicKey,
				Name:  "git-test",
				Queue: queue,
				Subscription: extqueue.DefaultSubscriptionConfig(
					"git-test-worker",
					testGitGroup,
				),
			}})
			require.NoError(t, err)

			serviceConsumer := consumer.New(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				registry,
				newPrimaryErrorProcessor(),
				consumergatenoop.New(),
			)
			require.NoError(t, serviceConsumer.Register(errorController{err: tt.controller}))
			require.NoError(t, serviceConsumer.Start(context.Background()))

			message := entityqueue.NewMessage("git-test-message", []byte("payload"), "partition", nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(message).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()
			done := make(chan struct{})
			if tt.wantOutcome == "nack" {
				delivery.EXPECT().Nack(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, failure.Failure) error {
					close(done)
					return nil
				})
			} else {
				delivery.EXPECT().Reject(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, failure.Failure) error {
					close(done)
					return nil
				})
			}

			deliveryChannel <- delivery
			<-done
			require.NoError(t, serviceConsumer.Stop(30000))
		})
	}
}
