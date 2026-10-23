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

package fix

import (
	"context"
	"fmt"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
)

var _ consumer.Controller = (*Controller)(nil)

func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	message := delivery.Message()
	request := &mq.FixRequest{}
	if err := mq.Unmarshal(message.Payload, request); err != nil {
		return fmt.Errorf("failed to deserialize repair request: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(message, request.GetQueueName()); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}
	if request.GetId() == "" {
		return fmt.Errorf("repair request requires a build ID")
	}
	return c.processBuild(ctx, request.GetQueueName(), request.GetId())
}
