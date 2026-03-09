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

package mysql

import (
	"errors"
	"fmt"
)

// ErrPublisherClosed is returned when attempting to publish after the publisher has been closed.
var ErrPublisherClosed = errors.New("publisher is closed")

// ErrSubscriberClosed is returned when attempting to subscribe after the subscriber has been closed.
var ErrSubscriberClosed = errors.New("subscriber is closed")

// ErrAlreadyAcknowledged is returned when attempting to ack/nack a delivery that was already processed
type ErrAlreadyAcknowledged struct {
	DeliveryID string
}

func (e *ErrAlreadyAcknowledged) Error() string {
	return fmt.Sprintf("delivery %s already acknowledged or nacked", e.DeliveryID)
}

// ErrLeaseExpired is returned when a lease renewal fails because the lease
// is no longer owned by this worker (rows affected == 0).
type ErrLeaseExpired struct {
	// Topic is the topic the lease was for.
	Topic string
	// PartitionKey is the partition the lease was for.
	PartitionKey string
}

func (e *ErrLeaseExpired) Error() string {
	return fmt.Sprintf("lease expired for topic=%s partition=%s", e.Topic, e.PartitionKey)
}
