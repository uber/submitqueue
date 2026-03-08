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

// Sentinel errors for closed resources. Callers can check these with errors.Is.
var (
	// ErrPublisherClosed is returned when publishing to a closed publisher.
	ErrPublisherClosed = errors.New("publisher is closed")

	// ErrSubscriberClosed is returned when subscribing on a closed subscriber.
	ErrSubscriberClosed = errors.New("subscriber is closed")
)

// ErrAlreadyAcknowledged is returned when attempting to ack/nack a delivery that was already processed.
type ErrAlreadyAcknowledged struct {
	DeliveryID string
}

func (e *ErrAlreadyAcknowledged) Error() string {
	return fmt.Sprintf("delivery %s already acknowledged or nacked", e.DeliveryID)
}

// ErrLeaseExpired is returned when a lease operation fails because the lease
// is not owned by this worker or has already expired.
type ErrLeaseExpired struct {
	Topic        string
	PartitionKey string
}

func (e *ErrLeaseExpired) Error() string {
	return fmt.Sprintf("lease expired or not owned for topic %s partition %s", e.Topic, e.PartitionKey)
}
