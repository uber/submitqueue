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

package messagequeue

import "fmt"

// ValidateTenantMetadata verifies that the persisted shard identity agrees
// with queue metadata when metadata is present.
func ValidateTenantMetadata(message Message) error {
	if message.Tenant == "" {
		return fmt.Errorf("message tenant is required")
	}
	queueName := message.Metadata[MetadataKeyQueueName]
	if queueName != "" && queueName != message.Tenant {
		return fmt.Errorf("message tenant %q does not match queue metadata %q", message.Tenant, queueName)
	}
	return nil
}

// ValidatePayloadQueue verifies that a queue-bearing payload belongs to the
// persisted message tenant.
func ValidatePayloadQueue(message Message, payloadQueue string) error {
	if err := ValidateTenantMetadata(message); err != nil {
		return err
	}
	if payloadQueue == "" {
		return fmt.Errorf("payload queue is required")
	}
	if payloadQueue != message.Tenant {
		return fmt.Errorf("message tenant %q does not match payload queue %q", message.Tenant, payloadQueue)
	}
	return nil
}
