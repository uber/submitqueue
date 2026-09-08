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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateTenantMetadata(t *testing.T) {
	tests := []struct {
		name    string
		message Message
		wantErr bool
	}{
		{name: "tenant without metadata", message: Message{Tenant: "queue-a"}},
		{name: "matching metadata", message: Message{Tenant: "queue-a", Metadata: map[string]string{MetadataKeyQueueName: "queue-a"}}},
		{name: "empty tenant", message: Message{}, wantErr: true},
		{name: "conflicting metadata", message: Message{Tenant: "queue-a", Metadata: map[string]string{MetadataKeyQueueName: "queue-b"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantMetadata(tt.message)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidatePayloadQueue(t *testing.T) {
	tests := []struct {
		name         string
		message      Message
		payloadQueue string
		wantErr      bool
	}{
		{name: "matching queue", message: Message{Tenant: "queue-a"}, payloadQueue: "queue-a"},
		{name: "empty payload queue", message: Message{Tenant: "queue-a"}, wantErr: true},
		{name: "conflicting payload queue", message: Message{Tenant: "queue-a"}, payloadQueue: "queue-b", wantErr: true},
		{name: "conflicting metadata", message: Message{Tenant: "queue-a", Metadata: map[string]string{MetadataKeyQueueName: "queue-b"}}, payloadQueue: "queue-a", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePayloadQueue(tt.message, tt.payloadQueue)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
