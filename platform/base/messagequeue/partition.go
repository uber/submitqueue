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

// PartitionIdentity uniquely identifies a partition within a tenant.
// It is comparable and may be used as a map key.
type PartitionIdentity struct {
	// Tenant is the shard isolation identity.
	Tenant string
	// PartitionKey is the partition key within the tenant.
	PartitionKey string
}

// PartitionIdentity returns the message's tenant-scoped partition identity.
func (m Message) PartitionIdentity() PartitionIdentity {
	return PartitionIdentity{
		Tenant:       m.Tenant,
		PartitionKey: m.PartitionKey,
	}
}
