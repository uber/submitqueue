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

// Package messagequeue holds message-queue configuration shared by service wiring.
package messagequeue

import (
	"fmt"
	"slices"
	"strings"
)

const maxTenantLength = 255

// ParseRequiredTenants parses a comma-separated tenant list.
// Tenants are unique, non-empty ASCII identifiers of at most 255 bytes.
func ParseRequiredTenants(value string) ([]string, error) {
	seen := make(map[string]struct{})
	var tenants []string
	for _, part := range strings.Split(value, ",") {
		tenant := strings.TrimSpace(part)
		if tenant == "" {
			continue
		}
		if len(tenant) > maxTenantLength {
			return nil, fmt.Errorf("tenant %q exceeds %d bytes", tenant, maxTenantLength)
		}
		for i := range len(tenant) {
			if tenant[i] > 0x7f {
				return nil, fmt.Errorf("tenant %q must contain only ASCII characters", tenant)
			}
			if tenant[i] == 0 {
				return nil, fmt.Errorf("tenant %q must not contain NUL", tenant)
			}
		}
		if _, exists := seen[tenant]; exists {
			continue
		}
		seen[tenant] = struct{}{}
		tenants = append(tenants, tenant)
	}
	if len(tenants) == 0 {
		return nil, fmt.Errorf("tenant list must contain at least one tenant")
	}
	return tenants, nil
}

// ValidateTenantSetsEqual requires both named configurations to contain the
// same tenants. Input order and duplicate entries do not affect equality.
func ValidateTenantSetsEqual(firstName string, first []string, secondName string, second []string) error {
	onlyFirst, onlySecond := tenantSetDifferences(first, second)
	if len(onlyFirst) == 0 && len(onlySecond) == 0 {
		return nil
	}
	return fmt.Errorf("%s and %s tenant sets differ: only in %s=%v, only in %s=%v",
		firstName, secondName, firstName, onlyFirst, secondName, onlySecond)
}

// ValidateTenantSubset requires every tenant in subset to be present in
// superset. Input order and duplicate entries do not affect membership.
func ValidateTenantSubset(supersetName string, superset []string, subsetName string, subset []string) error {
	_, onlySubset := tenantSetDifferences(superset, subset)
	if len(onlySubset) == 0 {
		return nil
	}
	return fmt.Errorf("%s contains tenants not present in %s: %v", subsetName, supersetName, onlySubset)
}

func tenantSetDifferences(first, second []string) (onlyFirst, onlySecond []string) {
	firstSet := tenantSet(first)
	secondSet := tenantSet(second)
	for tenant := range firstSet {
		if _, exists := secondSet[tenant]; !exists {
			onlyFirst = append(onlyFirst, tenant)
		}
	}
	for tenant := range secondSet {
		if _, exists := firstSet[tenant]; !exists {
			onlySecond = append(onlySecond, tenant)
		}
	}
	slices.Sort(onlyFirst)
	slices.Sort(onlySecond)
	return onlyFirst, onlySecond
}

func tenantSet(tenants []string) map[string]struct{} {
	set := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		set[tenant] = struct{}{}
	}
	return set
}
