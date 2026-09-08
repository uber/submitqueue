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

package mysql

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxIdentifierLength = 255

func validateASCIIIdentifier(name, value string) error {
	if len(value) > maxIdentifierLength {
		return fmt.Errorf("%s exceeds %d bytes", name, maxIdentifierLength)
	}
	for i := range len(value) {
		if value[i] > 0x7f {
			return fmt.Errorf("%s must contain only ASCII characters", name)
		}
		if value[i] == 0 {
			return fmt.Errorf("%s must not contain NUL", name)
		}
	}
	return nil
}

func validateTextIdentifier(name, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	if utf8.RuneCountInString(value) > maxIdentifierLength {
		return fmt.Errorf("%s exceeds %d characters", name, maxIdentifierLength)
	}
	return nil
}

func normalizeTenants(tenants []string) ([]string, error) {
	normalized := make([]string, 0, len(tenants))
	seen := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		tenant = strings.TrimSpace(tenant)
		if tenant == "" {
			continue
		}
		if err := validateASCIIIdentifier("tenant", tenant); err != nil {
			return nil, err
		}
		if _, exists := seen[tenant]; exists {
			continue
		}
		seen[tenant] = struct{}{}
		normalized = append(normalized, tenant)
	}
	return normalized, nil
}
