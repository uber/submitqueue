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

import "strings"

// inListPlaceholders returns "?,?,…" of length n. n < 1 is a no-op query.
func inListPlaceholders(n int) (string, bool) {
	if n < 1 {
		return "", false
	}
	return strings.Repeat(",?", n)[1:], true
}

func appendStrings(args []any, values []string) []any {
	for _, value := range values {
		args = append(args, value)
	}
	return args
}
