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

package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDescribeDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{name: "credentials and parameters omitted", dsn: "root:secret@tcp(h:3306)/db?parseTime=true", want: "h:3306/db"},
		{name: "no password", dsn: "root@tcp(h:3306)/db", want: "h:3306/db"},
		{name: "unparsable", dsn: "not a dsn", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DescribeDSN(tt.dsn)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "root")
			assert.NotContains(t, got, "secret")
		})
	}
}
