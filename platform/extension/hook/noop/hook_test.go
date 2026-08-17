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

package noop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	basehook "github.com/uber/submitqueue/api/base/hook"
)

func TestHandleAcceptsEveryEvent(t *testing.T) {
	events := map[string]*basehook.HookEvent{
		"well-formed": {Id: "submitqueue/batch.failed/batch-778/4", Source: "submitqueue", Type: "batch.failed"},
		"empty":       {},
		"nil":         nil,
	}

	for name, event := range events {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, New().Handle(context.Background(), event))
		})
	}
}
