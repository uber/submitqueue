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
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
)

func TestResolverFactory_ReturnsResolverWithoutProjectStatusResults(t *testing.T) {
	resolver, err := New().For(projectstatus.ResolverConfig{QueueName: "monorepo/main"})
	require.NoError(t, err)

	results, err := resolver.Resolve(context.Background(), entity.Request{}, "bk-1")
	require.NoError(t, err)
	require.Empty(t, results)
}
