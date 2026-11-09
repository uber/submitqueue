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
	"github.com/uber/submitqueue/stovepipe/extension/projectissues"
)

func TestAttributerSkipsAttribution(t *testing.T) {
	attributer, err := New().For(projectissues.AttributerConfig{QueueName: "example"})
	require.NoError(t, err)
	request := entity.Request{
		ID: "1", Queue: "example", URI: "git://example/head",
		BaseURI: "git://example/base", BuildStrategy: entity.BuildStrategyIncrementalSinceGreen,
		State: entity.RequestStateFailed, BuildID: "build-1", TerminalBuildID: "build-1",
	}

	attributions, err := attributer.Attribute(context.Background(), request)

	require.NoError(t, err)
	require.Empty(t, attributions)
}
