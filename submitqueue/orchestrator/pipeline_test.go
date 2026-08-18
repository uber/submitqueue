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

package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	hooknoop "github.com/uber/submitqueue/platform/extension/hook/noop"
	"github.com/uber/submitqueue/platform/pipeline"
	"go.uber.org/zap/zaptest"
)

func hookStage(t *testing.T) pipeline.Stage[Deps] {
	t.Helper()
	for _, s := range Stages {
		if s.Key == basehook.TopicKeyHook {
			return s
		}
	}
	require.FailNow(t, "no stage is registered for the hook topic key")
	return pipeline.Stage[Deps]{}
}

func TestHookStage(t *testing.T) {
	deps := func(t *testing.T) Deps {
		return Deps{
			Logger: zaptest.NewLogger(t).Sugar(),
			Scope:  tally.NoopScope,
			Hook:   hooknoop.New(),
		}
	}
	stageContext := func(key string) pipeline.StageContext {
		return pipeline.StageContext{
			TopicKey:      basehook.TopicKey(key),
			ConsumerGroup: "orchestrator",
		}
	}

	t.Run("dispatcher subscribes to the key the engine assigns", func(t *testing.T) {
		controller, err := hookStage(t).New(deps(t), stageContext("hook"))
		require.NoError(t, err)
		assert.Equal(t, basehook.TopicKeyHook, controller.TopicKey())
	})

	t.Run("reconciler subscribes to the dead-letter key the engine derives", func(t *testing.T) {
		controller, err := hookStage(t).DLQ(deps(t), stageContext("hook_dlq"))
		require.NoError(t, err)
		assert.Equal(t, basehook.TopicKey("hook_dlq"), controller.TopicKey())
	})

	t.Run("a host that leaves the hook unwired fails to construct", func(t *testing.T) {
		d := deps(t)
		d.Hook = nil
		_, err := hookStage(t).New(d, stageContext("hook"))
		require.Error(t, err)
	})
}
