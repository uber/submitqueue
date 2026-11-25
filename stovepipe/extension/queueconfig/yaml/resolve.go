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

package yaml

import (
	"fmt"

	"github.com/uber/submitqueue/stovepipe/core/queueconfig"
)

func (cfg fileConfig) resolveQueues(queueNames []string) ([]queueconfig.QueueConfig, error) {
	overridesByQueue, err := cfg.indexQueueOverrides()
	if err != nil {
		return nil, err
	}

	sharedStages := cfg.SharedStages.Apply(queueconfig.DefaultStages())
	registeredQueues := make(map[string]struct{}, len(queueNames))
	queues := make([]queueconfig.QueueConfig, 0, len(queueNames))

	for _, name := range queueNames {
		if _, exists := registeredQueues[name]; exists {
			return nil, fmt.Errorf("duplicate registered queue %q", name)
		}

		queue := queueconfig.QueueConfig{
			Name:   name,
			Stages: overridesByQueue[name].Apply(sharedStages),
		}

		if err := queue.Validate(); err != nil {
			return nil, err
		}

		registeredQueues[name] = struct{}{}
		queues = append(queues, queue)
	}

	for _, override := range cfg.QueueOverrides {
		if _, exists := registeredQueues[override.QueueName]; !exists {
			return nil, fmt.Errorf("override refers to unregistered queue %q", override.QueueName)
		}
	}

	return queues, nil
}

func (cfg fileConfig) indexQueueOverrides() (map[string]queueconfig.StageOverrides, error) {
	overridesByQueue := make(map[string]queueconfig.StageOverrides, len(cfg.QueueOverrides))
	for _, override := range cfg.QueueOverrides {
		if _, exists := overridesByQueue[override.QueueName]; exists {
			return nil, fmt.Errorf("duplicate queue override %q", override.QueueName)
		}

		overridesByQueue[override.QueueName] = override.Stages
	}

	return overridesByQueue, nil
}
