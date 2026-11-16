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

// Package yaml provides immutable queue configuration resolved from built-in
// defaults and optional YAML overrides for host-registered queues.
package yaml

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/core/queueconfig"
	queueconfigprovider "github.com/uber/submitqueue/stovepipe/extension/queueconfig"
)

// store holds immutable, resolved settings for host-registered queues.
// Get and List return independent values and are safe for concurrent use.
type store struct {
	queues []queueconfig.QueueConfig
}

// NewStore resolves settings for queueNames in the supplied order.
// An empty path uses built-in defaults; otherwise one file supplies optional
// shared settings and per-queue overrides. Files are read once and are not watched.
func NewStore(path string, queueNames []string) (queueconfigprovider.Store, error) {
	var cfg fileConfig
	if path != "" {
		var err error
		cfg, err = readConfigFile(path)
		if err != nil {
			return nil, err
		}
	}

	queues, err := cfg.resolveQueues(queueNames)
	if err != nil {
		if path != "" {
			return nil, fmt.Errorf("resolve queue configuration %q: %w", path, err)
		}

		return nil, err
	}

	return store{queues: queues}, nil
}

// Get returns ErrNotFound for queues not registered by the host.
func (s store) Get(_ context.Context, name string) (queueconfig.QueueConfig, error) {
	for _, cfg := range s.queues {
		if cfg.Name == name {
			return cfg, nil
		}
	}

	return queueconfig.QueueConfig{}, queueconfigprovider.ErrNotFound
}

// List returns independent values in host registration order.
func (s store) List(context.Context) ([]queueconfig.QueueConfig, error) {
	configs := make([]queueconfig.QueueConfig, len(s.queues))
	copy(configs, s.queues)

	return configs, nil
}

var _ queueconfigprovider.Store = store{}
