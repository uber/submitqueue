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

// Package yaml provides a YAML-file-backed queueconfig.Store.
package yaml

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueconfig"
	yamlv3 "gopkg.in/yaml.v3"
)

type fileContents struct {
	Queues []entity.QueueConfig `yaml:"queues"`
}

// Store is an immutable in-memory snapshot of a YAML queue configuration file.
type Store struct {
	byName map[string]entity.QueueConfig
	all    []entity.QueueConfig
}

// NewStore reads and validates queue configurations from path.
func NewStore(path string) (Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Store{}, fmt.Errorf("failed to read queue config file %q: %w", path, err)
	}

	var contents fileContents
	decoder := yamlv3.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&contents); err != nil {
		return Store{}, fmt.Errorf("failed to parse queue config file %q: %w", path, err)
	}

	byName := make(map[string]entity.QueueConfig, len(contents.Queues))
	for _, cfg := range contents.Queues {
		if err := validate(cfg); err != nil {
			return Store{}, fmt.Errorf("invalid queue config in %q: %w", path, err)
		}
		if _, exists := byName[cfg.Name]; exists {
			return Store{}, fmt.Errorf("queue config in %q has duplicate name %q", path, cfg.Name)
		}
		byName[cfg.Name] = cfg
	}

	all := make([]entity.QueueConfig, len(contents.Queues))
	copy(all, contents.Queues)
	return Store{byName: byName, all: all}, nil
}

func validate(cfg entity.QueueConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if cfg.MaxConcurrent <= 0 {
		return fmt.Errorf("max_concurrent for queue %q must be positive", cfg.Name)
	}
	if cfg.GateWaitDelayMs <= 0 {
		return fmt.Errorf("gate_wait_delay_ms for queue %q must be positive", cfg.Name)
	}
	return nil
}

// Get returns the configuration for name.
func (s Store) Get(_ context.Context, name string) (entity.QueueConfig, error) {
	cfg, ok := s.byName[name]
	if !ok {
		return entity.QueueConfig{}, queueconfig.ErrNotFound
	}
	return cfg, nil
}

// List returns a copy of all configurations in file order.
func (s Store) List(context.Context) ([]entity.QueueConfig, error) {
	configs := make([]entity.QueueConfig, len(s.all))
	copy(configs, s.all)
	return configs, nil
}
