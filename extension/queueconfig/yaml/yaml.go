// Copyright (c) 2025 Uber Technologies, Inc.
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

// Package yaml provides a YAML-file-backed implementation of
// queueconfig.Store. The file is read once at construction time and held
// in memory; the file is not watched for changes.
package yaml

import (
	"context"
	"fmt"
	"os"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/queueconfig"
	yamlv3 "gopkg.in/yaml.v3"
)

// fileContents is the top-level YAML schema. A configuration file is a
// single document with a "queues" key holding a list of QueueConfig.
type fileContents struct {
	Queues []entity.QueueConfig `yaml:"queues"`
}

// Store is a queueconfig.Store backed by an in-memory snapshot of a YAML
// file. Construct via NewStore. Safe for concurrent reads.
type Store struct {
	byName map[string]entity.QueueConfig
	all    []entity.QueueConfig
}

// NewStore reads queue configurations from the YAML file at path and
// returns a Store. If the file omits the top-level "queues" key, it is
// treated as an empty queue list.
// Returns an error if the file is unreadable, malformed, contains a queue
// with an empty name, or contains duplicate queue names.
func NewStore(path string) (Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Store{}, fmt.Errorf("failed to read queue config file %q: %w", path, err)
	}

	var contents fileContents
	if err := yamlv3.Unmarshal(data, &contents); err != nil {
		return Store{}, fmt.Errorf("failed to parse queue config file %q: %w", path, err)
	}

	byName := make(map[string]entity.QueueConfig, len(contents.Queues))
	for _, q := range contents.Queues {
		if q.Name == "" {
			return Store{}, fmt.Errorf("queue config in %q has empty name", path)
		}
		if _, dup := byName[q.Name]; dup {
			return Store{}, fmt.Errorf("queue config in %q has duplicate name %q", path, q.Name)
		}
		byName[q.Name] = q
	}

	all := make([]entity.QueueConfig, len(contents.Queues))
	copy(all, contents.Queues)

	return Store{byName: byName, all: all}, nil
}

// Get returns the configuration for the named queue, or queueconfig.ErrNotFound.
func (s Store) Get(_ context.Context, name string) (entity.QueueConfig, error) {
	cfg, ok := s.byName[name]
	if !ok {
		return entity.QueueConfig{}, queueconfig.ErrNotFound
	}
	return cfg, nil
}

// List returns a copy of all configured queues in file order.
func (s Store) List(_ context.Context) ([]entity.QueueConfig, error) {
	out := make([]entity.QueueConfig, len(s.all))
	copy(out, s.all)
	return out, nil
}
