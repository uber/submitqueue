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

// Store holds immutable, resolved settings for host-registered queues.
// Get and List return independent values and are safe for concurrent use.
type Store struct {
	byName map[string]queueconfig.QueueConfig
	all    []queueconfig.QueueConfig
}

var _ queueconfigprovider.Store = Store{}

// NewStore resolves settings for queueNames in the supplied order.
// An empty path uses built-in defaults; otherwise one file supplies optional
// shared settings and per-queue overrides. Files are read once and are not watched.
func NewStore(path string, queueNames []string) (Store, error) {
	if path == "" {
		return resolveQueueConfigs(document{}, queueNames)
	}

	cfg, err := readDocument(path)
	if err != nil {
		return Store{}, err
	}

	store, err := resolveQueueConfigs(cfg, queueNames)
	if err != nil {
		return Store{}, fmt.Errorf("resolve queue configuration %q: %w", path, err)
	}

	return store, nil
}

// Get returns ErrNotFound for queues not registered by the host.
func (s Store) Get(_ context.Context, name string) (queueconfig.QueueConfig, error) {
	cfg, ok := s.byName[name]
	if !ok {
		return queueconfig.QueueConfig{}, queueconfigprovider.ErrNotFound
	}

	return cfg, nil
}

// List returns independent values in host registration order.
func (s Store) List(context.Context) ([]queueconfig.QueueConfig, error) {
	configs := make([]queueconfig.QueueConfig, len(s.all))
	copy(configs, s.all)

	return configs, nil
}

func resolveQueueConfigs(cfg document, queueNames []string) (Store, error) {
	sharedStages := cfg.Stages.Apply(queueconfig.DefaultStages())

	overrides := make(map[string]queueconfig.StageOverrides, len(cfg.Queues))
	for _, settings := range cfg.Queues {
		if _, exists := overrides[settings.Name]; exists {
			return Store{}, fmt.Errorf("duplicate queue override %q", settings.Name)
		}

		overrides[settings.Name] = settings.Stages
	}

	store := Store{
		byName: make(map[string]queueconfig.QueueConfig, len(queueNames)),
		all:    make([]queueconfig.QueueConfig, 0, len(queueNames)),
	}

	for _, name := range queueNames {
		if _, exists := store.byName[name]; exists {
			return Store{}, fmt.Errorf("duplicate registered queue %q", name)
		}

		resolved := queueconfig.QueueConfig{
			Name:   name,
			Stages: overrides[name].Apply(sharedStages),
		}

		if err := resolved.Validate(); err != nil {
			return Store{}, err
		}

		store.byName[name] = resolved
		store.all = append(store.all, resolved)
	}

	for _, settings := range cfg.Queues {
		if _, exists := store.byName[settings.Name]; !exists {
			return Store{}, fmt.Errorf("override refers to unregistered queue %q", settings.Name)
		}
	}

	return store, nil
}
