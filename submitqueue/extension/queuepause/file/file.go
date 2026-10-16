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

// Package file provides a YAML-file-backed implementation of queuepause.Store.
// The file is re-read on every Get, so editing it pauses or unpauses a queue
// without a restart. The file lists the queues that have pause switches set:
//
//	queues:
//	  - name: my-queue
//	    reject_lands: true
//	    freeze_processing: false
//	    reason: repo freeze until 17:00
//
// A queue with no entry is not paused. The file must exist and parse when the
// Store is constructed, so a mistyped path fails at startup. A later read or
// parse failure is returned from Get as an error, never treated as unpaused.
package file

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queuepause"
	yamlv3 "gopkg.in/yaml.v3"
)

var _ queuepause.Store = Store{}

type fileContents struct {
	Queues []queueEntry `yaml:"queues"`
}

type queueEntry struct {
	Name             string `yaml:"name"`
	RejectLands      bool   `yaml:"reject_lands"`
	FreezeProcessing bool   `yaml:"freeze_processing"`
	Reason           string `yaml:"reason"`
}

// Store is a queuepause.Store backed by a YAML file that is re-read on every
// Get. Safe for concurrent use.
type Store struct {
	path string
}

// NewStore returns a Store reading the YAML file at path.
// Returns an error if the file is unreadable or invalid, so that a
// misconfigured path is caught at startup.
func NewStore(path string) (Store, error) {
	if _, err := load(path); err != nil {
		return Store{}, err
	}
	return Store{path: path}, nil
}

// Get re-reads the file and returns the pause state of the named queue, or the
// zero state if the queue has no entry.
func (s Store) Get(_ context.Context, queue string) (entity.QueuePause, error) {
	states, err := load(s.path)
	if err != nil {
		return entity.QueuePause{}, err
	}
	return states[queue], nil
}

func load(path string) (map[string]entity.QueuePause, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read queue pause file %q: %w", path, err)
	}

	// Unknown fields are rejected: a misspelled switch would otherwise be
	// ignored and leave the queue unpaused.
	decoder := yamlv3.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var contents fileContents
	if err := decoder.Decode(&contents); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to parse queue pause file %q: %w", path, err)
	}

	states := make(map[string]entity.QueuePause, len(contents.Queues))
	for _, q := range contents.Queues {
		if q.Name == "" {
			return nil, fmt.Errorf("queue pause entry in %q has empty name", path)
		}
		if _, dup := states[q.Name]; dup {
			return nil, fmt.Errorf("queue pause file %q has duplicate name %q", path, q.Name)
		}
		states[q.Name] = entity.QueuePause{
			RejectLands:      q.RejectLands,
			FreezeProcessing: q.FreezeProcessing,
			Reason:           q.Reason,
		}
	}
	return states, nil
}
