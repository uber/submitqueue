// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"fmt"
	"io"
	"sync"
)

// TextObserver prints timestamped request transitions.
type TextObserver struct {
	mu       sync.Mutex
	writer   io.Writer
	statuses map[string]string
}

// NewTextObserver returns a headless transition observer.
func NewTextObserver(writer io.Writer) *TextObserver {
	return &TextObserver{writer: writer, statuses: make(map[string]string)}
}

// Observe prints request statuses that changed since the previous snapshot.
func (o *TextObserver) Observe(snapshot Snapshot) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, request := range snapshot.Requests {
		key := request.Name
		status := request.Status
		if status == "" || o.statuses[key] == status {
			continue
		}
		o.statuses[key] = status
		fmt.Fprintf(o.writer, "[%s] %-16s %-18s %s\n",
			snapshot.Now.Sub(snapshot.StartedAt).Round(10_000_000),
			request.Name,
			status,
			request.SQID,
		)
	}
}
