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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const greenProjectWriteSize = 200

type greenProjectStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewGreenProjectStore creates a green project projection bound to queue.
func NewGreenProjectStore(db *sql.DB, scope tally.Scope, queue string) storage.GreenProjectStore {
	return &greenProjectStore{db: db, scope: scope, queue: queue}
}

func (s *greenProjectStore) CreateForCommit(ctx context.Context, lineage string, position int64, uri string, projects []string) (retErr error) {
	op := metrics.Begin(s.scope, "create_for_commit", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	for start := 0; start < len(projects); start += greenProjectWriteSize {
		batch := projects[start:min(start+greenProjectWriteSize, len(projects))]
		values := make([]string, 0, len(batch))
		args := make([]any, 0, len(batch)*5)
		for _, project := range batch {
			values = append(values, "(?, ?, ?, ?, ?)")
			args = append(args, s.queue, lineage, project, position, uri)
		}
		_, err := s.db.ExecContext(ctx, `INSERT INTO green_project_by_position (queue, lineage, project, position, uri) VALUES `+
			strings.Join(values, ", ")+` ON DUPLICATE KEY UPDATE uri = green_project_by_position.uri`, args...)
		if err != nil {
			return fmt.Errorf("create green project entries for %q: %w", uri, err)
		}
	}
	return nil
}

func (s *greenProjectStore) FindAtOrAfter(ctx context.Context, lineage, project string, position int64) (uri string, retErr error) {
	op := metrics.Begin(s.scope, "find_at_or_after", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	err := s.db.QueryRowContext(ctx, `SELECT uri FROM green_project_by_position
		WHERE queue = ? AND lineage = ? AND project = ? AND position >= ? ORDER BY position LIMIT 1`,
		s.queue, lineage, project, position).Scan(&uri)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("find green project %q: %w", project, err)
	}
	return uri, nil
}
