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
	"fmt"
	"math"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type projectFactByDegreeStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewProjectFactByDegreeStore creates a queue-scoped, primary-key-ordered mapping store.
func NewProjectFactByDegreeStore(db *sql.DB, scope tally.Scope, queue string) storage.ProjectFactByDegreeStore {
	return &projectFactByDegreeStore{db: db, scope: scope, queue: queue}
}

func (s *projectFactByDegreeStore) Create(ctx context.Context, entry entity.ProjectFactByDegree) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if entry.URI == "" || entry.Project == "" || math.IsNaN(entry.Degree) || entry.Degree < entity.DegreeGreen || entry.Degree > entity.DegreeBroken {
		return fmt.Errorf("project fact degree lookup requires a URI, project and degree in [0, 1]")
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO project_fact_by_degree (queue, uri, degree, project)
		VALUES (?, ?, ?, ?)`,
		s.queue, entry.URI, entry.Degree, entry.Project,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("project fact degree lookup uri=%q project=%q: %w", entry.URI, entry.Project, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to create project fact degree lookup uri=%q project=%q: %w", entry.URI, entry.Project, err)
	}

	return nil
}

func (s *projectFactByDegreeStore) List(ctx context.Context, uri string, after entity.ProjectFactByDegree, limit int) (ret []entity.ProjectFactByDegree, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if uri == "" || limit <= 0 {
		return nil, fmt.Errorf("project fact degree lookup requires a URI and positive limit")
	}
	hasCursor := after != entity.ProjectFactByDegree{}
	if hasCursor {
		if after.URI != uri {
			return nil, fmt.Errorf("project fact degree lookup cursor must match the requested URI")
		}
		if after.Project == "" {
			return nil, fmt.Errorf("project fact degree lookup cursor requires a project")
		}
		if math.IsNaN(after.Degree) || after.Degree < entity.DegreeGreen || after.Degree > entity.DegreeBroken {
			return nil, fmt.Errorf("project fact degree lookup cursor requires a degree in [0, 1]")
		}
	}

	query := `SELECT uri, degree, project FROM project_fact_by_degree WHERE queue = ? AND uri = ?`
	args := []any{s.queue, uri}
	if hasCursor {
		query += ` AND (degree < ? OR (degree = ? AND project > ?))`
		args = append(args, after.Degree, after.Degree, after.Project)
	}
	query += ` ORDER BY degree DESC, project ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list project fact degree lookup queue=%q uri=%q: %w", s.queue, uri, err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry entity.ProjectFactByDegree
		if err := rows.Scan(&entry.URI, &entry.Degree, &entry.Project); err != nil {
			return nil, fmt.Errorf("failed to scan project fact degree lookup: %w", err)
		}
		ret = append(ret, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate project fact degree lookup: %w", err)
	}

	return ret, nil
}
