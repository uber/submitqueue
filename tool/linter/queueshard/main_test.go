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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	tests := []struct {
		name           string
		schema         string
		wantTables     int
		wantViolations int
		wantProblem    string
	}{
		{
			name: "queue-leading composite key passes",
			schema: "CREATE TABLE IF NOT EXISTS request (\n" +
				"    queue VARCHAR(255) NOT NULL,\n" +
				"    id VARCHAR(255) NOT NULL,\n" +
				"    PRIMARY KEY (queue, id)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables: 1,
		},
		{
			name: "queue-free key is rejected",
			schema: "CREATE TABLE IF NOT EXISTS request_summary (\n" +
				"    request_id VARCHAR(255) NOT NULL,\n" +
				"    queue VARCHAR(255) NOT NULL,\n" +
				"    PRIMARY KEY (request_id)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables:     1,
			wantViolations: 1,
			wantProblem:    `leads its PRIMARY KEY with "request_id", not the queue column`,
		},
		{
			name: "queue present but not leading is rejected",
			schema: "CREATE TABLE IF NOT EXISTS batch (\n" +
				"    id VARCHAR(255) NOT NULL,\n" +
				"    queue VARCHAR(255) NOT NULL,\n" +
				"    PRIMARY KEY (id, queue)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables:     1,
			wantViolations: 1,
		},
		{
			name: "a table of queues may key on name",
			schema: "CREATE TABLE IF NOT EXISTS queue (\n" +
				"    name VARCHAR(255) NOT NULL,\n" +
				"    PRIMARY KEY (name)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables: 1,
		},
		{
			name: "backticked table and columns are parsed",
			schema: "CREATE TABLE IF NOT EXISTS `change` (\n" +
				"    `queue` VARCHAR(255) NOT NULL,\n" +
				"    `uri` VARCHAR(255) NOT NULL,\n" +
				"    PRIMARY KEY (`queue`, `uri`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables: 1,
		},
		{
			name: "a secondary index spanning queues is rejected",
			schema: "CREATE TABLE IF NOT EXISTS build (\n" +
				"    queue VARCHAR(255) NOT NULL,\n" +
				"    id VARCHAR(255) NOT NULL,\n" +
				"    status VARCHAR(64) NOT NULL,\n" +
				"    PRIMARY KEY (queue, id),\n" +
				"    INDEX idx_status (status)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables:     1,
			wantViolations: 1,
			wantProblem:    `has index "idx_status" leading with "status", which spans queues`,
		},
		{
			name: "a queue-leading secondary index passes",
			schema: "CREATE TABLE IF NOT EXISTS build (\n" +
				"    queue VARCHAR(255) NOT NULL,\n" +
				"    id VARCHAR(255) NOT NULL,\n" +
				"    status VARCHAR(64) NOT NULL,\n" +
				"    PRIMARY KEY (queue, id),\n" +
				"    INDEX idx_queue_status (queue, status)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables: 1,
		},
		{
			name: "a table with no primary key is rejected",
			schema: "CREATE TABLE IF NOT EXISTS loose (\n" +
				"    queue VARCHAR(255) NOT NULL\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables:     1,
			wantViolations: 1,
			wantProblem:    "has no PRIMARY KEY",
		},
		{
			name:       "leading comments are ignored",
			schema:     "-- a comment about the table\nCREATE TABLE IF NOT EXISTS t (\n    queue VARCHAR(255) NOT NULL,\n    PRIMARY KEY (queue)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			wantTables: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables, violations := check("test.sql", tt.schema)
			assert.Equal(t, tt.wantTables, tables)
			require.Len(t, violations, tt.wantViolations)
			if tt.wantProblem != "" {
				assert.Equal(t, tt.wantProblem, violations[0].problem)
			}
		})
	}
}

func TestSplitColumns(t *testing.T) {
	tests := []struct {
		name string
		list string
		want []string
	}{
		{name: "plain", list: "queue, id", want: []string{"queue", "id"}},
		{name: "backticked", list: "`queue`, `id`", want: []string{"queue", "id"}},
		{name: "length prefix", list: "queue(20), id", want: []string{"queue", "id"}},
		{name: "empty", list: "", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, splitColumns(tt.list))
		})
	}
}
