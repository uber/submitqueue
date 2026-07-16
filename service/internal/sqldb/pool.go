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

// Package sqldb contains SQL database wiring helpers shared by services.
package sqldb

import (
	"database/sql"
	"fmt"
	"strconv"
)

// ConfigureMaxOpenConnections applies a positive connection limit encoded as a
// decimal string. It retains up to the same number of idle connections so a
// busy bounded pool reuses sockets instead of repeatedly opening new ones. An
// empty string or zero preserves database/sql defaults.
func ConfigureMaxOpenConnections(db *sql.DB, value string) error {
	if db == nil {
		return fmt.Errorf("database is required")
	}
	if value == "" {
		return nil
	}
	maxOpen, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("parse maximum open connections %q: %w", value, err)
	}
	if maxOpen < 0 {
		return fmt.Errorf("maximum open connections must be non-negative")
	}
	if maxOpen > 0 {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxOpen)
	}
	return nil
}
