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

package git

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// parseNumstat reads the output of `git diff --numstat -z`.
//
// The NUL-delimited form is used rather than the line-based one because it is
// the only one that survives a path containing a newline, and because it is how
// a rename becomes unambiguous: a normal record is one NUL-terminated field
// holding "added\tdeleted\tpath", while a rename leaves the path empty and
// follows the record with the old and new paths as two further fields.
//
// A binary file reports "-" for both counts. Those are recorded as a changed
// path with no line counts, which is true — a binary has no lines — rather than
// dropped, since a path-keyed conflict analyzer still needs to see it.
func parseNumstat(out string) ([]entity.ChangedFile, error) {
	fields := strings.Split(out, "\x00")
	var files []entity.ChangedFile

	for i := 0; i < len(fields); i++ {
		record := fields[i]
		if record == "" {
			continue
		}

		parts := strings.SplitN(record, "\t", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("unparseable numstat record %q", record)
		}

		added, err := parseCount(parts[0])
		if err != nil {
			return nil, fmt.Errorf("numstat record %q: %w", record, err)
		}
		deleted, err := parseCount(parts[1])
		if err != nil {
			return nil, fmt.Errorf("numstat record %q: %w", record, err)
		}

		path := parts[2]
		if path == "" {
			// A rename or copy. The two fields that follow are the old path and
			// the new one; only the new one is kept, because a ChangedFile names
			// one path and the new one is where the content now lives.
			if i+2 >= len(fields) {
				return nil, fmt.Errorf("numstat rename record %q is missing its paths", record)
			}
			path = fields[i+2]
			i += 2
		}

		files = append(files, entity.ChangedFile{
			Path:         path,
			LinesAdded:   added,
			LinesDeleted: deleted,
		})
	}
	return files, nil
}

// parseCount reads one side of a numstat record. "-" means the file is binary,
// which is reported as zero rather than refused.
func parseCount(field string) (int, error) {
	if field == "-" {
		return 0, nil
	}
	n, err := strconv.Atoi(field)
	if err != nil {
		return 0, fmt.Errorf("%q is not a line count: %w", field, err)
	}
	return n, nil
}
