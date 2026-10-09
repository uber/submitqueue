-- Copyright (c) 2026 Uber Technologies, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE IF NOT EXISTS queue_policy_transition (
    queue VARBINARY(255) NOT NULL,
    id VARBINARY(255) NOT NULL,
    previous_id VARBINARY(255) NOT NULL,
    revision BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    effective_from_commit_uri VARBINARY(255) NOT NULL,
    changed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (queue, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
