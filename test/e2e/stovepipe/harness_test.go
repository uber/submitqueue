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

package e2e_test

// Reusable e2e helpers drive the stack through the real Stovepipe gRPC surface
// and assert durable synchronous storage effects. Shared process helpers assert
// asynchronous downstream outcomes.

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
)

// ingest admits a queue's head commit into the pipeline and returns the minted
// request id.
func (s *StovepipeE2ESuite) ingest(queue string) string {
	t := s.T()
	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err, "Ingest failed for queue %s", queue)
	require.NotEmpty(t, resp.Id, "Ingest returned an empty id for queue %s", queue)
	return resp.Id
}

// requestRowCount returns the number of request rows with the given id (0 or 1).
func (s *StovepipeE2ESuite) requestRowCount(id string) int {
	t := s.T()
	var count int
	require.NoError(t, s.db.QueryRow("SELECT COUNT(*) FROM request WHERE id = ?", id).Scan(&count),
		"failed to count request rows for %s", id)
	return count
}

// uriMapping returns the request id the (queue, URI) mapping points at.
func (s *StovepipeE2ESuite) uriMapping(queue string) string {
	t := s.T()
	var mappedID string
	require.NoError(t, s.db.QueryRow("SELECT request_id FROM request_uri WHERE queue = ?", queue).Scan(&mappedID),
		"failed to read request_uri mapping for queue %s", queue)
	return mappedID
}

// assertIngestPersisted asserts the synchronous side effects of a successful
// Ingest: the request row and the (queue, URI) mapping pointing at the minted
// id. Process publication is asserted through its durable downstream outcome.
func (s *StovepipeE2ESuite) assertIngestPersisted(queue, id string) {
	t := s.T()
	assert.Equal(t, 1, s.requestRowCount(id), "request row should be persisted for %s", id)
	assert.Equal(t, id, s.uriMapping(queue), "URI mapping should point at the minted request id")
}
