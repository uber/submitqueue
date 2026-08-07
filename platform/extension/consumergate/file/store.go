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

// Package file implements the consumergate contract with plain files in a
// shared directory. Presence of a gate file means the gate is closed; deleting
// the file opens it. Layout under the configured root:
//
//	gates/{consumer_group}/all                        gates every partition
//	gates/{consumer_group}/p-{urlenc(partition)}      gates one partition
//	parked/{consumer_group}/{topic}/{urlenc(id)}.json one parked delivery record
//
// Consumer groups and topics are filesystem-safe by the repo's naming rules;
// partition keys and message IDs may contain "/" (request IDs like "queue/1"),
// so they are URL-encoded in file names. Gate files hold human-readable JSON
// metadata so an operator finding a paused controller can tell why. All writes
// go through temp-file-plus-rename so readers never see partial JSON.
//
// Enter reads the applicable gate files for every delivery; the store itself
// never waits or polls. A blocked delivery's re-check cadence is owned by the
// caller (the consumer postpones the delivery and re-enters on redelivery), so
// gate state is re-read on every attempt regardless of filesystem event
// support — which varies across bind mounts, overlay or network filesystems,
// rootless Docker, and Docker Desktop's host/container filesystem bridge.
//
// The medium is deliberately scoped to E2E tests and single-host development:
// pausing a controller is writing a small file, resuming is rm, and a bind mount
// makes the state reachable outside the service process. A file gates only
// processes that see the directory; it is not a fleet-wide production control
// plane.
package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/uber/submitqueue/platform/extension/consumergate"
)

// Store implements consumergate.Gate and consumergate.Admin over a directory.
type Store struct {
	dir string
}

// Verify interface compliance at compile time.
var (
	_ consumergate.Gate  = (*Store)(nil)
	_ consumergate.Admin = (*Store)(nil)
)

// New returns a file-backed consumergate store rooted at dir. The directory
// does not need to exist yet — reads treat a missing tree as "no gates, no
// parked records", and writes create what they need.
func New(dir string) *Store {
	return &Store{dir: dir}
}

// gatePath returns the gate file path for a key: the "all" marker when the key
// has no partition, or the partition-scoped "p-..." marker otherwise.
func (s *Store) gatePath(key consumergate.Key) string {
	name := "all"
	if key.PartitionKey != "" {
		name = "p-" + url.QueryEscape(key.PartitionKey)
	}
	return filepath.Join(s.dir, "gates", key.ConsumerGroup, name)
}

// parkedPath returns the parked-record file path for one delivery.
func (s *Store) parkedPath(consumerGroup, topic, messageID string) string {
	return filepath.Join(s.dir, "parked", consumerGroup, topic, url.QueryEscape(messageID)+".json")
}

// isGated reports whether deliveries for the consumer group and partition are
// currently gated, either by an all-partitions gate or by a gate scoped to
// exactly this partition.
func (s *Store) isGated(consumerGroup, partitionKey string) (bool, error) {
	paths := []string{s.gatePath(consumergate.Key{ConsumerGroup: consumerGroup})}
	if partitionKey != "" {
		paths = append(paths, s.gatePath(consumergate.Key{ConsumerGroup: consumerGroup, PartitionKey: partitionKey}))
	}
	for _, p := range paths {
		switch _, err := os.Stat(p); {
		case err == nil:
			return true, nil
		case os.IsNotExist(err):
			// Not gated by this marker; check the next.
		default:
			return false, fmt.Errorf("failed to stat gate file %s: %w", p, err)
		}
	}
	return false, nil
}

// Enter implements consumergate.Gate. It returns an unblocked Entry when the
// gate identified by key is open, and a blocked Entry when it is closed.
func (s *Store) Enter(_ context.Context, key consumergate.Key) (consumergate.Entry, error) {
	gated, err := s.isGated(key.ConsumerGroup, key.PartitionKey)
	if err != nil {
		return nil, err
	}
	return entry{store: s, key: key, blocked: gated}, nil
}

// entry is the Entry for one delivery's gate check.
type entry struct {
	// store is the file store that performed the check.
	store *Store
	// key is the gate identity the delivery entered with.
	key consumergate.Key
	// blocked records whether the gate was closed at Enter time.
	blocked bool
}

// Blocked implements consumergate.Entry.
func (e entry) Blocked() bool { return e.blocked }

// Park implements consumergate.Entry. It writes the parked record, combining
// the caller's delivery description with the gate identity captured by Enter
// and a fresh ParkedAtMs stamp. Re-parking the same delivery overwrites the
// previous record.
func (e entry) Park(_ context.Context, descriptor consumergate.DeliveryDescriptor) error {
	return e.store.recordParked(consumergate.Parked{
		ConsumerGroup: e.key.ConsumerGroup,
		Topic:         descriptor.Topic,
		MessageID:     descriptor.MessageID,
		PartitionKey:  e.key.PartitionKey,
		Payload:       descriptor.Payload,
		Attempt:       descriptor.Attempt,
		ParkedAtMs:    time.Now().UnixMilli(),
	})
}

// Unpark implements consumergate.Entry. Removing an absent record is a no-op,
// so callers may invoke it unconditionally on the admit path.
func (e entry) Unpark(_ context.Context, descriptor consumergate.DeliveryDescriptor) error {
	return e.store.removeParked(e.key.ConsumerGroup, descriptor.Topic, descriptor.MessageID)
}

// recordParked writes a parked-delivery record. Re-recording the same delivery
// (e.g. after a redelivery) overwrites the previous record.
func (s *Store) recordParked(parked consumergate.Parked) error {
	path := s.parkedPath(parked.ConsumerGroup, parked.Topic, parked.MessageID)
	if err := writeJSON(path, parkedRecord(parked)); err != nil {
		return fmt.Errorf("failed to write parked record %s: %w", path, err)
	}
	return nil
}

// removeParked removes a parked-delivery record. Removing an already-absent
// record is a no-op.
func (s *Store) removeParked(consumerGroup, topic, messageID string) error {
	path := s.parkedPath(consumerGroup, topic, messageID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove parked record %s: %w", path, err)
	}
	return nil
}

// Close implements consumergate.Admin by writing the gate file for the key.
func (s *Store) Close(_ context.Context, key consumergate.Key, meta consumergate.Metadata) error {
	if key.ConsumerGroup == "" {
		return fmt.Errorf("gate key requires a consumer group")
	}
	path := s.gatePath(key)
	if err := writeJSON(path, gateRecord{
		Reason:      meta.Reason,
		CreatedBy:   meta.CreatedBy,
		CreatedAtMs: meta.CreatedAtMs,
	}); err != nil {
		return fmt.Errorf("failed to write gate file %s: %w", path, err)
	}
	return nil
}

// Open implements consumergate.Admin by removing the gate file for the key.
// Opening an already-open gate is a no-op.
func (s *Store) Open(_ context.Context, key consumergate.Key) error {
	path := s.gatePath(key)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove gate file %s: %w", path, err)
	}
	return nil
}

// ListParked implements consumergate.Admin. It returns every parked record for
// the consumer group across all topics; a missing tree yields an empty list.
func (s *Store) ListParked(_ context.Context, consumerGroup string) ([]consumergate.Parked, error) {
	groupDir := filepath.Join(s.dir, "parked", consumerGroup)
	topics, err := os.ReadDir(groupDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read parked dir %s: %w", groupDir, err)
	}

	var out []consumergate.Parked
	for _, topic := range topics {
		if !topic.IsDir() {
			continue
		}
		topicDir := filepath.Join(groupDir, topic.Name())
		entries, err := os.ReadDir(topicDir)
		if err != nil {
			return nil, fmt.Errorf("failed to read parked dir %s: %w", topicDir, err)
		}
		for _, entry := range entries {
			// Skip anything that is not a finished record (e.g. temp files
			// awaiting rename).
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			rec, err := readParked(filepath.Join(topicDir, entry.Name()))
			if err != nil {
				return nil, err
			}
			out = append(out, consumergate.Parked(rec))
		}
	}
	return out, nil
}

// gateRecord is the JSON layout of a gate file.
type gateRecord struct {
	// Reason is a human-readable explanation for the closure.
	Reason string `json:"reason"`
	// CreatedBy identifies who or what closed the gate.
	CreatedBy string `json:"created_by"`
	// CreatedAtMs is when the gate was closed (Unix milliseconds).
	CreatedAtMs int64 `json:"created_at_ms"`
}

// parkedRecord is the JSON layout of a parked-delivery record. It mirrors
// consumergate.Parked field-for-field; the named type only pins the wire tags.
type parkedRecord struct {
	// ConsumerGroup is the consumer group whose gate parked the delivery.
	ConsumerGroup string `json:"consumer_group"`
	// Topic is the topic name the delivery was consumed from.
	Topic string `json:"topic"`
	// MessageID is the queue message ID of the parked delivery.
	MessageID string `json:"message_id"`
	// PartitionKey is the partition the delivery belongs to.
	PartitionKey string `json:"partition_key"`
	// Payload is the message payload (base64 in the JSON encoding).
	Payload []byte `json:"payload"`
	// Attempt is the delivery attempt the parked message is on.
	Attempt int `json:"attempt"`
	// ParkedAtMs is when the delivery was parked (Unix milliseconds).
	ParkedAtMs int64 `json:"parked_at_ms"`
}

// readParked loads and decodes one parked-record file.
func readParked(path string) (parkedRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return parkedRecord{}, fmt.Errorf("failed to read parked record %s: %w", path, err)
	}
	var rec parkedRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return parkedRecord{}, fmt.Errorf("failed to decode parked record %s: %w", path, err)
	}
	return rec, nil
}

// writeJSON writes v as indented JSON via temp-file-plus-rename in the target
// directory, so concurrent readers never observe partial content. On any
// failure after the temp file is created, the temp file is removed in a single
// deferred cleanup; removal errors are joined with the causal error.
func writeJSON(path string, v any) (retErr error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create dir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() {
		if retErr != nil {
			if rmErr := os.Remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
				retErr = errors.Join(retErr, rmErr)
			}
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		closeErr := tmp.Close()
		return errors.Join(fmt.Errorf("failed to write temp file %s: %w", tmpName, err), closeErr)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("failed to rename %s to %s: %w", tmpName, path, err)
	}
	return nil
}
