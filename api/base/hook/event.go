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

package hook

import (
	"fmt"
	"strconv"
	"strings"
)

// Consumers never parse an id, so this is a minting convention rather than a
// wire format. All it must guarantee is that two different transitions cannot
// join to the same string.
const idSeparator = "/"

// NewEventID mints the id of an event describing a versioned state write.
//
// Deriving the id rather than randomizing it is what makes replay safe: the same
// transition mints the same id, so the queue dedupes a redelivery and a hook
// stays idempotent without a publisher-side outbox. version is the subject's
// version immediately after the write, which is what separates two transitions
// of the same subject.
func NewEventID(source, eventType, subjectID string, version int32) string {
	return strings.Join([]string{source, eventType, subjectID, strconv.Itoa(int(version))}, idSeparator)
}

// NewUnversionedEventID mints the id of an event whose transition was not a
// versioned write, so no version distinguishes one occurrence from the next.
//
// The causing message's id stands in for the version, being stable across
// redeliveries for the same reason a version is. ordinal separates several
// same-typed events published for one cause; pass 0 when there is only one.
func NewUnversionedEventID(source, eventType, subjectID, causeID string, ordinal int) string {
	return strings.Join(
		[]string{source, eventType, subjectID, causeID, strconv.Itoa(ordinal)},
		idSeparator,
	)
}

// Validate reports whether e carries the three envelope fields every consumer
// keys on. The rest cannot be checked generically: version is legitimately 0 for
// an unversioned transition and payload is shaped per type.
//
// Both sides call it — a publisher to catch a malformed event before it reaches
// the queue, a consumer because the producer may not have.
func Validate(e *HookEvent) error {
	if e == nil {
		return fmt.Errorf("hook event is nil")
	}
	if e.GetId() == "" {
		return fmt.Errorf("hook event has no id")
	}
	if e.GetSource() == "" {
		return fmt.Errorf("hook event %q has no source", e.GetId())
	}
	if e.GetType() == "" {
		return fmt.Errorf("hook event %q has no type", e.GetId())
	}
	return nil
}
