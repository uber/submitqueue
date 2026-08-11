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

// Package failure holds the shared description of why processing failed: a
// human-readable message, the entities the failure is about, and free-form
// detail. It is the vocabulary a producer of a failure and a consumer of it
// share when they are separated by a queue, so the consumer reads fields
// rather than parsing prose.
//
// The package is deliberately domain-agnostic. It says a failure has subjects
// and what shape a subject is; which subject types exist is a domain's own
// business.
package failure

import "encoding/json"

// Subject names one entity a failure is about.
//
// Its purpose is attribution: a consumer reconciling a failure has to know
// what to act on, and the entity named on the message that failed is not
// always the entity at fault — a job that reads many records can fail because
// of any of them, or because of none of them individually.
type Subject struct {
	// Type labels what kind of entity ID names, e.g. "batch" or "queue".
	// Values are chosen by the domain that raises the failure; this package
	// neither defines nor validates them. Empty means the type is unknown.
	Type string `json:"type"`
	// ID identifies the entity within its type. Opaque here: no format is
	// assumed and none is parsed.
	ID string `json:"id"`
}

// Failure describes why processing failed.
//
// A failure is always about something. When no single record is at fault, the
// subject is the wider thing that is — the queue, the tenant, the job — rather
// than an empty list. That keeps absence from carrying meaning: no subjects at
// all means the failure is *unattributed*, which is a genuine third state
// (nothing recorded one, or the record predates attribution) and not a claim
// that nothing was to blame.
type Failure struct {
	// Message is the human-readable reason, typically an error's text. It is
	// the one field always present, and the one a person reads first.
	Message string `json:"-"`
	// Subjects are the entities this failure is about, in no significant
	// order. Empty means unattributed — see the type comment.
	Subjects []Subject `json:"subjects,omitempty"`
	// Detail is free-form structured context: whatever the producer knows that
	// does not fit the message. Values survive a JSON round trip, so numbers
	// come back as float64 regardless of what went in.
	Detail map[string]any `json:"detail,omitempty"`
}

// New builds a Failure with a message and the subjects it is about.
func New(message string, subjects ...Subject) Failure {
	return Failure{Message: message, Subjects: subjects}
}

// IDsOfType returns the IDs of every subject with the given type, in the order
// they appear. The result is empty when the failure names no such subject,
// which is how a consumer asks "is this about one of mine?" without inspecting
// the slice itself.
func (f Failure) IDsOfType(subjectType string) []string {
	var ids []string
	for _, s := range f.Subjects {
		if s.Type == subjectType {
			ids = append(ids, s.ID)
		}
	}
	return ids
}

// Encode returns the JSON encoding of the structured half of f — its subjects
// and detail — or nil when there is no structure to store.
//
// Message is deliberately excluded. It travels as plain text alongside this
// blob so that it stays legible to anything reading the underlying store
// directly, and so decoding never has to guess whether a stored string is an
// encoded failure or a message that merely looks like one.
func Encode(f Failure) ([]byte, error) {
	if len(f.Subjects) == 0 && len(f.Detail) == 0 {
		return nil, nil
	}
	return json.Marshal(f)
}

// Decode parses the structured half produced by Encode. Empty input yields the
// zero Failure, which is how an unattributed failure reads.
//
// The returned Message is always empty: the caller holds it separately and
// fills it in.
func Decode(data []byte) (Failure, error) {
	if len(data) == 0 {
		return Failure{}, nil
	}
	var f Failure
	if err := json.Unmarshal(data, &f); err != nil {
		return Failure{}, err
	}
	return f, nil
}
