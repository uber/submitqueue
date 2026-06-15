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

// Package entity holds Runway's domain entities, including the wire contract for
// the merge queues that Runway owns. The contract crosses a service boundary (a
// calling service cannot read Runway's storage and vice versa), so these
// payloads carry the full data needed to perform a merge attempt rather than
// opaque entity IDs.
//
// One contract serves two queue pairs because a merge-conflict check is a dry
// run of a merge: Runway applies the same ordered Steps onto the same target
// branch, and the only difference is whether it commits the result and reports
// the revisions it produced. The queue a request arrives on encodes that choice
// — the merge-conflict-checker pair for a dry run, the merger pair for a
// committing merge — so MergeRequest and MergeResult are identical on both.
package entity

import (
	"encoding/json"

	"github.com/uber/submitqueue/entity/change"
	"github.com/uber/submitqueue/entity/mergestrategy"
)

// MergeStep is one step of an ordered merge: a single set of change(s) applied
// with a strategy. Runway applies the steps of a request in order on top of the
// target branch; the ordering encodes the base-layering (earlier steps are the
// in-flight base, the last step is the candidate).
type MergeStep struct {
	// StepID is an opaque, caller-assigned identifier for this step. Runway
	// treats it as an attribution token only — it echoes it back per-step in
	// StepResult so a multi-step result is attributable — and never interprets
	// its contents. (A caller might use, for example, its own request id here.)
	StepID string `json:"step_id"`
	// Changes are the code change(s) to apply for this step (provider URIs with
	// head commit SHAs; see entity/change.Change).
	Changes []change.Change `json:"changes"`
	// Strategy is how this step's changes are integrated into the target branch.
	Strategy mergestrategy.MergeStrategy `json:"strategy"`
}

// MergeRequest is the payload a client publishes to one of Runway's merge
// queues: TopicKeyMergeConflictCheck for a dry-run check, TopicKeyMerge for a
// committing merge. The ID is owned by the client so it can record the
// in-flight work before publishing and correlate the asynchronous result;
// runway echoes it back unchanged.
type MergeRequest struct {
	// ID is the client-owned correlation id for this request (one per request).
	// Runway echoes it back on the result unchanged.
	ID string `json:"id"`
	// QueueName is the caller-provided queue name the request belongs to. Runway
	// resolves the target branch and provider config per-queue from this name;
	// no target ref is passed.
	QueueName string `json:"queue_name"`
	// Steps is the ordered application sequence: in-flight steps first, the
	// candidate last. A single-element slice expresses "candidate vs target
	// branch".
	Steps []MergeStep `json:"steps"`
}

// ToBytes serializes the MergeRequest to JSON bytes for the queue payload.
func (r MergeRequest) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// MergeRequestFromBytes deserializes a MergeRequest from JSON bytes.
func MergeRequestFromBytes(data []byte) (MergeRequest, error) {
	var req MergeRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

// StepResult reports what happened to a single MergeStep, so a multi-step result
// is attributable to the step that produced (or failed to produce) it.
type StepResult struct {
	// StepID echoes the StepID of the step this result is for (see MergeStep.StepID).
	StepID string `json:"step_id"`
	// OutputIDs are the VCS-neutral identifiers of the revisions this step
	// produced on the target branch — a git commit SHA, a Mercurial changeset
	// hash, a Subversion revision number, a Perforce changelist, and so on —
	// opaque to the caller. Empty for a dry-run check (which produces nothing),
	// for a change already present on the target, or for a step that failed to
	// apply.
	OutputIDs []string `json:"output_ids,omitempty"`
	// Reason is a human-readable explanation when the step failed to apply.
	// Empty on success.
	Reason string `json:"reason,omitempty"`
}

// MergeResult is the payload runway publishes to the corresponding signal queue
// (TopicKeyMergeConflictCheckSignal for a check, TopicKeyMergeSignal for a
// merge) once a request completes.
type MergeResult struct {
	// ID echoes the client-owned correlation id from the request.
	ID string `json:"id"`
	// Success is true if the whole ordered step sequence applied cleanly:
	// mergeable for a dry-run check, merged for a committing merge.
	Success bool `json:"success"`
	// Reason is a human-readable explanation when Success is false. Empty on success.
	Reason string `json:"reason,omitempty"`
	// Steps optionally reports per-step outcomes, in request order. A committing
	// merge populates each step's OutputIDs with the revisions it produced; a
	// dry-run check leaves them empty.
	Steps []StepResult `json:"steps,omitempty"`
}

// ToBytes serializes the MergeResult to JSON bytes for the queue payload.
func (r MergeResult) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// MergeResultFromBytes deserializes a MergeResult from JSON bytes.
func MergeResultFromBytes(data []byte) (MergeResult, error) {
	var res MergeResult
	err := json.Unmarshal(data, &res)
	return res, err
}
