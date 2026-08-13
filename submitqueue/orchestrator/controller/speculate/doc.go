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

// Package speculate plans a queue's speculative builds and finalizes each
// batch's outcome from their results. It is the orchestrator's decision
// stage: builds are started by the build stage and watched — and stopped —
// by the buildsignal stage, but what to build and what a finished build
// means are decided here.
//
// # Why speculation
//
// Batches in a queue depend on the batches ahead of them, so without
// speculation everything is serial: C waits for B, B waits for A. Speculation
// builds a batch against a guess about how its dependencies turn out. When
// the guess holds, the batch merges the moment the guessed-on dependencies
// land — it never waits for a build of its own to start afterwards.
//
// # Paths
//
// The batch being speculated on is the head. One complete guess about it is a
// path: one assumption per dependency, each "succeeds" or "fails". A path's ID
// hashes the head and its assumptions, so a path *is* its guess; building the
// same guess again is a new attempt of the same path, and (path ID, attempt)
// names the resulting build.
//
// # A worked example
//
// A two-batch queue, where B depends on A and A is still building:
//
//	queue:  A ← B
//
//	B's speculation space is two paths:
//	  P1 = [A succeeds]   B built on top of A's result
//	  P2 = [A fails]      B built without A
//
// Fund both and every future is covered:
//
//   - A succeeds and P1 passed: B merges the moment A lands. P2's guess
//     ("A fails") is broken — it can no longer come true — so its build is
//     cancelled to free the slot.
//   - A fails and P2 passed: B merges without A, again with no new build.
//     P1's guess is broken.
//   - A resolved either way, and every unbroken path failed: no future
//     remains in which B passes, so B fails.
//
// # The life of a path
//
// A path's status tracks its current attempt:
//
//	              funded            observed running       build finished
//	(no entry) ─────────► pending ─────────► building ──────┬──► passed
//	                         │                    │         └──► failed
//	           "stop this":  │                    │
//	           broken,       ▼                    ▼
//	           superseded, ─────► cancelling ◄────┘
//	           head cancelled,        │
//	           or preempted           │  build observed stopped, or
//	                                  │  nothing was ever dispatched
//	                                  ▼
//	                              cancelled
//
// A path is funded — given one of the slots under the queue's cap on
// concurrent builds, the build budget — when its guess is judged worth
// building, and every pending, building, and cancelling path holds its slot
// until its build stops. A path is broken once a dependency's actual result
// proves one of its assumptions wrong: its guess can no longer come true, so
// its build is cancelled to free the slot. A path is superseded when a
// sibling path of the same head passes — that sibling will carry the head out
// of the queue, so the others are cancelled too.
//
// Cancelling is intent, not fact: the build keeps its slot until CI actually
// stops it, and only an observation of that stop (or proof nothing was ever
// dispatched) moves the path to cancelled. The intent needs no dispatch of its
// own — the poll loop reads it off the set and asks the runner to stop the
// build. A terminal path can be resurrected by a new build proposal — status
// returns to pending and Attempt increments, the one backwards step in the
// diagram.
//
// # The life of a batch, as seen from here
//
//	Created ──admit──► Speculating ──┬── merge ──► Merging   (merge stage takes over)
//	                                 └── fail ───► Failed
//	user cancel (cancel stage):
//	   ... ──► Cancelling ── every path stopped ──► Cancelled
//
// A batch is admitted either by the message that names it or by the next run
// that finds it still in Created, whichever comes first. Created is
// dependency-eligible, so a batch left there would be a dependency nothing can
// resolve; admission cannot be left to rest on one message arriving.
//
// Failed and Cancelled fan out to the conclude stage, which reconciles the
// batch's requests.
//
// # How a run works
//
// Every message is only a dirty signal — "this queue changed" — naming the
// batch that changed. The run then re-plans the whole queue from a single
// read; nothing carries over from earlier runs, so duplicated, delayed, or
// reordered signals are harmless, and a later run repairs whatever an
// earlier one left half-done.
//
//	signal ──► read ──► admit ──► finalize ──► ask ──► check ──► dispatch
//	           one      every     enact the    the     filter     save changes,
//	           read of  batch     outcomes     Specu-  its        hand builds to
//	           queue +  still in  the facts    lator   proposals  the build stage
//	           paths    Created   already
//	                              decide
//
// The Speculator is the extension that proposes which paths to fund or
// preempt. It only ever proposes: check.go filters its answer, and outcomes
// are computed here, never by the extension. Finalize runs before ask so the
// Speculator reasons over facts as they now stand, not over a picture the run
// is about to invalidate.
//
// # Ownership
//
// The path set has exactly one writer: this controller's run. The build stage
// starts builds and the buildsignal stage watches and stops them; both write
// only per-build records of their own (the build link and the build's status),
// which read folds into the snapshot, and buildsignal reads the set only as
// the kill list for builds nothing wants any more. One writer is what lets a
// run hold one version of a head's paths across its whole decision without a
// poll invalidating it mid-thought.
package speculate
