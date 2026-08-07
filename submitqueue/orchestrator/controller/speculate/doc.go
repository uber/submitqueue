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

// Package speculate plans a queue's speculative builds: which guesses about
// the queue's future are worth building, within the queue's cap on concurrent
// builds (the build budget).
//
// Batch outcomes — merge or fail — are still decided by the legacy per-batch
// finalizer in speculate.go, which waits on every dependency. Deriving them
// from the paths planned here replaces it in the next change; this package
// doc grows with it.
//
// # Why speculation
//
// Batches in a queue depend on the batches ahead of them, so without
// speculation everything is serial: C waits for B, B waits for A. Speculation
// builds a batch against a guess about how its dependencies turn out. When
// the guess holds, the head's build has already run by the time its
// dependencies resolve — it never waits for a build of its own to start
// afterwards.
//
// # Paths
//
// The batch being speculated on is the head. One complete guess about it is a
// path: one assumption per dependency, each "succeeds", "fails", or "ignored"
// (no claim either way). A path's ID hashes the head and its assumptions, so
// a path *is* its guess; building the same guess again is a new attempt of
// the same path, and (path ID, attempt) names the resulting build.
//
// # The life of a path
//
// A path's status tracks its current attempt:
//
//	              funded            observed running       build finished
//	(no entry) ─────────► pending ─────────► building ──────┬──► passed
//	                         │                    │         └──► failed
//	           "stop this":  │                    │
//	           broken or     ▼                    ▼
//	           preempted   ─────► cancelling ◄────┘
//	                                  │
//	                                  │  build observed stopped
//	                                  ▼
//	                              cancelled
//
// A path is funded — given one of the slots under the queue's cap on
// concurrent builds, the build budget — when its guess is judged worth
// building, and every pending, building, and cancelling path holds its slot
// until its build stops. A path is broken once a dependency's actual result
// proves one of its assumptions wrong: its guess can no longer come true, so
// its build is cancelled to free the slot.
//
// Cancelling is intent, not fact: the build keeps its slot until CI actually
// stops it, and only an observation of that stop moves the path to cancelled.
// The intent needs no dispatch of its own — the poll loop reads it off the set
// and asks the runner to stop the build. A terminal path can be resurrected by
// a new build proposal — status returns to pending and Attempt increments, the
// one backwards step in the diagram.
//
// # How a run works
//
// Every message is only a dirty signal — "this queue changed" — naming the
// batch that changed. The run then re-plans the whole queue from a single
// read; nothing carries over from earlier runs, so duplicated, delayed, or
// reordered signals are harmless, and a later run repairs whatever an
// earlier one left half-done.
//
//	signal ──► read ──► cancel ──► ask ──► check ──► dispatch
//	           one      broken     the     filter    save changes,
//	           read of  paths      Specu-  its       hand builds to
//	           queue +             lator   proposals the build stage
//	           paths
//
// The Speculator is the extension that proposes which paths to fund or
// preempt. It only ever proposes: check.go filters its answer, and broken
// paths are cancelled before it is asked, so it reasons over facts as they
// now stand rather than over a picture the run is about to invalidate.
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
