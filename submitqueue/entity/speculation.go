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

package entity

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// DependencyBetType is how a path treats one of its head's dependencies.
type DependencyBetType string

const (
	// BetUnknown is the zero-value sentinel; it is never a valid bet.
	BetUnknown DependencyBetType = ""
	// BetIncluded bets the dependency lands: the head is built on top of it, and
	// the path is invalidated if the dependency ultimately fails.
	BetIncluded DependencyBetType = "included"
	// BetExcluded bets the dependency does not land: the head is built without it,
	// and the path is invalidated if the dependency ultimately lands.
	BetExcluded DependencyBetType = "excluded"
	// BetDropped marks a dependency ignored by conflict relaxation: whether it
	// lands or fails never affects the path — it neither gates the merge nor
	// invalidates the path.
	BetDropped DependencyBetType = "dropped"
)

// DependencyBet is a path's bet on one dependency of its head.
type DependencyBet struct {
	// Batch is the dependency batch ID this bet is about.
	Batch string
	// Bet is how the path treats the dependency (included, excluded, or dropped).
	Bet DependencyBetType
}

// SpeculationPath is one guess at how a batch's dependencies resolve: a head
// batch plus a bet on each of its dependencies. Every dependency of the head
// appears exactly once, in queue order, so the path is self-describing — its
// full meaning can be read without consulting any external relaxed set or
// dependency list.
type SpeculationPath struct {
	// Head is the ID of the batch being built along this path.
	Head string
	// Bets is one bet per dependency of Head, in queue order.
	Bets []DependencyBet
}

// ID returns the path's stable identity: a hex-encoded SHA-256 over the head
// and its bets in order. Two paths with the same head and the same ordered
// bets share an ID; any difference in head, dependency, or bet yields a
// different ID. The hash is computed on every call and never cached, so a
// caller that compares IDs repeatedly should keep the result.
func (p SpeculationPath) ID() string {
	var b strings.Builder
	b.WriteString(p.Head)
	for _, bet := range p.Bets {
		b.WriteByte('\n')
		b.WriteString(bet.Batch)
		b.WriteByte('=')
		b.WriteString(string(bet.Bet))
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// SpeculationPathStatus is the lifecycle status of one speculation path's
// current build attempt.
type SpeculationPathStatus string

const (
	// SpeculationPathStatusUnknown is the zero-value sentinel; it should never be
	// seen in the system.
	SpeculationPathStatusUnknown SpeculationPathStatus = ""
	// SpeculationPathStatusPending indicates the path is funded under the build
	// budget but its build has not started yet.
	SpeculationPathStatusPending SpeculationPathStatus = "pending"
	// SpeculationPathStatusBuilding indicates the path's build is running in the
	// build system.
	SpeculationPathStatusBuilding SpeculationPathStatus = "building"
	// SpeculationPathStatusPassed indicates the path's build completed
	// successfully. This is a terminal state.
	SpeculationPathStatusPassed SpeculationPathStatus = "passed"
	// SpeculationPathStatusFailed indicates the path's build did not complete
	// successfully. This is a terminal state.
	SpeculationPathStatusFailed SpeculationPathStatus = "failed"
	// SpeculationPathStatusCancelling is the non-terminal intent state set when
	// the path's build is being cancelled. The build holds its slot until it
	// reaches a terminal state.
	SpeculationPathStatusCancelling SpeculationPathStatus = "cancelling"
	// SpeculationPathStatusCancelled indicates the path's build was cancelled.
	// This is a terminal state.
	SpeculationPathStatusCancelled SpeculationPathStatus = "cancelled"
)

// IsTerminal returns true if the status represents a final state
// (Passed, Failed, or Cancelled). Cancelling is intentionally excluded: it is
// a non-terminal intent, and the build may still reach Passed or Failed before
// it reaches Cancelled.
func (s SpeculationPathStatus) IsTerminal() bool {
	switch s {
	case SpeculationPathStatusPassed, SpeculationPathStatusFailed, SpeculationPathStatusCancelled:
		return true
	default:
		return false
	}
}

// SpeculationPathEntry is the stored record of one chosen speculation path,
// keyed by the hash of its content. It holds no build reference (that lives on
// the separate execution record, keyed by (ID, Attempt)) and no score (a score
// is meaningful only within a single speculation run).
type SpeculationPathEntry struct {
	// ID is the primary key: the hash of the path's content (head plus its
	// bets). It always equals Path.ID() — it is materialized here, rather than
	// recomputed from Path, because a stored record carries its own key: lookups
	// and comparisons read it without rehashing the path.
	ID string
	// Path is the head plus one bet per dependency, in queue order.
	Path SpeculationPath
	// Status is the lifecycle status of the current build attempt.
	Status SpeculationPathStatus
	// Attempt is the build attempt number for this path, starting at 1. A path
	// can be built more than once (e.g. after a prior build is cancelled to free
	// budget, or fails and is retried), so ID alone does not identify an
	// execution — (ID, Attempt) does. It increments with each new build.
	Attempt int
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
	// CreatedAtMs is the creation time in Unix epoch milliseconds.
	CreatedAtMs int64
	// UpdatedAtMs is the last-update time in Unix epoch milliseconds.
	UpdatedAtMs int64
}

// SpeculationPathSet is one head's chosen speculation paths under a single
// version. It holds both live paths and recently finished ones — finished
// entries linger briefly so that a re-run cannot collide with an old build.
// Every path in the set shares the same head and bets over the same ordered
// dependency list.
type SpeculationPathSet struct {
	// Head is the primary key: the ID of the head batch these paths speculate
	// on. Every path in the set carries this same head.
	Head string
	// Paths is the head's chosen paths, live and recently finished.
	Paths []SpeculationPathEntry
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}

// PathAction is an action proposed on a speculation path. The set is limited to
// build and cancel; there is no merge or fail action, because a batch's verdict
// is a controller-owned fact, not a proposed action.
type PathAction string

const (
	// PathActionUnknown is the zero-value sentinel; it is never a valid action.
	PathActionUnknown PathAction = ""
	// PathActionBuild proposes starting (or resurrecting) a build for the path.
	PathActionBuild PathAction = "build"
	// PathActionCancel proposes preempting an in-flight path to free build budget.
	PathActionCancel PathAction = "cancel"
)

// Speculation is one proposed action on one path. A path left as-is has no
// Speculation.
type Speculation struct {
	// Path is the path the action applies to; its ID hashes the head and its bets.
	Path SpeculationPath
	// Action is the proposed action (build or cancel).
	Action PathAction
}

// CandidatePath is a path paired with the transient ranking score assigned to it
// within a single speculation run. The ranking score orders candidates for that
// run only and is never stored — rankings go stale across runs.
type CandidatePath struct {
	// Path is the candidate: a head plus one bet per dependency.
	Path SpeculationPath
	// RankingScore is the ordering score assigned this run; higher sorts first.
	RankingScore float64
}
