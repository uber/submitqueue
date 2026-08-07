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

// Degree bounds. A degree answers "how broken was this scope?" on a closed
// interval, so the two endpoints are named and the values between them are
// meaningful rather than arbitrary.
const (
	// DegreeGreen is a fully healthy scope: nothing broken.
	DegreeGreen = 0.0
	// DegreeBroken is a fully broken scope.
	DegreeBroken = 1.0
)

// ValidationFact is the durable record of how broken one scope was at one commit.
//
// It is immutable and create-only: the first fact written for an identity is the
// permanent answer, and a later contradicting outcome for the same identity is
// dropped rather than overwriting it. Identity is (queue, URI, project) — the
// queue is the binding of the store the fact lives in and so is not carried on
// the fact itself.
//
// Absence is distinct from DegreeGreen: a URI with no fact has not been
// validated, which is not the same as having been validated and found healthy.
type ValidationFact struct {
	// URI is the commit the fact describes, in the queue's VCS-agnostic locator
	// form. Unique per (queue, URI, project).
	URI string `json:"uri"`
	// Project is the scope the degree applies to. Empty means the whole
	// repository; named projects narrow the fact to part of it.
	Project string `json:"project"`
	// Degree is how broken the scope was, on the closed interval
	// [DegreeGreen, DegreeBroken]. Intermediate values describe partial
	// breakage.
	Degree float64 `json:"degree"`
	// RequestID is the request that established the fact.
	RequestID string `json:"request_id"`
	// CreatedAt is the millisecond timestamp at which the fact was recorded.
	CreatedAt int64 `json:"created_at"`
}

// IsGreen reports whether the fact records a fully healthy scope. Any degree
// above DegreeGreen is some amount of broken.
func (f ValidationFact) IsGreen() bool {
	return f.Degree == DegreeGreen
}
