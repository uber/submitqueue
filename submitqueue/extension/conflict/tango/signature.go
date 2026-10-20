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

// Package tango converts Tango's changed-target responses into target
// signatures and defines the signature and dictionary records.
package tango

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
)

// LabelDistance is one affected target: its label exactly as Tango reported it
// and its distance from the nearest directly changed source.
type LabelDistance struct {
	// Label is the target label, unmodified from Tango's response.
	Label string

	// Distance is 0 for a target a change touches directly and grows by one for each dependency edge away from one.
	Distance int32
}

// hashedLabel records a label under the target ID it hashes to.
type hashedLabel struct {
	id    uint64
	label string
}

type hashedTarget struct {
	hashedLabel
	distance int32
}

// targetID returns the first 8 bytes of the SHA-256 of label, read big-endian.
// A collision only adds a false conflict.
func targetID(label string) uint64 {
	digest := sha256.Sum256([]byte(label))
	return binary.BigEndian.Uint64(digest[:])
}

// toSignatureTargets returns the signature targets sorted ascending by ID, one
// per ID at the smallest distance reported for it, and every distinct (ID,
// label) pair sorted by ID then label. targets is not modified. An empty input
// yields nil results, which is a valid signature that conflicts with nothing.
func toSignatureTargets(targets []LabelDistance) ([]SignatureTarget, []hashedLabel) {
	hashed := make([]hashedTarget, len(targets))
	for i, target := range targets {
		hashed[i] = hashedTarget{hashedLabel: hashedLabel{id: targetID(target.Label), label: target.Label}, distance: target.Distance}
	}
	return mergeHashedTargetsByID(hashed)
}

// mergeHashedTargetsByID sorts hashed in place; callers hand over ownership.
func mergeHashedTargetsByID(hashed []hashedTarget) ([]SignatureTarget, []hashedLabel) {
	slices.SortFunc(hashed, func(a, b hashedTarget) int {
		return cmp.Or(cmp.Compare(a.id, b.id), cmp.Compare(a.label, b.label))
	})

	var signatureTargets []SignatureTarget
	var labels []hashedLabel
	for _, target := range hashed {
		if n := len(labels); n == 0 || labels[n-1] != target.hashedLabel {
			labels = append(labels, target.hashedLabel)
		}
		if n := len(signatureTargets); n > 0 && signatureTargets[n-1].ID == target.id {
			signatureTargets[n-1].Distance = min(signatureTargets[n-1].Distance, target.distance)
			continue
		}
		signatureTargets = append(signatureTargets, SignatureTarget{ID: target.id, Distance: target.distance})
	}
	return signatureTargets, labels
}
