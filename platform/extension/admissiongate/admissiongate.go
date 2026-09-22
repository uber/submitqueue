// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package admissiongate defines the shared extension contract for deciding
// whether a domain entity may cross a logical pipeline boundary.
package admissiongate

//go:generate mockgen -source=admissiongate.go -destination=mock/admissiongate_mock.go -package=mock

import "context"

// Decision is the expected outcome of evaluating an admission gate.
type Decision string

const (
	// DecisionUnknown is the invalid zero value.
	DecisionUnknown Decision = ""
	// DecisionAdmitted means current policy permits the controller to continue
	// admitting the candidate.
	DecisionAdmitted Decision = "admitted"
	// DecisionDeferred means policy currently prevents admission.
	DecisionDeferred Decision = "deferred"
)

// Blocker identifies a policy that currently prevents admission.
type Blocker string

// Result describes an expected admission outcome.
type Result struct {
	// Decision is whether the candidate was admitted or deferred.
	Decision Decision
	// BlockedBy contains stable policy identifiers when Decision is deferred.
	BlockedBy []Blocker
}

// Gate decides whether candidates may cross one queue-scoped pipeline
// boundary. T is the owning domain's entity at that pipeline stage.
// Implementations resolve the policies and facts they need from the
// candidate's identity.
type Gate[T any] interface {
	// TryAdmit evaluates current policy. A policy denial returns
	// DecisionDeferred rather than an error.
	TryAdmit(ctx context.Context, candidate T) (Result, error)
}

// Config identifies the queue for which a Gate is resolved.
type Config struct {
	// QueueName identifies the queue the resolved Gate serves.
	QueueName string
}

// Gates resolves the gate for a queue and domain entity type. A controller
// receives the resolver for the pipeline boundary it owns; concrete queue
// routing belongs in service wiring rather than an extension implementation
// package.
type Gates[T any] interface {
	// For returns the Gate selected for config.
	For(config Config) (Gate[T], error)
}
