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

// Package admissiongate defines Stovepipe's extension contract for deciding
// whether a request may cross a logical pipeline admission point.
package admissiongate

//go:generate mockgen -source=admissiongate.go -destination=mock/admissiongate_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// Point identifies a logical pipeline boundary guarded by an admission gate.
type Point string

const (
	// PointUnknown is the invalid zero value.
	PointUnknown Point = ""
	// PointBuild guards admission of a request toward the build stage.
	PointBuild Point = "build"
)

// Decision is the expected outcome of evaluating an admission gate.
type Decision string

const (
	// DecisionUnknown is the invalid zero value.
	DecisionUnknown Decision = ""
	// DecisionAdmitted means the request's admission is durably reserved.
	DecisionAdmitted Decision = "admitted"
	// DecisionDeferred means policy currently prevents admission.
	DecisionDeferred Decision = "deferred"
)

// Result describes an expected admission outcome.
type Result struct {
	// Decision is whether the request was admitted or deferred.
	Decision Decision
	// BlockedBy contains stable policy identifiers when Decision is deferred.
	BlockedBy []string
}

// Gate decides whether requests may cross one queue-scoped admission point.
// Implementations resolve the durable facts they need from the request's
// identity and must make repeated calls for the same request idempotent.
type Gate interface {
	// TryAdmit evaluates current policy and durably reserves an allowed
	// admission before returning DecisionAdmitted. A policy denial returns
	// DecisionDeferred rather than an error.
	TryAdmit(ctx context.Context, request entity.Request) (Result, error)
}

// Gates resolves the gate for a request and logical admission point. Concrete
// routing belongs in service wiring rather than an extension implementation
// package.
type Gates interface {
	// For returns the Gate selected for point and request.
	For(point Point, request entity.Request) (Gate, error)
}
