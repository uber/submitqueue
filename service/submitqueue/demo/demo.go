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

// Package demo generates and submits workloads with injected change sources
// and gateway transports. Deployment configuration and credentials belong to
// the calling command.
package demo

import (
	"context"

	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

// Options controls a workload, independently of its provider and transport.
type Options struct {
	// Count is the number of changes to generate.
	Count int
	// Folders is the shard count; zero selects a reproducible count for the run.
	Folders int
	// Files is the minimum number of files per generated change.
	Files int
	// Concurrency bounds independent creation, readiness and submission.
	Concurrency int
	// Stacked submits an ordered chain as one request.
	Stacked bool
	// Burst prepares all independent changes before submitting any.
	Burst bool
	// Prefix is the generated branch namespace.
	Prefix string
	// RunID identifies a workload; empty generates a unique identifier.
	RunID string
	// Land enables submission; false only creates changes.
	Land bool
	// Watch waits for terminal request histories when Land is true.
	Watch bool
	// Queue is the exact queue accepting the requests.
	Queue string
	// Strategy is the wire strategy used for every submitted URI.
	Strategy mergestrategypb.Strategy
}

// File is one generated file and its source-specific commit description.
type File struct {
	// Path is relative to the repository root.
	Path string
	// Body is the complete file content.
	Body string
	// Message is the description when a provider commits files individually.
	Message string
}

// Change is a head-pinned revision, or an attempted branch returned on failure.
type Change struct {
	// Branch is the source branch.
	Branch string
	// HeadSHA is the full revision identifier, empty for an unconfirmed artifact.
	HeadSHA string
	// URI is the head-pinned identifier, empty for an unconfirmed artifact.
	URI string
	// Label identifies the change in terminal output.
	Label string
	// URL is an optional browser link.
	URL string
}

// ChangeSpec describes a new change. Sources own their configured base.
type ChangeSpec struct {
	// Branch is the branch to create.
	Branch string
	// Title describes the change.
	Title string
	// Files contains generated contents.
	Files []File
	// Parent identifies the preceding change when HasParent is true.
	Parent Change
	// HasParent distinguishes a stack link from a change on the source's base.
	HasParent bool
	// Note reports progress and may be called concurrently.
	Note func(string, ...any)
}

// Source creates independent changes concurrently and stack links sequentially.
type Source interface {
	Open(context.Context, ChangeSpec) (Change, error)
}

// Gateway is the transport-independent submission and history contract.
type Gateway interface {
	Land(context.Context, string, []string, mergestrategypb.Strategy) (string, error)
	History(context.Context, string, string) ([]*pb.HistoryEvent, error)
}

// Readiness waits for an exact revision to be eligible for submission.
type Readiness interface {
	Wait(context.Context, Change) error
}

// Dependencies supplies provider, transport and readiness integration.
type Dependencies struct {
	// Source is required for generation, unused by RunExisting.
	Source Source
	// Gateway is required when Land is enabled.
	Gateway Gateway
	// Readiness is optional; nil means no preparation is required.
	Readiness Readiness
}

// RequestResult records one accepted request and its observed history.
type RequestResult struct {
	// Changes is the ordered set submitted in this request.
	Changes []Change
	// ID is opaque and only meaningful together with the run's queue.
	ID string
	// Status is the last observed server status, not a local monitoring verdict.
	Status string
	// History is the last successful history read.
	History []*pb.HistoryEvent
}

// RunResult preserves known artifacts, including after a partially failed run.
type RunResult struct {
	// Changes contains known artifacts in input order, including unconfirmed branches.
	Changes []Change
	// Requests contains accepted requests in workload order.
	Requests []RequestResult
}
