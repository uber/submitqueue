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

package entity

// GetProjectStatusByURIRequest selects the authoritative validation request for a commit.
type GetProjectStatusByURIRequest struct {
	// Queue is the exact queue containing the request.
	Queue string
	// ChangeURI is the exact commit URI whose request is selected.
	ChangeURI string
	// Project is the exact project selector when HasProject is true.
	Project string
	// HasProject distinguishes an omitted project from an explicitly empty project.
	HasProject bool
	// PageSize is the maximum number of projects to return. Zero selects the server default.
	PageSize int32
	// PageToken is an opaque continuation token from a previous result.
	PageToken string
}

// GetProjectStatusByURIResult contains the authoritative request's current validation projection.
type GetProjectStatusByURIResult struct {
	// Request is the authoritative request selected by queue and commit URI.
	Request Request
	// RepositoryValidationFact is the whole-repository result when HasRepositoryValidationFact is true.
	RepositoryValidationFact ValidationFact
	// HasRepositoryValidationFact distinguishes a missing fact from a recorded green result.
	HasRepositoryValidationFact bool
}
