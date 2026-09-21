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

// GetProjectStatusByURIRequest selects a validation request by queue and commit URI.
type GetProjectStatusByURIRequest struct {
	// Queue identifies the queue containing the validation request.
	Queue string
	// ChangeURI identifies the exact commit under validation.
	ChangeURI string
	// Projects limits results to the supplied consumer-defined project IDs.
	Projects []string
	// PageSize is the requested maximum number of requested projects to inspect.
	PageSize int32
	// PageToken is an opaque continuation token for full project-result pagination.
	PageToken string
}

// GetProjectStatusByURIResult is the current validation projection for one request.
type GetProjectStatusByURIResult struct {
	// RequestSummary is the authoritative validation projection selected by the lookup.
	RequestSummary RequestSummary
	// RepositoryValidationFact is the repository result when HasRepositoryValidationFact is true.
	RepositoryValidationFact ValidationFact
	// HasRepositoryValidationFact distinguishes a missing fact from a recorded green result.
	HasRepositoryValidationFact bool
	// ProjectResultsComplete reports whether the implementation has finished its project-result set.
	ProjectResultsComplete bool
	// ProjectValidationFacts contains recorded results for requested projects in request order.
	ProjectValidationFacts []ValidationFact
	// NextPageToken continues requested-project result pagination when another page exists.
	NextPageToken string
	// UpdatedAtMs is the newest durable lifecycle or result timestamp represented by the response.
	UpdatedAtMs int64
}
