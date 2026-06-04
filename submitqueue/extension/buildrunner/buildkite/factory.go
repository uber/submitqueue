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

package buildkite

import (
	"fmt"
	"net/http"

	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// Factory implements buildrunner.Factory. It holds credentials shared across
// all queues and resolves the per-queue pipeline slug at For time.
//
// Typical wiring in an orchestrator server:
//
//	buildkite.Factory{
//	    APIToken: os.Getenv("BUILDKITE_API_TOKEN"),
//	    OrgSlug:  "my-org",
//	    Branch:   "main",
//	    Pipelines: map[string]string{
//	        "my-queue": "my-pipeline-slug",
//	    },
//	}
type Factory struct {
	// APIToken is the Buildkite personal or agent API token. Required.
	APIToken string

	// OrgSlug is the Buildkite organisation slug. Required.
	OrgSlug string

	// Branch is the target branch builds run against (e.g. "main"). Required.
	Branch string

	// Pipelines maps SQ queue names to Buildkite pipeline slugs.
	// For returns an error for any queue not present in this map.
	Pipelines map[string]string

	// HTTPClient overrides the HTTP client for all runners this factory
	// produces. If nil, http.DefaultClient is used. Intended for testing.
	HTTPClient *http.Client

	// BaseURL overrides the Buildkite API base URL for all produced runners.
	// Intended for testing.
	BaseURL string
}

var _ buildrunner.Factory = Factory{}

// For returns a BuildRunner bound to the Buildkite pipeline configured for
// cfg.QueueName. Returns an error if no pipeline is configured for the queue.
func (f Factory) For(cfg buildrunner.Config) (buildrunner.BuildRunner, error) {
	pipeline, ok := f.Pipelines[cfg.QueueName]
	if !ok {
		return nil, fmt.Errorf("buildkite: no pipeline configured for queue %q", cfg.QueueName)
	}
	return New(Config{
		APIToken:     f.APIToken,
		OrgSlug:      f.OrgSlug,
		PipelineSlug: pipeline,
		QueueName:    cfg.QueueName,
		Branch:       f.Branch,
		HTTPClient:   f.HTTPClient,
		BaseURL:      f.BaseURL,
	})
}
