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

package validator

//go:generate mockgen -source=validator.go -destination=mock/validator_mock.go -package=mock

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// ErrRejected signals that the request must not land: a policy verdict, not a
// failure. Validators wrap it into the error they return for a rejection, and
// the validate stage terminates the request with that error's message as the
// reason. Any other error is a failure to validate, classified and retried or
// dead-lettered like any other stage failure.
var ErrRejected = errors.New("rejected")

// Validator runs custom validation checks against a request.
// Implementations are provided by integrators for company-specific checks
// (e.g. org-policy enforcement, repo-specific rules). The built-in validate
// controller invokes this after fetching change metadata and before claiming
// URIs in the change store.
type Validator interface {
	// Validate runs custom validation on the request. It returns nil if the
	// request passes, an error wrapping ErrRejected if it must not land, and any
	// other error if it could not be validated.
	Validate(ctx context.Context, request entity.Request) error
}

// Config carries the routing identity handed to a Factory.
type Config struct {
	// QueueName identifies the queue this request belongs to.
	QueueName string
}

// Factory builds the Validator for a given config. Implementations are provided
// by integrators and inject whatever they need at construction.
type Factory interface {
	// For returns the Validator for the given config. Returning nil indicates
	// no custom validation is needed for this config.
	For(cfg Config) (Validator, error)
}
