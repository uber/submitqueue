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

package errs

import (
	"errors"

	"github.com/uber/submitqueue/platform/base/failure"
)

// attributedError carries attribution — what a failure is about — alongside an
// error, without saying anything about its severity or retryability. Those
// remain the classifiers' business, and this wrapper is deliberately invisible
// to them: it is neither a userError nor an infraError, so the classifier
// processor walks straight through it to the cause underneath.
type attributedError struct {
	// cause is the underlying error.
	cause error
	// subjects are the entities this failure is about.
	subjects []failure.Subject
	// detail is free-form structured context.
	detail map[string]any
}

// Attribute returns err carrying the entities it is about, so a consumer
// downstream can act on the right thing instead of guessing from the message.
//
// Use it where the code knows something the error value cannot express — most
// often that a failure inside a job covering many records is about one
// particular record, or about none of them individually. It does not change
// whether the error is retryable; a classifier still decides that from the
// cause.
//
// A nil error is returned unchanged, so it is safe to apply unconditionally.
func Attribute(err error, subjects ...failure.Subject) error {
	if err == nil {
		return nil
	}
	return &attributedError{cause: err, subjects: subjects}
}

// Detail returns err carrying free-form structured context. It composes with
// Attribute in either order; see Attribution for how several layers combine.
//
// A nil error is returned unchanged.
func Detail(err error, detail map[string]any) error {
	if err == nil {
		return nil
	}
	return &attributedError{cause: err, detail: detail}
}

// Error returns the error message.
func (e *attributedError) Error() string {
	return e.cause.Error()
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *attributedError) Unwrap() error {
	return e.cause
}

// Attribution reads back everything Attribute and Detail put on err, as the
// failure a consumer should record.
//
// Message is always the error's own text, so the result is usable whether or
// not anything was attributed; an unattributed error simply yields a failure
// with no subjects and no detail. Where several layers of the chain carry
// attribution, subjects accumulate outermost-first and deduplicate, and detail
// keys are resolved outermost-wins — the outer layer is the later writer and
// has the wider view.
//
// A nil error yields the zero Failure.
func Attribution(err error) failure.Failure {
	if err == nil {
		return failure.Failure{}
	}

	result := failure.Failure{Message: err.Error()}
	seen := make(map[failure.Subject]bool)

	for node := err; node != nil; node = errors.Unwrap(node) {
		attributed, ok := node.(*attributedError)
		if !ok {
			continue
		}

		for _, s := range attributed.subjects {
			if seen[s] {
				continue
			}
			seen[s] = true
			result.Subjects = append(result.Subjects, s)
		}

		for k, v := range attributed.detail {
			if _, taken := result.Detail[k]; taken {
				continue
			}
			if result.Detail == nil {
				result.Detail = make(map[string]any)
			}
			result.Detail[k] = v
		}
	}

	return result
}
