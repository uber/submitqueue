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

package http

import "fmt"

// StatusError reports a response whose status code the caller rejected.
//
// SendRequest does not build this error itself: which codes count as success
// varies by API — a 404 may be a sentinel, a 422 may be a no-op — so the caller
// still decides. What the caller should not do is report the rejection with a
// plain fmt.Errorf. The status code is the only thing that says whether a retry
// has any chance of working, and a formatted string throws it away. Returning
// this type keeps the code in the error chain, where platform/errs/http can read
// it and classify the failure.
//
// Use it for the "this status is a failure" branch of a response check:
//
//	if status < 200 || status >= 300 {
//	    return phttp.NewStatusError(status, respBody)
//	}
type StatusError struct {
	// StatusCode is the HTTP status code from the response.
	StatusCode int
	// Body is the response body as read from the wire, or empty when the
	// caller had no body to attach.
	Body string
}

// NewStatusError returns a StatusError for the given code and response body.
// body may be nil.
func NewStatusError(statusCode int, body []byte) *StatusError {
	return &StatusError{StatusCode: statusCode, Body: string(body)}
}

// Error renders the status and, when present, the response body. Callers are
// expected to wrap it with the operation that failed, giving messages like
// "get build org/pipeline/builds/123: unexpected status 502: proxy forward
// failed".
func (e *StatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("unexpected status %d", e.StatusCode)
	}
	return fmt.Sprintf("unexpected status %d: %s", e.StatusCode, e.Body)
}
