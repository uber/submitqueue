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

// Package http provides an errs.Classifier for failures returned by HTTP
// clients: a rejected status code (platform/http.StatusError) and a transport
// failure (*url.Error, which is what http.Client.Do returns).
//
// Wire it into any service whose extensions call an HTTP API — build runners,
// CI gateways, webhook senders. Without it every status code looks the same to
// the pipeline: unclassified, and therefore non-retryable, so a 502 from a proxy
// dead-letters the message on its first attempt rather than being retried.
//
// Order matters when wiring this alongside platform/errs/mysql. The MySQL
// classifier treats any net.Error as retryable infra, and *url.Error satisfies
// net.Error, so it will claim HTTP transport failures if it runs first. List
// this classifier before it to keep those failures attributed to the dependency
// they came from:
//
//	errs.NewClassifierProcessor(
//	    genericerrs.Classifier,
//	    httperrs.Classifier,
//	    mysqlerrs.Classifier,
//	)
package http

import (
	"context"
	nethttp "net/http"
	"net/url"

	"github.com/uber/submitqueue/platform/errs"
	phttp "github.com/uber/submitqueue/platform/http"
)

// Classifier implements errs.Classifier for HTTP client failures. It recognises:
//
//   - *phttp.StatusError — dispatches on the status code. Codes that describe a
//     server-side or overload condition (500, 502, 503, 504, other unassigned
//     5xx, 429, 408) are retryable dependency errors. Codes that describe a
//     verdict on the request itself (4xx, 3xx, and the permanently broken 501
//     and 505) are non-retryable dependency errors.
//   - *url.Error — the wrapper http.Client.Do puts around connection resets, DNS
//     failures, TLS errors and timeouts. A retryable dependency error, except
//     for our own context cancellation (see Classify).
//
// Everything else returns errs.Unknown so the classifier-processor walk can keep
// looking down the unwrap chain.
//
// The classifier never returns errs.User. A 400 or 403 says the request was
// rejected, not that a person did something wrong; only the controller knows
// whether the request was built from user input. Controllers express that by
// wrapping with errs.NewUserError, which short-circuits pass 1 of the
// classifier-processor before this classifier is consulted.
//
// The classifier is stateless; this package-level singleton is the canonical
// handle. Pass it as one of the variadic classifiers to
// errs.NewClassifierProcessor; the resulting processor is what gets handed to
// consumer.New.
var Classifier errs.Classifier = classifier{}

type classifier struct{}

// Classify inspects a single node. Per the errs.Classifier contract, this must
// not call errors.Is / errors.As — the classifier-processor owns the chain walk.
func (classifier) Classify(err error) errs.Verdict {
	if se, ok := err.(*phttp.StatusError); ok {
		return classifyStatusCode(se.StatusCode)
	}

	if ue, ok := err.(*url.Error); ok {
		// A cancelled or expired context means we gave up, not that the remote
		// end misbehaved: the deadline belongs to this process. Report Unknown
		// so the walk continues into the cause, where the generic classifier
		// picks up context.Canceled as plain retryable infra. That keeps
		// shutdown noise out of the dependency-error metrics for this backend.
		// Reading the Err field is not a chain walk, and declining here leaves
		// the framework's walk free to reach that node.
		//
		// This covers the shape http.Client.Do produces for context failures. A
		// context error buried under further transport wrapping falls through to
		// the retryable dependency verdict below, which retries the same way and
		// differs only in the dependency tag.
		if ue.Err == context.Canceled || ue.Err == context.DeadlineExceeded {
			return errs.Unknown
		}
		// Anything else at this layer is a failure to complete an exchange with
		// the remote end — reset connection, refused dial, DNS, TLS, client
		// timeout. None of those say the request was invalid, so another attempt
		// is worth making.
		return errs.InfraDependencyRetryable
	}

	return errs.Unknown
}

// classifyStatusCode maps an HTTP status code to a Verdict.
//
// The split is whether the code describes the state of the server or a verdict
// on the request. Server-state codes change on their own, so the same request
// can succeed later. Request-verdict codes will not: replaying the request
// reproduces the answer, and the message should dead-letter for a human or a
// reconciler instead of retrying until it runs out of attempts.
func classifyStatusCode(code int) errs.Verdict {
	switch code {
	case nethttp.StatusRequestTimeout, // 408 — the server stopped waiting; sending it again is reasonable.
		nethttp.StatusTooManyRequests: // 429 — over a rate limit that resets with time.
		return errs.InfraDependencyRetryable

	case nethttp.StatusNotImplemented, // 501 — the route will not appear because we retried.
		nethttp.StatusHTTPVersionNotSupported: // 505 — a client/server mismatch to fix in config.
		return errs.InfraDependency
	}

	// Remaining 5xx: the server reported its own failure. Covers 500, 502, 503
	// and 504, the shapes a proxy or overloaded backend produces, plus any
	// unassigned or vendor-specific 5xx, which follow the same convention.
	if code >= nethttp.StatusInternalServerError {
		return errs.InfraDependencyRetryable
	}

	// 4xx other than the two above, 3xx the client was not configured to follow,
	// and anything else a caller chose to reject: a verdict on this request. Not
	// retryable.
	return errs.InfraDependency
}
