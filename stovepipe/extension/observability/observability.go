// Package observability defines Stovepipe's metrics reporting boundary.
package observability

import "context"

// Reporter emits best-effort observability data for a queue.
type Reporter interface {
	Report(context.Context, string)
}
