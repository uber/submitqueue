// Package lastgreen reports the age of Stovepipe's last-known-green changes.
package lastgreen

import (
	"context"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/extension/observability"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type reporter struct {
	scope         tally.Scope
	stores        storage.Factory
	sourceControl sourcecontrol.Factory
}

var _ observability.Reporter = (*reporter)(nil)

// New creates a Reporter for a queue's last-known-green age.
func New(scope tally.Scope, stores storage.Factory, sourceControl sourcecontrol.Factory) observability.Reporter {
	return &reporter{
		scope:         scope.SubScope("last_green"),
		stores:        stores,
		sourceControl: sourceControl,
	}
}

// Report updates the queue's current last-known-green age gauge.
func (r *reporter) Report(ctx context.Context, queue string) {
	tags := map[string]string{"queue": queue}
	store, err := r.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		r.error(tags, "resolve_storage")
		return
	}
	queueRow, err := store.GetQueueStore().Get(ctx, queue)
	if err != nil {
		r.error(tags, "get_queue")
		return
	}
	if queueRow.LastGreenURI == "" {
		r.scope.Tagged(tags).Counter("age_missing").Inc(1)
		return
	}
	control, err := r.sourceControl.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		r.error(tags, "resolve_source_control")
		return
	}
	info, err := control.ChangeInfo(ctx, queueRow.LastGreenURI)
	if err != nil || info.CreatedAt.IsZero() {
		r.error(tags, "get_change_info")
		return
	}
	age := time.Since(info.CreatedAt)
	if age < 0 {
		r.error(tags, "future_change")
		return
	}
	r.scope.Tagged(tags).Gauge("age_seconds").Update(age.Seconds())
}

func (r *reporter) error(tags map[string]string, stage string) {
	tagsCopy := make(map[string]string, len(tags)+1)
	for key, value := range tags {
		tagsCopy[key] = value
	}
	tagsCopy["stage"] = stage
	r.scope.Tagged(tagsCopy).Counter("age_errors").Inc(1)
}
