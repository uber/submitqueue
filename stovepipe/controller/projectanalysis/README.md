# Project analysis

Project analysis consumes request IDs after terminal build analysis has retained
all final build issues. A missing terminal-analysis completion is an invalid
handoff, not a reason to poll.

The controller resolves project statuses and records their facts and lookup entries before attributing issues. It materializes both directions of each issue/project association. Each step retains its completion event after its writes; repository recording is queued only after both steps finish.

Retries adopt immutable facts and repair missing lookup entries. A completed
request skips provider calls and retries only the recording handoff. Empty
results still record completion. Cancelled requests bypass project analysis;
superseded requests require no work.

## Recovery

DLQ recovery leaves existing facts and association rows untouched. If completion
was already recorded, it retries only the repository-recording handoff. Otherwise
it records `project_analysis_abandoned` before handing off, without calling either
provider or marking attribution complete. History writes and handoff publication
are safe to retry.

Normal redeliveries also recognize abandonment and skip provider calls. An
already-running delivery may still finish immutable writes; abandonment does not
roll them back or make a partial association set complete.
