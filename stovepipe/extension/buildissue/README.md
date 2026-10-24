# Build issue detection

`Detector` discovers confirmed validation issues from a build. It returns failure observations, not a complete view of build state.

Issue values are defined in the [domain model](../../entity/build_issue.go). A validation task is a logical validation operation. A validation unit is an identifiable piece of work within a task.

Within a validation queue, TaskID and UnitID together identify the validation condition. Repeated observations of the same condition use the same pair across builds. UnitID is empty for a task-wide issue.

Each detection returns issues discoverable at that time and may repeat or omit earlier observations. Reported issues remain part of that build's accumulated results; later omissions do not retract them. Detection does not mark issues as handled.

A successful detection after build completion must discover every remaining reportable issue within the queue's relevant validation scope. Together with earlier persisted observations, this makes the accumulated set complete for final reconciliation. An empty final response means no additional issues, not removal of earlier observations. Incomplete final detection must return an error rather than report success. Implementations decide when a failure qualifies as a reportable issue.

Detectors must support concurrent calls. Dependency failures return plain errors; issue results returned with an error must not be used.

`DetectorFactory` resolves the detector for a validation queue.
