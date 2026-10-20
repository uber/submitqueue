# Build issue detection

`Detector` discovers confirmed validation issues from a build. It returns failure observations, not a complete view of build state.

Issue values are defined in the [domain model](../../entity/build_issue.go). A validation task is a logical validation operation. A validation unit is an identifiable piece of work within a task.

Within a validation queue, TaskID and UnitID together identify the validation condition. Repeated observations of the same condition use the same pair across builds. UnitID is empty for a task-wide issue.

Each detection returns issues discoverable at that time and may repeat earlier observations. Detection does not mark issues as handled. An issue's absence from a detection result does not mean it is resolved.

Detectors must support concurrent calls. Dependency failures return plain errors; issue results returned with an error must not be used.

`Factory` resolves the detector for a validation queue.
