# Build issue detection

`Detector` discovers confirmed validation issues from a build. It returns build issues, not a complete view of build state.

Issue values are defined in the [domain model](../../entity/build_issue.go). A validation task is a logical validation operation. A validation unit is an identifiable piece of work within a task.

Within a validation queue, TaskID and UnitID together identify the validation condition. Repeated reports of the same condition use the same pair across builds. UnitID is empty for a task-wide issue.

Nonterminal detection returns issues discoverable at that time and may repeat or omit previously reported issues. Reported issues remain part of that build's accumulated results; later omissions do not retract them. Detection does not mark issues as handled.

A successful detection after build completion must return the complete cumulative issue set for the build within the queue's relevant validation scope, including issues returned by earlier calls. The result must be complete independently of whether earlier calls finished persisting their results. An empty terminal result is valid only if the build has no reportable issues. If the complete set cannot be determined, detection must return an error rather than report success. Implementations decide when a failure qualifies as a reportable issue.

Detectors must support concurrent calls. Dependency failures return plain errors; issue results returned with an error must not be used.

The detection result separately reports whether all required validation work produced usable outcomes. Incomplete or unknown coverage does not prevent retaining confirmed issues or completing terminal issue analysis, but it cannot establish that an existing issue was resolved merely because it was not reported. A no-op detector does not establish coverage.

`DetectorFactory` resolves the detector for a validation queue.
