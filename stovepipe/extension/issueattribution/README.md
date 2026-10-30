# Issue project attribution

Issue attribution answers which projects an individual validation issue affects. It is separate from project status resolution, which reports whether a project is green or broken.

## Inputs and results

A queue-bound resolver accepts a source build ID and a batch of issue openings. Each opening is identified by task, unit, and first-seen build. The source build may be newer than the opening build. Implementations resolve the build's metadata and relevant projects through their injected dependencies; callers do not supply dependency graphs or project mappings.

The result identifies the projects successfully assessed for the entire batch and the affected projects for each requested issue. Affected projects must be within that assessed scope. An implementation using dependency analysis includes only projects both within the build's relevant validation scope and affected by the failed unit. Task-wide issues have no project associations.

For a nonempty assessed scope, a successful result must contain exactly one entry for every requested issue opening, including issues that affect none of the assessed projects. Input openings must be distinct by task and unit; output identities must match them, including the first-seen build. Project lists contain distinct, nonempty IDs.

## Absence and reassessment

An issue's empty project list means it affects none of the assessed projects. Existing associations within that scope can be removed; associations outside the scope have not been reassessed and must be preserved. Changing project associations does not close or reopen the underlying issue.

An empty assessed scope means no attribution was performed and must have no issue results. It cannot add or remove associations. The noop implementation returns this result, so leaving attribution unconfigured does not clear existing associations.

## Failure and concurrency

Resolvers must support concurrent calls, leave caller-owned inputs unchanged, and not consume validation evidence. A dependency failure or inability to complete the requested assessment is an error, not a successful empty attribution. Results returned alongside an error must not be used.

A source build does not necessarily freeze external project configuration. Repeated calls can produce different successful results; callers must retain a successful assessment before using it to update associations.

The factory contract resolves a resolver for one queue. Concrete backend selection and queue routing belong to service wiring. This package does not choose when attribution runs or change issue detection or project status recording.
