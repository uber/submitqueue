# Issue project attribution

Issue attribution answers which projects each confirmed build issue affects. The project names use the same identity as project status results.

## Inputs and results

A queue-bound attributer accepts the validation request. The request supplies the commit, baseline, build strategy, and build identity. Implementations load confirmed build issues, dependency graphs, and project configuration through injected dependencies; callers do not supply issue lists or graph data. For terminal validation, the issues belong to the request's terminal build and attribution must wait until terminal build analysis has finished recording them.

Each result identifies a unit-level build issue by task and unit and lists its affected projects. Projects must be both covered by that validation and affected by the issue. A dependency-based implementation follows the graph at the validated commit.

A completed assessment returns all results in one call, with one entry per distinct build issue, including an empty project list when none apply. The noop implementation returns no results when attribution is unconfigured.

## Errors and concurrency

Attribution is all-or-nothing: any task-wide failure, incomplete validation evidence, or dependency failure returns an error for the entire validation. Callers must discard results returned alongside an error.

Implementations must support concurrent calls, leave inputs unchanged, and not consume validation evidence. Recording can repeat attribution before completion is retained, so retries for the same request must return the same associations.
