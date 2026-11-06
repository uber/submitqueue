# Schema upgrades

Before deploying application code that reads or writes project types, existing databases
must add `project_type` as the last column in `validation_fact`, with type `VARCHAR(255)`,
`NOT NULL`, and an empty-string default. Existing facts retain an unknown project type;
their immutable recorded results are preserved. Fresh databases use `schema/`.

Before deploying code that reads or writes issue metadata, append a nullable `JSON` column named `metadata` as the last column in `issue_state`. New columns must be appended after all existing columns; do not reorder existing columns. Existing rows may retain SQL NULL; fresh databases use `schema/`. Metadata is namespaced extension state and uses the issue row's existing optimistic version. Updates replace the entire snapshot: preserve unrelated namespaces, copy maps before mutation, and reload after a version conflict. Lifecycle reconciliation preserves metadata when an issue closes or reopens.
