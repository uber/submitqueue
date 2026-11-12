# Schema upgrades

Before deploying application code that reads or writes project types, existing databases
must add `project_type` as the last column in `validation_fact`, with type `VARCHAR(255)`,
`NOT NULL`, and an empty-string default. Existing facts retain an unknown project type;
their immutable recorded results are preserved. Fresh databases use `schema/`.

Before deploying code that reads or writes issue metadata, append a nullable `JSON` column named `metadata` as the last column in `issue_state`. New columns must be appended after all existing columns; do not reorder existing columns. Existing rows may retain SQL NULL; fresh databases use `schema/`. Metadata is namespaced extension state and uses the issue row's existing optimistic version. Updates replace the entire snapshot: preserve unrelated namespaces, copy maps before mutation, and reload after a version conflict. Lifecycle reconciliation preserves metadata when an issue closes or reopens.

Before deploying request metadata readers and writers, append a nullable `JSON` column named `metadata` to `request`. Existing rows may retain SQL NULL; fresh databases use `schema/request.sql`. `CREATE TABLE IF NOT EXISTS` does not upgrade existing tables. Updates replace the complete mutable request snapshot, including metadata, under the existing request version guard. Preserve unrelated fields and namespaces, copy maps before mutation, and reload after a version conflict. Metadata-only updates advance the request version; storage does not publish request history.
