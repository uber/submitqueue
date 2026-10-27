# Schema upgrades

Before deploying application code that reads or writes project types, existing databases
must add `project_type` as the last column in `validation_fact`, with type `VARCHAR(255)`,
`NOT NULL`, and an empty-string default. Existing facts retain an unknown project type;
their immutable recorded results are preserved. Fresh databases use `schema/`.
