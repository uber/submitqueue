# Schema upgrades

For an existing database, apply `migrations/add_project_type.sql` once before deploying
application code that reads or writes project types. Existing facts retain an empty
project type (displayed as “Unknown”); their immutable recorded results are preserved.
Fresh databases use the definitions in `schema/` and do not run this migration.
