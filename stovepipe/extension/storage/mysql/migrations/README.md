# Project-result completion rollout

Fresh databases use `../schema/request_summary.sql`. Existing databases must add the defaulted `project_results_completed_at_ms` column with `project_results_completion.sql` before running the new application version. Apply that schema migration once; keep the column when reverting the application.

The backfill copies only the retained `event/project_facts_recorded/all` timestamp into existing request summaries whose completion timestamp is zero. It preserves lifecycle state and timestamps, leaves known completion unchanged, and increments the summary version so a concurrent projection must reload rather than overwrite completion. Requests without a completion event remain incomplete, including historical empty result sets.

Run the backfill before relying on historical completion, and run it again after old application instances drain to cover events they recorded during rollout. Use a MySQL session connected to the Stovepipe storage database and select a queue and bounded request-ID range:

```sql
SET @queue = 'example/main';
SET @first_request_id = '1';
SET @last_request_id = '1';
SOURCE stovepipe/extension/storage/mysql/migrations/backfill_project_results_completion.sql;
```

Paths are relative to the repository root. Range bounds are inclusive and use the `VARCHAR` primary-key ordering, not numeric request-ID order. Repeat for each queue and range requiring historical coverage. Repeating a completed batch updates no rows. The scripts are packaged for integration tests, which exercise the schema upgrade, backfill idempotency, queue and range isolation, version changes, and both public status APIs.
