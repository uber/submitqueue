# Queue build-slot ownership rollout

Fresh databases use `schema/queue.sql`. Existing databases need the nullable `in_flight_request_ids JSON DEFAULT NULL` column on `queue` before these binaries start. The Queue store reads and writes this column with the rest of the versioned snapshot.

1. Pause ingestion and drain all accepted and processing Requests, pending pipeline messages, and DLQ reconciliation with the old workers. Verify every Queue has `in_flight_count = 0`.
2. Stop every old writer. Append `in_flight_request_ids JSON DEFAULT NULL` as the final column of `queue`. Always append new columns so existing column positions remain unchanged. Existing drained rows need no backfill: SQL NULL represents an empty owner set.
3. Deploy all Queue readers and writers together, then resume ingestion. Do not mix old and new writers: an old writer cannot preserve slot ownership or the atomic baseline-and-release invariant.

An old nonzero counter cannot safely be converted into owner identities from the count alone. Admission and completion reject mismatched counts, empty owner ids, and duplicate owners rather than silently over-admitting. Resolve an undrained queue before deployment instead of resetting its counter while builds remain live. Rollback likewise requires draining and stopping the new workers first.
