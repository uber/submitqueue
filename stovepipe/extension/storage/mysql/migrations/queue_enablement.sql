-- Apply once to existing installations before deploying queue enablement storage.
-- Fresh installations use schema/queue.sql instead.
ALTER TABLE queue
    ADD COLUMN enablement VARCHAR(32) NOT NULL DEFAULT '',
    ADD COLUMN enablement_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN effective_from_commit_uri VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN enablement_changed_at_ms BIGINT NOT NULL DEFAULT 0;
