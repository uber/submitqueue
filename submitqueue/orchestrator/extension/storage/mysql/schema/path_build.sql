-- Resolves one attempt of one speculation path to the build started for it.
-- The runner chooses the build ID, so a caller holding a path cannot derive it;
-- this is the reverse lookup, made a keyed record rather than an index because
-- the store contract has no lookup by attribute.
--
-- A row is write-once: inserted only once the runner has named the build, and
-- never updated. The primary key decides concurrent dispatches for the same
-- attempt — the first insert wins, the duplicate is refused. queue leads the PK
-- so the table is shardable by queue.
CREATE TABLE IF NOT EXISTS path_build (
    queue VARCHAR(255) NOT NULL,
    path_id VARCHAR(255) NOT NULL,
    attempt INT NOT NULL,
    build_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (queue, path_id, attempt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
