-- build holds one CI build triggered for a request's commit. id is the runner-assigned build id
-- minted at Trigger (e.g. a Buildkite build number), opaque and never parsed or derived.
CREATE TABLE IF NOT EXISTS build (
    id          VARCHAR(255) NOT NULL,
    request_id  VARCHAR(255) NOT NULL,
    uri         VARCHAR(255) NOT NULL,
    base_uri    VARCHAR(255) NOT NULL DEFAULT '',
    status      VARCHAR(64)  NOT NULL,
    version     INT          NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
