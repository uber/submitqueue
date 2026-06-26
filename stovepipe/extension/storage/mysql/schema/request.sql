-- request holds one validation of a queue at a particular commit. uri is the opaque,
-- VCS-agnostic commit locator; it may be empty until SourceControl resolution is wired in.
-- No timestamps: created/updated times are not part of the Request entity.
CREATE TABLE IF NOT EXISTS request (
    id      VARCHAR(255) NOT NULL,
    queue   VARCHAR(255) NOT NULL,
    uri     VARCHAR(255) NOT NULL,
    state   VARCHAR(64)  NOT NULL,
    version INT          NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
