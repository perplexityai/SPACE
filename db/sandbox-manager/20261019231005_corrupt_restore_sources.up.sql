CREATE TABLE corrupt_restore_sources (
    sandbox_id TEXT NOT NULL,
    storage_uri TEXT NOT NULL,
    PRIMARY KEY (sandbox_id, storage_uri)
);
