CREATE TABLE verified_chunk_sources (
    source_id TEXT PRIMARY KEY,
    template_name TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    index_digest TEXT NOT NULL,
    subvolume_uuid TEXT NOT NULL,
    tree_id INTEGER NOT NULL,
    subvolume_generation INTEGER NOT NULL,
    file_device INTEGER NOT NULL,
    file_inode INTEGER NOT NULL,
    logical_bytes INTEGER NOT NULL CHECK (logical_bytes > 0),
    file_ctime_seconds INTEGER NOT NULL,
    file_ctime_nanos INTEGER NOT NULL,
    state TEXT NOT NULL,
    UNIQUE (template_name, content_hash)
);

CREATE TABLE verified_chunk_extents (
    source_id TEXT NOT NULL REFERENCES verified_chunk_sources(source_id),
    chunk_digest TEXT NOT NULL,
    raw_length INTEGER NOT NULL CHECK (raw_length > 0),
    source_offset INTEGER NOT NULL CHECK (source_offset >= 0),
    PRIMARY KEY (source_id, chunk_digest, raw_length)
);
CREATE INDEX verified_chunk_extents_lookup
    ON verified_chunk_extents (chunk_digest, raw_length, source_id);

-- One lease covers all chunks read from a source during an assembly.
-- Startup clears these only after exclusive service ownership is acquired.
CREATE TABLE verified_chunk_leases (
    lease_id TEXT NOT NULL,
    source_id TEXT NOT NULL REFERENCES verified_chunk_sources(source_id),
    PRIMARY KEY (lease_id, source_id)
);
CREATE INDEX verified_chunk_leases_source ON verified_chunk_leases (source_id);

CREATE TABLE chunk_template_deletions (template_name TEXT PRIMARY KEY);
