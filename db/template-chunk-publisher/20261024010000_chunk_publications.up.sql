-- Upload receipts become reusable only after the object store acknowledges
-- the complete immutable object. A namespace includes its endpoint and prefix.
CREATE TABLE chunk_uploaded_objects (
    namespace TEXT NOT NULL,
    object_digest TEXT NOT NULL,
    compressed_bytes INTEGER NOT NULL CHECK (compressed_bytes > 0),
    version_id TEXT,
    PRIMARY KEY (namespace, object_digest)
);

CREATE TABLE chunk_uploaded_locations (
    namespace TEXT NOT NULL,
    chunk_digest TEXT NOT NULL,
    raw_length INTEGER NOT NULL CHECK (raw_length > 0),
    object_digest TEXT NOT NULL,
    compressed_offset INTEGER NOT NULL CHECK (compressed_offset >= 0),
    compressed_length INTEGER NOT NULL CHECK (compressed_length > 0),
    PRIMARY KEY (namespace, chunk_digest, raw_length),
    FOREIGN KEY (namespace, object_digest)
        REFERENCES chunk_uploaded_objects (namespace, object_digest)
);

CREATE TABLE chunk_publications (
    namespace TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    index_digest TEXT NOT NULL,
    rootfs_digest TEXT NOT NULL,
    image_blake3 TEXT NOT NULL,
    manifest_bytes BLOB NOT NULL,
    phase TEXT NOT NULL,
    PRIMARY KEY (namespace, manifest_digest)
);
