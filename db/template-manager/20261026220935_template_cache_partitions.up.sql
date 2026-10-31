-- One rootfs digest can be cached once per environment it is bound to. Builds,
-- their components and base snapshots, and tag pins gain the build's cache
-- partition in their keys. The empty string is the legacy partition (unbound
-- builds and builds bound to the initial environment), so every existing row
-- keeps its identity and its on-disk directory. SQLite cannot change a primary
-- key in place, so each table is rebuilt; children are dropped before their
-- parent and renamed after it so foreign-key references follow the rename.

CREATE TABLE template_builds_partitioned (
    name                  TEXT NOT NULL,
    content_hash          TEXT NOT NULL,
    cache_partition       TEXT NOT NULL DEFAULT '',
    build_date            TEXT NOT NULL,
    manifest_json         TEXT NOT NULL,
    runtime               TEXT NOT NULL,
    arch                  TEXT NOT NULL,
    artifact_size_bytes   INTEGER NOT NULL DEFAULT 0,
    state                 TEXT NOT NULL DEFAULT 'ready',
    owner_type            TEXT NOT NULL DEFAULT '',
    owner_id              TEXT NOT NULL DEFAULT '',
    created_at            TEXT NOT NULL,
    last_used_at TEXT NOT NULL DEFAULT '0',
    runtime_version TEXT,
    guest_os TEXT,
    state_format_name TEXT,
    state_format_version TEXT,
    execution_metadata_json TEXT,
    PRIMARY KEY (name, content_hash, cache_partition)
);

INSERT INTO template_builds_partitioned (
    name, content_hash, cache_partition, build_date, manifest_json, runtime, arch,
    artifact_size_bytes, state, owner_type, owner_id, created_at, last_used_at,
    runtime_version, guest_os, state_format_name, state_format_version,
    execution_metadata_json
)
SELECT
    name, content_hash, '', build_date, manifest_json, runtime, arch,
    artifact_size_bytes, state, owner_type, owner_id, created_at, last_used_at,
    runtime_version, guest_os, state_format_name, state_format_version,
    execution_metadata_json
FROM template_builds;

CREATE TABLE template_build_components_partitioned (
    name            TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    cache_partition TEXT NOT NULL DEFAULT '',
    kind            TEXT NOT NULL,
    role_key        TEXT NOT NULL DEFAULT '',
    version         TEXT,
    digest          TEXT,
    format          TEXT,
    PRIMARY KEY (name, content_hash, cache_partition, kind, role_key),
    FOREIGN KEY (name, content_hash, cache_partition)
        REFERENCES template_builds_partitioned(name, content_hash, cache_partition)
        ON DELETE CASCADE
);

INSERT INTO template_build_components_partitioned (
    name, content_hash, cache_partition, kind, role_key, version, digest, format
)
SELECT c.name, c.content_hash, '', c.kind, c.role_key, c.version, c.digest, c.format
FROM template_build_components c
-- Template-manager's runtime connection doesn't enforce foreign keys, so
-- eviction and recovery leave child rows of deleted builds behind. The
-- migration runs with enforcement on; carry only rows whose build survives.
WHERE EXISTS (
    SELECT 1 FROM template_builds_partitioned b
    WHERE b.name = c.name AND b.content_hash = c.content_hash AND b.cache_partition = ''
);

CREATE TABLE template_snapshots_partitioned (
    name            TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    cache_partition TEXT NOT NULL DEFAULT '',
    vcpu            INTEGER NOT NULL,
    memory_mib      INTEGER NOT NULL,
    snapshot_hash   TEXT NOT NULL,
    built_at        TEXT NOT NULL,
    PRIMARY KEY (name, content_hash, cache_partition, vcpu, memory_mib),
    FOREIGN KEY (name, content_hash, cache_partition)
        REFERENCES template_builds_partitioned(name, content_hash, cache_partition)
);

INSERT INTO template_snapshots_partitioned (
    name, content_hash, cache_partition, vcpu, memory_mib, snapshot_hash, built_at
)
SELECT s.name, s.content_hash, '', s.vcpu, s.memory_mib, s.snapshot_hash, s.built_at
FROM template_snapshots s
WHERE EXISTS (
    SELECT 1 FROM template_builds_partitioned b
    WHERE b.name = s.name AND b.content_hash = s.content_hash AND b.cache_partition = ''
);

CREATE TABLE template_pinned_tags_partitioned (
    name            TEXT NOT NULL,
    tag             TEXT NOT NULL,
    cache_partition TEXT NOT NULL DEFAULT '',
    content_hash    TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (name, tag, cache_partition)
);

INSERT INTO template_pinned_tags_partitioned (
    name, tag, cache_partition, content_hash, updated_at
)
SELECT name, tag, '', content_hash, updated_at
FROM template_pinned_tags;

DROP TABLE template_snapshots;
DROP TABLE template_build_components;
DROP TABLE template_builds;
DROP TABLE template_pinned_tags;

ALTER TABLE template_builds_partitioned RENAME TO template_builds;
ALTER TABLE template_build_components_partitioned RENAME TO template_build_components;
ALTER TABLE template_snapshots_partitioned RENAME TO template_snapshots;
ALTER TABLE template_pinned_tags_partitioned RENAME TO template_pinned_tags;

CREATE INDEX idx_template_build_components_lookup
    ON template_build_components(kind, version, role_key);

CREATE INDEX idx_template_builds_execution
    ON template_builds(runtime, runtime_version, guest_os, arch);
