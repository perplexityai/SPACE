-- Down migration of the retired 20261026220935_template_cache_partitions,
-- kept verbatim except for this header. The migration isn't bundled, so
-- `pplx-space-sqlite` runs this file itself when a database still records it
-- and holds no environment-partition build, tag pin or unfinished stamp job,
-- then removes its history row. The DELETE and the `WHERE cache_partition = ''`
-- filters then drop only finished environment stamp jobs and, as the up
-- migration did, child rows whose build is gone.

-- An older template-manager reads every stamp job as the legacy build's, so
-- the other partitions' jobs go with their rows.
-- CASE fixes the evaluation order: payloads of other kinds can be empty.
DELETE FROM jobs
WHERE kind = 'template_stamp'
  AND CASE
        WHEN json_valid(payload) THEN json_extract(payload, '$.partition_environment')
      END IS NOT NULL;

CREATE TABLE template_builds_legacy (
    name                  TEXT NOT NULL,
    content_hash          TEXT NOT NULL,
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
    PRIMARY KEY (name, content_hash)
);

INSERT INTO template_builds_legacy (
    name, content_hash, build_date, manifest_json, runtime, arch,
    artifact_size_bytes, state, owner_type, owner_id, created_at, last_used_at,
    runtime_version, guest_os, state_format_name, state_format_version,
    execution_metadata_json
)
SELECT
    name, content_hash, build_date, manifest_json, runtime, arch,
    artifact_size_bytes, state, owner_type, owner_id, created_at, last_used_at,
    runtime_version, guest_os, state_format_name, state_format_version,
    execution_metadata_json
FROM template_builds
WHERE cache_partition = '';

CREATE TABLE template_build_components_legacy (
    name          TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    kind          TEXT NOT NULL,
    role_key      TEXT NOT NULL DEFAULT '',
    version       TEXT,
    digest        TEXT,
    format        TEXT,
    PRIMARY KEY (name, content_hash, kind, role_key),
    FOREIGN KEY (name, content_hash)
        REFERENCES template_builds_legacy(name, content_hash) ON DELETE CASCADE
);

INSERT INTO template_build_components_legacy (
    name, content_hash, kind, role_key, version, digest, format
)
SELECT c.name, c.content_hash, c.kind, c.role_key, c.version, c.digest, c.format
FROM template_build_components c
-- As in the up migration, only child rows whose build survives.
WHERE c.cache_partition = ''
  AND EXISTS (
      SELECT 1 FROM template_builds_legacy b
      WHERE b.name = c.name AND b.content_hash = c.content_hash
  );

CREATE TABLE template_snapshots_legacy (
    name           TEXT NOT NULL,
    content_hash   TEXT NOT NULL,
    vcpu           INTEGER NOT NULL,
    memory_mib     INTEGER NOT NULL,
    snapshot_hash  TEXT NOT NULL,
    built_at       TEXT NOT NULL,
    PRIMARY KEY (name, content_hash, vcpu, memory_mib),
    FOREIGN KEY (name, content_hash) REFERENCES template_builds_legacy(name, content_hash)
);

INSERT INTO template_snapshots_legacy (
    name, content_hash, vcpu, memory_mib, snapshot_hash, built_at
)
SELECT s.name, s.content_hash, s.vcpu, s.memory_mib, s.snapshot_hash, s.built_at
FROM template_snapshots s
WHERE s.cache_partition = ''
  AND EXISTS (
      SELECT 1 FROM template_builds_legacy b
      WHERE b.name = s.name AND b.content_hash = s.content_hash
  );

CREATE TABLE template_pinned_tags_legacy (
    name          TEXT NOT NULL,
    tag           TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (name, tag)
);

INSERT INTO template_pinned_tags_legacy (name, tag, content_hash, updated_at)
SELECT name, tag, content_hash, updated_at
FROM template_pinned_tags
WHERE cache_partition = '';

DROP TABLE template_snapshots;
DROP TABLE template_build_components;
DROP TABLE template_builds;
DROP TABLE template_pinned_tags;

ALTER TABLE template_builds_legacy RENAME TO template_builds;
ALTER TABLE template_build_components_legacy RENAME TO template_build_components;
ALTER TABLE template_snapshots_legacy RENAME TO template_snapshots;
ALTER TABLE template_pinned_tags_legacy RENAME TO template_pinned_tags;

CREATE INDEX idx_template_build_components_lookup
    ON template_build_components(kind, version, role_key);

CREATE INDEX idx_template_builds_execution
    ON template_builds(runtime, runtime_version, guest_os, arch);
