//! `template_builds` rows and the child rows keyed on them: the build reads,
//! the LRU touch, the guarded upsert that records a build with its
//! components, and the delete chain that removes a build with its components
//! and snapshots.
//!
//! A build is keyed by `(name, content_hash, cache_partition)`. Reads that
//! pass no partition get every partition of a hash; the legacy partition
//! sorts first because its key is the empty string. A row whose partition or
//! size doesn't decode fails the read with rusqlite's
//! `FromSqlConversionFailure` for that column.
//!
//! Template-manager's runtime connection leaves `foreign_keys` off, so
//! nothing cascades: each delete here removes the child rows by hand before
//! the build row.

use rusqlite::params;

use pplx_space_sql_locator::sql;

use crate::{Connection, Result, Transaction};
use pplx_space_template_model::TemplateCachePartition;

use crate::codec::{nonnegative_i64_to_u64, parse_cache_partition};
use crate::snapshots::delete_snapshots_for_build;

/// One `template_builds` row as the build reads select it.
#[derive(Debug)]
pub struct TemplateBuildRow {
    pub name: String,
    pub content_hash: String,
    pub cache_partition: TemplateCachePartition,
    pub build_date: String,
    pub manifest_json: String,
    pub runtime: String,
    pub arch: String,
    pub artifact_size_bytes: u64,
    pub state: String,
    pub owner_type: String,
    pub owner_id: String,
    pub execution_metadata_json: Option<String>,
}

/// One row's worth of eviction-planner input. Times are stored in the
/// DB as Unix-epoch-seconds strings; this struct hands them to the
/// planner as `u64` so the planner does not need to know about the
/// on-disk encoding.
pub struct BuildEvictionRow {
    pub name: String,
    pub content_hash: String,
    pub cache_partition: TemplateCachePartition,
    pub artifact_size_bytes: u64,
    pub created_at_secs: u64,
    pub last_used_at_secs: u64,
}

/// Maps the column list shared by every `template_builds` read below.
fn build_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TemplateBuildRow> {
    Ok(TemplateBuildRow {
        name: row.get(0)?,
        content_hash: row.get(1)?,
        cache_partition: parse_cache_partition(&row.get::<_, String>(2)?, 2)?,
        build_date: row.get(3)?,
        manifest_json: row.get(4)?,
        runtime: row.get(5)?,
        arch: row.get(6)?,
        artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(7)?, 7)?,
        state: row.get(8)?,
        owner_type: row.get(9)?,
        owner_id: row.get(10)?,
        execution_metadata_json: row.get(11)?,
    })
}

/// The build at one key, in any state.
pub fn get_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<Option<TemplateBuildRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3"
    ))?;
    let mut rows = stmt.query_map(
        params![name, content_hash, cache_partition.key()],
        build_row,
    )?;
    Ok(rows.next().transpose()?)
}

/// The ready build of a digest, preferring `preferred`'s partition, then
/// the legacy partition, then any other.
pub fn find_ready_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    preferred: Option<TemplateCachePartition>,
) -> Result<Option<TemplateBuildRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds
         WHERE name = ?1 AND content_hash = ?2 AND state = 'ready'
         ORDER BY cache_partition = ?3 DESC, cache_partition ASC LIMIT 1"
    ))?;
    let mut rows = stmt.query_map(
        params![
            name,
            content_hash,
            preferred.map(TemplateCachePartition::key)
        ],
        build_row,
    )?;
    Ok(rows.next().transpose()?)
}

/// Whether a row exists for the digest in any partition, in any state.
pub fn build_exists(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    Ok(conn
        .0
        .query_row(
            sql!(
                "SELECT EXISTS(SELECT 1 FROM template_builds WHERE name = ?1 AND content_hash = ?2)"
            ),
            params![name, content_hash],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)?)
}

/// The keys of builds in `state='deleting'`.
pub fn list_deleting_builds(
    conn: &Connection,
) -> Result<Vec<(String, String, TemplateCachePartition)>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition FROM template_builds WHERE state = 'deleting'"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            parse_cache_partition(&row.get::<_, String>(2)?, 2)?,
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Every build of one rootfs digest, in any state, legacy partition first.
pub fn list_builds_for_hash(
    conn: &Connection,
    name: &str,
    content_hash: &str,
) -> Result<Vec<TemplateBuildRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE name = ?1 AND content_hash = ?2
         ORDER BY cache_partition ASC"
    ))?;
    let rows = stmt.query_map(params![name, content_hash], build_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Whether a build of the digest, in any cache partition, is in
/// `state='deleting'`.
pub fn is_build_marked_deleting(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    let count: i64 = conn.0.query_row(
        sql!(
            "SELECT COUNT(*) FROM template_builds
         WHERE name = ?1 AND content_hash = ?2 AND state = 'deleting'"
        ),
        params![name, content_hash],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Set one build's `last_used_at` to `last_used_at`, the caller's clock.
/// A missing row updates nothing and is not an error.
pub fn touch_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
    last_used_at: &str,
) -> Result<()> {
    conn.0.execute(
        sql!(
            "UPDATE template_builds SET last_used_at = ?4
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3"
        ),
        params![name, content_hash, cache_partition.key(), last_used_at],
    )?;
    Ok(())
}

/// Every `ready` build with its size and timestamps. A timestamp that
/// doesn't parse as epoch seconds reads as 0.
pub fn list_eviction_candidates(conn: &Connection) -> Result<Vec<BuildEvictionRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, artifact_size_bytes, created_at, last_used_at
         FROM template_builds WHERE state = 'ready'"
    ))?;
    let rows = stmt.query_map([], |row| {
        let created_str: String = row.get(4)?;
        let last_used_str: String = row.get(5)?;
        Ok(BuildEvictionRow {
            name: row.get(0)?,
            content_hash: row.get(1)?,
            cache_partition: parse_cache_partition(&row.get::<_, String>(2)?, 2)?,
            artifact_size_bytes: nonnegative_i64_to_u64(row.get::<_, i64>(3)?, 3)?,
            created_at_secs: created_str.parse::<u64>().unwrap_or(0),
            last_used_at_secs: last_used_str.parse::<u64>().unwrap_or(0),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Every ready build, by name, build date, digest and partition.
pub fn list_builds(conn: &Connection) -> Result<Vec<TemplateBuildRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE state = 'ready'
         ORDER BY name ASC, build_date ASC, content_hash ASC, cache_partition ASC"
    ))?;
    let rows = stmt.query_map([], build_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Every ready build of `name`.
pub fn list_builds_by_name(conn: &Connection, name: &str) -> Result<Vec<TemplateBuildRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds WHERE name = ?1 AND state = 'ready'"
    ))?;
    let rows = stmt.query_map(params![name], build_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Newest ready build of `name` in any partition. Same-date builds go to
/// the larger content hash, then to `preferred`'s partition, then to the
/// legacy one.
pub fn get_latest_build(
    conn: &Connection,
    name: &str,
    preferred: Option<TemplateCachePartition>,
) -> Result<Option<TemplateBuildRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds
         WHERE name = ?1 AND state = 'ready'
         ORDER BY build_date DESC, content_hash DESC, cache_partition = ?2 DESC,
                  cache_partition = '' DESC,
                  cache_partition ASC
         LIMIT 1"
    ))?;
    let mut rows = stmt.query_map(
        params![name, preferred.map(TemplateCachePartition::key)],
        build_row,
    )?;
    Ok(rows.next().transpose()?)
}

/// The first of `name`'s ready builds in `cache_partition` or the legacy
/// partition, newest first, that `pick` maps to `Some`. A date tie goes to
/// the larger digest, and a tie on both to `cache_partition`'s own row. Rows
/// after the pick are not decoded.
pub fn find_latest_build_in_partition_or_legacy<T>(
    conn: &Connection,
    name: &str,
    cache_partition: TemplateCachePartition,
    mut pick: impl FnMut(TemplateBuildRow) -> Option<T>,
) -> Result<Option<T>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds
         WHERE name = ?1 AND cache_partition IN (?2, '') AND state = 'ready'
         ORDER BY build_date DESC, content_hash DESC, cache_partition = ?2 DESC"
    ))?;
    for row in stmt.query_map(params![name, cache_partition.key()], build_row)? {
        if let Some(picked) = pick(row?) {
            return Ok(Some(picked));
        }
    }
    Ok(None)
}

/// The first of `name`'s ready builds in exactly `cache_partition`, newest
/// first, a date tie going to the larger digest, that `pick` maps to `Some`.
/// Rows after the pick are not decoded.
pub fn find_latest_build_in_partition<T>(
    conn: &Connection,
    name: &str,
    cache_partition: TemplateCachePartition,
    mut pick: impl FnMut(TemplateBuildRow) -> Option<T>,
) -> Result<Option<T>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, owner_type, owner_id, execution_metadata_json
         FROM template_builds
         WHERE name = ?1 AND cache_partition = ?2 AND state = 'ready'
         ORDER BY build_date DESC, content_hash DESC"
    ))?;
    for row in stmt.query_map(params![name, cache_partition.key()], build_row)? {
        if let Some(picked) = pick(row?) {
            return Ok(Some(picked));
        }
    }
    Ok(None)
}

/// Partitions that hold at least one ready build of `name`, legacy first.
pub fn list_build_partitions(conn: &Connection, name: &str) -> Result<Vec<TemplateCachePartition>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT DISTINCT cache_partition FROM template_builds
         WHERE name = ?1 AND state = 'ready'
         ORDER BY cache_partition ASC"
    ))?;
    let rows = stmt.query_map(params![name], |row| {
        parse_cache_partition(&row.get::<_, String>(0)?, 0)
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The columns [`upsert_build`] writes, as template-manager derives them from
/// a build's manifest.
pub struct BuildUpsertRow<'a> {
    pub name: &'a str,
    pub content_hash: &'a str,
    pub cache_partition: TemplateCachePartition,
    pub build_date: &'a str,
    pub manifest_json: &'a str,
    pub runtime: &'a str,
    pub arch: &'a str,
    pub artifact_size_bytes: i64,
    pub state: &'a str,
    /// Unix-epoch seconds; an insert also seeds `last_used_at` with it.
    pub created_at: &'a str,
    pub runtime_version: Option<&'a str>,
    pub guest_os: Option<&'a str>,
    pub state_format_name: Option<&'a str>,
    pub state_format_version: Option<&'a str>,
    pub execution_metadata_json: Option<&'a str>,
}

/// One `template_build_components` row of a build [`upsert_build`] records.
pub struct BuildComponentRow<'a> {
    pub kind: &'a str,
    /// The empty string for a component without a role.
    pub role_key: &'a str,
    pub version: Option<&'a str>,
    pub digest: Option<String>,
    pub format: Option<&'a str>,
}

/// Insert the build, or refresh an existing row unless it is in
/// `state='deleting'`, and replace its components with `components`. Returns
/// `false`, having written nothing, when the row is mid-eviction. The
/// statements are only correct together, so it takes the caller's
/// transaction; the caller commits.
///
/// The `ON CONFLICT` path leaves `last_used_at` alone, since a re-upsert is
/// not an access.
///
/// The `WHERE state != 'deleting'` guard on the ON CONFLICT path
/// is the back-door fix for the GC-vs-pull race: when eviction
/// Phase 1 has already flipped this row to `state='deleting'`, a
/// concurrent pull's final upsert must NOT clobber the marker
/// back to `'ready'`. Without this, the pull would silently
/// resurrect a doomed row, eviction's Phase 3 would then DELETE
/// it, and the on-disk artifacts would be in an indeterminate
/// half-deleted half-written state. With the guard, the upsert
/// is a no-op when the row is mid-eviction; the pull's caller
/// sees the build as still NotFound after re-resolution and
/// surfaces NotFound to its caller cleanly.
///
/// It doesn't check the row against the build's manifest: the caller
/// validates the runtime, architecture and cache partition first.
pub fn upsert_build(
    tx: &Transaction<'_>,
    build: &BuildUpsertRow<'_>,
    components: &[BuildComponentRow<'_>],
) -> Result<bool> {
    let partition = build.cache_partition.key();
    let rows = tx.0.execute(
        sql!("INSERT INTO template_builds (name, content_hash, cache_partition, build_date, manifest_json, runtime, arch, artifact_size_bytes, state, created_at, last_used_at, runtime_version, guest_os, state_format_name, state_format_version, execution_metadata_json)
         VALUES (?1, ?2, ?15, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(name, content_hash, cache_partition) DO UPDATE SET build_date = ?3, manifest_json = ?4, runtime = ?5, arch = ?6, artifact_size_bytes = ?7, state = ?8, runtime_version = ?10, guest_os = ?11, state_format_name = ?12, state_format_version = ?13, execution_metadata_json = ?14
         WHERE template_builds.state != 'deleting'"),
        params![build.name, build.content_hash, build.build_date, build.manifest_json, build.runtime, build.arch, build.artifact_size_bytes, build.state, build.created_at, build.runtime_version, build.guest_os, build.state_format_name, build.state_format_version, build.execution_metadata_json, partition],
    )?;
    if rows == 0 {
        return Ok(false);
    }
    delete_build_components(tx, build.name, build.content_hash, &partition)?;
    for component in components {
        tx.0.execute(
            sql!("INSERT INTO template_build_components (name, content_hash, cache_partition, kind, role_key, version, digest, format)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"),
            params![
                build.name,
                build.content_hash,
                partition,
                component.kind,
                component.role_key,
                component.version,
                component.digest,
                component.format,
            ],
        )?;
    }
    Ok(true)
}

/// Delete the build's components, its snapshots and then the build row, each
/// as its own statement. Pass a transaction to delete them atomically.
pub fn delete_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<()> {
    let partition = cache_partition.key();
    delete_build_components(conn, name, content_hash, &partition)?;
    delete_snapshots_for_build(conn, name, content_hash, cache_partition)?;
    conn.0.execute(
        sql!(
            "DELETE FROM template_builds
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3"
        ),
        params![name, content_hash, partition],
    )?;
    Ok(())
}

fn delete_build_components(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    partition: &str,
) -> Result<()> {
    conn.0.execute(
        sql!(
            "DELETE FROM template_build_components
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3"
        ),
        params![name, content_hash, partition],
    )?;
    Ok(())
}
