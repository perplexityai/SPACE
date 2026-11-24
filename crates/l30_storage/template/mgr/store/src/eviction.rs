//! The statements template-manager's cache eviction issues: the pre-check's
//! pin check and its `ready → deleting` swap, the eviction reaper's
//! resolutions of a build left `deleting`, and startup recovery's reset of one
//! whose artifacts are intact.
//!
//! Each guarded write re-checks the build's state or absence in its `WHERE`,
//! binds only the key and bound its caller passes, and returns whether or how
//! many rows it matched: a build another writer already moved or removed
//! matches nothing, which is a normal outcome, not an error. Callers keep
//! their transactions and decide what each outcome means.

use rusqlite::params;

use pplx_space_sql_locator::sql;

use crate::{Connection, Result};
use pplx_space_template_model::TemplateCachePartition;

use crate::codec::saturating_u64_to_i64;

/// Whether a prefetch tag or release pins the digest. A pin names rootfs
/// bytes, so it covers every cache partition of the digest.
pub fn digest_is_pinned(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    let pinned: i64 = conn.0.query_row(
        sql!(
            "SELECT EXISTS(
             SELECT 1 FROM template_pinned_tags WHERE name = ?1 AND content_hash = ?2
             UNION ALL
             SELECT 1 FROM template_pinned_releases WHERE name = ?1 AND content_hash = ?2
         )"
        ),
        params![name, content_hash],
        |row| row.get(0),
    )?;
    Ok(pinned != 0)
}

/// The pre-check's compare-and-swap: move the build from `state='ready'` to
/// `'deleting'`. With `touched_before_secs`, the build must also have been
/// neither created nor used after that bound. Returns `false` when no row
/// matched: the build is gone, already `deleting`, or touched after the
/// bound.
pub fn mark_ready_build_deleting(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
    touched_before_secs: Option<u64>,
) -> Result<bool> {
    let updated = conn.0.execute(
        sql!(
            "UPDATE template_builds SET state = 'deleting'
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3 AND state = 'ready'
           AND (?4 IS NULL
                OR MAX(CAST(created_at AS INTEGER), CAST(last_used_at AS INTEGER)) <= ?4)"
        ),
        params![
            name,
            content_hash,
            cache_partition.key(),
            touched_before_secs.map(saturating_u64_to_i64),
        ],
    )?;
    Ok(updated != 0)
}

/// The eviction reaper's reset of a build still in `state='deleting'` whose
/// artifacts are intact back to `'ready'`. Returns `false` when the build is
/// gone or no longer `deleting`.
pub fn reaper_reset_deleting_build_to_ready(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<bool> {
    let updated = conn.0.execute(
        sql!(
            "UPDATE template_builds SET state = 'ready'
                         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
                           AND state = 'deleting'"
        ),
        params![name, content_hash, cache_partition.key()],
    )?;
    Ok(updated != 0)
}

/// The eviction reaper's delete of a build row still in `state='deleting'`
/// whose artifacts are missing. Returns `false` when the build is gone or no
/// longer `deleting`. The reaper deletes the build's snapshots before this and
/// its components after it, in the same transaction.
pub fn reaper_delete_deleting_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<bool> {
    let deleted = conn.0.execute(
        sql!(
            "DELETE FROM template_builds
                         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
                           AND state = 'deleting'"
        ),
        params![name, content_hash, cache_partition.key()],
    )?;
    Ok(deleted != 0)
}

/// Delete a build's components only if its `template_builds` row is gone,
/// and return how many were deleted. The reaper runs this after
/// [`reaper_delete_deleting_build`] in the same transaction, so the components
/// go once the row has, without relying on the schema's `ON DELETE CASCADE`.
pub fn delete_components_of_absent_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<usize> {
    Ok(conn.0.execute(
        sql!(
            "DELETE FROM template_build_components
                         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
                           AND NOT EXISTS (
                               SELECT 1 FROM template_builds
                               WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
                           )"
        ),
        params![name, content_hash, cache_partition.key()],
    )?)
}

/// Startup recovery's reset of a build left in `state='deleting'` whose
/// artifacts are intact back to `'ready'`. Returns `false` when the build is
/// gone or no longer `deleting`.
pub fn startup_reset_deleting_build_to_ready(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<bool> {
    let updated = conn.0.execute(
        sql!(
            "UPDATE template_builds SET state = 'ready'
                     WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
                       AND state = 'deleting'"
        ),
        params![name, content_hash, cache_partition.key()],
    )?;
    Ok(updated != 0)
}

#[cfg(test)]
mod tests;
