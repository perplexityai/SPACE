//! The digest pins: `template_pinned_tags`, keyed on
//! `(name, tag, cache_partition)`, and `template_pinned_releases`, keyed on
//! `(name, content_hash)`. A row in either says the digest is still wanted on
//! this node, so eviction keeps every cache partition of it.

use rusqlite::{Connection, Result, params};

use pplx_space_sql_locator::sql;

/// Insert `name`/`tag`'s pin in `cache_partition`, or point the existing one
/// at `content_hash`.
pub fn upsert_tag_pin(
    conn: &Connection,
    name: &str,
    tag: &str,
    cache_partition: &str,
    content_hash: &str,
    updated_at: &str,
) -> Result<usize> {
    conn.execute(
        sql!(
            "INSERT INTO template_pinned_tags (name, tag, cache_partition, content_hash, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name, tag, cache_partition) DO UPDATE SET content_hash = ?4, updated_at = ?5"
        ),
        params![name, tag, cache_partition, content_hash, updated_at],
    )
}

/// Delete every tag pin.
pub fn delete_all_tag_pins(conn: &Connection) -> Result<usize> {
    conn.execute(sql!("DELETE FROM template_pinned_tags"), [])
}

/// Delete `name`/`tag`'s pin in `cache_partition`.
pub fn delete_tag_pin(
    conn: &Connection,
    name: &str,
    tag: &str,
    cache_partition: &str,
) -> Result<usize> {
    conn.execute(
        sql!(
            "DELETE FROM template_pinned_tags WHERE name = ?1 AND tag = ?2 AND cache_partition = ?3"
        ),
        params![name, tag, cache_partition],
    )
}

/// Delete every tag pin of a non-legacy cache partition whose key is not in
/// `keep`. The legacy partition's key is the empty string, and its pins stay.
pub fn delete_tag_pins_of_partitions_outside(conn: &Connection, keep: &[String]) -> Result<usize> {
    // `json_each` binds the whole list as one value, so the statement
    // stays whole however many partitions the node has.
    let keep = serde_json::to_string(keep)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        sql!(
            "DELETE FROM template_pinned_tags
              WHERE cache_partition != ''
                AND cache_partition NOT IN (SELECT value FROM json_each(?1))"
        ),
        params![keep],
    )
}

/// Delete every tag pin whose `(name, content_hash)` has no local build in any
/// partition.
pub fn delete_tag_pins_without_local_builds(conn: &Connection) -> Result<usize> {
    conn.execute(
        sql!(
            "DELETE FROM template_pinned_tags
              WHERE NOT EXISTS (
                    SELECT 1
                      FROM template_builds b
                     WHERE b.name = template_pinned_tags.name
                       AND b.content_hash = template_pinned_tags.content_hash
                )"
        ),
        [],
    )
}

/// Delete every release pin whose `(name, content_hash)` has no local build in
/// any partition.
pub fn delete_release_pins_without_local_builds(conn: &Connection) -> Result<usize> {
    conn.execute(
        sql!(
            "DELETE FROM template_pinned_releases
              WHERE NOT EXISTS (
                    SELECT 1
                      FROM template_builds b
                     WHERE b.name = template_pinned_releases.name
                       AND b.content_hash = template_pinned_releases.content_hash
                )"
        ),
        [],
    )
}

/// Insert the release pin of `name`/`content_hash`, or refresh its
/// diagnostic `release_id` and `release_created_at`.
pub fn upsert_release_pin(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    release_id: &str,
    release_created_at: &str,
    updated_at: &str,
) -> Result<usize> {
    conn.execute(
        sql!(
            "INSERT INTO template_pinned_releases (name, content_hash, release_id, release_created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name, content_hash) DO UPDATE SET
                 release_id = ?3,
                 release_created_at = ?4,
                 updated_at = ?5"
        ),
        params![name, content_hash, release_id, release_created_at, updated_at],
    )
}

/// The distinct content hashes any tag or release pin holds for `name`.
pub fn list_pinned_hashes_for_template(conn: &Connection, name: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT content_hash FROM template_pinned_tags WHERE name = ?1
         UNION
         SELECT content_hash FROM template_pinned_releases WHERE name = ?1"
    ))?;
    let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
    rows.collect()
}

/// The content hashes release pins hold for `name`, newest release first.
pub fn list_release_pin_hashes(conn: &Connection, name: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT content_hash FROM template_pinned_releases
         WHERE name = ?1
         ORDER BY release_created_at DESC, content_hash ASC"
    ))?;
    let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
    rows.collect()
}

/// The distinct `(name, content_hash)` pairs any tag or release pin holds.
pub fn list_pinned_build_keys(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT name, content_hash FROM template_pinned_tags
         UNION
         SELECT name, content_hash FROM template_pinned_releases"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect()
}
