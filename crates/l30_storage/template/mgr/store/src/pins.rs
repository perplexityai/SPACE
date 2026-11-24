//! The digest pins: `template_pinned_tags`, keyed on
//! `(name, tag, cache_partition)`, and `template_pinned_releases`, keyed on
//! `(name, content_hash)`. A row in either says the digest is still wanted on
//! this node, so eviction keeps every cache partition of it.

use rusqlite::params;

use pplx_space_sql_locator::sql;

use crate::{Connection, Result};

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
    Ok(conn.0.execute(
        sql!(
            "INSERT INTO template_pinned_tags (name, tag, cache_partition, content_hash, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name, tag, cache_partition) DO UPDATE SET content_hash = ?4, updated_at = ?5"
        ),
        params![name, tag, cache_partition, content_hash, updated_at],
    )?)
}

/// Delete every tag pin.
pub fn delete_all_tag_pins(conn: &Connection) -> Result<usize> {
    Ok(conn
        .0
        .execute(sql!("DELETE FROM template_pinned_tags"), [])?)
}

/// Delete every tag pin whose `(name, tag)` is not in `keep`, in every cache
/// partition. `keep` must not be empty, since the statement would list no
/// rows: a caller with nothing to keep calls [`delete_all_tag_pins`].
pub fn delete_tag_pins_outside(conn: &Connection, keep: &[(String, String)]) -> Result<()> {
    // SQLite's parameter binding doesn't expand a `Vec` into a
    // tuple list, so build the predicate by enumerating
    // placeholders. The const is small, so the dynamic SQL is
    // fine.
    let mut sql =
        String::from("DELETE FROM template_pinned_tags WHERE (name, tag) NOT IN (VALUES ");
    for i in 0..keep.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        let a = i * 2 + 1;
        let b = a + 1;
        sql.push_str(&format!("(?{a}, ?{b})"));
    }
    sql.push(')');
    let mut stmt = conn.0.prepare(&sql)?;
    let mut params_vec: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(keep.len() * 2);
    for (n, t) in keep {
        params_vec.push(n);
        params_vec.push(t);
    }
    stmt.execute(rusqlite::params_from_iter(params_vec.iter().copied()))?;
    Ok(())
}

/// Delete `name`/`tag`'s pin in `cache_partition`.
pub fn delete_tag_pin(
    conn: &Connection,
    name: &str,
    tag: &str,
    cache_partition: &str,
) -> Result<usize> {
    Ok(conn.0.execute(
        sql!(
            "DELETE FROM template_pinned_tags WHERE name = ?1 AND tag = ?2 AND cache_partition = ?3"
        ),
        params![name, tag, cache_partition],
    )?)
}

/// Delete every tag pin of a non-legacy cache partition whose key is not in
/// `keep`. The legacy partition's key is the empty string, and its pins stay.
pub fn delete_tag_pins_of_partitions_outside(conn: &Connection, keep: &[String]) -> Result<usize> {
    // `json_each` binds the whole list as one value, so the statement
    // stays whole however many partitions the node has.
    let keep = serde_json::to_string(keep)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    Ok(conn.0.execute(
        sql!(
            "DELETE FROM template_pinned_tags
              WHERE cache_partition != ''
                AND cache_partition NOT IN (SELECT value FROM json_each(?1))"
        ),
        params![keep],
    )?)
}

/// Delete every tag pin whose `(name, content_hash)` has no local build in any
/// partition.
pub fn delete_tag_pins_without_local_builds(conn: &Connection) -> Result<usize> {
    Ok(conn.0.execute(
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
    )?)
}

/// Delete every release pin whose `(name, content_hash)` has no local build in
/// any partition.
pub fn delete_release_pins_without_local_builds(conn: &Connection) -> Result<usize> {
    Ok(conn.0.execute(
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
    )?)
}

/// Delete every release pin of a template in `templates`. `templates` must
/// not be empty, since the statement would list no names. Like
/// [`delete_tag_pins_outside`], it builds one placeholder per listed template.
pub fn delete_release_pins_of_templates(conn: &Connection, templates: &[String]) -> Result<()> {
    let mut sql = String::from("DELETE FROM template_pinned_releases WHERE name IN (");
    for i in 0..templates.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("?{}", i + 1));
    }
    sql.push(')');
    let mut stmt = conn.0.prepare(&sql)?;
    let params_vec: Vec<&dyn rusqlite::ToSql> = templates
        .iter()
        .map(|t| t as &dyn rusqlite::ToSql)
        .collect();
    stmt.execute(rusqlite::params_from_iter(params_vec.iter().copied()))?;
    Ok(())
}

/// Delete every release pin of a template in `templates` whose
/// `(name, content_hash)` is not in `keep`; pins of other templates stay.
/// Neither list may be empty, since the statement would list no rows: a
/// caller with nothing to keep calls [`delete_release_pins_of_templates`].
/// Like [`delete_tag_pins_outside`], it builds one placeholder per listed
/// template and two per kept key, the templates' first.
pub fn delete_release_pins_of_templates_outside(
    conn: &Connection,
    templates: &[String],
    keep: &[(String, String)],
) -> Result<()> {
    let mut sql = String::from("DELETE FROM template_pinned_releases WHERE name IN (");
    for i in 0..templates.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("?{}", i + 1));
    }
    sql.push_str(") AND (name, content_hash) NOT IN (VALUES ");
    let template_count = templates.len();
    for i in 0..keep.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        let a = template_count + i * 2 + 1;
        let b = a + 1;
        sql.push_str(&format!("(?{a}, ?{b})"));
    }
    sql.push(')');
    let mut stmt = conn.0.prepare(&sql)?;
    let mut params_vec: Vec<&dyn rusqlite::ToSql> =
        Vec::with_capacity(template_count + keep.len() * 2);
    for t in templates {
        params_vec.push(t);
    }
    for (n, h) in keep {
        params_vec.push(n);
        params_vec.push(h);
    }
    stmt.execute(rusqlite::params_from_iter(params_vec.iter().copied()))?;
    Ok(())
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
    Ok(conn.0.execute(
        sql!(
            "INSERT INTO template_pinned_releases (name, content_hash, release_id, release_created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(name, content_hash) DO UPDATE SET
                 release_id = ?3,
                 release_created_at = ?4,
                 updated_at = ?5"
        ),
        params![name, content_hash, release_id, release_created_at, updated_at],
    )?)
}

/// The distinct content hashes any tag or release pin holds for `name`.
pub fn list_pinned_hashes_for_template(conn: &Connection, name: &str) -> Result<Vec<String>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT content_hash FROM template_pinned_tags WHERE name = ?1
         UNION
         SELECT content_hash FROM template_pinned_releases WHERE name = ?1"
    ))?;
    let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The content hashes release pins hold for `name`, newest release first.
pub fn list_release_pin_hashes(conn: &Connection, name: &str) -> Result<Vec<String>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT content_hash FROM template_pinned_releases
         WHERE name = ?1
         ORDER BY release_created_at DESC, content_hash ASC"
    ))?;
    let rows = stmt.query_map(params![name], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The distinct `(name, content_hash)` pairs any tag or release pin holds.
pub fn list_pinned_build_keys(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, content_hash FROM template_pinned_tags
         UNION
         SELECT name, content_hash FROM template_pinned_releases"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

#[cfg(test)]
mod tests;
