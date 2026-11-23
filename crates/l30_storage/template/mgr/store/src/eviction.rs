//! Reads template-manager's cache eviction makes inside its pre-check
//! transaction.

use rusqlite::{Connection, Result, params};

use pplx_space_sql_locator::sql;

/// Whether a prefetch tag or release pins the digest. A pin names rootfs
/// bytes, so it covers every cache partition of the digest.
pub fn digest_is_pinned(conn: &Connection, name: &str, content_hash: &str) -> Result<bool> {
    let pinned: i64 = conn.query_row(
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
