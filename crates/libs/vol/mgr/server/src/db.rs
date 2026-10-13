use pplx_space_common_core::types::{SandboxId, SandboxIdParseError};
use rusqlite::{Connection, params};
use std::{fmt, str::FromStr};

use crate::VolumeAttachment;

/// Validated UUID spelling used as an attachment table key.
///
/// Historical rows use case-sensitive TEXT keys supplied by callers. Keep the
/// original spelling so upgrades and rollbacks can address the same rows; UUID
/// aliases may already identify separate attachments with conflicting contents.
/// This is a storage key, not the canonical sandbox identity used elsewhere.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AttachmentSandboxKey(Box<str>);

impl FromStr for AttachmentSandboxKey {
    type Err = SandboxIdParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse::<SandboxId>()?;
        Ok(Self(raw.into()))
    }
}

impl fmt::Display for AttachmentSandboxKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Open the volume manager SQLite database. The service must run its embedded
/// migrations after taking ownership and before calling this function.
pub fn open(path: &str) -> rusqlite::Result<Connection> {
    Connection::open(path)
}

// ---------------------------------------------------------------------------
// Volume attachments
// ---------------------------------------------------------------------------

pub fn upsert_attachment(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
    a: &VolumeAttachment,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO volume_attachments
             (sandbox_id, name, s3_bucket, s3_prefix, s3_region, read_only, byok_key_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(sandbox_id, name) DO UPDATE SET
             s3_bucket   = excluded.s3_bucket,
             s3_prefix   = excluded.s3_prefix,
             s3_region   = excluded.s3_region,
             read_only   = excluded.read_only,
             byok_key_id = excluded.byok_key_id",
        params![
            sandbox_id.0.as_ref(),
            a.name,
            a.s3_bucket,
            a.s3_prefix,
            a.s3_region,
            a.read_only as i32,
            a.byok_key_id,
        ],
    )?;
    Ok(())
}

pub fn delete_attachment(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
    volume_name: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM volume_attachments WHERE sandbox_id = ?1 AND name = ?2",
        params![sandbox_id.0.as_ref(), volume_name],
    )?;
    Ok(())
}

pub fn list_attachments(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
) -> rusqlite::Result<Vec<VolumeAttachment>> {
    let mut stmt = conn.prepare(
        "SELECT name, s3_bucket, s3_prefix, s3_region, read_only, byok_key_id
         FROM volume_attachments
         WHERE sandbox_id = ?1
         ORDER BY name",
    )?;
    let rows = stmt.query_map(params![sandbox_id.0.as_ref()], |row| {
        Ok(VolumeAttachment {
            name: row.get(0)?,
            s3_bucket: row.get(1)?,
            s3_prefix: row.get(2)?,
            s3_region: row.get(3)?,
            read_only: row.get::<_, i32>(4)? != 0,
            byok_key_id: row.get(5)?,
        })
    })?;
    rows.collect()
}

pub fn get_attachment(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
    volume_name: &str,
) -> rusqlite::Result<Option<VolumeAttachment>> {
    let mut stmt = conn.prepare(
        "SELECT name, s3_bucket, s3_prefix, s3_region, read_only, byok_key_id
         FROM volume_attachments
         WHERE sandbox_id = ?1 AND name = ?2
         LIMIT 1",
    )?;
    let mut rows = stmt.query_map(params![sandbox_id.0.as_ref(), volume_name], |row| {
        Ok(VolumeAttachment {
            name: row.get(0)?,
            s3_bucket: row.get(1)?,
            s3_prefix: row.get(2)?,
            s3_region: row.get(3)?,
            read_only: row.get::<_, i32>(4)? != 0,
            byok_key_id: row.get(5)?,
        })
    })?;
    rows.next().transpose()
}

#[cfg(test)]
mod tests;

/// Complete keep-set for the node's privileged send leases. Cancelling work
/// retains ownership until the executor's cleanup barrier has completed.
pub(crate) fn active_btrfs_upload_payloads(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    use pplx_space_common_core::sql;
    let mut query = conn.prepare(sql!("SELECT payload FROM jobs WHERE kind = 'push_btrfs_send' AND status IN ('pending', 'running', 'cancelling')"))?;
    query.query_map([], |row| row.get(0))?.collect()
}
