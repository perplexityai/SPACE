//! volume-manager's node-local SQLite store: `volume_attachments`, and
//! volume-manager's reads of its shared `jobs` table that the job queue's
//! paged listing cannot answer.
//!
//! [`database`] migrates and opens the database for `pplx-space-volmgr`; the
//! service shares the one [`Connection`] it opens through a
//! [`SharedConnection`]. The job queue runs through [`jobs::JobWorker`], which
//! [`SharedConnection::background_worker`] builds over that same connection.
//! Every other function takes a [`Connection`], so it runs under the
//! caller's held lock. `AGENTS.md` lists the rules for this crate's public
//! surface.

#![forbid(unsafe_code)]

pub mod database;
mod handle;
pub mod jobs;

pub use handle::{Connection, ConnectionGuard, Error, Result, SharedConnection};

use pplx_space_job_id::JobId;
use pplx_space_sandbox_model::{SandboxId, SandboxIdParseError};
use pplx_space_snapshot_model::TransferScopeId;
use pplx_space_sql_locator::sql;
use rusqlite::params;
use std::{fmt, str::FromStr};

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

/// In-memory representation of a volume attachment row.
#[derive(Debug, Clone)]
pub struct VolumeAttachment {
    pub name: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub s3_region: String,
    pub read_only: bool,
    pub byok_key_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Volume attachments
// ---------------------------------------------------------------------------

pub fn upsert_attachment(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
    a: &VolumeAttachment,
) -> Result<()> {
    conn.0.execute(
        sql!(
            "INSERT INTO volume_attachments
             (sandbox_id, name, s3_bucket, s3_prefix, s3_region, read_only, byok_key_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(sandbox_id, name) DO UPDATE SET
             s3_bucket   = excluded.s3_bucket,
             s3_prefix   = excluded.s3_prefix,
             s3_region   = excluded.s3_region,
             read_only   = excluded.read_only,
             byok_key_id = excluded.byok_key_id"
        ),
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
) -> Result<()> {
    conn.0.execute(
        sql!("DELETE FROM volume_attachments WHERE sandbox_id = ?1 AND name = ?2"),
        params![sandbox_id.0.as_ref(), volume_name],
    )?;
    Ok(())
}

pub fn list_attachments(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
) -> Result<Vec<VolumeAttachment>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, s3_bucket, s3_prefix, s3_region, read_only, byok_key_id
         FROM volume_attachments
         WHERE sandbox_id = ?1
         ORDER BY name"
    ))?;
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
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub fn get_attachment(
    conn: &Connection,
    sandbox_id: &AttachmentSandboxKey,
    volume_name: &str,
) -> Result<Option<VolumeAttachment>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT name, s3_bucket, s3_prefix, s3_region, read_only, byok_key_id
         FROM volume_attachments
         WHERE sandbox_id = ?1 AND name = ?2
         LIMIT 1"
    ))?;
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
    Ok(rows.next().transpose()?)
}

// ---------------------------------------------------------------------------
// Job censuses
// ---------------------------------------------------------------------------

/// Complete keep-set for the node's privileged send leases. Cancelling work
/// retains ownership until the executor's cleanup barrier has completed.
pub fn active_btrfs_upload_payloads(conn: &Connection) -> Result<Vec<String>> {
    let mut query = conn.0.prepare(sql!("SELECT payload FROM jobs WHERE kind = 'push_btrfs_send' AND status IN ('pending', 'running', 'cancelling')"))?;
    Ok(query
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

/// Jobs that have not reached a terminal state: the job queue's own count,
/// under the caller's held connection.
pub fn count_active_jobs(conn: &Connection) -> Result<u64> {
    Ok(pplx_space_common_jobs::db::count_active_jobs(&conn.0)?)
}

/// Unfinished pull jobs submitted under `scope`, newest first.
pub fn transfer_scope_job_ids(conn: &Connection, scope: TransferScopeId) -> Result<Vec<JobId>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT job_id
           FROM jobs
          WHERE kind = 'pull'
            AND status IN ('pending', 'running', 'cancelling')
            AND json_extract(payload, '$.transfer_scope_id') = ?1
          ORDER BY created_at DESC, job_id DESC"
    ))?;
    Ok(stmt
        .query_map([scope.to_string()], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

/// A pending or running job's kind and raw payload.
#[derive(Debug, Clone)]
pub struct JobPayloadRow {
    pub kind: String,
    pub payload: String,
}

/// Which step of [`pending_or_running_job_payloads`] failed.
#[derive(Debug, thiserror::Error)]
pub enum JobPayloadsError {
    #[error("prepare: {0}")]
    Prepare(#[source] Error),
    #[error("execute: {0}")]
    Execute(#[source] Error),
    #[error("read row: {0}")]
    Row(#[source] Error),
}

/// Every pending or running job of either kind, unbounded: unlike the job
/// queue's paged listing, the result is complete.
pub fn pending_or_running_job_payloads(
    conn: &Connection,
    kinds: [&str; 2],
) -> Result<Vec<JobPayloadRow>, JobPayloadsError> {
    let mut stmt = conn
        .0
        .prepare(sql!(
            "SELECT kind, payload FROM jobs
                 WHERE kind IN (?1, ?2) AND status IN ('pending', 'running')"
        ))
        .map_err(|error| JobPayloadsError::Prepare(error.into()))?;
    let rows = stmt
        .query_map(kinds, |row| {
            Ok(JobPayloadRow {
                kind: row.get::<_, String>(0)?,
                payload: row.get::<_, String>(1)?,
            })
        })
        .map_err(|error| JobPayloadsError::Execute(error.into()))?;
    rows.map(|row| row.map_err(|error| JobPayloadsError::Row(error.into())))
        .collect()
}
