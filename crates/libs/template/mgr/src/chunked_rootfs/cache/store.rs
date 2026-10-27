//! Template-manager owns source admission and leases in its existing SQLite
//! database. Filesystem finalization precedes admission in the ready transaction.

use pplx_space_common_core::{
    host::btrfs::ReadonlySubvolumeIdentity,
    sql,
    types::{Digest, TemplateName},
};
use pplx_space_sqlite::SqliteU64;
use rusqlite::{Connection, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use super::super::{
    Leaf,
    delivery::{AuthenticatedManifest, Binding},
};
use super::SourceIdentity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceState {
    Admitted,
    Retiring,
    Invalid,
}

impl SourceState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Retiring => "retiring",
            Self::Invalid => "invalid",
        }
    }
}
impl std::fmt::Display for SourceState {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(self.as_str())
    }
}
#[derive(Debug, thiserror::Error)]
#[error("unknown verified source state")]
pub struct InvalidSourceState;
impl std::str::FromStr for SourceState {
    type Err = InvalidSourceState;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "admitted" => Ok(Self::Admitted),
            "retiring" => Ok(Self::Retiring),
            "invalid" => Ok(Self::Invalid),
            _ => Err(InvalidSourceState),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Candidate {
    pub source_id: Uuid,
    pub template: TemplateName,
    pub rootfs: Digest,
    pub digest: Digest,
    pub offset: u64,
    pub length: u64,
    pub identity: SourceIdentity,
}

pub struct Selection {
    pub lease_id: Uuid,
    pub candidates: Vec<Candidate>,
}

fn text_column<T: std::str::FromStr>(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    row.get::<_, String>(column)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn invalid(message: &'static str) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    )))
}

/// Called in the same transaction that marks this build ready, after all data
/// is persisted and the subvolume has been made read-only. Never replaces an
/// older admission: its leases must first be retired and forgotten.
pub fn admit(
    transaction: &Transaction<'_>,
    name: &TemplateName,
    binding: Binding,
    manifest: &AuthenticatedManifest,
    identity: SourceIdentity,
) -> rusqlite::Result<Uuid> {
    if identity.length != manifest.index().logical_bytes()
        || binding.rootfs_digest != manifest.index().rootfs_digest()
    {
        return Err(invalid(
            "source admission does not match the authenticated image",
        ));
    }
    let source_id = Uuid::now_v7();
    let inserted = transaction.execute(sql!(
        "INSERT INTO verified_chunk_sources
            (source_id, template_name, content_hash, manifest_digest, index_digest,
             subvolume_uuid, tree_id, subvolume_generation, file_device, file_inode,
             logical_bytes, file_ctime_seconds, file_ctime_nanos, state)
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
         WHERE EXISTS (SELECT 1 FROM template_builds WHERE name = ?2 AND content_hash = ?3 AND state = 'ready')"
    ), params![source_id.to_string(), name.as_str(), binding.rootfs_digest.to_string(), binding.manifest_digest.to_string(), binding.index_digest.to_string(),
        identity.subvolume.uuid.to_string(), SqliteU64(identity.subvolume.tree_id), SqliteU64(identity.subvolume.generation),
        SqliteU64(identity.device), SqliteU64(identity.inode), SqliteU64(identity.length), identity.ctime_seconds, identity.ctime_nanos, SourceState::Admitted.as_str()])?;
    if inserted != 1 {
        return Err(invalid(
            "source admission requires a ready build in the same transaction",
        ));
    }
    let mut insert = transaction.prepare_cached(sql!(
        "INSERT INTO verified_chunk_extents (source_id, chunk_digest, raw_length, source_offset)
         VALUES (?1, ?2, ?3, ?4) ON CONFLICT (source_id, chunk_digest, raw_length) DO NOTHING"
    ))?;
    let mut offset = 0;
    for leaf in manifest.index().leaves() {
        if let Leaf::Zstd { length, digest } = leaf {
            insert.execute(params![
                source_id.to_string(),
                digest.to_string(),
                SqliteU64(*length),
                SqliteU64(offset)
            ])?;
        }
        offset += leaf.length();
    }
    Ok(source_id)
}

/// One batched query and one lease per eligible source, atomically with its
/// state check. Bound candidates per chunk; an invalid candidate becomes a
/// miss if no other candidate survives descriptor validation.
pub fn select_and_lease(
    conn: &mut Connection,
    manifest: &AuthenticatedManifest,
) -> rusqlite::Result<Selection> {
    let wanted: BTreeMap<_, _> = manifest
        .index()
        .leaves()
        .iter()
        .filter_map(|leaf| match leaf {
            Leaf::Zstd { digest, length } => Some((digest.to_string(), *length)),
            Leaf::Zero { .. } => None,
        })
        .collect();
    let wanted: Vec<_> = wanted.into_iter().collect();
    let json = serde_json::to_string(&wanted)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let transaction = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let candidates = {
        let mut query = transaction.prepare(sql!(
            "WITH wanted AS (
                 SELECT json_extract(value, '$[0]') AS digest, json_extract(value, '$[1]') AS length FROM json_each(?1)
             ), ranked AS (
                 SELECT s.source_id, s.template_name, s.content_hash, e.chunk_digest, e.source_offset, e.raw_length,
                        s.subvolume_uuid, s.tree_id, s.subvolume_generation, s.file_device, s.file_inode,
                        s.logical_bytes, s.file_ctime_seconds, s.file_ctime_nanos,
                        row_number() OVER (PARTITION BY e.chunk_digest, e.raw_length ORDER BY s.source_id) AS rank
                 FROM wanted w JOIN verified_chunk_extents e ON e.chunk_digest = w.digest AND e.raw_length = w.length
                 JOIN verified_chunk_sources s ON s.source_id = e.source_id
                 JOIN template_builds b ON b.name = s.template_name AND b.content_hash = s.content_hash
                 WHERE s.state = ?2 AND b.state = 'ready'
             )
             SELECT source_id, template_name, content_hash, chunk_digest, source_offset, raw_length,
                    subvolume_uuid, tree_id, subvolume_generation, file_device, file_inode,
                    logical_bytes, file_ctime_seconds, file_ctime_nanos
             FROM ranked WHERE rank <= 4 ORDER BY chunk_digest, rank"
        ))?;
        query
            .query_map(params![json, SourceState::Admitted.as_str()], |row| {
                Ok(Candidate {
                    source_id: text_column(row, 0)?,
                    template: text_column(row, 1)?,
                    rootfs: text_column(row, 2)?,
                    digest: text_column(row, 3)?,
                    offset: row.get::<_, SqliteU64>(4)?.0,
                    length: row.get::<_, SqliteU64>(5)?.0,
                    identity: SourceIdentity {
                        subvolume: ReadonlySubvolumeIdentity {
                            uuid: text_column(row, 6)?,
                            tree_id: row.get::<_, SqliteU64>(7)?.0,
                            generation: row.get::<_, SqliteU64>(8)?.0,
                        },
                        device: row.get::<_, SqliteU64>(9)?.0,
                        inode: row.get::<_, SqliteU64>(10)?.0,
                        length: row.get::<_, SqliteU64>(11)?.0,
                        ctime_seconds: row.get(12)?,
                        ctime_nanos: row.get(13)?,
                    },
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let lease_id = Uuid::now_v7();
    {
        let mut insert = transaction.prepare_cached(sql!(
            "INSERT INTO verified_chunk_leases (lease_id, source_id) VALUES (?1, ?2)"
        ))?;
        for source in candidates
            .iter()
            .map(|candidate| candidate.source_id)
            .collect::<BTreeSet<_>>()
        {
            insert.execute(params![lease_id.to_string(), source.to_string()])?;
        }
    }
    transaction.commit()?;
    Ok(Selection {
        lease_id,
        candidates,
    })
}

pub fn release(conn: &Connection, lease: Uuid) -> rusqlite::Result<()> {
    conn.execute(
        sql!("DELETE FROM verified_chunk_leases WHERE lease_id = ?1"),
        [lease.to_string()],
    )?;
    Ok(())
}

pub fn invalidate(conn: &Connection, source: Uuid) -> rusqlite::Result<()> {
    conn.execute(
        sql!("UPDATE verified_chunk_sources SET state = ?2 WHERE source_id = ?1 AND state = ?3"),
        params![
            source.to_string(),
            SourceState::Invalid.as_str(),
            SourceState::Admitted.as_str()
        ],
    )?;
    Ok(())
}

pub fn has_registered_source(conn: &Connection, name: &str, hash: &str) -> rusqlite::Result<bool> {
    conn.query_row(sql!(
        "SELECT EXISTS (SELECT 1 FROM verified_chunk_sources WHERE template_name = ?1 AND content_hash = ?2)"
    ), params![name, hash], |row| row.get(0))
}

/// Stop new readers before reporting whether disk deletion can proceed.
/// A false result requires retry after readers finish; it never permits an
/// unlink or clearing the subvolume's read-only protection.
pub fn begin_retirement(conn: &Connection, name: &str, hash: &str) -> rusqlite::Result<bool> {
    let transaction = Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    let removable = retire_in_transaction(&transaction, name, hash)?;
    if removable {
        // Fence admission even when the target is an orphan with no build row.
        // An UPDATE of zero rows would let a concurrent writer publish ready
        // between this transaction and the filesystem delete.
        transaction.execute(
            sql!(
                "INSERT INTO template_builds
                (name, content_hash, build_date, manifest_json, runtime, arch, state, created_at)
             VALUES (?1, ?2, '', '{}', 'fc', '', 'deleting', ?3)
             ON CONFLICT(name, content_hash) DO UPDATE SET state = 'deleting'"
            ),
            params![name, hash, crate::db::now()],
        )?;
    }
    transaction.commit()?;
    Ok(removable)
}

pub fn retire_in_transaction(
    transaction: &Transaction<'_>,
    name: &str,
    hash: &str,
) -> rusqlite::Result<bool> {
    transaction.execute(sql!("UPDATE verified_chunk_sources SET state = ?3 WHERE template_name = ?1 AND content_hash = ?2"), params![name, hash, SourceState::Retiring.as_str()])?;
    let leased: bool = transaction.query_row(sql!(
        "SELECT EXISTS (SELECT 1 FROM verified_chunk_leases l JOIN verified_chunk_sources s ON s.source_id = l.source_id
         WHERE s.template_name = ?1 AND s.content_hash = ?2)"
    ), params![name, hash], |row| row.get(0))?;
    Ok(!leased)
}

pub fn forget_retired(
    transaction: &Transaction<'_>,
    name: &str,
    hash: &str,
) -> rusqlite::Result<()> {
    let blocked: bool = transaction.query_row(sql!(
        "SELECT EXISTS (SELECT 1 FROM verified_chunk_sources s WHERE s.template_name = ?1 AND s.content_hash = ?2
         AND (s.state != ?3 OR EXISTS (SELECT 1 FROM verified_chunk_leases l WHERE l.source_id = s.source_id)))"
    ), params![name, hash, SourceState::Retiring.as_str()], |row| row.get(0))?;
    if blocked {
        return Err(invalid(
            "source cannot be forgotten before retirement and lease release",
        ));
    }
    transaction.execute(sql!("DELETE FROM verified_chunk_extents WHERE source_id IN (SELECT source_id FROM verified_chunk_sources WHERE template_name = ?1 AND content_hash = ?2)"), params![name, hash])?;
    transaction.execute(
        sql!("DELETE FROM verified_chunk_sources WHERE template_name = ?1 AND content_hash = ?2"),
        params![name, hash],
    )?;
    Ok(())
}

/// Only after the daemon has exclusive process ownership; previous readers
/// and their descriptors must already be gone. Admissions remain conditional
/// on their recorded descriptor identity, read-only backing and build state.
pub fn recover_leases_after_exclusive_start(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(sql!("DELETE FROM verified_chunk_leases"), [])?;
    Ok(())
}

pub fn forget_template_retired(transaction: &Transaction<'_>, name: &str) -> rusqlite::Result<()> {
    let hashes = {
        let mut query = transaction.prepare(sql!(
            "SELECT content_hash FROM verified_chunk_sources WHERE template_name = ?1"
        ))?;
        query
            .query_map([name], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for hash in hashes {
        forget_retired(transaction, name, &hash)?;
    }
    Ok(())
}

pub fn begin_template_retirement(conn: &Connection, name: &str) -> rusqlite::Result<bool> {
    let transaction = Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    transaction.execute(sql!("INSERT INTO chunk_template_deletions (template_name) VALUES (?1) ON CONFLICT DO NOTHING"), [name])?;
    transaction.execute(
        sql!("UPDATE verified_chunk_sources SET state = ?2 WHERE template_name = ?1"),
        params![name, SourceState::Retiring.as_str()],
    )?;
    let leased: bool = transaction.query_row(sql!(
        "SELECT EXISTS (SELECT 1 FROM verified_chunk_leases l JOIN verified_chunk_sources s ON s.source_id = l.source_id WHERE s.template_name = ?1)"
    ), [name], |row| row.get(0))?;
    transaction.commit()?;
    Ok(!leased)
}

pub fn finish_template_retirement(
    transaction: &Transaction<'_>,
    name: &str,
) -> rusqlite::Result<()> {
    transaction.execute(
        sql!("DELETE FROM chunk_template_deletions WHERE template_name = ?1"),
        [name],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
