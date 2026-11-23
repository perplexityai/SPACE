//! `template_snapshots`: the base snapshots captured from a build, one per
//! compute shape.

use rusqlite::{Connection, Result, params};

use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateCachePartition;

use crate::codec::parse_cache_partition;
use crate::now;

pub struct SnapshotRow {
    pub name: String,
    pub content_hash: String,
    pub cache_partition: TemplateCachePartition,
    pub vcpu: u32,
    pub memory_mib: u32,
    pub snapshot_hash: String,
    pub built_at: String,
}

fn snapshot_row(row: &rusqlite::Row<'_>) -> Result<SnapshotRow> {
    Ok(SnapshotRow {
        name: row.get(0)?,
        content_hash: row.get(1)?,
        cache_partition: parse_cache_partition(&row.get::<_, String>(2)?, 2)?,
        vcpu: row.get(3)?,
        memory_mib: row.get(4)?,
        snapshot_hash: row.get(5)?,
        built_at: row.get(6)?,
    })
}

/// One base snapshot: a build and the compute shape it was captured at.
#[derive(Clone, Copy)]
pub struct SnapshotKey<'a> {
    pub name: &'a str,
    pub content_hash: &'a str,
    pub cache_partition: TemplateCachePartition,
    pub vcpu: u32,
    pub memory_mib: u32,
}

pub fn get_snapshot(conn: &Connection, key: SnapshotKey<'_>) -> Result<Option<SnapshotRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT name, content_hash, cache_partition, vcpu, memory_mib, snapshot_hash, built_at
         FROM template_snapshots
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3 AND vcpu = ?4 AND memory_mib = ?5"
    ))?;
    let mut rows = stmt.query_map(
        params![
            key.name,
            key.content_hash,
            key.cache_partition.key(),
            key.vcpu,
            key.memory_mib
        ],
        snapshot_row,
    )?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(row)),
        _ => Ok(None),
    }
}

pub fn upsert_snapshot(conn: &Connection, key: SnapshotKey<'_>, snapshot_hash: &str) -> Result<()> {
    let ts = now();
    conn.execute(
        sql!("INSERT INTO template_snapshots (name, content_hash, cache_partition, vcpu, memory_mib, snapshot_hash, built_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(name, content_hash, cache_partition, vcpu, memory_mib) DO UPDATE SET snapshot_hash = ?6, built_at = ?7"),
        params![
            key.name,
            key.content_hash,
            key.cache_partition.key(),
            key.vcpu,
            key.memory_mib,
            snapshot_hash,
            ts
        ],
    )?;
    Ok(())
}

pub fn list_snapshots(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<Vec<SnapshotRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT name, content_hash, cache_partition, vcpu, memory_mib, snapshot_hash, built_at
         FROM template_snapshots WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
         ORDER BY vcpu ASC, memory_mib ASC"
    ))?;
    let rows = stmt.query_map(
        params![name, content_hash, cache_partition.key()],
        snapshot_row,
    )?;
    rows.collect()
}

pub fn delete_snapshots_for_build(
    conn: &Connection,
    name: &str,
    content_hash: &str,
    cache_partition: TemplateCachePartition,
) -> Result<()> {
    conn.execute(
        sql!(
            "DELETE FROM template_snapshots
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3"
        ),
        params![name, content_hash, cache_partition.key()],
    )?;
    Ok(())
}

pub fn delete_snapshot(conn: &Connection, key: SnapshotKey<'_>) -> Result<()> {
    conn.execute(
        sql!("DELETE FROM template_snapshots
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3 AND vcpu = ?4 AND memory_mib = ?5"),
        params![
            key.name,
            key.content_hash,
            key.cache_partition.key(),
            key.vcpu,
            key.memory_mib
        ],
    )?;
    Ok(())
}
