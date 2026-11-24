//! `pools`: the warm-pool target for each template.

use rusqlite::params;

use pplx_space_sql_locator::sql;

use crate::{Connection, Result};

use crate::now;

pub fn insert_pool(
    conn: &Connection,
    template: &str,
    target_count: u32,
    boot_mode: &str,
    vcpu: u32,
    memory_mib: u32,
) -> Result<()> {
    let ts = now();
    conn.0.execute(
        sql!("INSERT INTO pools (template, target_count, boot_mode, vcpu, memory_mib, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
         ON CONFLICT(template) DO UPDATE SET target_count = ?2, boot_mode = ?3, vcpu = ?4, memory_mib = ?5, updated_at = ?6"),
        params![template, target_count, boot_mode, vcpu, memory_mib, ts],
    )?;
    Ok(())
}

pub fn delete_pool(conn: &Connection, template: &str) -> Result<()> {
    conn.0.execute(
        sql!("DELETE FROM pools WHERE template = ?1"),
        params![template],
    )?;
    Ok(())
}

pub struct PoolRow {
    pub template: String,
    pub target_count: u32,
    pub boot_mode: String,
    pub vcpu: u32,
    pub memory_mib: u32,
}

impl PoolRow {
    pub fn from_snapshot(&self) -> bool {
        self.boot_mode == "snapshot"
    }
}

pub fn get_pool(conn: &Connection, template: &str) -> Result<Option<PoolRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT template, target_count, boot_mode, vcpu, memory_mib FROM pools WHERE template = ?1"
    ))?;
    let mut rows = stmt.query_map(params![template], |row| {
        Ok(PoolRow {
            template: row.get(0)?,
            target_count: row.get(1)?,
            boot_mode: row.get(2)?,
            vcpu: row.get(3)?,
            memory_mib: row.get(4)?,
        })
    })?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(row)),
        _ => Ok(None),
    }
}

pub fn get_all_pools(conn: &Connection) -> Result<Vec<PoolRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT template, target_count, boot_mode, vcpu, memory_mib FROM pools"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok(PoolRow {
            template: row.get(0)?,
            target_count: row.get(1)?,
            boot_mode: row.get(2)?,
            vcpu: row.get(3)?,
            memory_mib: row.get(4)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
