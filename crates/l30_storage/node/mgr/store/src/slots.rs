//! `network_slots`: one row per network slot, with its persisted state,
//! health and lease.

use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use pplx_space_sql_locator::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    Missing,
    Creating,
    Ready,
    Leasing,
    Leased,
    Dirty,
    Resetting,
    Quarantined,
    Destroying,
}

impl SlotState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Leasing => "leasing",
            Self::Leased => "leased",
            Self::Dirty => "dirty",
            Self::Resetting => "resetting",
            Self::Quarantined => "quarantined",
            Self::Destroying => "destroying",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotHealth {
    Unknown,
    Healthy,
    Degraded,
    Invalid,
}

impl SlotHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseKind {
    Pooled,
    Ephemeral,
}

impl LeaseKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pooled => "pooled",
            Self::Ephemeral => "ephemeral",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid persisted {kind}: {value}")]
pub struct PoolValueParseError {
    kind: &'static str,
    value: String,
}

impl FromStr for SlotState {
    type Err = PoolValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "missing" => Ok(Self::Missing),
            "creating" => Ok(Self::Creating),
            "ready" => Ok(Self::Ready),
            "leasing" => Ok(Self::Leasing),
            "leased" => Ok(Self::Leased),
            "dirty" => Ok(Self::Dirty),
            "resetting" => Ok(Self::Resetting),
            "quarantined" => Ok(Self::Quarantined),
            "destroying" => Ok(Self::Destroying),
            _ => Err(PoolValueParseError {
                kind: "slot state",
                value: value.into(),
            }),
        }
    }
}

impl FromStr for SlotHealth {
    type Err = PoolValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unknown" => Ok(Self::Unknown),
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "invalid" => Ok(Self::Invalid),
            _ => Err(PoolValueParseError {
                kind: "slot health",
                value: value.into(),
            }),
        }
    }
}

impl FromStr for LeaseKind {
    type Err = PoolValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pooled" => Ok(Self::Pooled),
            "ephemeral" => Ok(Self::Ephemeral),
            _ => Err(PoolValueParseError {
                kind: "lease kind",
                value: value.into(),
            }),
        }
    }
}

impl fmt::Display for SlotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SlotHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for LeaseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The `SELECT` list [`SlotRow::from_row`] decodes, in its column order.
///
/// node-mgr's slot reads that still build their predicate at the call site
/// interpolate it with [`SlotRow::from_row`] until they move here.
pub const SLOT_COLUMNS: &str = "slot_id, generation, state, health, sandbox_id, sandbox_uid,
                            pod_netns_path, lease_kind, pid, last_error, memory_reservation_mib";

/// One `network_slots` row, read through [`SLOT_COLUMNS`].
#[derive(Clone, Debug)]
pub struct SlotRow {
    pub slot_id: u32,
    pub generation: u64,
    pub state: SlotState,
    pub health: SlotHealth,
    pub sandbox_id: Option<String>,
    pub sandbox_uid: Option<u32>,
    pub pod_netns_path: Option<String>,
    pub lease_kind: LeaseKind,
    pub pid: Option<u32>,
    pub last_error: Option<String>,
    pub memory_reservation_mib: Option<u64>,
}

impl SlotRow {
    /// Decode a row selected with [`SLOT_COLUMNS`]. An unknown state, health
    /// or lease kind is a conversion failure of its column.
    pub fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            slot_id: row.get::<_, u32>(0)?,
            generation: row.get::<_, SqliteU64>(1)?.0,
            state: pool_value(row, 2)?,
            health: pool_value(row, 3)?,
            sandbox_id: row.get::<_, Option<String>>(4)?,
            sandbox_uid: row.get::<_, Option<u32>>(5)?,
            pod_netns_path: row.get::<_, Option<String>>(6)?,
            lease_kind: pool_value(row, 7)?,
            pid: row.get::<_, Option<u32>>(8)?,
            last_error: row.get::<_, Option<String>>(9)?,
            memory_reservation_mib: row.get::<_, Option<SqliteU64>>(10)?.map(|value| value.0),
        })
    }
}

fn pool_value<T: FromStr<Err = PoolValueParseError>>(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<T> {
    row.get::<_, String>(index)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

/// Up to `limit` dirty or resetting slots as `(slot_id, generation)`, in slot
/// order starting after `cursor` and wrapping around.
pub fn reset_candidates(
    conn: &Connection,
    cursor: u32,
    limit: u32,
) -> rusqlite::Result<Vec<(u32, u64)>> {
    conn.prepare(sql!(
        "SELECT slot_id, generation FROM network_slots
         WHERE state IN ('dirty', 'resetting')
         ORDER BY (slot_id <= ?1), slot_id LIMIT ?2"
    ))?
    .query_map(params![cursor, limit], |row| {
        Ok((row.get(0)?, row.get::<_, SqliteU64>(1)?.0))
    })?
    .collect()
}

/// Up to `limit` creating or destroying slots that no sandbox owns, as
/// `(slot_id, generation, state)`, in slot order starting after `cursor` and
/// wrapping around.
pub fn abandoned_maintenance(
    conn: &Connection,
    cursor: u32,
    limit: u32,
) -> rusqlite::Result<Vec<(u32, u64, SlotState)>> {
    conn.prepare(sql!(
        "SELECT slot_id, generation, state FROM network_slots
         WHERE state IN ('creating', 'destroying') AND sandbox_id IS NULL
         ORDER BY (slot_id <= ?1), slot_id LIMIT ?2"
    ))?
    .query_map(params![cursor, limit], |row| {
        Ok((
            row.get(0)?,
            row.get::<_, SqliteU64>(1)?.0,
            pool_value(row, 2)?,
        ))
    })?
    .collect()
}

pub fn pending_reset_count(conn: &Connection) -> rusqlite::Result<u32> {
    conn.query_row(
        sql!("SELECT COUNT(*) FROM network_slots WHERE state IN ('dirty', 'resetting')"),
        [],
        |row| row.get(0),
    )
}

/// Add slot `slot_id` as a missing pooled slot unless it already has a row.
pub fn insert_missing_slot(
    conn: &Connection,
    slot_id: u32,
    slot_label: &str,
    schema_version: u32,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "INSERT OR IGNORE INTO network_slots
             (slot_id, slot_label, generation, schema_version, state, health,
              created_at, updated_at, lease_kind)
             VALUES (?1, ?2, 0, ?3, 'missing', 'unknown', ?4, ?4, ?5)"
        ),
        params![
            slot_id,
            slot_label,
            schema_version,
            SqliteU64(now),
            LeaseKind::Pooled.as_str()
        ],
    )?;
    Ok(())
}

/// Startup repair: clear every slot at or beyond `max_slots` back to a
/// missing pooled slot on `schema_version`.
pub fn clear_slots_beyond_capacity(
    conn: &Connection,
    max_slots: u32,
    schema_version: u32,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'missing', health = 'unknown', sandbox_id = NULL,
                 sandbox_uid = NULL, pid = NULL, pod_netns_path = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL, last_error = NULL, schema_version = ?2,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 last_schema_migration_at = ?3, updated_at = ?3
             WHERE slot_id >= ?4"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            schema_version,
            SqliteU64(now),
            max_slots
        ],
    )?;
    Ok(())
}

/// Startup repair: rebuild unowned slots written by another schema version,
/// except those being destroyed.
pub fn degrade_unowned_slots_on_other_schema(
    conn: &Connection,
    schema_version: u32,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL, last_error = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 schema_version = ?2, last_schema_migration_at = ?3, updated_at = ?3
             WHERE schema_version != ?2 AND sandbox_id IS NULL AND state != 'destroying'"
        ),
        params![LeaseKind::Pooled.as_str(), schema_version, SqliteU64(now)],
    )?;
    Ok(())
}

/// Startup repair: ready slots are unvalidated until startup checks them.
pub fn mark_ready_slots_unvalidated(conn: &Connection, now: u64) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'ready', health = 'unknown', updated_at = ?1
             WHERE state = 'ready'"
        ),
        params![SqliteU64(now)],
    )?;
    Ok(())
}

/// Startup repair: rebuild unowned slots whose setup or reset was interrupted.
pub fn degrade_interrupted_setup(conn: &Connection, now: u64) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 last_error = COALESCE(last_error, 'interrupted network slot setup found at startup'),
                 updated_at = ?2
             WHERE state IN ('creating', 'resetting') AND sandbox_id IS NULL"
        ),
        params![LeaseKind::Pooled.as_str(), SqliteU64(now)],
    )?;
    Ok(())
}

/// Startup repair: stamp every remaining row with `schema_version`.
pub fn stamp_schema_version(
    conn: &Connection,
    schema_version: u32,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET schema_version = ?1, last_schema_migration_at = ?2
             WHERE schema_version != ?1"
        ),
        params![schema_version, SqliteU64(now)],
    )?;
    Ok(())
}

/// Give active leases recorded without a memory reservation the legacy one.
pub fn backfill_active_memory_reservations(
    conn: &Connection,
    legacy_active_memory_reservation_mib: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET memory_reservation_mib = ?1
             WHERE state IN ('leasing', 'leased') AND memory_reservation_mib IS NULL"
        ),
        params![SqliteU64(legacy_active_memory_reservation_mib)],
    )?;
    Ok(())
}

pub struct MemoryReservationRow {
    pub slot_id: u32,
    pub sandbox_id: String,
    pub generation: u64,
    pub memory_reservation_mib: Option<u64>,
}

/// The memory reservation of every leasing or leased slot that a sandbox owns.
pub fn active_memory_reservations(
    conn: &Connection,
) -> rusqlite::Result<Vec<MemoryReservationRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT slot_id, sandbox_id, generation, memory_reservation_mib
         FROM network_slots
         WHERE state IN ('leasing', 'leased') AND sandbox_id IS NOT NULL"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok(MemoryReservationRow {
            slot_id: row.get(0)?,
            sandbox_id: row.get(1)?,
            generation: row.get::<_, SqliteU64>(2)?.0,
            memory_reservation_mib: row.get::<_, Option<SqliteU64>>(3)?.map(|value| value.0),
        })
    })?;
    rows.collect()
}

/// Runtime reconcile: quarantine slot `slot_id` as invalid with `error`.
/// Keyed on `slot_id` alone, with no generation or state check: correct only
/// while nothing else can write the slot between the caller's read and this
/// write.
pub fn quarantine_slot(
    conn: &Connection,
    slot_id: u32,
    error: &str,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'quarantined', health = 'invalid',
                 lease_kind = ?1, last_error = ?2, updated_at = ?3,
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?4"
        ),
        params![LeaseKind::Pooled.as_str(), error, SqliteU64(now), slot_id],
    )?;
    Ok(())
}

/// Runtime reconcile: schedule slot `slot_id`, if below `max_slots`, for
/// rebuild as a missing pooled slot with `error`.
/// Keyed on `slot_id` (bounded by `max_slots`), with no generation or state
/// check: correct only while nothing else can write the slot between the
/// caller's read and this write.
pub fn degrade_slot_for_rebuild(
    conn: &Connection,
    slot_id: u32,
    max_slots: u32,
    error: &str,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 last_error = ?2, updated_at = ?3
             WHERE slot_id = ?4 AND slot_id < ?5"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now),
            slot_id,
            max_slots
        ],
    )?;
    Ok(())
}

pub fn slot_by_id(conn: &Connection, slot_id: u32) -> rusqlite::Result<Option<SlotRow>> {
    static SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT {SLOT_COLUMNS} FROM network_slots WHERE slot_id = ?1"
        ))
    });
    let mut stmt = conn.prepare(&SQL)?;
    stmt.query_row(params![slot_id], SlotRow::from_row)
        .optional()
}

/// Leased slots that a sandbox owns, in slot order.
pub fn live_leased_slots(conn: &Connection) -> rusqlite::Result<Vec<SlotRow>> {
    static SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT {SLOT_COLUMNS}
             FROM network_slots
             WHERE state = 'leased' AND sandbox_id IS NOT NULL
             ORDER BY slot_id"
        ))
    });
    let mut stmt = conn.prepare(&SQL)?;
    let rows = stmt.query_map([], SlotRow::from_row)?;
    rows.collect()
}

/// A lease allocated but not yet launched: `leased` with no recorded PID.
pub struct UnlaunchedLease {
    pub slot_id: u32,
    pub generation: u64,
    pub sandbox_id: String,
    pub updated_at: u64,
}

pub fn unlaunched_leases(conn: &Connection) -> rusqlite::Result<Vec<UnlaunchedLease>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT slot_id, generation, sandbox_id, updated_at FROM network_slots
         WHERE state = 'leased' AND sandbox_id IS NOT NULL AND pid IS NULL"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok(UnlaunchedLease {
            slot_id: row.get(0)?,
            generation: row.get::<_, SqliteU64>(1)?.0,
            sandbox_id: row.get(2)?,
            updated_at: row.get::<_, SqliteU64>(3)?.0,
        })
    })?;
    rows.collect()
}

/// The leasing or leased slot of `sandbox_id`: the one exact match, else
/// the one slot whose sandbox id starts with it. More than one match at
/// either step is no lease.
pub fn current_lease_by_sandbox(
    conn: &Connection,
    sandbox_id: &str,
) -> rusqlite::Result<Option<SlotRow>> {
    static EXACT_SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT {SLOT_COLUMNS} FROM network_slots
             WHERE sandbox_id = ?1 AND state IN ('leasing', 'leased') ORDER BY slot_id"
        ))
    });
    static PREFIX_SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT {SLOT_COLUMNS} FROM network_slots
             WHERE sandbox_id LIKE ?1 AND state IN ('leasing', 'leased') ORDER BY slot_id"
        ))
    });
    let exact = slot_rows(conn, &EXACT_SQL, sandbox_id)?;
    if exact.len() == 1 {
        return Ok(exact.into_iter().next());
    }
    if exact.len() > 1 {
        return Ok(None);
    }

    let prefix = format!("{sandbox_id}%");
    let records = slot_rows(conn, &PREFIX_SQL, &prefix)?;
    if records.len() == 1 {
        Ok(records.into_iter().next())
    } else {
        Ok(None)
    }
}

fn slot_rows(conn: &Connection, sql: &str, value: &str) -> rusqlite::Result<Vec<SlotRow>> {
    let mut stmt = conn.prepare(sql)?;
    stmt.query_map(params![value], SlotRow::from_row)?.collect()
}

/// A sandbox's current lease, read in a release's transaction
/// ([`lease_to_release`]).
pub struct LeaseToRelease<'tx> {
    conn: &'tx Connection,
    row: SlotRow,
}

/// [`current_lease_by_sandbox`] inside `tx`, holding the write keyed on the
/// slot it read.
pub fn lease_to_release<'tx>(
    tx: &'tx Transaction<'_>,
    sandbox_id: &str,
) -> rusqlite::Result<Option<LeaseToRelease<'tx>>> {
    let conn: &Connection = tx;
    Ok(current_lease_by_sandbox(conn, sandbox_id)?.map(|row| LeaseToRelease { conn, row }))
}

impl LeaseToRelease<'_> {
    pub fn row(&self) -> &SlotRow {
        &self.row
    }

    /// Quarantine the slot read, which is active with no sandbox lease.
    pub fn quarantine_unowned(self, now: u64) -> rusqlite::Result<()> {
        self.conn.execute(
            sql!(
                "UPDATE network_slots
                 SET state = 'quarantined', health = 'invalid',
                     last_error = 'active slot has no sandbox lease',
                     updated_at = ?1,
                     quarantine_count = quarantine_count + 1
                 WHERE slot_id = ?2"
            ),
            params![SqliteU64(now), self.row.slot_id],
        )?;
        Ok(())
    }
}

/// The lowest healthy ready slot no sandbox owns, read in an allocation's
/// transaction ([`ready_slot_to_lease`]).
pub struct ReadySlot<'tx> {
    conn: &'tx Connection,
    slot_id: u32,
    generation: u64,
}

/// What [`ReadySlot::lease`] writes onto the slot.
pub struct PooledLease<'a> {
    /// The lease's generation, the slot's next one.
    pub generation: u64,
    pub sandbox_id: &'a str,
    pub sandbox_uid: u32,
    pub memory_reservation_mib: u64,
    pub now: u64,
}

/// The lowest healthy ready unowned slot on `pod_netns_path` (the root
/// namespace when `None`) inside `tx`, holding the lease write keyed on it.
pub fn ready_slot_to_lease<'tx>(
    tx: &'tx Transaction<'_>,
    pod_netns_path: Option<&str>,
) -> rusqlite::Result<Option<ReadySlot<'tx>>> {
    let conn: &Connection = tx;
    let row = conn
        .query_row(
            sql!(
                "SELECT slot_id, generation FROM network_slots
                 WHERE state = 'ready' AND health = 'healthy' AND sandbox_id IS NULL
                   AND ((pod_netns_path IS NULL AND ?1 IS NULL) OR pod_netns_path = ?1)
                 ORDER BY slot_id
                 LIMIT 1"
            ),
            params![pod_netns_path],
            |row| Ok((row.get::<_, u32>(0)?, row.get::<_, SqliteU64>(1)?.0)),
        )
        .optional()?;
    Ok(row.map(|(slot_id, generation)| ReadySlot {
        conn,
        slot_id,
        generation,
    }))
}

impl ReadySlot<'_> {
    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Lease the slot read to `lease.sandbox_id` as a healthy pooled lease.
    pub fn lease(self, lease: &PooledLease<'_>) -> rusqlite::Result<()> {
        self.conn.execute(
            sql!(
                "UPDATE network_slots
                 SET generation = ?1, state = 'leased', health = 'healthy',
                     sandbox_id = ?2, sandbox_uid = ?3, pid = NULL,
                     lease_kind = ?4, memory_reservation_mib = ?5, last_error = NULL,
                     cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                     updated_at = ?6, reuse_count = reuse_count + 1
                 WHERE slot_id = ?7"
            ),
            params![
                SqliteU64(lease.generation),
                lease.sandbox_id,
                lease.sandbox_uid,
                LeaseKind::Pooled.as_str(),
                SqliteU64(lease.memory_reservation_mib),
                SqliteU64(lease.now),
                self.slot_id
            ],
        )?;
        Ok(())
    }
}

/// A free slot an ephemeral lease can take ([`IdleSlotScan::rows`]).
pub struct IdleSlot {
    pub slot_id: u32,
    pub generation: u64,
    pub state: String,
    pub pod_netns_path: Option<String>,
}

/// The candidate read of an ephemeral claim, prepared in the caller's
/// transaction. Drop it before committing.
pub struct IdleSlotScan<'tx> {
    statement: rusqlite::Statement<'tx>,
}

pub fn idle_slot_scan<'tx>(tx: &'tx Transaction<'_>) -> rusqlite::Result<IdleSlotScan<'tx>> {
    let conn: &'tx Connection = tx;
    Ok(IdleSlotScan {
        statement: conn.prepare(sql!(
            "SELECT slot_id, generation, state, pod_netns_path FROM network_slots
             WHERE slot_id < ?1 AND sandbox_id IS NULL
               AND (state = 'missing' OR (state = 'ready' AND health = 'healthy'))
             ORDER BY CASE state WHEN 'missing' THEN 0 ELSE 1 END, slot_id"
        ))?,
    })
}

impl IdleSlotScan<'_> {
    /// The unowned slots below `max_slots` that are missing or healthy and
    /// ready, missing ones first. Each row is decoded, and offered to the
    /// caller, only when the caller steps to it.
    pub fn rows(
        &mut self,
        max_slots: u32,
    ) -> rusqlite::Result<impl Iterator<Item = rusqlite::Result<IdleSlot>> + '_> {
        self.statement.query_map(params![max_slots], |row| {
            Ok(IdleSlot {
                slot_id: row.get::<_, u32>(0)?,
                generation: row.get::<_, SqliteU64>(1)?.0,
                state: row.get::<_, String>(2)?,
                pod_netns_path: row.get::<_, Option<String>>(3)?,
            })
        })
    }
}

/// The number of slots in each `(state, health)` pair, as stored.
pub fn state_health_counts(conn: &Connection) -> rusqlite::Result<Vec<(String, String, u32)>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT state, health, COUNT(*) FROM network_slots GROUP BY state, health"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, u32>(2)?,
        ))
    })?;
    rows.collect()
}

/// The lowest missing slot with `health` below `max_slots`, as
/// `(slot_id, generation)`. It does not check ownership; the caller's guarded
/// write does.
pub fn missing_slot_to_create(
    conn: &Connection,
    health: &str,
    max_slots: u32,
) -> rusqlite::Result<Option<(u32, u64)>> {
    conn.query_row(
        sql!(
            "SELECT slot_id, generation FROM network_slots
             WHERE state = 'missing' AND health = ?1 AND slot_id < ?2
             ORDER BY slot_id
             LIMIT 1"
        ),
        params![health, max_slots],
        |row| Ok((row.get::<_, u32>(0)?, row.get::<_, SqliteU64>(1)?.0)),
    )
    .optional()
}

/// The highest slot to destroy: an idle ready slot while more than
/// `retained` are ready, or with `retained` zero an idle missing slot that
/// still has health or a namespace to tear down.
pub fn slot_to_destroy(conn: &Connection, retained: u32) -> rusqlite::Result<Option<u32>> {
    conn.query_row(
        sql!(
            "SELECT slot_id FROM network_slots
             WHERE sandbox_id IS NULL AND sandbox_uid IS NULL AND pid IS NULL
               AND ((state = 'ready' AND
                    (SELECT COUNT(*) FROM network_slots WHERE state = 'ready') > ?1)
                 OR (?1 = 0 AND state = 'missing' AND
                     (health != 'unknown' OR pod_netns_path IS NOT NULL)))
             ORDER BY slot_id DESC
             LIMIT 1"
        ),
        params![retained],
        |row| row.get::<_, u32>(0),
    )
    .optional()
}
