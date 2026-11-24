//! The maintenance worker's transitions: creating and destroying pooled
//! slots, validating idle ones, and settling the leases found at startup.

use std::sync::LazyLock;

use pplx_space_sql_locator::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::{Connection, params};

use super::{LeaseKind, SLOT_COLUMNS, SlotHealth, SlotRow, SlotState};

/// Unowned slots below `max_slots` that are ready but not healthy, in slot
/// order: the idle slots revalidation may adopt.
pub fn adoptable_ready_slots(conn: &Connection, max_slots: u32) -> rusqlite::Result<Vec<SlotRow>> {
    static SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT {SLOT_COLUMNS}
         FROM network_slots
         WHERE slot_id < ?1
           AND sandbox_id IS NULL
           AND (state = 'ready' AND health != 'healthy')
         ORDER BY CASE state
                    WHEN 'ready' THEN 0
                    WHEN 'missing' THEN 1
                  END,
                  slot_id"
        ))
    });
    idle_slot_rows(conn, &SQL, max_slots)
}

/// [`adoptable_ready_slots`], then the missing degraded ones: the idle slots
/// startup revalidates.
pub fn startup_recoverable_idle_slots(
    conn: &Connection,
    max_slots: u32,
) -> rusqlite::Result<Vec<SlotRow>> {
    static SQL: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT {SLOT_COLUMNS}
         FROM network_slots
         WHERE slot_id < ?1
           AND sandbox_id IS NULL
           AND ((state = 'ready' AND health != 'healthy')
         OR (state = 'missing' AND health = 'degraded'))
         ORDER BY CASE state
                    WHEN 'ready' THEN 0
                    WHEN 'missing' THEN 1
                  END,
                  slot_id"
        ))
    });
    idle_slot_rows(conn, &SQL, max_slots)
}

fn idle_slot_rows(conn: &Connection, sql: &str, max_slots: u32) -> rusqlite::Result<Vec<SlotRow>> {
    let mut stmt = conn.prepare(sql)?;
    stmt.query_map(params![max_slots], SlotRow::from_row)?
        .collect()
}

/// Claim an unowned missing slot with `health` for creation, leaving it
/// `creating` as a pooled slot.
pub fn claim_missing_for_create(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    health: &str,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'creating', health = 'unknown',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 updated_at = ?2
             WHERE slot_id = ?3 AND generation = ?4
               AND state = 'missing' AND health = ?5 AND sandbox_id IS NULL"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            SqliteU64(now),
            slot_id,
            SqliteU64(generation),
            health
        ],
    )
}

/// Record a creation that set up the slot's network: the slot is a healthy
/// ready pooled slot on `pod_netns_path`.
pub fn mark_created_ready(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    pod_netns_path: Option<&str>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'ready', health = 'healthy', sandbox_id = NULL,
                 sandbox_uid = NULL, pid = NULL, last_error = NULL,
                 pod_netns_path = ?1, lease_kind = ?2, memory_reservation_mib = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 updated_at = ?3, last_reset_at = ?3
             WHERE slot_id = ?4 AND generation = ?5 AND state = 'creating'"
        ),
        params![
            pod_netns_path,
            LeaseKind::Pooled.as_str(),
            SqliteU64(now),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

/// Record a creation that failed and may be retried: the slot is missing and
/// degraded with `error`.
pub fn degrade_failed_create(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    error: &str,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'missing', health = 'degraded',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 last_error = ?2, updated_at = ?3
             WHERE slot_id = ?4 AND generation = ?5 AND state = 'creating'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

/// Record a creation that failed for good: the slot is quarantined as
/// invalid with `error`.
pub fn quarantine_failed_create(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    error: &str,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
             SET state = 'quarantined', health = 'invalid',
                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                 lease_kind = ?1, memory_reservation_mib = NULL,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 last_error = ?2, updated_at = ?3,
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?4 AND generation = ?5 AND state = 'creating'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

/// Return a `creating` slot whose setup was abandoned to missing and
/// degraded, so its host resources are revalidated.
pub fn revalidate_abandoned_setup(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots SET state = 'missing', health = 'degraded',
             last_error = 'abandoned setup completion; revalidating host resources'
             WHERE slot_id = ?1 AND generation = ?2 AND state = 'creating' AND sandbox_id IS NULL"
        ),
        params![slot_id, SqliteU64(generation)],
    )
}

/// Claim an idle ready or missing slot for destruction, leaving it
/// `destroying`. The guard is ownership and state, not generation.
pub fn claim_for_destroy(conn: &Connection, slot_id: u32, now: u64) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
         SET state = 'destroying', health = 'unknown', memory_reservation_mib = NULL,
             updated_at = ?1
         WHERE slot_id = ?2 AND state IN ('ready', 'missing') AND sandbox_id IS NULL
           AND sandbox_uid IS NULL AND pid IS NULL"
        ),
        params![SqliteU64(now), slot_id],
    )
}

/// Record a destroyed slot: it is missing, with no namespace, as a pooled
/// slot.
pub fn finish_destroy(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!("UPDATE network_slots
                     SET state = 'missing', health = 'unknown',
                         sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                         pod_netns_path = NULL, lease_kind = ?1, memory_reservation_mib = NULL,
                         cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                         last_error = NULL, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4 AND state = 'destroying' AND sandbox_id IS NULL"),
        params![LeaseKind::Pooled.as_str(), SqliteU64(now), slot_id, SqliteU64(generation)],
    )
}

/// Record an incomplete teardown on a slot that stays `destroying`.
pub fn record_destroy_failure(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    error: &str,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!("UPDATE network_slots
                 SET state = 'destroying', last_error = ?1, updated_at = ?2
                 WHERE slot_id = ?3 AND generation = ?4 AND state = 'destroying' AND sandbox_id IS NULL"),
        params![error, SqliteU64(now), slot_id, SqliteU64(generation)],
    )
}

/// Record an unowned slot that passed validation in `previous_state`: it is a
/// healthy ready pooled slot on `pod_netns_path`.
pub fn mark_validated_ready(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    previous_state: SlotState,
    pod_netns_path: Option<&str>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                         SET state = 'ready', health = 'healthy', last_error = NULL,
                             sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                             pod_netns_path = ?1, lease_kind = ?2, memory_reservation_mib = NULL,
                             cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                             updated_at = ?3
                         WHERE slot_id = ?4 AND generation = ?5 AND state = ?6
                           AND sandbox_id IS NULL"
        ),
        params![
            pod_netns_path,
            LeaseKind::Pooled.as_str(),
            SqliteU64(now),
            slot_id,
            SqliteU64(generation),
            previous_state.as_str()
        ],
    )
}

/// Record an unowned slot that failed validation in `previous_state`: it is
/// missing and degraded with `error`.
pub fn degrade_failed_validation(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    previous_state: SlotState,
    error: &str,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                         SET state = 'missing', health = 'degraded',
                             sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                             lease_kind = ?1, memory_reservation_mib = NULL,
                             cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                             last_error = ?2, updated_at = ?3
                         WHERE slot_id = ?4 AND generation = ?5 AND state = ?6
                           AND sandbox_id IS NULL"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            error,
            SqliteU64(now),
            slot_id,
            SqliteU64(generation),
            previous_state.as_str()
        ],
    )
}

/// A startup `leasing` lease whose runtime evidence is unknown: mark it so
/// and keep it for a retry. The guard includes the `pid` it was probed with.
pub fn keep_unknown_startup_leasing(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    sandbox_id: &str,
    pid: Option<u32>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots SET health = 'unknown',
                 last_error = 'startup runtime evidence unknown; retrying', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
               AND state = 'leasing' AND pid IS ?5"
        ),
        params![
            SqliteU64(now),
            slot_id,
            SqliteU64(generation),
            sandbox_id,
            pid
        ],
    )
}

/// A startup `leasing` lease whose runtime is live: promote it to `leased`.
/// The guard includes the `pid` it was probed with.
pub fn promote_live_startup_leasing(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    sandbox_id: &str,
    pid: Option<u32>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!("UPDATE network_slots SET state = 'leased', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4 AND state = 'leasing' AND pid IS ?5"),
        params![SqliteU64(now), slot_id, SqliteU64(generation), sandbox_id, pid],
    )
}

/// A startup `leasing` lease whose runtime is gone: hand it to cleanup.
/// The guard includes the `pid` it was probed with.
pub fn dirty_gone_startup_leasing(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    sandbox_id: &str,
    pid: Option<u32>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!("UPDATE network_slots SET state = 'dirty', health = 'unknown',
                 memory_reservation_mib = NULL,
                 last_error = 'interrupted lease runtime is gone after startup re-probe', updated_at = ?1
             WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4 AND state = 'leasing' AND pid IS ?5"),
        params![SqliteU64(now), slot_id, SqliteU64(generation), sandbox_id, pid],
    )
}

/// A startup `leased` lease whose runtime is gone: hand it to cleanup. The
/// guard includes the `pid` it was probed with.
pub fn dirty_gone_startup_leased(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    sandbox_id: &str,
    pid: Option<u32>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots SET state = 'dirty', health = 'unknown',
                         memory_reservation_mib = NULL,
                         last_error = 'startup runtime gone; cleanup queued', updated_at = ?1
                     WHERE slot_id = ?2 AND generation = ?3 AND sandbox_id = ?4
                       AND state = 'leased' AND pid IS ?5"
        ),
        params![
            SqliteU64(now),
            slot_id,
            SqliteU64(generation),
            sandbox_id,
            pid
        ],
    )
}

/// Record the validated `health` and `last_error` of a `leased` lease.
pub fn record_lease_health(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    sandbox_id: &str,
    health: SlotHealth,
    last_error: Option<&str>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                     SET health = ?1, last_error = ?2, updated_at = ?3
                     WHERE slot_id = ?4 AND generation = ?5 AND sandbox_id = ?6
                       AND state = 'leased'"
        ),
        params![
            health.as_str(),
            last_error,
            SqliteU64(now),
            slot_id,
            SqliteU64(generation),
            sandbox_id
        ],
    )
}
