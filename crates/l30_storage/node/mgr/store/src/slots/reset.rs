//! The reset worker's transitions: a dirty or resetting slot is claimed as
//! `resetting`, then finished, deferred back to `dirty`, or quarantined. The
//! claim is guarded on the slot's generation and a `dirty` or `resetting`
//! state, the others on its generation and `resetting`.

use pplx_space_sql_locator::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::{Connection, OptionalExtension as _, params};

use super::LeaseKind;

/// Claim a dirty or resetting slot for reset, leaving it `resetting`.
pub fn begin_reset(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                 SET state = 'resetting', updated_at = ?1
                 WHERE slot_id = ?2 AND generation = ?3
                   AND state IN ('dirty', 'resetting')"
        ),
        params![SqliteU64(now), slot_id, SqliteU64(generation)],
    )
}

/// Finish a pooled reset: the slot is a healthy ready pooled slot on
/// `pod_netns_path`.
pub fn finish_pooled_reset(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    pod_netns_path: Option<&str>,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                             SET state = 'ready', health = 'healthy',
                                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                                 last_error = NULL, pod_netns_path = ?1, lease_kind = ?2,
                                 memory_reservation_mib = NULL,
                                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                                 updated_at = ?3, last_reset_at = ?3
                             WHERE slot_id = ?4 AND generation = ?5
                               AND state = 'resetting'"
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

/// Finish an ephemeral reset: the slot is torn down to a missing pooled slot.
pub fn finish_ephemeral_reset(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                             SET state = 'missing', health = 'unknown',
                                 sandbox_id = NULL, sandbox_uid = NULL, pid = NULL,
                                 last_error = NULL, pod_netns_path = NULL, lease_kind = ?1,
                                 memory_reservation_mib = NULL,
                                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                                 updated_at = ?2, last_reset_at = ?2
                             WHERE slot_id = ?3 AND generation = ?4
                               AND state = 'resetting'"
        ),
        params![
            LeaseKind::Pooled.as_str(),
            SqliteU64(now),
            slot_id,
            SqliteU64(generation)
        ],
    )
}

/// Defer a reset that cannot finish yet: the slot returns to `dirty`,
/// degraded with `error`.
pub fn defer_reset(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    error: &str,
    now: u64,
) -> rusqlite::Result<usize> {
    conn.execute(
        sql!(
            "UPDATE network_slots
                     SET state = 'dirty', health = 'degraded',
                         last_error = ?1, updated_at = ?2
                     WHERE slot_id = ?3 AND generation = ?4
                       AND state = 'resetting'"
        ),
        params![error, SqliteU64(now), slot_id, SqliteU64(generation)],
    )
}

/// Count a failed cleanup reset of the resetting slot and its previous lease
/// `sandbox_id` (`None` for none). Returns the attempts so far, or `None`
/// when the guard matched no row.
pub fn record_cleanup_reset_failure(
    conn: &Connection,
    slot_id: u32,
    generation: u64,
    sandbox_id: Option<&str>,
    now: u64,
) -> rusqlite::Result<Option<u32>> {
    conn.query_row(
        sql!(
            "UPDATE network_slots
         SET cleanup_reset_attempts = cleanup_reset_attempts + 1,
             last_reset_retry_at = ?1,
             updated_at = ?1
         WHERE slot_id = ?2 AND generation = ?3
           AND state = 'resetting'
           AND ((?4 IS NULL AND sandbox_id IS NULL) OR sandbox_id = ?4)
         RETURNING cleanup_reset_attempts"
        ),
        params![SqliteU64(now), slot_id, SqliteU64(generation), sandbox_id],
        |row| row.get::<_, u32>(0),
    )
    .optional()
}

/// Quarantine a resetting slot as invalid with `error`, clearing its previous
/// lease.
pub fn quarantine_reset(
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
                 last_error = ?2, updated_at = ?3,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?4 AND generation = ?5
               AND state = 'resetting'"
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

/// [`quarantine_reset`], leaving the previous lease's columns in place.
pub fn quarantine_reset_preserving_lease(
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
                 last_error = ?1, updated_at = ?2,
                 cleanup_reset_attempts = 0, last_reset_retry_at = NULL,
                 quarantine_count = quarantine_count + 1
             WHERE slot_id = ?3 AND generation = ?4
               AND state = 'resetting'"
        ),
        params![error, SqliteU64(now), slot_id, SqliteU64(generation)],
    )
}
