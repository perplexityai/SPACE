//! Suspended-sandbox reads behind `crate::sandbox_eviction`'s candidate
//! scans and local eviction evidence.

use crate::db::{Connection, OptionalExtension, Result};
use pplx_space_common_svc::sql::sqlite::SqliteU64;
use rusqlite::params;

use crate::db::timestamps::EpochSeconds;

const VALID_SUSPENDED_UPDATED_AT_SQL: &str =
    "s.updated_at <> '' AND s.updated_at NOT GLOB '*[^0-9]*'";

/// Idle witness for eviction candidacy: the sandbox's last real
/// activity, not the row's last write. `updated_at` has an unbounded
/// writer set (periodic reconcilers rewrite rows value-identically and
/// stamp it), so a row can look seconds-old for weeks while its bytes
/// sit cold — the activity clocks inside `auto_lifecycle_state_json`
/// only advance on genuine touches and transitions. NULL-safe two-way
/// max of `last_access_at`/`last_command_at`; rows carrying neither
/// clock (operator/legacy/orphan-adopted rows) fall back to
/// `updated_at`, preserving their previous behavior exactly. The
/// `json_valid` gate is load-bearing: `json_extract` aborts the whole
/// statement on malformed JSON, so without it one corrupt row would
/// fail every scan and shield the valid cold rows around it — a
/// malformed blob falls back to `updated_at` like a missing one. Each
/// clock is additionally type-gated to a non-negative integer: our
/// writers only ever store u64 epoch seconds, so anything else
/// (negative, fractional, string) is corruption and must read as
/// absent rather than fail the `SqliteU64` row decode. The CAS claim
/// deliberately keeps raw `updated_at`: "any write means the row is in
/// flux" is the right semantic there and the wrong one here.
///
/// The partial eviction index prunes to suspended rows before evaluating
/// this expression. If scan cost grows with node row counts, the escape
/// hatch is an index on this deterministic expression or a virtual generated
/// column — not a writer-maintained copy, which would reintroduce the drift
/// this witness exists to escape.
/// Scalar `MAX` returns NULL if ANY argument is NULL, so absent or
/// corrupt clocks are mapped to a -1 sentinel first (real clocks are
/// type-gated non-negative, so the sentinel cannot collide) and
/// `NULLIF` restores NULL when both were absent.
const IDLE_WITNESS_SQL: &str = "COALESCE(
             CASE
               WHEN json_valid(s.auto_lifecycle_state_json) THEN
                 NULLIF(
                   MAX(
                     COALESCE(CASE
                       WHEN typeof(json_extract(s.auto_lifecycle_state_json, '$.last_access_at')) = 'integer'
                        AND json_extract(s.auto_lifecycle_state_json, '$.last_access_at') >= 0
                       THEN json_extract(s.auto_lifecycle_state_json, '$.last_access_at')
                     END, -1),
                     COALESCE(CASE
                       WHEN typeof(json_extract(s.auto_lifecycle_state_json, '$.last_command_at')) = 'integer'
                        AND json_extract(s.auto_lifecycle_state_json, '$.last_command_at') >= 0
                       THEN json_extract(s.auto_lifecycle_state_json, '$.last_command_at')
                     END, -1)
                   ),
                 -1)
             END,
             CAST(s.updated_at AS INTEGER)
           )";

/// `(sandbox_id, idle witness)` of cold suspended sandboxes after the
/// `(cursor_witness, cursor_sandbox_id)` keyset position, in witness then id
/// order.
pub(crate) fn idle_suspended_candidates(
    conn: &Connection,
    cutoff: i64,
    cursor_witness: Option<i64>,
    cursor_sandbox_id: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, u64)>> {
    let sql = format!(
        "SELECT s.id, {IDLE_WITNESS_SQL} FROM sandboxes s
         WHERE s.state = 'suspended'
           AND {VALID_SUSPENDED_UPDATED_AT_SQL}
           AND {IDLE_WITNESS_SQL} <= ?1
           AND (
             ?2 IS NULL
             OR {IDLE_WITNESS_SQL} > ?2
             OR ({IDLE_WITNESS_SQL} = ?2 AND s.id > ?3)
           )
           AND (
             s.current_local_artifact_id IS NOT NULL
             OR EXISTS (
               SELECT 1 FROM jobs j
               WHERE j.kind = 'snapshot_upload'
                 AND j.subject_id = s.id
                 AND json_extract(j.payload, '$.snapshot_id') IS NOT NULL
             )
           )
           AND NOT EXISTS (
             SELECT 1 FROM jobs j
             WHERE j.kind = 'snapshot_upload'
               AND j.subject_id = s.id
               AND j.status IN ('pending', 'running')
           )
         ORDER BY {IDLE_WITNESS_SQL} ASC, s.id ASC
         LIMIT ?4"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(
            params![cutoff, cursor_witness, cursor_sandbox_id, limit as i64],
            |row| {
                let sandbox_id: String = row.get(0)?;
                let idle_witness_secs = row.get::<_, SqliteU64>(1)?.0;
                Ok((sandbox_id, idle_witness_secs))
            },
        )?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

/// Ids of cold suspended sandboxes with neither a pinned artifact nor an
/// upload job carrying a snapshot id, oldest witness first.
pub(crate) fn legacy_reupload_candidates(
    conn: &Connection,
    idle_cutoff_epoch_secs: u64,
    limit: usize,
) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT s.id FROM sandboxes s
         WHERE s.state = 'suspended'
           AND {VALID_SUSPENDED_UPDATED_AT_SQL}
           AND {IDLE_WITNESS_SQL} <= ?1
           AND s.current_local_artifact_id IS NULL
           AND NOT EXISTS (
             SELECT 1 FROM jobs j
             WHERE j.kind = 'snapshot_upload'
               AND j.subject_id = s.id
               AND j.status IN ('pending', 'running')
           )
           AND NOT EXISTS (
             SELECT 1 FROM jobs j
             WHERE j.kind = 'snapshot_upload'
               AND j.subject_id = s.id
               AND json_extract(j.payload, '$.snapshot_id') IS NOT NULL
           )
         ORDER BY {IDLE_WITNESS_SQL} ASC
         LIMIT ?2"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(
            params![SqliteU64(idle_cutoff_epoch_secs), limit as i64],
            |r| r.get(0),
        )?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

pub(crate) fn count_malformed_suspended_updated_at(conn: &Connection) -> Result<usize> {
    let sql = format!(
        "SELECT COUNT(*) FROM sandboxes s
         WHERE s.state = 'suspended'
           AND NOT ({VALID_SUSPENDED_UPDATED_AT_SQL})"
    );
    conn.query_row(&sql, [], |row| {
        let count: i64 = row.get(0)?;
        Ok(usize::try_from(count).unwrap_or_default())
    })
}

/// Every local `sandboxes` row, whatever its state. node-ctl's full census
/// publishes one entry per row, so this is the count that sizes it.
pub(crate) fn count_resident_rows(conn: &Connection) -> Result<usize> {
    conn.query_row("SELECT COUNT(*) FROM sandboxes", [], |row| {
        let count: i64 = row.get(0)?;
        usize::try_from(count).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, count))
    })
}

/// The sandbox's pinned artifact id and `updated_at`, `None` without a row.
pub(crate) fn artifact_pin_and_updated_at(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<(Option<String>, EpochSeconds)>> {
    conn.query_row(
        "SELECT current_local_artifact_id, updated_at FROM sandboxes WHERE id = ?1",
        params![sandbox_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
}

/// `(artifact_kind, local_artifact_id, backed up)` of the artifact `pin`
/// names, only if `sandbox_id` owns it.
pub(crate) fn owned_artifact_evidence(
    conn: &Connection,
    pin: &str,
    sandbox_id: &str,
) -> Result<Option<(String, String, bool)>> {
    conn.query_row(
        "SELECT artifact_kind, local_artifact_id, backup_completed_at IS NOT NULL
         FROM local_artifacts
        WHERE local_artifact_id = ?1 AND sandbox_id = ?2",
        params![pin, sandbox_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .optional()
}

/// The payload `snapshot_id` and `created_at` of the sandbox's newest
/// snapshot-upload job, whatever its status.
pub(crate) fn latest_upload_job(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<(Option<String>, String)>> {
    conn.query_row(
        "SELECT json_extract(j.payload, '$.snapshot_id'), j.created_at FROM jobs j
         WHERE j.kind = 'snapshot_upload' AND j.subject_id = ?1
         ORDER BY j.created_at DESC, j.job_id DESC
         LIMIT 1",
        params![sandbox_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
}
