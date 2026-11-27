//! Bounded idle retention of runtime-upgrade attempt outcomes and abandonments.
//!
//! `sandbox_runtime_upgrade_attempt_outcomes` and
//! `sandbox_runtime_upgrade_abandoned` outlive `sandboxes` so delayed retries
//! of a settled attempt and late calls for an abandoned operation keep getting
//! their deterministic refusal after local copy eviction or deletion. Initial
//! writes stamp `last_seen_at = recorded_at`, and refusal or repeat-abandon
//! hits refresh `last_seen_at` monotonically under the same connection lock
//! that read the record without changing `recorded_at`. Once neither the
//! sandbox nor the operation holds local state and `last_seen_at` (or
//! `recorded_at` when `last_seen_at` is `NULL`) has aged past the retention
//! window, [`RuntimeUpgradeRetentionPruner`] deletes the idle rows in bounded
//! batches.

use std::{collections::BTreeMap, time::Duration};

use pplx_space_sandbox_model::SandboxId;
use rusqlite::{OptionalExtension as _, params, types::Value};

use crate::db::{Connection, Result, RuntimeUpgradeOperationId};

const ROWS_PER_BATCH: usize = 128;

/// SQLite predicate matching `NULL` or a valid non-negative integer epoch
/// timestamp older than `$bound`, leaving malformed values untouched so refresh
/// paths follow the same fail-closed timestamp rule as pruning.
macro_rules! stale_last_seen_sql {
    ($col:literal, $bound:literal) => {
        concat!(
            "(",
            $col,
            " IS NULL OR (typeof(",
            $col,
            ") = 'integer' AND ",
            $col,
            " >= 0 AND ",
            $col,
            " < ",
            $bound,
            ") OR (typeof(",
            $col,
            ") = 'text' AND ",
            $col,
            " != '' AND instr(",
            $col,
            ", char(0)) = 0 AND NOT ",
            $col,
            " GLOB '*[^0-9]*' AND CAST(",
            $col,
            " AS INTEGER) < ",
            $bound,
            "))"
        )
    };
}
pub(super) use stale_last_seen_sql;

/// Refresh `last_seen_at` monotonically on one settled attempt outcome without
/// changing `recorded_at`.
pub fn touch_runtime_upgrade_attempt_outcome(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
) -> Result<()> {
    let seen_at = super::now()?;
    let seen_secs: i64 = seen_at
        .parse()
        .map_err(crate::db::Error::to_sql_conversion)?;
    conn.execute(
        concat!(
            "UPDATE sandbox_runtime_upgrade_attempt_outcomes
                SET last_seen_at = ?3
              WHERE operation_id = ?1
                AND attempt_id = ?2
                AND ",
            stale_last_seen_sql!("last_seen_at", "?4"),
        ),
        params![operation_id.to_string(), attempt_id, seen_at, seen_secs],
    )?;
    Ok(())
}

/// Refresh `last_seen_at` monotonically on every retained row of `operation_id`
/// belonging to `sandbox_id` without changing `recorded_at`.
pub fn touch_retained_runtime_upgrade_operation(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
) -> Result<()> {
    let seen_at = super::now()?;
    let seen_secs: i64 = seen_at
        .parse()
        .map_err(crate::db::Error::to_sql_conversion)?;
    let operation_key = operation_id.to_string();
    let tx = super::immediate_transaction(conn)?;
    tx.execute(
        concat!(
            "UPDATE sandbox_runtime_upgrade_abandoned
                SET last_seen_at = ?3
              WHERE operation_id = ?1
                AND sandbox_id = ?2
                AND ",
            stale_last_seen_sql!("last_seen_at", "?4"),
        ),
        params![operation_key, sandbox_id, seen_at, seen_secs],
    )?;
    touch_attempt_outcomes_for_sandbox(&tx, &operation_key, sandbox_id, &seen_at, seen_secs)?;
    tx.commit()
}

pub(super) fn touch_attempt_outcomes_for_sandbox(
    conn: &Connection,
    operation_key: &str,
    sandbox_id: &str,
    seen_at: &str,
    seen_secs: i64,
) -> Result<()> {
    conn.execute(
        concat!(
            "UPDATE sandbox_runtime_upgrade_attempt_outcomes
                SET last_seen_at = ?3
              WHERE operation_id = ?1
                AND sandbox_id = ?2
                AND ",
            stale_last_seen_sql!("last_seen_at", "?4"),
        ),
        params![operation_key, sandbox_id, seen_at, seen_secs],
    )?;
    Ok(())
}

#[derive(Debug)]
struct TableCursor {
    next: i64,
    through: Option<i64>,
}

impl Default for TableCursor {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

impl TableCursor {
    fn advance(&mut self, rows_len: usize, last_key: Option<i64>, through: i64) {
        if rows_len < ROWS_PER_BATCH || last_key == Some(through) {
            *self = Self::default();
        } else if let Some(last_key) = last_key {
            self.next = last_key + 1;
        }
    }
}

/// Cursor-based pruner over `sandbox_runtime_upgrade_attempt_outcomes` and
/// `sandbox_runtime_upgrade_abandoned`. Retained or unparseable rows never
/// starve newer rows behind them.
#[derive(Debug, Default)]
pub struct RuntimeUpgradeRetentionPruner {
    attempt_outcomes: TableCursor,
    abandoned: TableCursor,
}

/// Summary of one [`RuntimeUpgradeRetentionPruner`] batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeUpgradePruneBatch {
    pub examined_attempt_outcomes: usize,
    pub deleted_attempt_outcomes: usize,
    pub examined_abandoned: usize,
    pub deleted_abandoned: usize,
}

impl RuntimeUpgradePruneBatch {
    pub const fn total_deleted(&self) -> usize {
        self.deleted_attempt_outcomes + self.deleted_abandoned
    }
}

impl RuntimeUpgradeRetentionPruner {
    /// Scan up to one batch per table at Unix epoch second `now`, deleting
    /// rows whose effective last-seen timestamp is strictly older than
    /// `retention` and whose sandbox and operation hold no local state.
    pub fn next_batch(
        &mut self,
        conn: &Connection,
        now: u64,
        retention: Duration,
    ) -> Result<RuntimeUpgradePruneBatch> {
        self.next_batch_with_busy_guard(conn, now, retention, |_| Some(()))
    }

    /// Like [`Self::next_batch`], skipping any candidate whose sandbox fails
    /// `acquire_idle_sandbox` (for example, when an in-flight RPC holds the
    /// per-sandbox runtime-upgrade mutex). Returned guards stay held until the
    /// batch transaction commits or rolls back.
    pub fn next_batch_with_busy_guard<G, F>(
        &mut self,
        conn: &Connection,
        now: u64,
        retention: Duration,
        acquire_idle_sandbox: F,
    ) -> Result<RuntimeUpgradePruneBatch>
    where
        F: FnMut(&str) -> Option<G>,
    {
        Ok(self
            .next_batch_with_clock(conn, retention, acquire_idle_sandbox, || Ok(Some(now)))?
            .unwrap_or_default())
    }

    /// Like [`Self::next_batch_with_busy_guard`], sampling `sample_now` only
    /// after opening the `IMMEDIATE` transaction and acquiring every candidate
    /// sandbox's idle guard so the cutoff reflects the instant of the row
    /// decisions. Returning `Ok(None)` skips the batch without advancing or
    /// resetting cursors.
    pub fn next_batch_with_clock<G, F, C>(
        &mut self,
        conn: &Connection,
        retention: Duration,
        mut acquire_idle_sandbox: F,
        sample_now: C,
    ) -> Result<Option<RuntimeUpgradePruneBatch>>
    where
        F: FnMut(&str) -> Option<G>,
        C: FnOnce() -> Result<Option<u64>>,
    {
        let result = self.prune_batch(conn, retention, &mut acquire_idle_sandbox, sample_now);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn prune_batch<G, F, C>(
        &mut self,
        conn: &Connection,
        retention: Duration,
        acquire_idle_sandbox: &mut F,
        sample_now: C,
    ) -> Result<Option<RuntimeUpgradePruneBatch>>
    where
        F: FnMut(&str) -> Option<G>,
        C: FnOnce() -> Result<Option<u64>>,
    {
        // Declare `sandbox_guards` before `tx` so reverse drop order keeps
        // every acquired per-sandbox guard held until `tx` commits or aborts.
        let mut sandbox_guards: BTreeMap<String, Option<G>> = BTreeMap::new();
        let tx = super::immediate_transaction(conn)?;
        let attempt_page = load_attempt_page(&tx, &self.attempt_outcomes)?;
        let abandoned_page = load_abandoned_page(&tx, &self.abandoned)?;
        let mut coupled_abandoned: BTreeMap<String, CoupledAbandoned> = BTreeMap::new();
        let mut attempt_candidates = Vec::new();
        let mut abandoned_candidates = Vec::new();
        {
            let mut ensure_idle_sandbox = |sandbox_id: &str| -> bool {
                if let Some(cached) = sandbox_guards.get(sandbox_id) {
                    return cached.is_some();
                }
                let acquired = acquire_idle_sandbox(sandbox_id);
                let is_idle = acquired.is_some();
                sandbox_guards.insert(sandbox_id.to_owned(), acquired);
                is_idle
            };
            if let Some((_, rows)) = &attempt_page {
                for (
                    recorded_seq,
                    operation_id,
                    attempt_id,
                    sandbox_id,
                    recorded_at,
                    last_seen_at,
                ) in rows
                {
                    let Some(operation_id) = canonical_operation_id(operation_id) else {
                        continue;
                    };
                    let Value::Text(attempt_id) = attempt_id else {
                        continue;
                    };
                    if attempt_id.is_empty() {
                        continue;
                    }
                    let Some(sandbox_id) = canonical_sandbox_id(sandbox_id) else {
                        continue;
                    };
                    let Some(seen_at) = effective_seen_timestamp(recorded_at, last_seen_at) else {
                        continue;
                    };
                    if !ensure_idle_sandbox(sandbox_id) {
                        continue;
                    }
                    if !coupled_abandoned.contains_key(operation_id) {
                        let state =
                            load_coupled_abandoned(&tx, operation_id, &mut ensure_idle_sandbox)?;
                        coupled_abandoned.insert(operation_id.to_owned(), state);
                    }
                    attempt_candidates.push((
                        *recorded_seq,
                        operation_id,
                        sandbox_id,
                        attempt_id.as_str(),
                        seen_at,
                    ));
                }
            }
            if let Some((_, rows)) = &abandoned_page {
                for (rowid, operation_id, sandbox_id, recorded_at, last_seen_at) in rows {
                    let Some(operation_id) = canonical_operation_id(operation_id) else {
                        continue;
                    };
                    let Some(sandbox_id) = canonical_sandbox_id(sandbox_id) else {
                        continue;
                    };
                    let Some(seen_at) = effective_seen_timestamp(recorded_at, last_seen_at) else {
                        continue;
                    };
                    if !ensure_idle_sandbox(sandbox_id) {
                        continue;
                    }
                    abandoned_candidates.push((*rowid, operation_id, sandbox_id, seen_at));
                }
            }
        }

        let Some(now) = sample_now()? else {
            return Ok(None);
        };
        let Some(cutoff) = now.checked_sub(retention.as_secs()) else {
            return Ok(Some(RuntimeUpgradePruneBatch::default()));
        };

        let mut report = RuntimeUpgradePruneBatch::default();
        if let Some((through, rows)) = &attempt_page {
            report.examined_attempt_outcomes = rows.len();
            let mut delete = tx.prepare_cached(DELETE_ATTEMPT_OUTCOME_SQL)?;
            for (recorded_seq, operation_id, sandbox_id, attempt_id, seen_at) in attempt_candidates
            {
                if seen_at >= cutoff
                    || !coupled_abandoned
                        .get(operation_id)
                        .is_some_and(|coupled| coupled.is_ready(cutoff))
                {
                    continue;
                }
                report.deleted_attempt_outcomes +=
                    delete.execute(params![recorded_seq, operation_id, sandbox_id, attempt_id])?;
            }
            self.attempt_outcomes.through = Some(*through);
            self.attempt_outcomes
                .advance(rows.len(), rows.last().map(|row| row.0), *through);
        }
        if let Some((through, rows)) = &abandoned_page {
            report.examined_abandoned = rows.len();
            let mut delete = tx.prepare_cached(DELETE_ABANDONED_SQL)?;
            for (rowid, operation_id, sandbox_id, seen_at) in abandoned_candidates {
                if seen_at >= cutoff {
                    continue;
                }
                report.deleted_abandoned +=
                    delete.execute(params![rowid, operation_id, sandbox_id])?;
            }
            self.abandoned.through = Some(*through);
            self.abandoned
                .advance(rows.len(), rows.last().map(|row| row.0), *through);
        }
        tx.commit()?;
        drop(sandbox_guards);
        Ok(Some(report))
    }
}

type RawAttemptRow = (i64, Value, Value, Value, Value, Value);
type RawAbandonedRow = (i64, Value, Value, Value, Value);

fn load_attempt_page(
    tx: &Connection,
    cursor: &TableCursor,
) -> Result<Option<(i64, Vec<RawAttemptRow>)>> {
    let through = match cursor.through {
        Some(through) => Some(through),
        None => tx.query_row(
            "SELECT MAX(recorded_seq) FROM sandbox_runtime_upgrade_attempt_outcomes",
            [],
            |row| row.get(0),
        )?,
    };
    let Some(through) = through else {
        return Ok(None);
    };
    let mut statement = tx.prepare_cached(
        "SELECT recorded_seq, operation_id, attempt_id, sandbox_id, recorded_at, last_seen_at
           FROM sandbox_runtime_upgrade_attempt_outcomes
          WHERE recorded_seq >= ?1 AND recorded_seq <= ?2
          ORDER BY recorded_seq
          LIMIT 128",
    )?;
    let rows = statement
        .query_map(params![cursor.next, through], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Value>(1)?,
                row.get::<_, Value>(2)?,
                row.get::<_, Value>(3)?,
                row.get::<_, Value>(4)?,
                row.get::<_, Value>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(Some((through, rows)))
}

fn load_abandoned_page(
    tx: &Connection,
    cursor: &TableCursor,
) -> Result<Option<(i64, Vec<RawAbandonedRow>)>> {
    let through = match cursor.through {
        Some(through) => Some(through),
        None => tx.query_row(
            "SELECT MAX(rowid) FROM sandbox_runtime_upgrade_abandoned",
            [],
            |row| row.get(0),
        )?,
    };
    let Some(through) = through else {
        return Ok(None);
    };
    let mut statement = tx.prepare_cached(
        "SELECT rowid, operation_id, sandbox_id, recorded_at, last_seen_at
           FROM sandbox_runtime_upgrade_abandoned
          WHERE rowid >= ?1 AND rowid <= ?2
          ORDER BY rowid
          LIMIT 128",
    )?;
    let rows = statement
        .query_map(params![cursor.next, through], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Value>(1)?,
                row.get::<_, Value>(2)?,
                row.get::<_, Value>(3)?,
                row.get::<_, Value>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(Some((through, rows)))
}

/// Shared local-state guards for `?2 = operation_id` and `?3 = sandbox_id`.
/// `sandboxes` covers `ON DELETE CASCADE` child tables (`sandbox_cleanup_episodes`,
/// `sandbox_runtime_upgrades.sandbox_id`, `sandbox_runtime_upgrade_outcomes.sandbox_id`);
/// tables without a foreign key to `sandboxes` (`sandbox_restore_transfer_scopes`,
/// `auto_lifecycle_delete_outbox`, `local_artifacts`) and same-operation rows
/// are checked explicitly.
macro_rules! idle_guard_sql {
    () => {
        " AND NOT EXISTS (SELECT 1 FROM sandboxes WHERE id = ?3)
          AND NOT EXISTS (SELECT 1 FROM sandbox_runtime_upgrades WHERE operation_id = ?2)
          AND NOT EXISTS (SELECT 1 FROM sandbox_runtime_upgrade_outcomes WHERE operation_id = ?2)
          AND NOT EXISTS (SELECT 1 FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?3)
          AND NOT EXISTS (SELECT 1 FROM auto_lifecycle_delete_outbox WHERE sandbox_id = ?3)
          AND NOT EXISTS (SELECT 1 FROM local_artifacts WHERE sandbox_id = ?3)"
    };
}

const DELETE_ATTEMPT_OUTCOME_SQL: &str = concat!(
    "DELETE FROM sandbox_runtime_upgrade_attempt_outcomes
      WHERE recorded_seq = ?1
        AND operation_id = ?2
        AND sandbox_id = ?3
        AND attempt_id = ?4",
    idle_guard_sql!(),
);

const DELETE_ABANDONED_SQL: &str = concat!(
    "DELETE FROM sandbox_runtime_upgrade_abandoned
      WHERE rowid = ?1
        AND operation_id = ?2
        AND sandbox_id = ?3
        AND NOT EXISTS (
            SELECT 1 FROM sandbox_runtime_upgrade_attempt_outcomes
             WHERE operation_id = ?2
        )",
    idle_guard_sql!(),
);

#[derive(Clone, Copy)]
enum CoupledAbandoned {
    Absent,
    Blocked,
    Idle { seen_at: u64 },
}

impl CoupledAbandoned {
    const fn is_ready(self, cutoff: u64) -> bool {
        match self {
            Self::Absent => true,
            Self::Blocked => false,
            Self::Idle { seen_at } => seen_at < cutoff,
        }
    }
}

fn load_coupled_abandoned<F>(
    tx: &Connection,
    operation_id: &str,
    ensure_idle_sandbox: &mut F,
) -> Result<CoupledAbandoned>
where
    F: FnMut(&str) -> bool,
{
    let mut statement = tx.prepare_cached(
        "SELECT a.sandbox_id,
                a.recorded_at,
                a.last_seen_at,
                NOT EXISTS (SELECT 1 FROM sandboxes WHERE id = a.sandbox_id)
                    AND NOT EXISTS (SELECT 1 FROM sandbox_restore_transfer_scopes WHERE sandbox_id = a.sandbox_id)
                    AND NOT EXISTS (SELECT 1 FROM auto_lifecycle_delete_outbox WHERE sandbox_id = a.sandbox_id)
                    AND NOT EXISTS (SELECT 1 FROM local_artifacts WHERE sandbox_id = a.sandbox_id)
           FROM sandbox_runtime_upgrade_abandoned AS a
          WHERE a.operation_id = ?1",
    )?;
    let Some((sandbox_id, recorded_at, last_seen_at, local_idle)) = statement
        .query_row([operation_id], |row| {
            Ok((
                row.get::<_, Value>(0)?,
                row.get::<_, Value>(1)?,
                row.get::<_, Value>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .optional()?
    else {
        return Ok(CoupledAbandoned::Absent);
    };
    let Some(sandbox_id) = canonical_sandbox_id(&sandbox_id) else {
        return Ok(CoupledAbandoned::Blocked);
    };
    let Some(seen_at) = effective_seen_timestamp(&recorded_at, &last_seen_at) else {
        return Ok(CoupledAbandoned::Blocked);
    };
    if !local_idle || !ensure_idle_sandbox(sandbox_id) {
        return Ok(CoupledAbandoned::Blocked);
    }
    Ok(CoupledAbandoned::Idle { seen_at })
}

fn canonical_sandbox_id(value: &Value) -> Option<&str> {
    let Value::Text(sandbox_id) = value else {
        return None;
    };
    let parsed = sandbox_id.parse::<SandboxId>().ok()?;
    (parsed.to_string() == *sandbox_id).then_some(sandbox_id.as_str())
}

fn canonical_operation_id(value: &Value) -> Option<&str> {
    let Value::Text(operation_id) = value else {
        return None;
    };
    let parsed = operation_id.parse::<RuntimeUpgradeOperationId>().ok()?;
    (parsed.to_string() == *operation_id).then_some(operation_id.as_str())
}

fn parse_epoch_secs(value: &Value) -> Option<u64> {
    match value {
        Value::Integer(secs) => u64::try_from(*secs).ok(),
        Value::Text(secs) if !secs.is_empty() && secs.bytes().all(|b| b.is_ascii_digit()) => {
            secs.parse().ok()
        }
        _ => None,
    }
}

fn effective_seen_timestamp(recorded_at: &Value, last_seen_at: &Value) -> Option<u64> {
    let recorded = parse_epoch_secs(recorded_at)?;
    match last_seen_at {
        Value::Null => Some(recorded),
        other => Some(recorded.max(parse_epoch_secs(other)?)),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn backdate_retained_runtime_upgrade_for_test(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    recorded_at: u64,
    last_seen_at: Option<u64>,
) -> Result<()> {
    let tx = super::immediate_transaction(conn)?;
    let operation_key = operation_id.to_string();
    let recorded_text = recorded_at.to_string();
    let seen_text = last_seen_at.map(|secs| secs.to_string());
    tx.execute(
        "UPDATE sandbox_runtime_upgrade_attempt_outcomes
            SET recorded_at = ?2,
                last_seen_at = ?3
          WHERE operation_id = ?1",
        params![operation_key, recorded_text, seen_text],
    )?;
    tx.execute(
        "UPDATE sandbox_runtime_upgrade_abandoned
            SET recorded_at = ?2,
                last_seen_at = ?3
          WHERE operation_id = ?1",
        params![operation_key, recorded_text, seen_text],
    )?;
    tx.commit()
}

#[cfg(test)]
mod tests;
