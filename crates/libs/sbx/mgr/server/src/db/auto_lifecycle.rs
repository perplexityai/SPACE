//! Auto-lifecycle policy, runtime state, and due-deadline scans.

use super::*;

/// Scan position only, never lifecycle authority. The rowid lane makes progress
/// past unchanged failures and rows without due work; the deadline lane reserves prompt
/// admission for newly due work. Neither lane counts the full backlog.
#[derive(Clone, Copy)]
pub(crate) struct AutoLifecycleScan {
    pub(super) after: i64,
    pub(super) fresh_turn: bool,
}

impl Default for AutoLifecycleScan {
    fn default() -> Self {
        Self {
            after: i64::MIN,
            fresh_turn: true,
        }
    }
}

// Preserve the selected database key: parsing a UUID validates its identity,
// but formatting it again need not reproduce the stored key.
pub(crate) struct DueAutoLifecycleSandbox {
    pub storage_id: String,
    pub sandbox_id: SandboxId,
}

/// The one state in which no idle action can ever act: a `deleting` row is
/// already being torn down. Every other state can take at least one action
/// once it settles — `Delete` is reachable from every settled state and
/// from every transient one except `creating` (the state machine
/// force-deletes from `restoring`, `suspending`, … but a row still being
/// created has nothing to delete yet) — so eligibility is decided per
/// action in the transition, once the due stage is known — see
/// [`auto_lifecycle_action_can_act`].
///
/// This literal is the deadline lane's filter. The rowid lane cannot use
/// it (that lane must return every row so its cursor keeps advancing) and
/// applies the same rule in Rust inside `eligible`
/// (`state == SandboxState::Deleting`); the two lane tests in
/// `cleanup_discovery_tests` hold the two encodings to the same answer.
const AUTO_LIFECYCLE_NEVER_ACTIONABLE_STATES_SQL: &str = "('deleting')";

/// Push a row's scan deadline out without touching anything else, so a row
/// whose due stage cannot act right now (mid-transition; see
/// `auto_lifecycle_action_can_act`) stops being re-picked every tick, while
/// `updated_at` — the quiescence clock other loops read — and the runtime
/// state JSON (which still carries the true deadline) stay as they were. Any
/// later state write rewrites the column from the JSON, so the deferral is
/// self-healing and never outlives the next real transition.
pub(crate) fn defer_auto_lifecycle_deadline(
    conn: &Connection,
    id: &str,
    until_secs: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET next_auto_event_at = ?1 WHERE id = ?2",
        params![SqliteU64(until_secs), id],
    )?;
    Ok(())
}

/// Whether `action` can act on a row in `state`, or has already been
/// satisfied by it. `Pause`/`Suspend`/`Stop` act on settled states and
/// treat a more dormant settled state as satisfied (a `Suspend` due on an
/// already-`suspended` row advances the policy to its later stages); on a
/// mid-transition row — `starting`, `stopping`, `pausing`, `resuming`,
/// `checkpointing`, `suspending`, `restoring` (which includes a failed
/// cross-node restore's retained row) — they can neither act nor claim
/// satisfaction, so the transition must record nothing (recording
/// the stage as fired bumped `updated_at` and reset the cluster-deleted
/// eviction's quiescence clock on exactly that retained row). `Delete`
/// follows the state machine's force-delete contract and acts wherever
/// `Deleting` is reachable. `None` is a no-op that may always be recorded.
pub(crate) fn auto_lifecycle_action_can_act(
    action: pplx_space_common_core::types::AutoLifecycleIdleAction,
    state: SandboxState,
) -> bool {
    use pplx_space_common_core::types::AutoLifecycleIdleAction as Action;
    match action {
        Action::None => true,
        Action::Delete => state.can_transition_to(SandboxState::Deleting),
        Action::Pause | Action::Suspend | Action::Stop => auto_lifecycle_state_is_settled(state),
    }
}

/// A row another writer is not in the middle of moving. The auto-lifecycle
/// transition may write runtime state only on a settled row: on a
/// mid-transition one — `restoring` included, which is also the shape a
/// failed cross-node restore retains — every write bumps `updated_at`, the
/// quiescence clock the cluster-deleted eviction of that row waits on,
/// and the in-flight transition owns the row anyway.
pub(crate) fn auto_lifecycle_state_is_settled(state: SandboxState) -> bool {
    // The state machine owns the transient set; a state added there is
    // non-settled here without a second list to update.
    !state.is_transient()
}

pub(crate) fn due_auto_lifecycle_batch(
    conn: &Connection,
    now_secs: u64,
    active: &[SandboxId],
    available: usize,
    cursor: &mut AutoLifecycleScan,
) -> Result<Vec<DueAutoLifecycleSandbox>> {
    if available == 0 {
        return Ok(Vec::new());
    }
    let now = now_secs.min(i64::MAX as u64) as i64;
    let mut selected = Vec::new();
    let fresh_slots = if available == 1 {
        usize::from(cursor.fresh_turn)
    } else {
        available / 2
    };
    let eligible = |row: &rusqlite::Row<'_>,
                    selected: &[DueAutoLifecycleSandbox]|
     -> Result<Option<DueAutoLifecycleSandbox>> {
        let (id, deadline) = match (row.get::<_, String>(1), row.get::<_, Option<i64>>(2)) {
            (Ok(id), Ok(deadline)) => (id, deadline),
            (Err(error), _) | (_, Err(error)) if is_row_decode_error(&error) => {
                tracing::warn!(rowid = row.get::<_, i64>(0)?, error = %describe_error(&error), "skipping malformed auto lifecycle discovery row");
                return Ok(None);
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
        };
        let Some(deadline) = deadline else {
            return Ok(None);
        };
        let Ok(parsed) = id.parse::<SandboxId>() else {
            return Ok(None);
        };
        // The rowid lane sees every row; a row no action can ever act on is
        // not due work (see `AUTO_LIFECYCLE_NEVER_ACTIONABLE_STATES_SQL`).
        // A state that does not decode is skipped like a malformed id above;
        // a statement-level failure propagates. An unknown-but-decodable
        // state string is left to the transition's own lookup error.
        let state = match row.get::<_, String>(3) {
            Ok(state) => state,
            Err(error) if is_row_decode_error(&error) => {
                tracing::warn!(rowid = row.get::<_, i64>(0)?, error = %describe_error(&error), "skipping malformed auto lifecycle discovery row");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if state.parse::<SandboxState>().ok() == Some(SandboxState::Deleting) {
            return Ok(None);
        }
        if !(0..=now).contains(&deadline)
            || active.contains(&parsed)
            || selected.iter().any(|due| due.sandbox_id == parsed)
        {
            return Ok(None);
        }
        Ok(Some(DueAutoLifecycleSandbox {
            storage_id: id,
            sandbox_id: parsed,
        }))
    };
    // LIMIT applies to raw index entries, before identity/metadata eligibility.
    if fresh_slots > 0 {
        let mut statement = conn.prepare(&sql!(format!(
            "SELECT rowid, id, next_auto_event_at, state
             FROM sandboxes INDEXED BY idx_sandboxes_next_auto_event_at
             WHERE next_auto_event_at <= ?1
               AND state NOT IN {AUTO_LIFECYCLE_NEVER_ACTIONABLE_STATES_SQL}
             ORDER BY next_auto_event_at DESC, created_at DESC LIMIT 32"
        )))?;
        let mut rows = statement.query([now])?;
        while let Some(row) = rows.next()? {
            if let Some(id) = eligible(row, &selected)? {
                selected.push(id);
            }
            if selected.len() == fresh_slots {
                break;
            }
        }
    }
    // The rowid lane must still ADVANCE past `deleting` rows (it is a scan
    // position, not a work list), so they stay in the result set of this
    // statement and are filtered by `eligible`, not by SQL.
    let mut statement = conn.prepare(sql!(
        "SELECT rowid, id, next_auto_event_at, state
         FROM sandboxes WHERE rowid > ?1 ORDER BY rowid LIMIT 128"
    ))?;
    let mut rows = statement.query([cursor.after])?;
    let mut scanned = 0;
    while selected.len() < available {
        let Some(row) = rows.next()? else {
            if scanned < 128 {
                cursor.after = i64::MIN;
            }
            break;
        };
        scanned += 1;
        cursor.after = row.get(0)?;
        if let Some(id) = eligible(row, &selected)? {
            selected.push(id);
        }
    }
    if available == 1 {
        cursor.fresh_turn = !cursor.fresh_turn;
    }
    Ok(selected)
}

pub fn update_auto_lifecycle(
    conn: &Connection,
    id: &str,
    auto_lifecycle: &AutoLifecyclePolicy,
    authority: PolicyAuthority,
) -> Result<()> {
    let mut auto_lifecycle_state = crate::auto_lifecycle::initial_runtime_state(
        auto_lifecycle,
        crate::auto_lifecycle::now_secs(),
    );
    crate::auto_lifecycle::recompute_runtime_state(&mut auto_lifecycle_state, auto_lifecycle);
    update_auto_lifecycle_with_state(conn, id, auto_lifecycle, &auto_lifecycle_state, authority)
}

pub fn update_auto_lifecycle_with_state(
    conn: &Connection,
    id: &str,
    auto_lifecycle: &AutoLifecyclePolicy,
    auto_lifecycle_state: &AutoLifecycleRuntimeState,
    authority: PolicyAuthority,
) -> Result<()> {
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET auto_lifecycle_json = ?1,
             auto_lifecycle_state_json = ?2,
             next_auto_event_at = ?3,
             auto_lifecycle_authority = ?4,
             auto_lifecycle_delete_block_reason = NULL,
             auto_lifecycle_delete_blocked_at = NULL,
             updated_at = ?5
         WHERE id = ?6",
        params![
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            authority.as_str(),
            now()?,
            id
        ],
    )?;
    Ok(())
}

/// Persist the auto-lifecycle runtime state, if it actually differs from what
/// is stored.
///
/// A state identical to the row's own is not a transition and must not be
/// written. The scheduler re-runs a due stage every tick, and a stage that
/// keeps failing the same way re-derives a byte-identical runtime state each
/// time (`handle_auto_lifecycle_error` records the same error text, with
/// the same deadline). Writing it anyway bumps `updated_at` — the quiescence
/// clock the cluster-deleted eviction reads — every few seconds on a row that
/// has not moved. `auto_lifecycle_action_can_act` already keeps the gated
/// actions from recording on a mid-transition row; this closes the same gap
/// for the ungated ones, such as delete. Nothing observable is lost: the two
/// columns this statement owns are compared to the exact values it would have
/// written.
///
/// One reader moves the eager way. The disk-pressure eviction ages a
/// `suspended` row with no activity clocks by `updated_at`
/// (`sandbox_eviction::IDLE_WITNESS_SQL`), so such a row whose stage keeps
/// failing identically now ages from its last real change rather than its
/// last repeated record, and can become a candidate sooner than before. That
/// is the intended meaning of idle, and the eviction's backup proof and claim
/// CAS still apply unchanged.
pub fn update_auto_lifecycle_state(
    conn: &Connection,
    id: &str,
    auto_lifecycle_state: &AutoLifecycleRuntimeState,
) -> Result<()> {
    let state_json = serde_json::to_string(auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.prepare_cached(
        "UPDATE sandboxes
         SET auto_lifecycle_state_json = ?1, next_auto_event_at = ?2, updated_at = ?3
         WHERE id = ?4
           AND (auto_lifecycle_state_json IS NOT ?1 OR next_auto_event_at IS NOT ?2)",
    )?
    .execute(params![
        state_json,
        auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
        now()?,
        id
    ])?;
    Ok(())
}
