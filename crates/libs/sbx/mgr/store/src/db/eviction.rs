//! Eviction claims over settled sandboxes.

use super::*;

/// Atomically claim a `suspended` sandbox for cold-sandbox eviction by
/// flipping it to `deleting`. The CAS guards two invariants at once:
///
/// - `state = 'suspended'` — a concurrent resume/restore that already
///   moved the state wins; once the row is in `deleting`, the
///   lifecycle state machine rejects incoming restores and the
///   standard `CleanupCompletion::DeleteRow` machinery treats the
///   cleanup as current.
/// - `current_local_artifact_id` must still equal the identity the
///   eviction decision **proved** (`expected_pin`; `None` asserts the
///   row is still unpinned for the legacy class). This binds the
///   delete to the exact bytes that were proven backed: a fast
///   resume + re-suspend between the decision read and this claim
///   mints a new, unproven pin and the claim fails.
///
/// - `updated_at` must still equal `expected_updated_at` when the
///   caller proved additional predicates (owner, pool, quiescence)
///   against a specific row version: every row mutation bumps
///   `updated_at`, so binding the claim to it extends the CAS to the
///   whole proof, not just state + pin. `None` keeps the historical
///   state + pin contract for the disk-pressure loop, whose proof is
///   the pin itself.
///
/// Returns `false` (and changes nothing) when any condition no longer
/// holds.
pub fn claim_suspended_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_pin: Option<&str>,
    expected_updated_at: Option<&str>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = match (expected_pin, expected_updated_at) {
        (Some(pin), Some(updated_at)) => tx.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id = ?3
               AND updated_at = ?4",
            params![now()?, id, pin, updated_at],
        )?,
        (Some(pin), None) => tx.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id = ?3",
            params![now()?, id, pin],
        )?,
        (None, Some(updated_at)) => tx.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id IS NULL
               AND updated_at = ?3",
            params![now()?, id, updated_at],
        )?,
        (None, None) => tx.execute(
            "UPDATE sandboxes
             SET state = 'deleting',
                 state_changed_at = CASE WHEN state != 'deleting' THEN ?1 ELSE state_changed_at END,
                 updated_at = ?1
             WHERE id = ?2 AND state = 'suspended'
               AND current_local_artifact_id IS NULL",
            params![now()?, id],
        )?,
    };
    if changed == 1 {
        teardown_jobs::fail_active(&tx, id, TeardownSuspendFailure::Deleting)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Atomically claim a `restoring` row that carries a persisted cross-node
/// staging failure for cluster-deleted eviction: `restoring → deleting`.
/// The CAS re-asserts the whole shape the eviction decision was made
/// against:
///
/// - `state = 'restoring'` with a non-blank `failure_reason` — the
///   retained-for-retry row `mark_cross_node_restore_staging_failed`
///   leaves. Blank means the marker is absent, matching the Rust guards on
///   both sides (node-ctl's selection and the service's refusal), which
///   use `str::trim`; SQLite's one-argument `trim` strips spaces only, so
///   the whitespace set is spelled out. A `NULL` marker never matches
///   because `trim(NULL) != ''` is `NULL`, so no separate `IS NOT NULL`
///   test is needed.
/// - `current_local_artifact_id IS NULL` — the retryable interrupted
///   restore shape (`sandbox_is_retryable_interrupted_restore`) never
///   carries a pin; asserting it here makes the accepted shape equal the
///   described one instead of following from an argument about writers.
/// - `updated_at = expected_updated_at` — exactly the version the caller
///   read; a restore phase advance, a retry, or any other writer bumps it
///   and fails the claim.
///
/// The marker alone does not mean no restore is live (a retry reuses the
/// row without clearing it); the version check, the caller's quiescence
/// and staging-lock proofs, and the cluster's absent-row report are what
/// make the claim safe. A first-attempt restore with no persisted failure
/// is never claimable by this statement. Returns `false` (and changes
/// nothing) when any condition no longer holds.
pub fn claim_failed_restoring_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_updated_at: &str,
) -> Result<bool> {
    // The restore columns are only valid on a `restoring` row (the table's
    // CHECK ties `restore_origin` / `restore_phase` / `restore_storage_uri`
    // to that state); every transition out of `restoring` clears them, and
    // so must this one. `state_changed_at` is set unconditionally: the WHERE
    // clause already forbids a row that is not leaving `restoring`.
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'deleting',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = ?1,
             updated_at = ?1
         WHERE id = ?2 AND state = 'restoring'
           AND trim(failure_reason, char(9) || char(10) || char(13) || ' ') != ''
           AND current_local_artifact_id IS NULL
           AND updated_at = ?3",
        params![now()?, id, expected_updated_at],
    )?;
    if changed == 1 {
        teardown_jobs::fail_active(&tx, id, TeardownSuspendFailure::Deleting)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Claim quarantined local state after an authoritative cluster-deleted report.
/// The caller checks platform ownership, pool exclusion, quiescence and holds
/// the cleanup fence and staging lock. Re-checking the row version and state
/// keeps a recovery that advanced the sandbox from losing its local resources.
/// Artifact pins remain until the normal DeleteRow cleanup removes all children.
pub fn claim_quarantined_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_updated_at: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'deleting', state_changed_at = ?1, updated_at = ?1
         WHERE id = ?2 AND state = 'quarantined' AND updated_at = ?3",
        params![now()?, id, expected_updated_at],
    )?;
    if changed == 1 {
        teardown_jobs::fail_active(&tx, id, TeardownSuspendFailure::Deleting)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Reclaim retained stopped bytes only after an authoritative cluster deletion.
/// The caller holds the cleanup fence and checks ownership and quiescence;
/// the exact row-version CAS prevents racing a subsequent activation.
pub fn claim_stopped_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    expected_updated_at: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        sql!(
            "UPDATE sandboxes SET state = 'deleting', state_changed_at = ?1, updated_at = ?1
              WHERE id = ?2 AND state = 'stopped' AND updated_at = ?3"
        ),
        params![now()?, id, expected_updated_at],
    )?;
    if changed == 1 {
        teardown_jobs::fail_active(&tx, id, TeardownSuspendFailure::Deleting)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Claim a `suspending` row whose teardown suspend is terminally stuck, for
/// the cluster-deleted eviction.
///
/// The CAS re-asserts the row's **identity**, not its version: still
/// `suspending`, still carrying exactly the terminal marker (a `suspending`
/// row with any other failure reason still has a publication path and is not
/// this shape), still pinning the exact artifact the refused backup staged,
/// and still at the `state_changed_at` the decision was taken against.
/// Nothing can leave that tuple intact while moving the sandbox on: every
/// path out of `suspending` rewrites `state` and `state_changed_at` together,
/// and a re-stage rewrites `current_local_artifact_id`.
///
/// It deliberately does **not** assert `updated_at`. This row's runtime is
/// still resident and its auto-lifecycle policy still fires, so two loops
/// re-stamp `updated_at` on it every few seconds with values identical to
/// what is already there (`update_network_counters` and
/// `update_auto_lifecycle_state`). A version CAS against that is not a
/// safety guard, it is a livelock: the claim can lose forever while the row
/// never actually changes.
///
/// The staged artifact pin is left in place for the `DeleteRow` cleanup to
/// erase with the rest of the row's children; unlike the failed-restoring
/// claim there are no state-tied restore columns to clear, because
/// `begin_teardown_suspend` nulls them on the way into `suspending`.
/// `state_changed_at` is set unconditionally: the predicate already forbids a
/// row that is not leaving `suspending`.
pub fn claim_teardown_suspend_failed_sandbox_for_eviction(
    conn: &Connection,
    id: &str,
    staged_artifact: LocalArtifactId,
    expected_state_changed_at: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'deleting',
             state_changed_at = ?1,
             updated_at = ?1
         WHERE id = ?2 AND state = 'suspending'
           AND failure_reason = ?3
           AND current_local_artifact_id = ?4
           AND state_changed_at = ?5",
        params![
            now()?,
            id,
            pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON,
            staged_artifact.to_string(),
            expected_state_changed_at
        ],
    )?;
    if changed == 1 {
        teardown_jobs::fail_active(&tx, id, TeardownSuspendFailure::Deleting)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}
