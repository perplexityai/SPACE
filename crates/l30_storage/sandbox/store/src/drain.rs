//! Guarded writes the cluster controller's node drain makes to `sandboxes`:
//! the evacuation claims and their revert, the suspend commit and the restore
//! finalize, the stopped evacuation's artifact pin, the restore retry backoff,
//! the stranded-suspend redrive, the give-ups that fail a sandbox or adopt its
//! remote snapshot when the drain can't move it, and the pod-less give-up's
//! revert to `suspended`.
//!
//! The drain keeps everything around them: the candidate lists, the node row
//! lock and lock timeout, the standalone `FOR UPDATE` / `FOR SHARE` reads, the
//! transactions, and the snapshot and node writes beside them. Three
//! statements lock their own rows with `FOR UPDATE SKIP LOCKED` in a CTE
//! ([`mark_sandbox_failed_on_node_query`],
//! [`abandon_stale_resident_sandboxes_query`] and
//! [`fail_resident_sandboxes_on_gone_node_query`]); those locks moved with
//! them. Every write function runs on the executor its caller passes.
//! [`revert_pod_less_resident_to_suspended`] requires the caller's
//! transaction: it has no row-state guard of its own and relies on the
//! precheck its caller re-ran under that transaction's locks.
//!
//! Three statements bind a lifecycle set (`$3` of
//! [`abandon_stale_resident_sandboxes_query`] and of
//! [`mark_sandbox_failed_on_node_query`], `$2` of
//! [`fail_resident_sandboxes_on_gone_node_query`]). The caller chooses the
//! set (the drain's non-evacuable and suspend-evacuable sets, and
//! `pplx-space-common-svc`'s resident set) and passes its wire strings.
//!
//! [`gone_node_identity_guards`] is also interpolated by the drain's
//! in-flight snapshot give-up, a `snapshots` write that stays with the drain,
//! so it is public to keep one definition. The statement builders
//! (`*_query`) are public only because the drain's Postgres tests run them
//! directly. Production code calls the function here that binds each one.

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, NodeDrainMode, NodeDrainPhase, NodeId,
    PositiveDuration, SandboxId, SandboxLifecycle,
};

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    SuspendedK8sFinalizationSql, SuspendedTargetNodeAssignment,
    clear_current_local_artifact_marker_assignments, current_disk_artifact_backup_ready,
    current_suspend_snapshot_ready, failed_repair_revert_eligibility,
    inflight_suspend_upload_exists, latest_full_snapshot_ready, newest_full_snapshot_unambiguous,
    pod_less_drain_revert_loses_no_newer_bytes, recoverable_suspend_snapshot_exists,
    snapshot_durable_for_source_release, suspended_k8s_finalization_sql,
};
use sqlx::{Postgres, Transaction};

/// Saturating seconds-as-`i64` for SQL binds; any value above `i64::MAX`
/// is "never" in human terms.
fn secs_bind(duration: PositiveDuration) -> i64 {
    i64::try_from(duration.get().as_secs()).unwrap_or(i64::MAX)
}

/// Claim one resident sandbox for an evacuation suspend.
///
/// The claim re-asserts the **node's** drain state, not just the sandbox's, and
/// that is load-bearing rather than defensive. The reconciler resolves a node's
/// mode when it lists draining nodes, then works through the node's residents; an
/// operator who lands `:undrain` followed by a gentle `:drain` inside that window
/// leaves the tick holding a stale `Forced`. Without this clause the claim would
/// happily suspend and migrate residents of a drain that is now explicitly
/// gentle — precisely the promise gentle mode makes. Checking it here makes the
/// decision atomic with the write instead of a time-of-check/time-of-use gap.
///
/// `drain_mode = 'forced'` is matched literally rather than "not gentle": a NULL
/// or unrecognized mode reads as gentle everywhere else
/// ([`NodeDrainMode::from_column_str`]), so requiring the explicit token keeps
/// this consistent with every other reader and keeps evacuation reachable only
/// through a mode someone actually asked for.
///
/// Writes an **operation step**: it claims the row for the evacuation by moving
/// it to `suspending` with the pre-evacuation lifecycle as the marker, or
/// resumes a claim that already carries the same marker.
pub async fn claim_evacuation_suspend(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    pre_lifecycle: SandboxLifecycle,
) -> Result<bool, sqlx::Error> {
    let claimed = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspending',
                evacuation_restore_target = $2
          WHERE id = $1
            AND active_node_id = $3
            AND (
                lifecycle = $2
                OR (
                    lifecycle = 'suspending'
                    AND evacuation_restore_target = $2
                )
            )
            AND EXISTS (
                SELECT 1
                  FROM nodes n
                 WHERE n.id = $3
                   AND n.drain_phase = $4
                   AND n.drain_mode = $5
            )"
    ))
    .bind(sandbox_id)
    .bind(pre_lifecycle.as_str())
    .bind(source_node_id)
    .bind(NodeDrainPhase::Draining)
    .bind(NodeDrainMode::Forced)
    .execute(executor)
    .await?;
    Ok(claimed.rows_affected() == 1)
}

/// Claim one `stopped` resident for evacuation by writing the
/// `evacuation_restore_target = 'stopped'` marker, under the same
/// `active_node_id` + node-still-force-draining guards as
/// [`claim_evacuation_suspend`].
///
/// **The marker is the whole claim.** There is no transient lifecycle to move
/// to: the sandbox is `stopped` before the evacuation, `stopped` throughout it,
/// and `stopped` after it lands on the new node, so a `'stopping'`-shaped claim
/// would invent a user-visible transition that never happens. Leaving the
/// lifecycle alone also keeps the row out of `NON_EVACUATABLE_RESIDENT_LIFECYCLES`
/// and therefore out of the stale-abandon sweep, which would otherwise age a
/// slow upload out to `failed`.
///
/// Two consequences worth stating, because both are load-bearing and neither is
/// obvious from the statement:
///
/// * **The marker-clearing trigger never fires on this write.**
///   `sandboxes_clear_evacuation_restore_target` is `BEFORE UPDATE` gated on
///   `NEW.lifecycle IS DISTINCT FROM OLD.lifecycle`; both are `'stopped'` here,
///   so the marker survives the very statement that sets it. The trigger still
///   does its job at the two ends that matter — a user takeover moving the row
///   out of `stopped` clears it, and the re-placement's `restoring -> stopped`
///   finalize clears it on completion.
/// * **Re-claiming is a no-op, not a conflict.** The `= 'stopped'` disjunct
///   makes every later tick's claim match the row it already owns, which is
///   what lets the evacuation be driven by re-listing instead of by a task held
///   across the S3 upload.
///
/// Matching `drain_mode = 'forced'` literally (rather than "not gentle") is the
/// same decision [`claim_evacuation_suspend`] documents: a NULL or unrecognized
/// mode reads as gentle everywhere else, and gentle promises not to touch
/// residents.
///
/// **A fresh claim requires `current_local_artifact_id IS NULL`, or the one pin
/// its caller matched against the node while that pin's backup is durable — a
/// backstop, not the guarantee.** The second shape is a re-placed sandbox drained
/// again; see step 0d of `evacuate_stopped_sandbox`. The
/// guarantee lives one call earlier, in the node
/// drain's `SubstrateSink::node_reported_artifact`, because the row that
/// decides whether the next publish mints or reuses is sbx-mgr's, not this one.
/// Read that first; this conjunct is here to make the refusal *atomic with the
/// marker write*, which an RPC a moment earlier cannot be.
///
/// The teardown gives up the node-local rootfs on the strength of an artifact
/// being durable. For that to be sound the artifact has to describe *the rootfs
/// being given up* — and no clock the cluster can see establishes that. The
/// node mints a `d-stop-{id}` btrfs snapshot only from a pinless row
/// (`grpc_lifecycle`'s `(Stopped, None)` arm); every other request reuses the
/// existing pin (`(_, Some(id)) => id`) and short-circuits entirely when its
/// backup is already `ready`, so no new catalog row appears.
///
/// A successful start clears the node's pin (`mark_start_succeeded`), and a
/// start that boots the guest and then fails is cleaned up back to `stopped`
/// through `apply_cleanup_state`, which drops it too, so a node's pin
/// describes the rootfs as of the latest stop. That's node behaviour the
/// cluster can't observe: a node too old to drop the pin on that path keeps
/// one older than its bytes, while the pin's `snapshots.timestamp` —
/// *registration* time — still looks recent. Nothing cluster-side can tell
/// the two apart, which is why the node's own capture time is what decides
/// (below).
///
/// Freshness cannot be recovered from a cluster timestamp either. The
/// sandbox's `lifecycle_changed_at` does not move for a pin minted after the
/// stop was recorded, and the node's `drain_started_at` is defeated by gentle
/// mode: a gentle drain does not gate user operations, and escalation to
/// forced rewrites `drain_mode` while leaving `drain_started_at` at the gentle
/// drain's start, so `stop → mint → a start that fails after the guest writes
/// → escalate` yields a pin that passes that anchor and predates the writes.
///
/// What *is* checkable is provenance: an evacuation may only give up a copy
/// whose mint it caused, or one the node shows was captured since the sandbox
/// last stopped. A pinless row mints, so the artifact is of the rootfs as it
/// stands. A pinned row reuses, so what's published is the pin's bytes, and
/// those are the rootfs's only if the pin was captured during the current
/// stopped episode — which only the node's own clocks can say (below).
///
/// **But "pinned" has to mean pinned on the node.** For a `stopped` sandbox this
/// column is NULL almost always, whatever sbx-mgr holds: the projection that
/// carries the node's pin into Postgres is gated to `status = 'suspended'`, and
/// the retire that causes the stop-time mint discards the enqueue response. So
/// this conjunct alone fails **open** — it admits the row it exists to refuse.
/// The node drain's `SubstrateSink::node_reported_artifact` asks the node
/// directly and is what makes the property hold; what this conjunct adds is
/// atomicity, closing the window between that answer and the marker write for anything that would also
/// have updated the cluster row. A refusal here or there leaves the sandbox
/// alone and parks the node visibly — on
/// `node_drain.stopped_evacuation{outcome=unattributable_artifact}` and a log
/// line naming the node and the sandbox, not on the stall gauge, which selects
/// on a marker this refusal returns before writing — rather than destroying a
/// user's disk on evidence about different bytes.
///
/// **Every ordinarily stopped resident carries a pin**: the execution retire
/// issues `EnqueueArtifactBackup(CurrentInstance, Disk)` on every stop, and on
/// a pinless row that mints and pins. Refusing on presence alone would refuse
/// nearly every stopped resident, so the node's answer is made precise
/// instead: it reports the pinned artifact's capture time, its
/// `local_artifacts.created_at`, beside the instant its row entered
/// `stopped`. A stopped rootfs can't change, so an artifact captured since
/// the row entered `stopped` describes the current bytes, and the refusal
/// fires only for a pin that the node can't show is that recent. Both clocks
/// are the node's — one machine, one `now()` — which is what separates this
/// from the timestamp arguments above: those compared *cluster registration*
/// time against a cluster clock, and a cross-machine version would accept a
/// stale artifact under clock skew in one direction.
/// `NodeReportedArtifact::pin_capture_order` spells out the comparison,
/// including why a pin captured in the stop's own second is accepted.
///
/// That relies on node-side facts. `created_at` is write-once (see
/// `insert_local_artifact` in sbx-mgr). The node drops a `stopped` row's pin
/// wherever its rootfs changes without a stamp that's sure to be later: a
/// start that launched and failed, whose restamp can land in the capture's
/// own second, and a process-snapshot rollback, which rewrites the disk
/// without a state change at all. And it refuses to pin a disk backup
/// whose row started since the backup took its snapshot; `created_at` is the
/// catalog insert, just after the snapshot, so without that a start that
/// boots, writes and fails in between would leave old bytes reading as
/// current. A node reports a capture time only for an artifact it recorded
/// under all three; see `NodeReportedArtifact::pin_capture_order`.
///
/// A node too old to report the capture time leaves it `None`, which the
/// refusal treats exactly as before: any pin refuses.
///
/// A **resumed** claim is not re-checked against the node on every tick, and
/// what keeps that sound is the **conjunction** every read of this claim
/// requires: the marker *and* `lifecycle = 'stopped'` (see the drain's
/// `SubstrateSink::stopped_evacuation_state`). A completed start or a restore
/// has to move the lifecycle, and on moving it the claim stops holding whatever
/// the marker says — the `sandboxes_clear_evacuation_restore_target` trigger
/// is the second line of that, not the first, because its allowlist *keeps*
/// the marker for `suspending`, `suspended` and `restoring`.
///
/// The conjunction does not cover a start that fails. A Kubernetes start of a
/// `stopped` sandbox boots its pod while the row still reads `stopped`, so one
/// that boots, lets the guest write and then fails leaves the row exactly as
/// the claim last saw it. The teardown re-asks the node before releasing
/// anything (`tear_down_stopped_local_copy`), which catches the case on a
/// node that drops the pin on such a rollback; against one that doesn't, the
/// kept pin still matches. The window is widest for a marker an abandoned
/// evacuation left behind, which a later forced drain resumes.
///
/// **`allow_new_claims` gates opening an evacuation, never finishing one.**
/// This is where the feature flag lands, and the asymmetry is the point: a row
/// already carrying the marker is mid-flight, and turning the flag off must not
/// abandon it. Refusing a resumption would leave a sandbox whose disk artifact
/// is published but whose local copy is still on a draining node — holding the
/// drain through the fourth gate term with nothing willing to service it, which
/// is the hazard `RESIDENT_LIFECYCLES`' own docs warn about, and which no
/// reconciler would clear: every writer that drops the marker needs a lifecycle
/// change this resting row will not get.
///
/// What that costs the owner depends on the node, and both cases are worth
/// stating because only one of them recovers:
///
/// * **Node still `draining` under `forced`.** `start` is refused with a 409 by
///   `controllers::sandbox::lifecycle::ensure_resolved_node_not_draining`,
///   before the substrate is reached at all, and the portability escape hatch
///   cannot help because it is scoped to `suspended`/`restoring`. No lifecycle
///   change happens, so the marker persists. The row is parked *and*
///   unstartable.
/// * **After `:undrain`.** The gate opens, the `start` proceeds against the
///   copy still on the node, and the lifecycle change clears the marker on the
///   way past.
///
/// Cheap to avoid either way, which is why the flag sits here rather than on
/// the listing: the listing keeps offering a marked row (see the third
/// disjunct of the drain's `evacuatable_sandbox_rows_query`) so this claim is
/// the only thing that has to distinguish opening from resuming.
///
/// Disabling the feature therefore stops the fleet acquiring new stopped
/// evacuations and lets the ones in progress drain out. A marker an abandoned
/// evacuation left behind (an `:undrain` before its teardown) counts as in
/// progress: a later forced drain resumes it, flag or not.
///
/// Writes an **operation step**: the marker is the evacuation's claim on the
/// row.
pub async fn claim_stopped_evacuation(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    allow_new_claims: bool,
    reused_durable_artifact: Option<LocalArtifactId>,
) -> Result<StoppedEvacuationClaim, sqlx::Error> {
    let durable = current_disk_artifact_backup_ready("s");
    // The `prior` CTE reads the marker as it was before this statement: a
    // single statement evaluates against one snapshot, so the join yields the
    // pre-UPDATE value even though the SET overwrites it. That distinction is
    // not bookkeeping — it is what keeps the "backup queued" sandbox event to
    // one per evacuation instead of one per tick for the length of a
    // multi-gigabyte upload, which on a node carrying dozens of stopped
    // residents is the difference between a timeline and a flood.
    let query = format!(
        // `prior` exists for one reason: this UPDATE overwrites
        // `evacuation_restore_target`, so `RETURNING` cannot tell a fresh claim
        // from a resumed one without the pre-write value. Nothing
        // safety-critical reads through it — see the `s.`-qualified pin check
        // below for why that distinction is load-bearing.
        //
        // Its staleness is bounded and benign in both directions. A marker
        // concurrently *set* reads NULL from the stale CTE, so
        // `prior.evacuation_restore_target IS NULL` is TRUE and the statement
        // reports `Claimed` for what is really a resumption — one extra
        // `ArtifactBackupQueued` event, and near-unreachable anyway, since
        // `spawn_bounded`'s per-node dedup and single-leader election mean
        // nothing else is setting this marker. A marker concurrently *cleared*
        // can only happen through a lifecycle change — that is what
        // `sandboxes_clear_evacuation_restore_target` keys on — and the
        // lifecycle qual below is a target-relation qual, so EvalPlanQual
        // catches it.
        "WITH prior AS (
             SELECT id, evacuation_restore_target
               FROM sandboxes
              WHERE id = $1
         )
         UPDATE sandboxes s
            SET evacuation_restore_target = 'stopped'
           FROM prior
          WHERE s.id = prior.id
            AND s.active_node_id = $2
            AND s.lifecycle = 'stopped'
            AND (
                (
                    prior.evacuation_restore_target IS NULL
                    AND $5
                    -- `s.`, not `prior.`. Under READ COMMITTED an UPDATE that
                    -- blocks on a row lock re-checks its target-relation quals
                    -- via EvalPlanQual when it resumes, but a `WITH` join input
                    -- is evaluated once against the statement's original
                    -- snapshot. Read through `prior`, this conjunct is
                    -- satisfiable by pre-writer data: a writer that installs an
                    -- artifact pin while leaving `lifecycle` and
                    -- `active_node_id` alone hands over the lock and the claim
                    -- still sees a pinless row.
                    --
                    -- The recheck happens because it is a target-relation qual,
                    -- not because of what the statement writes. What this
                    -- statement never writing the column buys is a different
                    -- thing: the recheck is *stable* across the retry, because
                    -- the statement's own `SET` cannot defeat it. And there is no third window: a writer
                    -- that commits before this statement's snapshot is visible
                    -- directly, and one that commits after it makes
                    -- `heap_update` return `HeapTupleUpdated`, which is what
                    -- routes us through EPQ in the first place.
                    --
                    -- Proved by a two-transaction test; reading the SQL does not
                    -- show it.
                    --
                    -- The one pinned row a fresh claim takes is the one the
                    -- caller already matched against the node (`$6`, NULL
                    -- otherwise, and `= NULL` is never true), and only while
                    -- its artifact is still a durable disk backup of this
                    -- sandbox; see step 0d of `evacuate_stopped_sandbox`.
                    AND (
                        s.current_local_artifact_id IS NULL
                        OR (s.current_local_artifact_id = $6 AND {durable})
                    )
                )
                OR prior.evacuation_restore_target = 'stopped'
            )
            AND EXISTS (
                SELECT 1
                  FROM nodes n
                 WHERE n.id = $2
                   AND n.drain_phase = $3
                   AND n.drain_mode = $4
            )
      RETURNING prior.evacuation_restore_target IS NULL"
    );
    let claimed: Option<bool> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(NodeDrainPhase::Draining)
        .bind(NodeDrainMode::Forced)
        .bind(allow_new_claims)
        .bind(reused_durable_artifact)
        .fetch_optional(executor)
        .await?;
    Ok(match claimed {
        Some(true) => StoppedEvacuationClaim::Claimed,
        Some(false) => StoppedEvacuationClaim::Resumed,
        None => StoppedEvacuationClaim::Refused,
    })
}

/// What [`claim_stopped_evacuation`] found: a fresh claim, a resumption of one
/// this reconciler already owns, or a row that is no longer a stopped resident
/// of a force-draining node.
///
/// `Claimed` and `Resumed` are the same decision — proceed — and differ only in
/// what is worth saying out loud about it. Kept apart because every tick after
/// the first is a `Resumed`, and treating them alike is how a per-tick retry
/// becomes a per-tick event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoppedEvacuationClaim {
    Claimed,
    Resumed,
    Refused,
}

impl StoppedEvacuationClaim {
    pub fn holds(self) -> bool {
        matches!(self, Self::Claimed | Self::Resumed)
    }
}

/// Mirror the disk artifact sbx-mgr just published into the cluster row's
/// current-artifact pin, under the claim's own guards.
///
/// The suspend path gets this for free — `record_suspend_commit` writes the pin
/// in the same transaction that commits the suspend — but a stopped sandbox's
/// pin is minted lazily on the node, at backup-request time, and nothing
/// reports it back. Without this write the cluster knows the artifact only as a
/// `snapshots` row, and both consumers that need the *identity* would fall back
/// to guessing: the durability gate
/// ([`pplx_space_sandbox_portability::current_disk_artifact_backup_ready`]) and
/// the disk-only restore-source selector, which resolve the artifact through
/// `snapshot_is_current_local_artifact`.
///
/// It also keeps the artifact out of the retention sweep. The sweeper reclaims
/// `stop` disk rows once they are past its age gate, and
/// `snapshot_is_live_restore_source`, which is kind-agnostic, shields the row
/// this pin names.
///
/// **Re-asserts the whole claim, including the node's drain state, and refuses
/// to overwrite a pin it did not expect.** Both halves were missing and both are
/// cheap:
///
/// * Without the `nodes` term an `:undrain` landing between the claim and here
///   would still plant a pin. The claim and the teardown authorization both
///   ask too; the teardown's placement clear does not (it re-proves only that
///   no execution is active), which is recorded as an enablement blocker.
/// * Without the pin term it wrote whatever identity the node handed back over
///   whatever was already there. Benign in the shape the claim guarantees —
///   the claim proved the row pinless, so there is nothing to overwrite — but
///   that is an argument about a *sibling* statement's guard, and the whole
///   reason this path was unsafe once already is a guard whose premise lived
///   somewhere else. `IS NULL OR = $4` keeps it self-evident and makes the
///   claim → publish window explicit: a pin appearing in it is drift, not
///   something to silently replace.
///
/// **The pin outlives an abandoned evacuation.** An `:undrain` before the
/// teardown leaves the row `stopped` with this pin *and* the marker, so a later
/// forced drain resumes the evacuation rather than opening a new one. Once the
/// owner starts the sandbox, the lifecycle change drops the marker, and the
/// start's own post-start write drops this `disk` pin
/// (`maybe_mark_started_after_start` in the API, and the transient sweeper's
/// repair of a start the node completed), so a later forced drain opens a
/// fresh evacuation. Until then a start of the stopped row is pre-bound to the
/// node holding its rootfs (`stopped_restore_plan`), never offered the artifact
/// as a cross-node fallback.
///
/// Neither term substitutes for the node drain's
/// `SubstrateSink::node_reported_artifact`. This row's pin is NULL for
/// essentially every stopped sandbox, so `IS NULL` here
/// passes trivially on exactly the rows that matter; it constrains the *second*
/// write of a pin, not the first.
///
/// Writes an **observed projection**: the identity of the artifact the node
/// just published.
pub async fn record_stopped_evacuation_artifact(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    local_artifact_id: LocalArtifactId,
) -> Result<bool, sqlx::Error> {
    let written = sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $4,
                current_local_artifact_kind = $5,
                current_local_artifact_origin = $6
          WHERE id = $1
            AND active_node_id = $2
            AND lifecycle = $3
            -- Bound separately from the lifecycle even though both are
            -- `'stopped'`: they are two different facts that happen to share a
            -- spelling, and sharing a placeholder makes changing one of them
            -- silently change the other.
            AND evacuation_restore_target = $7
            AND (
                current_local_artifact_id IS NULL
                OR current_local_artifact_id = $4
            )
            AND EXISTS (
                SELECT 1
                  FROM nodes n
                 WHERE n.id = $2
                   AND n.drain_phase = $8
                   AND n.drain_mode = $9
            )"
    ))
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(SandboxLifecycle::Stopped.as_str())
    .bind(local_artifact_id.as_uuid())
    .bind(ArtifactKind::Disk.as_str())
    .bind(ArtifactOrigin::Stop.as_str())
    .bind(SandboxLifecycle::Stopped.as_str())
    .bind(NodeDrainPhase::Draining)
    .bind(NodeDrainMode::Forced)
    .execute(executor)
    .await?;
    Ok(written.rows_affected() == 1)
}

/// Put a [`claim_evacuation_suspend`] claim back to `pre_lifecycle` and clear
/// its marker after the evacuation's suspend failed. Returns whether the row
/// went back.
///
/// Writes an **operation step**: the claim's compensation. It applies only
/// while the row is still `suspending` and no active execution has committed
/// the suspend: once one has, pod retirement owns the retry and the row stays
/// `suspending`. It doesn't re-check the node or the marker, so a
/// `suspending` row another path came to own would be reverted too. The drain
/// calls it only from the `SubstrateSink::evacuate_sandbox` call that made the
/// claim, after that call's suspend failed.
pub async fn revert_evacuation_suspend_claim(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    pre_lifecycle: SandboxLifecycle,
) -> Result<bool, sqlx::Error> {
    let reverted = sqlx::query(sql!(
        "UPDATE sandboxes s
            SET lifecycle = $2,
                evacuation_restore_target = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'suspending'
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_k8s_executions e
                 WHERE e.sandbox_id = s.id
                   AND e.active
                   AND e.state = 'suspended'
            )"
    ))
    .bind(sandbox_id)
    .bind(pre_lifecycle.as_str())
    .execute(executor)
    .await?;
    Ok(reverted.rows_affected() == 1)
}

/// Commit an evacuation suspend in the lifecycle column: `suspending` to
/// `suspended`, after the substrate suspended the execution.
///
/// Writes an **operation step**: it advances the evacuation. The guard is the
/// `suspending` lifecycle alone; neither the marker nor the node is
/// re-checked. A row that already left `suspending` matches nothing, and the
/// drain does not ask whether it matched.
pub async fn commit_evacuation_suspend(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended'
             WHERE id = $1 AND lifecycle = 'suspending'"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Finish an evacuation restore: lifecycle to `target_lifecycle`, placement to
/// `on_node`, and the evacuation marker cleared, so the sandbox stops blocking
/// the source node's drain and leaves the restore-pending scan. Returns
/// whether the row moved; `false` means something moved it out of
/// `restoring` after the restore returned.
///
/// Writes an **operation step**: it ends the evacuation. The guard is the
/// `restoring` lifecycle alone; the marker is not re-checked.
pub async fn finalize_evacuation_restore(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    target_lifecycle: SandboxLifecycle,
) -> Result<bool, sqlx::Error> {
    let finalized = sqlx::query(sql!(
        "UPDATE sandboxes
                SET lifecycle = $2,
                    placement = 'on_node',
                    evacuation_restore_target = NULL
              WHERE id = $1 AND lifecycle = 'restoring'"
    ))
    .bind(sandbox_id)
    .bind(target_lifecycle.as_str())
    .execute(executor)
    .await?;
    Ok(finalized.rows_affected() == 1)
}

/// `$1` = sandbox id, `$2` = backoff base seconds (BIGINT), `$3` = backoff
/// cap seconds (BIGINT). Records one failed cohort-restore attempt: bumps
/// `restore_attempt_count` and schedules the earliest next attempt at
/// `NOW() + min(base * 2^(previous attempt count), cap)` — so the first
/// failure waits `base`, the second `2*base`, doubling up to `cap`.
///
/// Computed in one atomic UPDATE (the doubling reads the pre-increment
/// count) so concurrent writers can't interleave a read-modify-write. The
/// exponent is clamped before `POWER` so a long-lived failer can't overflow
/// the interval arithmetic — past the clamp the `LEAST` cap has long since
/// taken over anyway.
///
/// Guarded on `evacuation_restore_target IS NOT NULL`: if a user takeover
/// cleared the marker while the failed restore attempt was in flight, the
/// sandbox is no longer the sweep's to retry and writing backoff state to it
/// would plant stale columns on a user-owned row (the marker-clear trigger's
/// backoff reset would also fight the write). `RETURNING
/// restore_attempt_count` reports the post-increment count; no row means the
/// guard didn't match.
pub fn schedule_restore_retry_query() -> &'static str {
    sql!(
        "UPDATE sandboxes
        SET restore_attempt_count = restore_attempt_count + 1,
            next_restore_attempt_at = NOW() + LEAST(
                make_interval(secs => $2::BIGINT)
                    * POWER(2, LEAST(restore_attempt_count, 16))::DOUBLE PRECISION,
                make_interval(secs => $3::BIGINT))
      WHERE id = $1
        AND evacuation_restore_target IS NOT NULL
      RETURNING restore_attempt_count"
    )
}

/// Record one failed cohort-restore attempt on an evacuation row and schedule
/// its next one ([`schedule_restore_retry_query`]). Returns the post-increment
/// attempt count, or `None` when the marker is gone.
///
/// Writes an **operation step**: the retry backoff of an evacuation restore.
/// A backoff beyond `i64::MAX` seconds saturates rather than wraps, so an
/// absurd setting degrades to "interval out of range" instead of a negative
/// interval that would silently disable the backoff.
pub async fn schedule_restore_retry(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    backoff_base: PositiveDuration,
    backoff_cap: PositiveDuration,
) -> Result<Option<i32>, sqlx::Error> {
    sqlx::query_scalar::<_, i32>(schedule_restore_retry_query())
        .bind(sandbox_id)
        .bind(secs_bind(backoff_base))
        .bind(secs_bind(backoff_cap))
        .fetch_optional(executor)
        .await
}

/// Re-drive evacuation rows stranded after their local suspended artifact was
/// published but before the controller committed `suspended`.
///
/// New nodes persist `current_local_artifact_id` in the same retirement
/// transaction that clears `active_node_id`. For those rows the published local
/// artifact is sufficient to advance to `suspended`; the pinned-local pass can
/// then enqueue backup for that exact artifact. Nullable rollout-skew rows have
/// no immutable local identity, so they retain the legacy ready-snapshot gate.
///
/// TODO(legacy): Remove the nullable-row ready-snapshot gate once every
/// suspended/evacuating row is written with `current_local_artifact_id`.
///
/// The `active_node_id IS NULL` guard is required for both branches: it proves
/// source execution retirement completed before a cross-node restore is
/// eligible. Rows stranded before that bookkeeping remains complete stay
/// visible and block drain rather than risking two live executions.
pub fn redrive_stranded_suspending_query() -> String {
    let snapshot_ready = current_suspend_snapshot_ready("s");
    format!(
        "UPDATE sandboxes AS s
            SET lifecycle = 'suspended'
          WHERE s.lifecycle = 'suspending'
            AND s.evacuation_restore_target IS NOT NULL
            AND s.active_node_id IS NULL
            AND (
                s.current_local_artifact_id IS NOT NULL
                OR (s.current_local_artifact_id IS NULL AND {snapshot_ready})
            )
        RETURNING s.id"
    )
}

/// Advance every stranded evacuation row to `suspended`
/// ([`redrive_stranded_suspending_query`]), returning the ids it advanced.
///
/// Writes an **inference**: the control plane concluding, from the retired
/// execution and the published artifact or ready snapshot, that a suspend the
/// controller never committed did finish.
pub async fn redrive_stranded_suspending_evacuations(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<SandboxId>, sqlx::Error> {
    let query = redrive_stranded_suspending_query();
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .fetch_all(executor)
        .await
}

/// `$1` = sandbox id, `$2` = source node id. Atomic mark-failed for a sandbox
/// the reconciler determined is unrecoverable. The `NOT {inflight_upload}`
/// clause closes a race that otherwise loses sandboxes: never flip a sandbox
/// to `failed` while its backup is still in flight from the source node — the
/// upload worker (which holds staged copies independent of the on-disk
/// artifacts) may finalize `ready`
/// milliseconds later.
pub fn mark_sandbox_lost_query() -> String {
    // Marker-aware, matching `pinned_local_only_sandboxes_query` and the
    // source-release guards. Give-up runs only after a backup attempt returned
    // `ArtifactMissing`, so a marked sandbox is backed up again first and only
    // marked `failed` if its artifacts are genuinely gone, never restored
    // from a stale older snapshot. For an unmarked sandbox this is identical
    // to `fresh_ready_snapshot_exists`.
    let snapshot_exists = snapshot_durable_for_source_release("sandboxes");
    // Node-agnostic: marking the sandbox `failed` is a terminal flip on the
    // sandbox row, so it must not race a durable upload from *any* node. A
    // `$2`-scoped check would miss an upload still in flight from the
    // original source after `target_node_id` drifted to `$2`.
    let inflight_upload = inflight_suspend_upload_exists("sandboxes");
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE id = $1
            AND lifecycle = 'suspended'
            AND target_node_id = $2
            AND NOT {snapshot_exists}
            AND NOT {inflight_upload}"
    )
}

/// Fail a `suspended` sandbox the drain gave up on after its node reported the
/// local artifact missing ([`mark_sandbox_lost_query`]). Returns the rows
/// flipped, `0` or `1`.
///
/// Writes an **inference**: a drain give-up. It refuses while a durable
/// snapshot exists or an upload is in flight from any node.
pub async fn mark_sandbox_lost(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
) -> Result<u64, sqlx::Error> {
    let query = mark_sandbox_lost_query();
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(source_node_id)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

/// `$1` = sandbox id, `$2` = source node id, `$3` = the observed
/// `node_k8s.k8s_node_uid` (nullable; see [`gone_node_identity_guards`]).
/// The KubeNodeGone variant of [`mark_sandbox_lost_query`], used when the
/// node's kube Node is gone and EC2 has confirmed the backing instance
/// terminated — the local artifact bytes died with the instance, so "the
/// artifact is gone" needs no on-node RPC to establish.
///
/// **Stricter snapshot guard than [`mark_sandbox_lost_query`], on purpose.**
/// That query refuses on `snapshot_durable_for_source_release`, whose
/// unmarked branch is placement-freshness-anchored — a ready snapshot left
/// placement-stale by a stuck restore's pod bump does *not* refuse it. On
/// the artifact-missing path that's correct (the freshness test is the
/// portability question, and the predicates must agree with the blocker
/// count). On this path it would be a data-loss hazard: the gone-node
/// give-up chains adopt → lost, and the adopt CAS
/// ([`adopt_remote_snapshot_for_drain_query`]) can return 0 transiently
/// (settle window) for exactly such a row even though it would adopt the
/// ready snapshot on a later tick. So this DELETE-equivalent refuses while
/// **any** `state='ready'` full snapshot exists, regardless of freshness or
/// artifact identity: when a durable copy of *some* cycle exists, the row
/// parks (visible, alertable) for adoption or operator review rather than
/// being terminally failed. With no ready snapshot at all, both portability
/// branches are necessarily false, so this guard subsumes the original
/// `NOT durable` clause. The in-flight-upload guard is kept verbatim: a
/// `creating` row from the dead node never finalizes on its own, but the
/// stuck-snapshot reaper terminalizes it (≈10h), after which this fires.
pub fn mark_sandbox_lost_on_gone_node_query() -> String {
    let inflight_upload = inflight_suspend_upload_exists("sandboxes");
    let identity_guards = gone_node_identity_guards("$2", "$3");
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE id = $1
            AND lifecycle = 'suspended'
            AND target_node_id = $2
            AND NOT EXISTS (
                SELECT 1
                  FROM snapshots sn
                  JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                 WHERE sn.sandbox_id = sandboxes.id
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
            )
            AND NOT {inflight_upload}
            {identity_guards}"
    )
}

/// Fail a `suspended` sandbox pinned to a node whose kube Node is gone and
/// whose instance is terminated ([`mark_sandbox_lost_on_gone_node_query`]).
/// `expected_uid` is the `node_k8s.k8s_node_uid` the cross-check observed.
/// Returns the rows flipped, `0` or `1`.
///
/// Writes an **inference**: a drain give-up on the cloud provider's verdict.
pub async fn mark_sandbox_lost_on_gone_node(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    expected_uid: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let query = mark_sandbox_lost_on_gone_node_query();
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(expected_uid)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

/// Shared node-identity guard clauses for the gone-node give-up writes.
/// `node_param` / `uid_param` are the bind placeholders for the node id and
/// the `node_k8s.k8s_node_uid` observed by the EC2 cross-check (they differ
/// per query). Identical in intent to the guards on
/// [`fail_resident_sandboxes_on_gone_node_query`]: between the EC2 cross-check
/// returning `Gone` and these writes landing, a same-name kube Node
/// replacement can register (`nodes.id` is `v5(name, role)`, so it reuses the
/// same row id) — its registration bumps `node_k8s.k8s_node_uid` and the
/// inventory reflector resets `drain_phase`, and either change must refuse the
/// give-up so the replacement's freshly suspended sandboxes (and its live
/// in-flight uploads) can't be mutated under the dead node's verdict.
pub fn gone_node_identity_guards(node_param: &str, uid_param: &str) -> String {
    format!(
        "AND EXISTS (
                SELECT 1 FROM nodes
                 WHERE id = {node_param}
                   AND drain_phase = 'draining'
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                 WHERE node_id = {node_param}
                   AND k8s_node_uid IS NOT DISTINCT FROM {uid_param}
            )"
    )
}

/// `$1` = sandbox id, `$2` = source node id, `$3` = the observed
/// `node_k8s.k8s_node_uid` (nullable), `$4` = settle seconds (BIGINT). The
/// KubeNodeGone variant of [`adopt_remote_snapshot_for_drain_query`]: makes a
/// pinned local-only `suspended` row durable from its remote snapshot so a
/// node whose EC2 instance is confirmed terminated can finish draining. Two
/// guards are added on top of the shared adopt CAS:
///
/// - **Node identity** ([`gone_node_identity_guards`]): refuse once a
///   same-name kube Node replacement bumped the uid or the inventory
///   reflector reset `drain_phase` — the EC2 `Gone` verdict no longer
///   describes the row's node.
/// - **Current-suspend-cycle anchor** ([`current_suspend_snapshot_ready`]).
///   The reupload give-up reaches the shared adopt CAS only with a
///   `SandboxRestoringOnNode` proof — the VM is loading *from* the snapshot,
///   so the snapshot is canonical by construction and `latest_full_snapshot_
///   ready` alone is safe. The gone-node path has no such proof: it runs for
///   *every* pinned local-only row on a terminated node. For a legacy
///   nullable row whose last `wake → modify → re-suspend` cycle never
///   registered a snapshot record at all, the newest full snapshot is the
///   prior cycle's ready one — `latest_full_snapshot_ready` would adopt it
///   and silently roll the sandbox back to pre-modification bytes. The
///   current-cycle anchor (`sn.timestamp >= lifecycle_changed_at`, the same
///   gate the crash-recovery redrive uses) refuses that: the row parks
///   (visible, alertable) or, if no ready snapshot exists at all, is marked
///   lost — an explicit failure instead of a silent rollback. For
///   identity-carrying rows the anchor adds nothing (their exact-artifact
///   branch already pins the snapshot to the current artifact), so the
///   composition only tightens the legacy branch.
pub fn adopt_remote_snapshot_on_gone_node_query() -> String {
    let inflight_upload = inflight_suspend_upload_exists("s");
    let latest_ready = latest_full_snapshot_ready("s");
    let current_cycle_ready = current_suspend_snapshot_ready("s");
    // The identity guards are uncorrelated EXISTS subqueries on `nodes` /
    // `node_k8s`, so they compose unchanged under this query's `s` alias.
    let identity_guards = gone_node_identity_guards("$2", "$3");
    format!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL,
                evacuation_restore_target = NULL,
                placement_changed_at = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'suspended'
            AND s.target_node_id = $2
            AND s.lifecycle_changed_at < NOW() - make_interval(secs => $4::BIGINT)
            AND NOT {inflight_upload}
            AND {latest_ready}
            AND {current_cycle_ready}
            {identity_guards}"
    )
}

/// Make a pinned local-only `suspended` sandbox on a gone node durable from its
/// remote snapshot ([`adopt_remote_snapshot_on_gone_node_query`]), once it has
/// rested in `suspended` for `settle`. Returns the rows flipped, `0` or `1`.
///
/// Writes an **inference**: a drain give-up that adopts the remote snapshot in
/// place of the local artifact that died with the node. The lifecycle stays
/// `suspended`.
pub async fn adopt_remote_snapshot_on_gone_node(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    settle: PositiveDuration,
    expected_uid: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(
        adopt_remote_snapshot_on_gone_node_query(),
    )))
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(expected_uid)
    .bind(secs_bind(settle))
    .execute(executor)
    .await?
    .rows_affected();
    Ok(flipped)
}

/// `$1` = sandbox id, `$2` = source node id, `$3` = the suspend-evacuable
/// lifecycle strings (the drain's `mark_sandbox_failed_on_node_lifecycles`).
/// Terminal give-up flip for a
/// sandbox the node-drain evacuation could not suspend because it is already
/// `failed` on the node while the cluster SSOT still reads it `started`/
/// `paused`. Flips the row to `'failed'` and clears any
/// `evacuation_restore_target` marker so the sandbox drops out of the resident
/// set and the node can finish draining.
///
/// Guards:
/// - `active_node_id = $2` — the sandbox is still resident on the node we
///   observed the divergence on. If it drifted (a user woke it elsewhere
///   between the failed suspend and this flip), the CAS matches nothing and
///   the next tick re-evaluates against current state.
/// - `lifecycle = ANY($3)` — still in an evacuable resting lifecycle
///   (`started`/`paused`). The evacuate task reverts the `'suspending'` claim
///   back to the pre-evacuation lifecycle before the reconciler calls this, so
///   the row is in one of these states; the guard also tolerates a concurrent
///   user `pause`/`resume` in the gap (both stay evacuable, and the sandbox is
///   failed on the node regardless, so the flip is still correct).
///
/// The data-modifying CTE captures the pre-flip lifecycle (the bare
/// `RETURNING` would only see the post-flip `'failed'`) so the caller can emit
/// the `space.sandbox` event with the real `from_state`. `FOR UPDATE SKIP
/// LOCKED` lets a concurrent completion CAS that already holds the row's lock
/// be skipped this tick rather than fought over. Extracted as a builder so the
/// guard is unit-testable against a live Postgres without a `KubernetesSubstrate`.
pub fn mark_sandbox_failed_on_node_query() -> &'static str {
    sql!(
        "WITH claimed AS (
         SELECT id, lifecycle
           FROM sandboxes
          WHERE id = $1
            AND active_node_id = $2
            AND lifecycle = ANY($3)
          FOR UPDATE SKIP LOCKED
     )
     UPDATE sandboxes s
        SET lifecycle = 'failed',
            evacuation_restore_target = NULL
       FROM claimed c
      WHERE s.id = c.id
      RETURNING c.lifecycle"
    )
}

/// Fail a sandbox the evacuation could not suspend because the node reports
/// it already `failed` ([`mark_sandbox_failed_on_node_query`]).
/// `suspend_evacuable` is the drain's suspend-evacuable lifecycle set, as
/// wire strings. Returns the pre-flip lifecycle of the row it flipped, or
/// nothing when the row drifted or another writer held its lock.
///
/// Writes an **inference**: a drain give-up on the node's refusal.
pub async fn mark_sandbox_failed_on_node(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    suspend_evacuable: &[&str],
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(mark_sandbox_failed_on_node_query())
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(suspend_evacuable)
        .fetch_all(executor)
        .await
}

/// `$1` = sandbox id, `$2` = source node id, `$3` = settle seconds (BIGINT).
/// Give-up flip for a `suspended` sandbox whose local-snapshot reupload is
/// permanently refused because it is not `Suspended` on the node (a stuck
/// on-node `restoring` while the DB reads `suspended`). Adopts an existing
/// ready remote snapshot as the canonical recovery source so the row becomes
/// durably restorable and stops blocking the node's drain, while staying
/// `suspended` and recoverable.
///
/// The flip clears the current-artifact pointer (`current_local_artifact_id`
/// /kind/origin → NULL) and the `evacuation_restore_target` marker, and resets
/// `placement_changed_at` to NULL. Together these route the portability gate
/// ([`snapshot_durable_for_source_release`]) through the unmarked legacy
/// `fresh_ready_snapshot_exists` branch with the placement-freshness
/// comparison short-circuited, so the existing ready full snapshot satisfies
/// "durable for source release" unconditionally and the row drops out of both
/// the drain's `pinned_local_only_sandboxes_query` and the drain blocker count.
/// The placement reset is load-bearing: a stuck restore that placed a pod bumped
/// `placement_changed_at = NOW()` past the (older) ready snapshot's timestamp,
/// which would otherwise keep the legacy freshness test false and leave the
/// drain wedged even after the marker/pointer were cleared.
///
/// Guards, all in one statement so a concurrent transition is either observed
/// (and matched-out) or loses its own completion CAS:
/// - `lifecycle = 'suspended' AND target_node_id = $2` — still the pinned
///   suspended row we observed; a drifted/woken sandbox matches nothing.
/// - `lifecycle_changed_at < NOW() - make_interval(secs => $3)` — the settle
///   window; a sandbox whose on-node state is merely a quick transient (a
///   freshly-completed suspend whose node is still finishing) hasn't aged in.
/// - `NOT {inflight_upload}` — never race a finalizing suspend upload from
///   **any** node, which would make the current artifact durable on its own.
/// - the **newest** full snapshot is durably `ready`
///   ([`latest_full_snapshot_ready`]) — the recoverability precondition **and**
///   the data-loss guard: it adopts only the most-recent capture, never a stale
///   prior-cycle one. If a `wake → modify → re-suspend` left a newer non-ready
///   snapshot (a failed re-suspend upload), `latest` is false and the give-up
///   does not fire, so a later restore can't load pre-modification bytes. With
///   no ready newest snapshot the UPDATE matches no row, the sandbox keeps
///   blocking, and the stuck drain stays visible (alertable). This is the same
///   newest-full-snapshot condition the evacuation cohort restore selects on,
///   so adoption and restore agree on which snapshot is canonical. Anchoring on
///   the newest snapshot — not `lifecycle_changed_at` — is also required for
///   correctness: an evacuation row's snapshot is created while it is
///   `suspending`, then `evacuate_sandbox` advances `lifecycle_changed_at` to
///   the later `suspended` transition, so a `sn.timestamp >= lifecycle_changed_at`
///   gate would never match the legitimately-current snapshot.
///
/// The caller reads `rows_affected()` to detect whether the flip fired (1 row)
/// or was a no-op (0 rows: still settling, newest snapshot not ready, in-flight
/// upload, or drifted). Extracted as a builder so the guard is unit-testable
/// against a live Postgres without constructing a `KubernetesSubstrate`.
pub fn adopt_remote_snapshot_for_drain_query() -> String {
    // Node-agnostic, matching `mark_sandbox_lost_query`: adopting the remote
    // snapshot is a sandbox-global decision, so it must not race an upload
    // still in flight from a node the row's `target_node_id` has drifted away
    // from.
    let inflight_upload = inflight_suspend_upload_exists("s");
    // Recoverability precondition + data-loss guard: the **newest** full
    // snapshot must be durably `ready`. A row reaching this give-up is pinned
    // only because of placement-staleness (a stuck restore bumped
    // `placement_changed_at` past the snapshot), not because the newest capture
    // is unready — so the snapshot we adopt is the current canonical one.
    // Keying on the newest snapshot (not `lifecycle_changed_at`, which an
    // evacuation advances to the post-suspend transition, nor
    // `placement_changed_at`, which the stuck restore bumped) is what both
    // unwedges the drain and refuses a stale prior-cycle snapshot left behind a
    // newer failed re-suspend.
    let latest_ready = latest_full_snapshot_ready("s");
    format!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = NULL,
                current_local_artifact_kind = NULL,
                current_local_artifact_origin = NULL,
                evacuation_restore_target = NULL,
                placement_changed_at = NULL
          WHERE s.id = $1
            AND s.lifecycle = 'suspended'
            AND s.target_node_id = $2
            AND s.lifecycle_changed_at < NOW() - make_interval(secs => $3::BIGINT)
            AND NOT {inflight_upload}
            AND {latest_ready}"
    )
}

/// Make a pinned local-only `suspended` sandbox durable from its newest ready
/// remote snapshot after its reupload was refused for good
/// ([`adopt_remote_snapshot_for_drain_query`]), once it has rested in
/// `suspended` for `settle`. Returns the rows flipped, `0` or `1`.
///
/// Writes an **inference**: a drain give-up that adopts the remote snapshot.
/// The lifecycle stays `suspended`.
pub async fn adopt_remote_snapshot_for_drain(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    settle: PositiveDuration,
) -> Result<u64, sqlx::Error> {
    // `PositiveDuration` guarantees a non-zero settle, so the SQL never
    // sees a zero interval that would fire on a just-suspended row.
    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(
        adopt_remote_snapshot_for_drain_query()
    )))
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(secs_bind(settle))
    .execute(executor)
    .await?
    .rows_affected();
    Ok(flipped)
}

/// `$1` = node id, `$2` = age seconds (BIGINT), `$3` = the non-evacuable
/// resident transient lifecycle strings (the drain's
/// `NON_EVACUATABLE_RESIDENT_LIFECYCLES` as `text[]`). The
/// abandon-stuck-transient sweep: flip every sandbox
/// attached to the node (`active_node_id = $1 OR target_node_id = $1`) wedged
/// in one of those lifecycles past `$2` seconds (measured by
/// `lifecycle_changed_at`) to `'failed'`, returning the claimed ids, while the
/// node is still `drain_phase = 'draining'`.
///
/// The node match is `active_node_id OR target_node_id` so the sweep covers
/// exactly the rows the drain-complete gate counts as blockers: the resident
/// set keys on `active_node_id`, but `count_drain_blocking_sandboxes_on_node`
/// counts a non-evacuation `restoring`/`suspending` via `target_node_id`. A
/// user restore that began before the drain has its `active_node_id` cleared
/// by `finish_suspended_execution` while `target_node_id` still points here,
/// so an `active_node_id`-only match would leave it inflating `blocking_count`
/// and wedge the drain despite the age-out policy.
///
/// `(evacuation_restore_target IS NULL OR lifecycle = 'suspending')` mirrors
/// the cluster-wide transient sweeper: the drain's own evacuation **restore**
/// (a marked `restoring` row, re-driven by the cluster-wide restore sweep) is
/// exempt, but a marked `suspending` row is generally not — nothing else
/// recovers a crashed/hung evacuation suspend, so leaving it exempt would let it
/// block the drain forever (`suspending` always counts toward `blocking_count`,
/// unlike a marked `restoring`).
///
/// The marked-`suspending` case this sweep must spare is a row the
/// crash-recovery redrive can advance. For new rows, a published local artifact
/// is protected even before its backup is enqueued. For nullable rollout-skew
/// rows, the current-cycle backup attempt must still be `creating` or `ready`.
/// This sweep runs before the restore sweep, so failing either recoverable form
/// here would clear the marker before redrive could proceed. Rows with neither
/// form still age out, allowing drain to converge.
///
/// `RETURNING id` makes the SELECT + UPDATE one atomic
/// statement so a concurrent transition out of the lifecycle is either
/// observed (and skipped) or loses its own completion CAS. Returns
/// `(id, pre_failure_lifecycle)` per claimed row — the data-modifying CTE
/// captures the lifecycle each row was in *before* the flip (the bare
/// `UPDATE ... RETURNING` only sees the post-flip `'failed'`) so the caller
/// can emit a per-sandbox `space.sandbox` event with the real `from_state`.
/// `FOR UPDATE SKIP LOCKED` locks the matched rows for the flip while
/// letting a concurrent completion CAS that already holds a row's lock be
/// skipped this tick (re-evaluated next tick) rather than fought over.
/// Extracted as a builder so the lifecycle/marker/age/draining gating is
/// unit-testable against a live Postgres without constructing a
/// `KubernetesSubstrate`.
pub fn abandon_stale_resident_sandboxes_query() -> String {
    // Marker-scoped: identical predicate to the transient sweeper's
    // `suspending` guard, so both age-out paths agree on the redrive-owned set.
    let recoverable = recoverable_suspend_snapshot_exists("sandboxes");
    format!(
        "WITH claimed AS (
         SELECT id, lifecycle
           FROM sandboxes
          WHERE (active_node_id = $1 OR target_node_id = $1)
            AND lifecycle = ANY($3)
            AND (evacuation_restore_target IS NULL OR lifecycle = 'suspending')
            AND NOT (
                lifecycle = 'suspending'
                AND evacuation_restore_target IS NOT NULL
                AND {recoverable}
            )
            AND lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)
            AND EXISTS (
                SELECT 1 FROM nodes
                 WHERE id = $1
                   AND drain_phase = 'draining'
            )
          FOR UPDATE SKIP LOCKED
     )
     UPDATE sandboxes s
        SET lifecycle = 'failed'
       FROM claimed c
      WHERE s.id = c.id
      RETURNING s.id, c.lifecycle"
    )
}

/// Fail every sandbox attached to a draining node that has been wedged in one
/// of `lifecycles` for longer than `age` ([`abandon_stale_resident_sandboxes_query`]).
/// `lifecycles` is the drain's non-evacuable resident transient set, as wire
/// strings. Returns `(id, pre-failure lifecycle)` for each row flipped.
///
/// Writes an **inference**: the drain's age limit.
pub async fn abandon_stale_resident_sandboxes(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    age: PositiveDuration,
    lifecycles: &[&str],
) -> Result<Vec<(SandboxId, String)>, sqlx::Error> {
    let query = abandon_stale_resident_sandboxes_query();
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(secs_bind(age))
        .bind(lifecycles)
        .fetch_all(executor)
        .await
}

/// `$1` = node id, `$2` = the resident lifecycle strings
/// (`RESIDENT_LIFECYCLES` as `text[]`), `$3` = the
/// `node_k8s.k8s_node_uid` observed by the EC2 cross-check (nullable). The
/// KubeNodeGone recovery's bulk give-up: flip every sandbox still resident on
/// a draining node (`active_node_id = $1`) to `'failed'` once the node's kube
/// Node is gone **and** EC2 has confirmed the backing instance terminated.
/// Every resident lifecycle is dead at that point — a running VM, an
/// in-flight transition, and a published-but-unbacked local artifact all
/// lived on the terminated instance's disk/memory — so unlike the age-gated
/// abandon sweep there is no settling to wait for: nothing on a terminated
/// instance resolves on its own. `suspended` is deliberately **not** in `$2`:
/// a suspended row's recoverability depends on its remote snapshot, which the
/// per-sandbox adopt/mark-lost give-up evaluates individually.
///
/// Two sparing clauses are shared with [`abandon_stale_resident_sandboxes_query`]
/// because those rows are recoverable *without* the dead node:
/// - A **marked `restoring`** row (`evacuation_restore_target IS NOT NULL`)
///   reads from the durable S3 snapshot; the cluster-wide restore sweep
///   re-drives it onto a healthy cohort node (its crash-recovery arm selects
///   `restoring` + marker rows), so failing it would lose a recoverable
///   sandbox.
/// - A **marked `suspending`** row whose current cycle still has recoverable
///   evidence ([`recoverable_suspend_snapshot_exists`]) is spared. Within
///   this sweep's `active_node_id = $1` scope only the legacy
///   current-cycle-`creating`/`ready` snapshot branch can match (the
///   exact-identity branch requires `active_node_id IS NULL`): a `ready`
///   backup means the bytes are durable, and a `creating` one is settled by
///   the stuck-snapshot reaper (its terminal flip makes the row eligible
///   here on a later tick). Failing either now would discard a durable or
///   pending-durable copy. The `ready`-evidence row is spared knowing it
///   has no automatic path forward today — the crash-redrive requires
///   `active_node_id IS NULL` (proof of execution retirement, which the
///   dead node never completed) and the restore sweep selects only
///   `suspended`/`restoring` — so it parks the drain, lossless and visible
///   (the stuck-drain warn plus `node_drain.sandboxes_remaining`), until an
///   operator advances it. A gone-node redrive that performs the missing
///   execution-retirement bookkeeping under the EC2-terminated verdict is
///   future work; it belongs in the substrate store, which owns that
///   bookkeeping.
///
/// Guards, atomic with the flip:
/// - `EXISTS (... drain_phase = 'draining')` — an admin `:undrain` landing
///   between `list_draining_nodes()` and this UPDATE aborts the sweep
///   (mirrors [`abandon_stale_resident_sandboxes_query`]).
/// - `EXISTS (node_k8s.k8s_node_uid IS NOT DISTINCT FROM $3)` — refuse if a
///   same-name kube Node replacement bumped the uid since the EC2 cross-check
///   observed it: `nodes.id` is `v5(name, role)`, so a replacement instance
///   reuses the same row id, and without this guard fresh sandboxes placed on
///   the replacement could be swept in the window before the inventory
///   reflector resets `drain_phase`. Mirrors `drained_node_terminator`'s
///   uid-guarded delete.
///
/// Returns `(id, pre_failure_lifecycle)` per claimed row via the
/// data-modifying CTE (the bare `RETURNING` would only see the post-flip
/// `'failed'`) so the caller can emit per-sandbox `space.sandbox` events with
/// the real `from_state`. `FOR UPDATE SKIP LOCKED` lets a concurrent
/// completion CAS that already holds a row's lock be skipped this tick rather
/// than fought over. No substrate teardown follows the flip: the kube Node is
/// gone, so its pods are reaped by Kubernetes' pod garbage collector, not by
/// us. Extracted as a builder so the guard set is unit-testable against a
/// live Postgres without constructing a `KubernetesSubstrate`.
pub fn fail_resident_sandboxes_on_gone_node_query() -> String {
    let recoverable = recoverable_suspend_snapshot_exists("sandboxes");
    format!(
        "WITH claimed AS (
         SELECT id, lifecycle
           FROM sandboxes
          WHERE active_node_id = $1
            AND lifecycle = ANY($2)
            AND (evacuation_restore_target IS NULL OR lifecycle = 'suspending')
            AND NOT (
                lifecycle = 'suspending'
                AND evacuation_restore_target IS NOT NULL
                AND {recoverable}
            )
            AND EXISTS (
                SELECT 1 FROM nodes
                 WHERE id = $1
                   AND drain_phase = 'draining'
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                 WHERE node_id = $1
                   AND k8s_node_uid IS NOT DISTINCT FROM $3
            )
          FOR UPDATE SKIP LOCKED
     )
     UPDATE sandboxes s
        SET lifecycle = 'failed',
            evacuation_restore_target = NULL
       FROM claimed c
      WHERE s.id = c.id
      RETURNING s.id, c.lifecycle"
    )
}

/// Fail every sandbox still resident in one of `lifecycles` on a draining node
/// whose kube Node is gone and whose instance is terminated
/// ([`fail_resident_sandboxes_on_gone_node_query`]). `lifecycles` is the
/// resident set, as wire strings; `expected_uid` is the
/// `node_k8s.k8s_node_uid` the cross-check observed. Returns `(id,
/// pre-failure lifecycle)` for each row flipped.
///
/// Writes an **inference**: a drain give-up on the cloud provider's verdict.
pub async fn fail_resident_sandboxes_on_gone_node(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    lifecycles: &[&str],
    expected_uid: Option<&str>,
) -> Result<Vec<(SandboxId, String)>, sqlx::Error> {
    let query = fail_resident_sandboxes_on_gone_node_query();
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(lifecycles)
        .bind(expected_uid)
        .fetch_all(executor)
        .await
}

/// The pod-less give-up's revert of a resident with no pod to `suspended`,
/// unplaced. Matches nothing unless a durable suspend snapshot passes
/// [`failed_repair_revert_eligibility`] and both
/// [`pod_less_drain_revert_loses_no_newer_bytes`] and
/// [`newest_full_snapshot_unambiguous`] hold, all evaluated by this
/// statement: the same predicates the drain's precheck reads. Returns the
/// finalization when the row flipped, so the caller retires the executions
/// and resets the port exposures with the same assignments.
///
/// Writes an **inference**: a drain give-up, the control plane concluding
/// from the apiserver, the node's teardown record and its own rows that
/// nothing runs the sandbox. The statement has no lifecycle, placement or
/// node guard of its own; it relies on the caller's precheck re-run in the
/// same transaction under the node row's shared lock and the sandbox and
/// execution row locks.
pub async fn revert_pod_less_resident_to_suspended(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<Option<SuspendedK8sFinalizationSql>, sqlx::Error> {
    let eligibility = failed_repair_revert_eligibility("s");
    let marker_assignments =
        clear_current_local_artifact_marker_assignments("s", "pre.clear_marker");
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "pre",
        &marker_assignments,
        SuspendedTargetNodeAssignment::Clear,
    );
    let query = format!(
        "WITH pre AS (
             SELECT id,
                    {clear_marker} AS clear_marker
               FROM sandboxes s
              WHERE s.id = $1
         )
         UPDATE sandboxes s
            SET {assignments}
           FROM pre
          WHERE s.id = pre.id
            AND {eligible}
            AND {loses_no_newer_bytes}
            AND {unambiguous}",
        clear_marker = eligibility.clear_marker,
        assignments = finalization.sandbox_assignments(),
        eligible = eligibility.eligible,
        loses_no_newer_bytes = pod_less_drain_revert_loses_no_newer_bytes("s"),
        unambiguous = newest_full_snapshot_unambiguous("s"),
    );
    let reverted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    Ok((reverted > 0).then_some(finalization))
}
