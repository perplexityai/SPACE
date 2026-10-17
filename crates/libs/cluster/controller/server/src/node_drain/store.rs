use pplx_space_common_core::sql;
use pplx_space_common_core::types::SnapshotId;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::ActivationOptions;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_api::controllers::substrate::{
    EnqueuedArtifactBackup, NodeApiTarget, NodeReportedArtifact, NodeSchedulability,
    NodeSchedulabilityRequest, OwnedNodeApiTarget, PublishedLocalArtifact, SubstrateAdapter,
    SubstrateError, WaitReadyTimeoutPhase, connect_retrying_node_api_client_for_target,
    enqueue_artifact_backup_with_node_api, sandbox_state_with_node_api,
};
use pplx_space_api::reconcile_queue::{ReconcileRef, enqueue_sandbox_reconcile};
use pplx_space_common_core::kubernetes::EvacuationPlacement;
use pplx_space_common_core::node_drain_signals::{
    EVACUATION_RESTORE_READY_CHANNEL, REUPLOAD_ARTIFACT_MISSING_MARKER,
    REUPLOAD_SANDBOX_RESTORING_MARKER, REUPLOAD_SANDBOX_UNKNOWN_MARKER,
    SUSPEND_SANDBOX_FAILED_MARKER,
};
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, BackupSource, LocalArtifactId, NodeDrainMode, NodeId,
    NodeManagedBy, NodeRole, PositiveDuration, SandboxId, SubstrateKind,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_sandbox_portability::{
    current_disk_artifact_backup_ready, current_suspend_snapshot_ready,
    drain_gate_blockers_for_node, inflight_suspend_upload_exists, latest_full_snapshot_ready,
    recoverable_suspend_snapshot_exists, snapshot_durable_for_source_release,
    snapshot_is_current_local_artifact,
};
use pplx_space_sandbox_store as sandbox_store;

use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_node_api_client::NodeApiClient;
use sqlx::PgPool;

use pplx_space_common_svc::sql::sandbox_residency::{
    active_resident_predicate, occupies_node_predicate, resident_lifecycle, resident_lifecycle_strs,
};

use super::{
    DrainingNode, NON_EVACUATABLE_RESIDENT_LIFECYCLES, NodeDrainSink, PHASE_DRAINED,
    PHASE_DRAINING, PHASE_TERMINATING, PromoteError, ResidencyEvidence, ResidentSandboxCounts,
    SUSPEND_EVACUABLE_LIFECYCLES, TerminationBlockingResidents, enabled_evacuable_lifecycles,
    is_evacuable_lifecycle, is_suspend_evacuable_lifecycle,
};
use pplx_space_common_core::types::SandboxLifecycle;

fn emit_node_drain_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    reason: &'static str,
    details: SandboxEventDetails,
) {
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.reason(reason).trigger(SandboxEventTrigger::System));
}

fn emit_node_drain_sandbox_failed_event(
    sandbox_id: SandboxId,
    reason: &'static str,
    details: SandboxEventDetails,
) {
    emit_node_drain_event(
        sandbox_id,
        SandboxEventType::SandboxFailed,
        reason,
        details.operation(reason).with_to_state("failed"),
    );
}

/// Whether an upload-promotion failure should drop the cached client for its
/// node. Transport-shaped errors only — as with the sweeper's probe, a
/// `LockContention` abort is a database race and leaves the connection fine.
fn should_evict_promotion_client(error: &SubstrateError) -> bool {
    matches!(
        error,
        SubstrateError::Unavailable { .. }
            | SubstrateError::Failed { .. }
            | SubstrateError::Timeout { .. }
            | SubstrateError::Cancelled { .. }
    )
}

/// Is this the `draining → drained` transition? Two things key off that edge and
/// no other: the promotion node-api client is evicted (the node is done being
/// asked to upload), and the phase UPDATE re-checks the resident guard (see
/// [`set_drain_phase_query`]).
fn is_drain_completion_edge(expected_phase: &str, new_phase: &str) -> bool {
    expected_phase == PHASE_DRAINING && new_phase == PHASE_DRAINED
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
async fn claim_evacuation_suspend(
    pool: &PgPool,
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
    .bind(PHASE_DRAINING)
    .bind(NodeDrainMode::Forced)
    .execute(pool)
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
/// **A fresh claim requires `current_local_artifact_id IS NULL` — a backstop,
/// not the guarantee.** The guarantee lives one call earlier, in
/// [`SubstrateSink::node_reported_artifact`], because the row that decides
/// whether the next publish mints or reuses is sbx-mgr's, not this one. Read
/// that first; this conjunct is here to make the refusal *atomic with the
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
/// A successful start clears the node's pin (`mark_start_succeeded`), so the
/// pin normally describes the rootfs as of the latest stop. Other paths into
/// `stopped` need not: a start that boots the guest and then fails is rolled
/// back through `apply_cleanup_state`, which keeps the existing pin
/// (`COALESCE(?11, current_local_artifact_id)`) while the guest may have
/// written to the rootfs in between. Such a row carries a pin older than its
/// bytes, and its `snapshots.timestamp` — *registration* time — still looks
/// recent.
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
/// whose mint it caused. A pinless row mints, so the artifact is of the rootfs
/// as it stands; a pinned row reuses, so nothing published can be attributed.
///
/// **But "pinned" has to mean pinned on the node.** For a `stopped` sandbox this
/// column is NULL almost always, whatever sbx-mgr holds: the projection that
/// carries the node's pin into Postgres is gated to `status = 'suspended'`, and
/// the retire that causes the stop-time mint discards the enqueue response. So
/// this conjunct alone fails **open** — it admits the row it exists to refuse.
/// [`SubstrateSink::node_reported_artifact`] asks the node directly and is what
/// makes the property hold; what this conjunct adds is atomicity, closing the
/// window between that answer and the marker write for anything that would also
/// have updated the cluster row. A refusal here or there leaves the sandbox
/// alone and parks the node visibly — on
/// `node_drain.stopped_evacuation{outcome=unattributable_artifact}` and a log
/// line naming the node and the sandbox, not on the stall gauge, which selects
/// on a marker this refusal returns before writing — rather than destroying a
/// user's disk on evidence about different bytes.
///
/// **Which today means: almost always.** Every ordinarily stopped resident
/// carries a pin: the execution retire issues
/// `EnqueueArtifactBackup(CurrentInstance, Disk)` on every stop, and on a
/// pinless row that mints and pins. That pin is usually current, but this guard
/// cannot tell it from one a failed start left behind, so a fresh evacuation
/// refuses nearly every stopped resident and the feature is safe-but-inert
/// until the node side changes. That is the right trade for a default-disabled
/// path whose failure mode is silent data loss, but it is a trade, not a corner
/// case.
///
/// **Two routes out, and they cost very different things.**
///
/// One is to have the node drop any pin that could be stale on every path into
/// `stopped`, so a stopped row's pin is always one minted from its current
/// rootfs. It is still *present*, though, so this refusal still fires, and
/// relaxing it becomes a co-requisite: the two would land together or not at
/// all.
///
/// The other is to make the refusal *precise* instead of relaxing it, by
/// comparing the pinned artifact's capture time against the node's own
/// stopped-since time. A stopped rootfs cannot change, so an artifact captured
/// at or after the row entered `stopped` describes the current bytes. Both
/// clocks are node-local — one machine, one `now()` — which is what separates
/// this from the timestamp arguments above: those compared *cluster
/// registration* time against a cluster clock, and a cross-machine version
/// would accept a stale artifact under clock skew in one direction. The node
/// already reports `state_changed_at` and this path already receives it; the
/// artifact's `created_at` is write-once on the node and not yet exposed. That
/// route is read-only, purely additive, needs no change to the stop path, and
/// leaves this guard sound whether or not the pin is ever cleared.
///
/// A **resumed** claim is not re-checked against the node on every tick, and
/// what keeps that sound is the **conjunction** every read of this claim
/// requires: the marker *and* `lifecycle = 'stopped'` (see
/// [`SubstrateSink::stopped_evacuation_state`]). A completed start or a restore
/// has to move the lifecycle, and on moving it the claim stops holding whatever
/// the marker says — the `sandboxes_clear_evacuation_restore_target` trigger
/// is the second line of that, not the first, because its allowlist *keeps*
/// the marker for `suspending`, `suspended` and `restoring`.
///
/// The conjunction does not cover a start that fails. A Kubernetes start of a
/// `stopped` sandbox boots its pod while the row still reads `stopped`, so one
/// that boots, lets the guest write and then fails leaves the row exactly as
/// the claim last saw it. The teardown re-asks the node before releasing
/// anything (`tear_down_stopped_local_copy`), but that only catches the case
/// once the node drops the pin on such a rollback; until then the pin it keeps
/// still matches. The window is widest for a marker an abandoned evacuation
/// left behind, which a later forced drain resumes.
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
/// the listing: the listing keeps offering a marked row (see
/// [`evacuatable_sandbox_rows_query`]'s third disjunct) so this claim is the
/// only thing that has to distinguish opening from resuming.
///
/// Disabling the feature therefore stops the fleet acquiring new stopped
/// evacuations and lets the ones in progress drain out. A marker an abandoned
/// evacuation left behind (an `:undrain` before its teardown) counts as in
/// progress: a later forced drain resumes it, flag or not.
async fn claim_stopped_evacuation(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    allow_new_claims: bool,
) -> Result<StoppedEvacuationClaim, sqlx::Error> {
    // The `prior` CTE reads the marker as it was before this statement: a
    // single statement evaluates against one snapshot, so the join yields the
    // pre-UPDATE value even though the SET overwrites it. That distinction is
    // not bookkeeping — it is what keeps the "backup queued" sandbox event to
    // one per evacuation instead of one per tick for the length of a
    // multi-gigabyte upload, which on a node carrying dozens of stopped
    // residents is the difference between a timeline and a flood.
    let claimed: Option<bool> = sqlx::query_scalar(sql!(
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
                    AND s.current_local_artifact_id IS NULL
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
    ))
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(PHASE_DRAINING)
    .bind(NodeDrainMode::Forced)
    .bind(allow_new_claims)
    .fetch_optional(pool)
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
enum StoppedEvacuationClaim {
    Claimed,
    Resumed,
    Refused,
}

impl StoppedEvacuationClaim {
    fn holds(self) -> bool {
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
/// It does **not** buy retention safety, though the shared fragment makes it
/// look as if it should. `snapshot_is_live_restore_source` is kind-agnostic and
/// would shield a pinned disk row, but the retention sweeper filters
/// `type = 'full'` in both its discovery and its claim gates, so a disk row
/// never reaches that guard — it is outside the candidate set entirely. Disk
/// artifacts are therefore never pruned today, which is safe in the direction
/// that matters here and a growth problem in the other one.
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
/// owner starts the sandbox, the lifecycle change drops the marker but nothing
/// on the Kubernetes start path clears the pin: a later forced drain then
/// refuses the sandbox at the cluster-pin check, and the disk-only restore
/// selector offers this artifact to later starts, whose plan turns the source
/// node into a preference with S3 as the fallback. A start that lands on
/// another node takes the cross-node disk path, which cannot finish today and
/// would restore stale bytes once it can. That blocks enabling the flag until
/// the pin is cleared on a start or written later in the sequence.
///
/// Neither term substitutes for [`SubstrateSink::node_reported_artifact`]. This
/// row's pin is NULL for essentially every stopped sandbox, so `IS NULL` here
/// passes trivially on exactly the rows that matter; it constrains the *second*
/// write of a pin, not the first.
async fn record_stopped_evacuation_artifact(
    pool: &PgPool,
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
    .bind(PHASE_DRAINING)
    .bind(NodeDrainMode::Forced)
    .execute(pool)
    .await?;
    Ok(written.rows_affected() == 1)
}

/// The inputs `connect_retrying_node_api_client_for_target` needs to reach one
/// node's node-api, read from the `nodes` row and its substrate-specific
/// sibling.
///
/// Shared by the two paths that talk to a *source* node during a drain — the
/// suspended-artifact promotion and the stopped-disk publish — because both ask
/// the same question of the same row, and a second copy of this join is a
/// second place for the `managed_by` / role resolution to drift.
/// `None` when the node row is gone — the caller decides whether that is a
/// retryable lookup failure or a give-up, since the two consumers differ.
async fn node_api_row(
    pool: &PgPool,
    node_id: NodeId,
) -> Result<Option<OwnedNodeApiTarget>, sqlx::Error> {
    type Row = (NodeRole, NodeManagedBy, Option<String>, Option<String>);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT n.role,
                n.managed_by,
                nd.node_api_endpoint AS direct_endpoint,
                nk.k8s_node_name AS substrate_node_id
           FROM nodes n
           LEFT JOIN node_direct nd ON nd.node_id = n.id
           LEFT JOIN node_k8s nk ON nk.node_id = n.id
          WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(role, managed_by, direct_endpoint, substrate_node_id)| OwnedNodeApiTarget {
            managed_by,
            role,
            direct_endpoint,
            substrate_node_id,
        },
    ))
}

async fn revert_evacuation_suspend_claim(
    pool: &PgPool,
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
    .execute(pool)
    .await?;
    Ok(reverted.rows_affected() == 1)
}

/// `$1` = node id, `$2` = evacuable lifecycles as `text[]`. The suspend →
/// restore work queue for one draining node.
///
/// Census-absent rows are excluded here rather than filtered in Rust: the whole
/// point is to keep them out of the per-node parallelism budget. A large
/// historical set could otherwise consume every slot with nonexistent sandboxes.
///
/// The third disjunct is the **in-flight** clause, and it deliberately does not
/// consult `$2`. `$2` carries the lifecycles this environment has *enabled*, so
/// a stopped evacuation opened while the feature was on would vanish from this
/// list the moment an operator turned it off — with its disk artifact published
/// and its local copy still on the node, holding the drain and refusing its
/// owner's `start` forever. A row already carrying the marker is work in
/// progress, and this queue has to keep offering it until it finishes.
/// [`claim_stopped_evacuation`] is where the flag actually bites: it refuses to
/// open a new evacuation while resuming an existing one.
///
/// **The `ORDER BY` is load-bearing, not tidiness.** `spawn_bounded` walks this
/// list in order and stops at the per-node slot budget (16), so list position
/// decides who is dispatched at all. Every other population in this list
/// eventually leaves it — a suspend commits and the lifecycle moves, or the
/// stale-abandon sweep flips a wedged transient to `failed`. A `stopped` row
/// that refuses on provenance writes **nothing**: it returns before the claim,
/// so its lifecycle, marker and placement are untouched and it matches the
/// first disjunct again on the next tick, unchanged. With no ordering, Postgres
/// returns an unmodified heap in a stable order, so the same rows win every
/// tick — and if 16 or more of them are `stopped` refusals sitting ahead of the
/// `started` residents, those residents are never dispatched and the drain
/// never converges. That is the inverse of this feature's purpose: it would
/// hold live sandboxes on a node in order to keep retrying rows that cannot
/// move.
///
/// So the suspend population is dispatched first. It is finite and it drains —
/// a draining node is cordoned, so no new residents arrive — after which the
/// stopped rows get the whole budget. Ordering the other way, or not at all, is
/// the only version that can starve.
///
/// Oldest-first within each population, because under budget pressure list
/// position is the only fairness knob there is.
///
/// One residual: a `stopped` row that can *never* be attributed still
/// re-competes for a slot every tick, so a large enough set of them can crowd
/// out the *evacuable* stopped rows behind them. That is bounded by the
/// `unattributable_artifact` counter being visible, and it stops mattering when
/// the refusal stops being the common case — see [`claim_stopped_evacuation`]
/// for the node-side fact that makes it rare.
fn evacuatable_sandbox_rows_query(residency: ResidencyEvidence) -> String {
    let occupies = occupies_node_predicate("s", "$1", residency);
    format!(
        "SELECT s.id,
                CASE
                    WHEN s.lifecycle = 'suspending' THEN s.evacuation_restore_target
                    ELSE s.lifecycle
                END AS lifecycle
           FROM sandboxes s
          WHERE s.active_node_id = $1
            AND (
                s.lifecycle = ANY($2)
                OR (
                    s.lifecycle = 'suspending'
                    AND s.evacuation_restore_target = ANY($2)
                )
                OR (
                    s.lifecycle = 'stopped'
                    AND s.evacuation_restore_target = 'stopped'
                )
            )
            AND {occupies}
          ORDER BY CASE WHEN s.lifecycle = 'stopped' THEN 1 ELSE 0 END,
                   s.lifecycle_changed_at,
                   s.id"
    )
}

/// `$1` = node id, `$2` = `RESIDENT_LIFECYCLES` as `text[]`.
/// `(occupying, total)` for one draining node: rows that still block
/// `draining → drained`, and every row in a resident lifecycle pinned to it.
/// The caller derives the discounted count by subtraction.
///
/// Counts, not ids — the reconciler only asks "how many" and "any left?", and
/// returning ids would ship discarded historical rows over the wire every tick.
///
/// Flat, with the expensive absence proof appearing exactly once inside the
/// `FILTER`: the bare `COUNT(*)` does not reference it, so there is nothing for
/// the planner to duplicate. Returning `total` rather than a second
/// `COUNT(*) FILTER (WHERE NOT …)` is what keeps that true.
fn resident_sandbox_counts_query(residency: ResidencyEvidence) -> String {
    let occupies = occupies_node_predicate("s", "$1", residency);
    format!(
        "SELECT COUNT(*) FILTER (WHERE {occupies})::BIGINT AS occupying,
                COUNT(*)::BIGINT AS total
           FROM sandboxes s
          WHERE s.active_node_id = $1
            AND s.lifecycle = ANY($2)"
    )
}

/// `$1` = node id. Per-lifecycle count of the sandboxes that would make the
/// drained-node terminator's resident guard refuse this node.
///
/// The predicate is `sandbox_residency::active_resident_predicate` itself — the
/// same string `mark_terminating_query` and the KubeNodeGone reap re-check
/// atomically — not a restatement of it. Its docs carry which lifecycles that
/// is and why it differs from [`resident_sandbox_counts_query`]'s include-list.
///
/// Unordered on purpose: `GROUP BY` alone lets the planner hash-aggregate, while
/// `ORDER BY lifecycle` can tip it into sorting every input row (thousands, on
/// the nodes where this matters) to order a handful of groups. The column is
/// plain `TEXT` with no CHECK, and the predicate is an exclusion list, so an
/// unmodelled string counts too and the group count has no hard bound. In
/// practice: 14 of the 16 `SandboxLifecycle` variants qualify, and at the only
/// call site the resident scan has already reported zero, so the ten
/// include-list lifecycles are absent and four modelled groups remain. The
/// caller sorts them.
fn termination_blocking_residents_query(residency: ResidencyEvidence) -> String {
    let resident = active_resident_predicate("sandboxes", "$1", residency);
    format!(
        "SELECT lifecycle, COUNT(*)::BIGINT AS blocking
           FROM sandboxes
          WHERE {resident}
          GROUP BY lifecycle"
    )
}

/// How long the drain-completion write waits for the `nodes` row lock before
/// giving up and deferring to the next tick.
///
/// Bounded because `run_tick` iterates draining nodes sequentially: an unbounded
/// wait on one contended row stalls every node behind it. A timeout costs that
/// one node a tick, which is the right trade for an advisory pre-check plus an
/// authoritative write. Named rather than inlined so
/// `termination_gate_tests::wait_until_blocked` can bound its own poll well
/// inside it — a poll that outlasts this turns a clean assertion failure into a
/// `55P03` on the `?`.
pub(crate) const DRAIN_COMPLETION_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// True iff `error` is Postgres `55P03` (`lock_not_available`) — the
/// [`DRAIN_COMPLETION_LOCK_TIMEOUT`] firing rather than a database problem.
///
/// Load-bearing for log volume, not just tidiness: without it a node whose
/// `nodes` row is held by a long transaction produces one
/// `set_drain_phase failed` WARN every [`super::DEFAULT_TICK`] — ~720 an hour,
/// in the same channel this reconciler's pacer exists to protect — and it reads
/// identical to a real outage for anyone alerting on it.
pub(crate) fn is_lock_timeout(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "55P03")
}

/// `$1` = node id, `$2` = new phase, `$3` = last error, `$4` = expected phase.
///
/// When `guard_residency` is set the UPDATE additionally re-checks the
/// drained-node terminator's resident guard **inside the statement**, mirroring
/// `drained_node_terminator::store::mark_terminating_query`, which re-checks its
/// own guards for exactly the same reason.
///
/// Load-bearing, not belt-and-braces. `handle_draining_node` evaluates the
/// gate's four terms as separate reads, so a sandbox that attaches to the node
/// between the last read and this UPDATE would otherwise commit `drained` and
/// then be refused by the terminator on every tick — the permanent park this
/// whole gate exists to prevent, reintroduced through the race. Because the
/// terminator's exclusion list is a superset of the resident include-list, one
/// guard closes the window for the resident-scan term too, which predates it.
///
/// It does **not** cover the two terms that aren't attributed through
/// `active_node_id`: the in-flight evacuation set (process-local) and the
/// snapshot blockers (pinned via `target_node_id`). Those keep the pre-existing
/// pre-check-only behavior, and a row landing in either window parks the node in
/// `drained` exactly as it does today.
/// Which population the `draining → drained` write refuses on.
///
/// Two different jobs used to share one predicate, and that conflated them: the
/// guard closes a TOCTOU race for the resident term (which is enforced on every
/// tick, in every environment), *and* it can enforce the wider
/// termination-blocking term (which is behind
/// `SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE`). Rendering the wide predicate
/// unconditionally made the flag a no-op at the SQL layer: with the gate off, a
/// node holding a `stopped` sandbox still matched no row and stayed in
/// `draining`, which is exactly the behaviour the flag exists to withhold. The
/// reconciler-side hold was gated; this write was not, so the flag bought
/// nothing but a missing WARN.
///
/// [`Self::Resident`] is therefore the gate-off scope: it keeps the race closed
/// for the term that is always live, while enforcing nothing the flag has not
/// enabled. Dropping the guard entirely when the gate is off would have been the
/// smaller diff and the worse trade — it reopens a race that strands a `started`
/// sandbox on a `drained` node, in every cluster as shipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuardScope {
    /// Not the completion edge (cordon convergence, terminating-phase error
    /// writes). No residency guard at all.
    None,
    /// Gate off: the resident include-list only — the same population
    /// `resident_sandbox_counts_query` counts, so this refuses exactly what the
    /// pre-check already refused and adds only atomicity.
    Resident,
    /// Gate on: the drained-node terminator's exclusion list, which is a
    /// superset of [`Self::Resident`] and additionally covers `created`,
    /// `stopped`, `deleting`, and `quarantined`.
    Wide,
}

fn set_drain_phase_query(residency: ResidencyEvidence, scope: GuardScope) -> String {
    let resident_guard = match scope {
        GuardScope::None => String::new(),
        GuardScope::Resident => {
            // Mirrors `resident_sandbox_counts_query`'s `occupying` count: the
            // include-list plus the census term, over `active_node_id`.
            let occupies = occupies_node_predicate("sandboxes", "$1", residency);
            let resident = resident_lifecycle("sandboxes");
            format!(
                "\n               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE sandboxes.active_node_id = $1
                      AND {resident}
                      AND {occupies}
               )"
            )
        }
        GuardScope::Wide => {
            let resident = active_resident_predicate("sandboxes", "$1", residency);
            format!(
                "\n               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes WHERE {resident}
               )"
            )
        }
    };
    format!(
        "UPDATE nodes
            SET drain_phase = $2,
                drain_last_error = $3
          WHERE id = $1
            AND drain_phase = $4{resident_guard}"
    )
}

async fn list_evacuatable_sandbox_rows(
    pool: &PgPool,
    node_id: NodeId,
    residency: ResidencyEvidence,
    evacuate_stopped: bool,
) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
    // The *enabled* subset, not the whole vocabulary: `EVACUABLE_LIFECYCLES`
    // says what the pipeline can service, this says what this environment has
    // turned on. Filtering here rather than in Rust keeps a disabled `stopped`
    // row out of the per-node evacuation budget entirely.
    let evacuable_strs: Vec<&'static str> = enabled_evacuable_lifecycles(evacuate_stopped)
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    let query = evacuatable_sandbox_rows_query(residency);
    sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(&evacuable_strs)
        .fetch_all(pool)
        .await
}

// ---------------------------------------------------------------------------
// SQL builders (extracted so the backup/portability gate is unit-testable against
// a live Postgres without constructing a full `KubernetesSubstrate`).
// ---------------------------------------------------------------------------

/// `$1` = node id. Counts sandboxes that **block** `draining → drained` for
/// the node: the mid-flight / not-yet-portable suspended set pinned to the
/// node, **plus** any sandbox whose artifact backup is still in flight *from*
/// this node (`state='creating'`, `source_node_id = $1`).
/// The in-flight-upload term is load-bearing: a backup-in-flight sandbox must
/// hold the node until the volume manager finalizes the attempt `ready`,
/// otherwise terminating the node kills the in-flight upload worker and
/// strands the only durable copy.
///
/// ## Why this counts the batched pair form rather than scanning `sandboxes`
///
/// `drain_gate_blocks_source_release` cannot drive a scan of `sandboxes`. Its
/// two disjuncts attribute to different columns — one to the sandbox's
/// `target_node_id`, one to a snapshot's `source_node_id` — so an `OR` over
/// both leaves the planner no indexable driver and it seq-scans the table.
/// That is documented on [`drain_gate_blocker_pairs`], which exists because of
/// it, giving each attribution its own indexable driver and an
/// already-differential-tested equivalence with the single-node form. Even
/// a single-node request must avoid scanning the whole cluster.
///
/// **Deliberately uncapped, unlike [`pinned_local_only_sandboxes_query`].** An
/// outer `LIMIT` is the lever there and is not one here, because
/// [`drain_gate_blocker_pairs`] is a `UNION`: Postgres has to consume and
/// deduplicate both branches before an outer limit can return anything, so the
/// limit truncates the result without bounding the scan. The indexable driver
/// and reshaped in-flight predicate bound the work; the uncapped result keeps
/// `node_drain.drain_blockers` exact.
///
/// Cost tracks the node's accumulated `target_node_id` history, with lifecycle
/// filtering and per-row durability probes. Naming `ARRAY[$1]` inline and
/// filtering `node_id = $1` outside keeps the node filter visible to the planner
/// instead of encouraging a cluster-wide lifecycle bitmap.
pub(crate) fn drain_blocking_sandboxes_query() -> String {
    // The predicate itself lives in `pplx-space-sandbox-portability` so
    // anything that predicts this gate counts the same rows it counts. Its docs
    // carry the per-state rationale, including why an evacuation `restoring` is
    // excluded and why two of the blocking states pin through `target_node_id`
    // with `active_node_id` NULL.
    //
    // `drain_gate_blockers_for_node` rather than a wrapper written here: the
    // drained-node terminator needs the same rows for the same node, and one
    // builder is what keeps the two gates counting the same set. Its docs carry
    // why the single-node predicate cannot drive a `sandboxes` scan.
    let blockers = drain_gate_blockers_for_node("$1");
    format!(
        "SELECT COUNT(*)::BIGINT
           FROM (
{blockers}
               ) node_blockers"
    )
}

/// Sandboxes anywhere in the cluster that were evacuated off a draining node
/// and are now ready to be placed onto a target-cohort node: a non-NULL
/// `evacuation_restore_target` marker, a lifecycle this evacuation can rest or
/// re-drive in, **and** a ready remote backup of the bytes its restore will
/// read.
///
/// Which backup that is depends on the marker. A `started`/`paused` evacuation
/// suspended the VM, so it waits on the newest full snapshot; a `stopped`
/// evacuation published a disk artifact instead, so it waits on that pinned
/// artifact. See the comment on the two arms below.
///
/// For identity-backed rows, `latest_full_snapshot_ready` requires the ready
/// backup to match `current_local_artifact_id`. Nullable rollout-skew rows
/// retain the newest-full legacy test so a prior-cycle snapshot cannot satisfy
/// a later suspend. This predicate matches restore storage-URI selection.
///
/// `restoring` is included for **crash recovery**: if the controller dies
/// after `begin_restore_lifecycle` claimed `suspended → restoring` but before
/// the finalize cleared the marker, the row is left `restoring` with the
/// marker still set. A live controller's in-memory `restore_in_flight` dedups
/// the active task so this only re-drives after a restart (when that set is
/// empty); the evacuation `begin_restore_lifecycle` accepts an already-
/// `restoring` row, so the re-drive resumes the wake + finalize idempotently.
///
/// Intentionally **not** scoped to any node: the cohort restore is decoupled
/// from the source node's drain (the source may already have drained and
/// terminated once the artifact was backed up — the restore reads from S3).
/// Keying on the mutable `target_node_id` would lose the sandbox the moment
/// the restore's pod placement moves it to the cohort node, so the scan keys
/// only on the immutable marker + portability. The marker is set in
/// [`SubstrateSink::evacuate_sandbox`]'s suspend claim and cleared when the
/// restore finalizes (or by a user restore/Start that takes the sandbox over,
/// via the marker-clearing trigger), so a plain user-suspended sandbox (NULL
/// marker) is never selected.
///
/// **Retry backoff + composed ordering (attempt tiers, then recency).** A
/// row whose previous cohort restore failed carries
/// `next_restore_attempt_at` ([`schedule_restore_retry_query`]); when
/// `honor_backoff_windows` is true the gate here keeps it out of the sweep
/// until that instant passes, so a deterministic failer is retried on an
/// exponential schedule instead of on every reconciler tick (and every
/// backup-ready NOTIFY). When the operator disables the backoff
/// (`honor_backoff_windows = false`) the scan ignores the backoff state
/// entirely — the window gate is omitted, **and** the attempt-tier ordering
/// key is dropped in favor of the pure recency ordering. Both halves matter
/// for the escape hatch: stale windows written before the disable must not
/// keep suppressing rows, and frozen pre-disable attempt counts must not
/// keep sorting a row behind every fresh evacuation (under slot pressure
/// that starves it just as effectively as the gate). Disabled mode is
/// therefore exactly the pre-backoff recency behavior, immediately.
///
/// The ordering composes the backoff tiers with the recency ordering:
/// `restore_attempt_count ASC` first, then `lifecycle_changed_at DESC, id
/// DESC` within a tier. Attempt count must be the **primary** key: every
/// failed restore cycle bumps `lifecycle_changed_at` (the `suspended →
/// restoring` claim and the post-failure revert both fire the
/// lifecycle-changed trigger), so recency alone would perpetually classify a
/// serial failer as the hottest row and hand it the scarce slots ahead of
/// never-failed evacuations. Within a tier, most-recently-active first:
/// [`super::Reconciler::spawn_restores`] fills the bounded cluster budget in
/// list order, and for a *suspend* evacuation `lifecycle_changed_at` is the
/// evacuation-suspend (or crash-left `restoring`) transition time — the
/// closest activity-recency proxy while nothing populates
/// `sandboxes.last_used_at`. **That reading does not carry to the stopped
/// arm**, whose claim and teardown both leave the lifecycle alone, so there the
/// column holds the user's original stop and says nothing about the evacuation;
/// the per-arm key below is what makes the ordering mean one thing. The `id DESC` tiebreaker (UUIDv7, so
/// time-ordered) keeps the order deterministic for rows suspended in the
/// same instant. The backoff state
/// is reset by the `sandboxes_clear_evacuation_restore_target` trigger the
/// moment the marker clears (restore success or user takeover).
pub(crate) fn restore_pending_evacuations_query(honor_backoff_windows: bool) -> String {
    let snapshot_exists = latest_full_snapshot_ready("s");
    let disk_backup_ready = current_disk_artifact_backup_ready("s");
    let (backoff_gate, attempt_tier_key) = if honor_backoff_windows {
        (
            "AND (s.next_restore_attempt_at IS NULL OR s.next_restore_attempt_at <= NOW())",
            "s.restore_attempt_count ASC,",
        )
    } else {
        ("", "")
    };
    // Two populations, discriminated by the marker rather than by the
    // lifecycle, because `restoring` is reachable from both and says nothing
    // about which kind of evacuation is mid-flight.
    //
    // The **stopped** arm differs from the suspend arm in three ways, each
    // load-bearing:
    //
    // * Its resting lifecycle is `stopped`, not `suspended` — a stopped
    //   evacuation never enters the suspend pipeline, so there is no
    //   `suspended` state to rest in.
    // * Its durability proof is the pinned *disk* artifact, not the newest
    //   full snapshot. A stopped sandbox has no memory image, so
    //   `latest_full_snapshot_ready` is unsatisfiable for it and would hide
    //   the row from the sweep forever.
    // * `active_node_id IS NULL` gates the resting case. That is the teardown
    //   having completed, and it is what keeps the sweep off a row whose bytes
    //   are already durable but whose node the drain has not released
    //   yet — re-placing that sandbox while it is still resident would give it
    //   two homes. The `restoring` case is deliberately outside the term: the
    //   restore's own pod placement re-populates the column, so applying it
    //   there would strand a crash-recovery re-drive.
    // The recency key is per-arm, because `lifecycle_changed_at` does not mean
    // the same thing on both. For a suspend evacuation it is the
    // evacuation-suspend transition — when the row became this sweep's
    // responsibility. For a stopped one it is the **user's original stop**: the
    // claim deliberately changes no lifecycle, and neither does the teardown,
    // so a sandbox stopped three weeks ago and evacuated a minute ago carries a
    // three-week-old key and sorts behind every fresh suspend evacuation.
    //
    // That inverts the priority exactly where it matters most. Under budget
    // pressure the starved row is the only one in the list with no live copy
    // anywhere: a suspend evacuation still has its snapshot and its owner gets
    // a retryable error, while a torn-down stopped sandbox is on no node and
    // its owner's `start` is a hard 503 until the sweep reaches it.
    //
    // `placement_changed_at` is the stopped arm's equivalent: the teardown's
    // placement clear (`forget_pod_tracking_if_no_active_execution`, in delete
    // mode) runs under `PlacementClock::Bump` (only `Retire` preserves), so it
    // is stamped at exactly the moment the row became
    // unplaced — the same "became the sweep's responsibility" instant the
    // suspend arm gets from its own column. The `CASE` costs an in-memory sort
    // the sweep was already doing over a set bounded by the marker's partial
    // index.
    format!(
        "SELECT s.id, s.evacuation_restore_target
           FROM sandboxes s
          WHERE s.evacuation_restore_target IS NOT NULL
            AND s.lifecycle IN ('suspended', 'restoring', 'stopped')
            {backoff_gate}
            AND (
                (
                    s.evacuation_restore_target <> 'stopped'
                    AND s.lifecycle IN ('suspended', 'restoring')
                    AND {snapshot_exists}
                )
                OR (
                    s.evacuation_restore_target = 'stopped'
                    AND (
                        (s.lifecycle = 'stopped' AND s.active_node_id IS NULL)
                        -- Unreachable today and kept deliberately: nothing can
                        -- move a marked `stopped` row to `restoring`, because
                        -- `begin_restore_lifecycle`'s claim admits only
                        -- `('suspended','restoring')`. The moment that
                        -- claim widens, this is the crash-recovery arm — a
                        -- re-placement whose controller died mid-restore — and
                        -- omitting it would strand exactly those rows on the
                        -- deploy that made them possible.
                        OR s.lifecycle = 'restoring'
                    )
                    AND {disk_backup_ready}
                )
            )
          ORDER BY {attempt_tier_key}
                   CASE
                       WHEN s.evacuation_restore_target = 'stopped'
                       THEN COALESCE(s.placement_changed_at, s.lifecycle_changed_at)
                       ELSE s.lifecycle_changed_at
                   END DESC,
                   s.id DESC"
    )
}

/// Every evacuated `stopped` sandbox that is on no node — cluster-wide, and
/// **deliberately not** the retry-eligible subset
/// [`restore_pending_evacuations_query`] returns.
///
/// This is the population an on-call alert reads, so it has to answer "how many
/// sandboxes are currently nowhere", not "how many will the next sweep try".
/// Those differ by exactly the rows that matter most: a re-placement that keeps
/// failing carries a `next_restore_attempt_at` up to the 30-minute backoff cap,
/// which would drop it out of the work list — and therefore out of the gauge —
/// for the whole window, precisely while it is most stuck. The alert would go
/// quiet as the incident got worse.
///
/// It also omits the durability term the work list applies. A torn-down row
/// whose backup is *not* resolvable (its only storage location orphaned, say)
/// is unplaceable rather than un-ready, and dropping it from the count would
/// silence the one case with no automatic recovery at all.
///
/// `active_node_id IS NULL` is what makes this "nowhere" rather than "mid
/// evacuation": a claimed row still on its node is held by the drain, visible
/// as a drain blocker, and startable again the moment the marker clears.
pub(crate) fn stopped_replacements_pending_query() -> &'static str {
    sql!(
        "SELECT COUNT(*)::BIGINT
           FROM sandboxes s
          WHERE s.evacuation_restore_target = 'stopped'
            AND s.active_node_id IS NULL
            -- `restoring` is unreachable until the restore claim admits a
            -- `stopped` row; included for the same reason as in
            -- `restore_pending_evacuations_query`, so the two populations stay
            -- the same population.
            AND s.lifecycle IN ('stopped', 'restoring')"
    )
}

/// How long the longest-stalled stopped evacuation has been waiting for its
/// bytes, in seconds — `0` when none is waiting, plus the row it measured.
///
/// **The gap this closes.** [`stopped_replacements_pending_query`] counts rows
/// with `active_node_id IS NULL`, i.e. ones already torn down and waiting to be
/// placed. An evacuation stuck *before* the teardown still has its node, so it
/// is invisible there — and it is invisible everywhere else too: the only other
/// signals are a `debug` log and
/// `node_drain.stopped_evacuation{outcome=awaiting_durable_disk}`, which by
/// design looks identical to a healthy multi-gigabyte upload. So a claim that
/// can never satisfy its durability gate parks the node with the alert flat at
/// zero — precisely the failure this feature's safety argument leans on being
/// *visible*, since fail-closed is only a defence if someone finds out.
///
/// **An age rather than a thresholded count**, so the operator owns the window.
/// Any constant chosen here would be a guess about the largest legitimate
/// rootfs upload on the slowest link in the fleet; a monitor can say
/// `> 1h for 10m` and be retuned without a deploy.
///
/// The age is measured from the pinned artifact's **earliest** catalog
/// registration — `snapshots.timestamp`, stamped when a publish first reached
/// the cluster. `MIN`, not `MAX`: `insert_snapshot` conflicts only on `id`, so
/// re-registering the same artifact adds a row, and taking the newest would
/// *shorten* the reported age on exactly the sandboxes that have been retried
/// most.
///
/// Three clocks, tried in order, so the expression can never resolve to NULL —
/// which the caller would publish as a reassuring `0` for the very node nobody
/// is watching. A row with no catalog row has not published, and falls back to
/// the drain's start; a draining node with no start time (a bug to notice in its
/// own right) falls back to when the sandbox became `stopped`. The first clock
/// is registration time, which comes after the claim, so it can under-report;
/// the value also drops when the first catalog row appears and the clock
/// switches from the drain's start to it. None is a
/// claim about the artifact's *contents* — see [`claim_stopped_evacuation`] for
/// why no timestamp here could be.
///
/// **Scoped to claimed evacuations, and only those.** This selects on the
/// marker, which means it sees exactly the population that has work in flight:
/// a claim whose upload is slow or whose durability gate is not opening. It does
/// **not** see the refusal — a sandbox whose *node* reports an artifact this
/// drain did not mint — and that is deliberate rather than a gap. An earlier
/// revision carried a second disjunct for it, `current_local_artifact_id IS NOT
/// NULL`, which reads the **cluster** column; on the `managed_by = 'k8s'` nodes
/// this reconciler drains, nothing ever writes a `disk`/`stop` pin there (see
/// [`SubstrateSink::node_reported_artifact`] for the three reasons), so the
/// disjunct could not fire and only its tests, which set the column by hand,
/// suggested otherwise. The refusal is counted on
/// `node_drain.stopped_evacuation{outcome=unattributable_artifact}` and logged
/// with the node, the sandbox and the node's stopped-since time, which is where
/// a responder should be sent for it.
///
/// The `s.id` tiebreak is not decoration: the age is an integer number of
/// seconds, so two evacuations that started in the same second compare equal,
/// and without it the paced WARN would name a different sandbox from one hour to
/// the next for an unchanged incident. `restore_pending_evacuations_query` orders
/// on `s.id DESC` for the same reason.
///
/// **Forced drains only.** A claim can only be opened on one, and a marker that
/// outlives an `:undrain` is drift the claim refuses as `Deferred` rather than a
/// stall. Without this term the first gentle drain of a node holding a marked
/// row would report an age against a documented one-hour alert.
pub(crate) fn stopped_evacuation_stall_secs_query() -> String {
    let pinned = snapshot_is_current_local_artifact("s", "sn");
    format!(
        "SELECT EXTRACT(EPOCH FROM (NOW() - COALESCE(
                    (SELECT MIN(sn.timestamp)
                       FROM snapshots sn
                      WHERE sn.sandbox_id = s.id
                        AND {pinned}),
                    n.drain_started_at,
                    s.lifecycle_changed_at
                )))::BIGINT AS stall_secs,
                s.id AS sandbox_id,
                n.id AS node_id
           FROM sandboxes s
           JOIN nodes n ON n.id = s.active_node_id
          WHERE s.lifecycle = 'stopped'
            AND {node_force_draining}
            AND s.evacuation_restore_target = 'stopped'
          ORDER BY stall_secs DESC, s.id DESC
          LIMIT 1",
        node_force_draining = node_is_force_draining("n.id"),
    )
}

/// The substrate whose nodes this reconciler drains: its draining and
/// terminating listings bind it, and it is the one substrate that
/// [`has_node_drain_reconciler`](SubstrateKind::has_node_drain_reconciler).
pub(crate) const DRAIN_RECONCILER_SUBSTRATE: SubstrateKind = SubstrateKind::Kubernetes;

/// `$1` = drain phase (`draining`), `$2` = [`DRAIN_RECONCILER_SUBSTRATE`]. The
/// reconciler's per-tick work list: every k8s-managed node in that phase, with
/// the drain mode chosen for it.
///
/// Scoped to `managed_by = 'k8s'` for the same reason as the other two node
/// scans in this file — `managed_by = 'space'` rows must not be fed to a
/// Kubernetes/ASG-specific pipeline.
pub(crate) fn draining_nodes_query() -> &'static str {
    sql!(
        "SELECT id, drain_mode FROM nodes
          WHERE drain_phase = $1 AND managed_by = $2"
    )
}

/// `$1` = drain phase (`terminating`), `$2` = [`DRAIN_RECONCILER_SUBSTRATE`].
/// The terminating-node work list, scoped like [`draining_nodes_query`].
pub(crate) fn terminating_nodes_query() -> &'static str {
    sql!("SELECT id FROM nodes WHERE drain_phase = $1 AND managed_by = $2")
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
pub(crate) fn schedule_restore_retry_query() -> &'static str {
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

/// Re-drive evacuation rows stranded after their local suspended artifact was
/// published but before the controller committed `suspended`.
///
/// New nodes persist `current_local_artifact_id` in the same retirement
/// transaction that clears `active_node_id`. For those rows the published local
/// artifact is sufficient to advance to `suspended`; the pinned-local pass can
/// then enqueue backup for that exact artifact. Nullable rollout-skew rows have
/// no immutable local identity, so they retain the legacy ready-snapshot gate.
///
/// TODO: [legacy] Remove the nullable-row ready-snapshot gate once every
/// suspended/evacuating row is written with `current_local_artifact_id`.
///
/// The `active_node_id IS NULL` guard is required for both branches: it proves
/// source execution retirement completed before a cross-node restore is
/// eligible. Rows stranded before that bookkeeping remains complete stay
/// visible and block drain rather than risking two live executions.
pub(crate) fn redrive_stranded_suspending_query() -> String {
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

/// Per-call ceiling on [`pinned_local_only_sandboxes_query`]'s enumeration.
///
/// Two different numbers bound this, and only the smaller one is a correctness
/// floor. No consumer reads past its own per-tick ceiling — the promote path
/// takes `PROMOTION_MAX_PER_TICK` (8) and the gone-node give-up
/// `GONE_NODE_GIVEUP_MAX_PER_TICK` (64) — so anything from 64 up starves
/// neither, and 64 would do.
///
/// 512 is chosen above that floor for the gauge, not the consumers:
/// `node_drain.pinned_suspended` reads as a converged count, and at 512 it
/// stays exact below the cap; larger backlogs need the caller's floor warning.
/// Raising the cap weakens the planner's low-startup-cost bias.
pub(crate) const PINNED_LOCAL_ONLY_ENUMERATION_CAP: NonZeroUsize = NonZeroUsize::new(512).unwrap();

/// `$1` = node id. Suspended, locally-pinned sandboxes whose current artifact
/// is not yet portable: the work queue the promotion step backs up. Excludes
/// sandboxes whose backup is still in flight from this node
/// (`state='creating'`): repeating those attempts is redundant and racy, and
/// the compatibility reupload may read artifacts already pruned (or with a
/// different codec) — it can wrongly surface `ArtifactMissing` and mark a
/// recoverable sandbox `failed` while the original upload is about to finalize
/// `ready`. Such sandboxes are handled by the blocking count instead: hold the
/// node until the upload finalizes (→ portable, drops out) or fails (→ row
/// flips to `state='failed'`, the sandbox re-enters this list, and retry /
/// give-up then applies legitimately).
///
/// `exclude_legacy_rows` drops rows with a NULL `current_local_artifact_id` — the
/// ones whose backup can only go through the legacy sandbox-id-only reupload.
/// That path uploads from the **live** `/space/cache/<id>/snapshot/` directory
/// rather than an artifact-id-keyed staging copy, and it validates the sandbox is
/// `Suspended` only at enqueue time. It is therefore safe only while nothing can
/// wake the sandbox mid-upload — which used to be guaranteed by the drain gate
/// 409ing every wake on a draining node. A gentle drain deliberately leaves that
/// gate open, so a wake + re-suspend there could overwrite those bytes while the
/// durable worker is still reading them and register a torn snapshot as `ready`.
/// Gentle drains therefore skip these rows: the node keeps counting them as
/// blockers and parks instead of converging, which is the right trade against
/// publishing a snapshot that may not restore. Rows carrying an artifact id are
/// unaffected — their staging directory is keyed by that id, so a re-suspend
/// allocates a new one and cannot disturb an upload in flight.
///
/// ## Why `limit` exists, and why it is a literal
///
/// `limit` caps how many ids one call enumerates, and is interpolated as a SQL
/// **literal** rather than bound as `$2`. Both choices are load-bearing rather
/// than stylistic.
///
/// Note that `limit` is the value the statement emits, not the cap: the sink
/// passes `PINNED_LOCAL_ONLY_ENUMERATION_CAP + 1`, so the rendered statement
/// says `LIMIT 513`, and the extra row is the sentinel that lets the caller
/// tell "exactly the cap" from "more than the cap".
///
/// Read this section after the scalar-subquery fence below, not before it. The
/// fence is what makes the expensive half's plan deterministic; the cap does
/// something narrower and still necessary: it biases the outer driver.
///
/// `target_node_id` is a placement diagnostic that suspend does not clear, so
/// it accumulates every sandbox ever suspended from a node.
///
/// The cap biases the outer driver toward `idx_sandboxes_target_node` rather
/// than a bitmap intersection with all suspended rows in the cluster. It does
/// not bound rows examined: an empty result still probes every candidate in
/// the node's lifetime history. Sequential draining nodes pay that cost on
/// every tick. Watch `node_drain.store_query_duration_ms`; bounding candidates
/// needs a more selective driver or a maintained "still pinned locally" fact.
///
/// ## What the cap means for the callers, given there is no `ORDER BY`
///
/// The page is deliberately unordered. `ORDER BY s.id` would make it stable
/// across calls, but ordering can steer the planner onto a cluster-wide
/// `sandboxes_pkey` scan to avoid a sort. An unordered page keeps the
/// `target_node_id` driver.
///
/// The admin `:force-upload-local-snapshots` twin does need the stable order,
/// because it publishes its deferred tail to an operator. It pays for it with
/// an index rather than with a plan shape: `idx_sandboxes_target_node_ordered`
/// carries `(target_node_id, id)`, so the equality fixes the leading column and
/// the scan emits that node's rows already ordered — no sort, and the scan
/// reads at most that node's rows (`node_store::pinned_local_only_sandboxes_query`).
/// This statement needs no page identity, so it stays unordered. The admin
/// twin also fences its in-flight check, because its node-ordered scan makes a
/// merge anti-join against `snapshots` available; this unordered statement
/// leaves that check bare.
///
/// So "the remainder is picked up next tick" is the wrong mechanism to claim,
/// and an earlier revision of this comment claimed it. What actually converges
/// is that promotion is **absorbing**: a sandbox whose backup lands becomes
/// durable and leaves this result set permanently, so the set shrinks whichever
/// 512 rows a given call returns, and every row is eventually inside a page.
/// No caller depends on page identity — the promote path takes
/// `PROMOTION_MAX_PER_TICK` (8) and the gone-node give-up
/// `GONE_NODE_GIVEUP_MAX_PER_TICK` (64), both far below the cap, and both are
/// idempotent per sandbox.
///
/// The residual is a head that never clears: if the same sandboxes keep failing
/// promotion, the rows behind them wait. That is not introduced here — the
/// consumers already `take(n)` from an unordered list — but the cap is the
/// reason it is worth naming, because above 512 rows the set is no longer even
/// fully visible per tick. It surfaces as a `node_drain.pinned_suspended` that
/// stops falling together with repeated
/// `node_drain.local_snapshot_promotion{outcome:failed}`.
///
/// ## Why the durability check is wrapped in a scalar subquery
///
/// `AND NOT (SELECT {snapshot_exists})` rather than `AND NOT
/// {snapshot_exists}`. The two are identical in truth — `EXISTS` yields a
/// non-NULL boolean and the scalar subquery returns exactly one row — but the
/// bare sublink is *pullable*: Postgres may realise it as a nested loop, a hash
/// anti-join, or a merge anti-join. The latter two can scan the entire snapshot
/// table. Inside a scalar subquery it cannot be pulled up, so each execution
/// remains a per-row `SubPlan` with an `idx_snapshots_sandbox_id` probe.
/// Routine autoanalyze can change cost estimates and reverse a `LIMIT`-only
/// plan choice; the scalar subquery removes that alternative structurally.
///
/// Fenced here at the query site rather than inside
/// [`snapshot_durable_for_source_release`], because most of that fragment's
/// callers read it positively, where a sublink pulls up into a semi-join and
/// should. The in-flight sublink beside it is deliberately left unfenced: it
/// quantifies over the cluster's `state='creating'` full snapshots, a few
/// hundred rows, so every plan over it is cheap.
///
/// The literal matters because the `tuple_fraction` above is computed at plan
/// time from the `LIMIT` expression. A bound `LIMIT $2` is not a constant when
/// a *generic* plan is built, so there is no fraction to apply and the bias is
/// lost — and sqlx prepares every statement, so the plan cache decides which
/// kind of plan the reconciler gets. Keep the limit literal to preserve this
/// bias even under `plan_cache_mode = force_generic_plan`. `limit` is an
/// in-process constant, never caller- or operator-supplied, and `NonZeroUsize`
/// renders as digits only, so interpolating it cannot inject SQL.
pub(crate) fn pinned_local_only_sandboxes_query(
    exclude_legacy_rows: bool,
    limit: NonZeroUsize,
) -> String {
    // Marker-aware (`snapshot_durable_for_source_release`), matching the
    // source-release guards and the restore predicate: an evacuation-marked
    // sandbox needs backup when its current artifact has no ready copy
    // (not when it lacks a *placement-fresh* one). This is what re-drives a
    // marked sandbox whose current evacuation upload **failed**: its newest
    // snapshot is `failed`, so it is not durable -> enumerated here -> retried
    // → new `ready` → restore-pending → restored. Using `fresh_ready` instead
    // would skip such a row if an older placement-fresh `ready` snapshot also
    // existed, wedging it (blocks drain, not restore-pending, not retried).
    // For an unmarked sandbox the predicate is identical to `fresh_ready`.
    let snapshot_exists = snapshot_durable_for_source_release("s");
    // Node-agnostic: skip the backup/give-up path for a sandbox whose
    // upload is in flight from *any* node — including the case where its
    // `target_node_id` drifted to this node while the upload started
    // elsewhere. A compatibility reupload from this node could probe pruned
    // or wrong-codec local artifacts and surface a spurious `ArtifactMissing`.
    let inflight_upload = inflight_suspend_upload_exists("s");
    let legacy_filter = if exclude_legacy_rows {
        "AND s.current_local_artifact_id IS NOT NULL"
    } else {
        ""
    };
    format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.target_node_id = $1
            AND s.lifecycle = 'suspended'
            AND NOT (SELECT {snapshot_exists})
            AND NOT {inflight_upload}
            {legacy_filter}
          LIMIT {limit}"
    )
}

/// `$1` = sandbox id, `$2` = source node id. Atomic mark-failed for a sandbox
/// the reconciler determined is unrecoverable. The `NOT {inflight_upload}`
/// clause closes a race that otherwise loses sandboxes: never flip a sandbox
/// to `failed` while its backup is still in flight from the source node — the
/// upload worker (which holds staged copies independent of the on-disk
/// artifacts) may finalize `ready`
/// milliseconds later.
pub(crate) fn mark_sandbox_lost_query() -> String {
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
pub(crate) fn mark_sandbox_lost_on_gone_node_query() -> String {
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

/// The "this node is still being force-drained" guard, as a composable
/// conjunct. `node_ref` is whatever names the node — a bind placeholder like
/// `$2`, or a joined alias's column like `n.id`.
///
/// **One condition, five call sites, three spellings.** Three of them bind the
/// two values (`claim_evacuation_suspend`, [`claim_stopped_evacuation`],
/// [`record_stopped_evacuation_artifact`]) and so still track
/// [`PHASE_DRAINING`] and [`NodeDrainMode::Forced`] — those are left as they
/// are, because converting a guarded `UPDATE` from a `sql!` literal to a
/// `format!` is three chances to shift a bind index for no behavioural gain.
///
/// **`drain_mode = 'forced'` is matched literally rather than "not gentle".** A
/// NULL or unrecognized mode reads as gentle everywhere else
/// ([`NodeDrainMode::from_column_str`]), so requiring the explicit token keeps
/// every reader consistent and keeps evacuation reachable only through a mode
/// someone actually asked for. That decision used to be argued in two separate
/// docstrings and silently assumed at the other three sites; it belongs here,
/// once.
///
/// The alias is `n_fd` rather than `n` so the fragment composes into a query
/// that already joins `nodes n` — as the stall gauge does — without shadowing
/// it.
///
/// Local rather than in `common/svc/src/sql/`: this is a predicate over a
/// `nodes` row that only this file's callers need to agree on, unlike the
/// residency predicates, which live in the shared crate precisely because
/// several crates must count the same rows. The sibling
/// [`gone_node_identity_guards`] is the pattern being followed.
fn node_is_force_draining(node_ref: &str) -> String {
    format!(
        "EXISTS (
                SELECT 1
                  FROM nodes n_fd
                 WHERE n_fd.id = {node_ref}
                   AND n_fd.drain_phase = '{PHASE_DRAINING}'
                   AND n_fd.drain_mode = '{forced}'
            )",
        forced = NodeDrainMode::Forced.as_str(),
    )
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
fn gone_node_identity_guards(node_param: &str, uid_param: &str) -> String {
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

/// `failure_reason` stamped on `creating` suspend-snapshot rows terminalized
/// by the gone-node recovery (mirrors the stuck-snapshot reaper's stamp).
pub(crate) const GONE_NODE_UPLOAD_FAILURE_REASON: &str = "source node's kube Node is gone and its EC2 instance is terminated; \
     the upload worker died with the instance";

/// `$1` = node id, `$2` = the observed `node_k8s.k8s_node_uid` (nullable),
/// `$3` = the failure reason to stamp. Terminalize every in-flight
/// (`state='creating'`, `type='full'`) suspend-snapshot upload sourced from a
/// node whose kube Node is gone and whose EC2 instance is confirmed
/// terminated: the upload worker ran on that instance, so no finalize can
/// ever arrive. This is exactly the flip the stuck-snapshot reaper would
/// apply after its age window — applied immediately under the EC2 verdict
/// instead, because the drain blocker count's in-flight-upload term counts
/// these rows regardless of the owning sandbox's lifecycle: without this, a
/// bulk-failed resident with a `creating` row keeps the node wedged in
/// `draining` until the reaper runs (≈10h), or forever where the reaper is
/// disabled. The identity guards refuse once a same-name replacement
/// registered, so a fresh node's genuinely-live uploads are never killed.
/// `failure_reason` is stamped only when still NULL, preserving any reason
/// the worker recorded on an earlier best-effort attempt (same contract as
/// the reaper). Returns `(snapshot_id, sandbox_id)` per flipped row so the
/// caller can emit the per-snapshot `suspend_failed` event.
pub(crate) fn fail_inflight_snapshots_on_gone_node_query() -> String {
    let identity_guards = gone_node_identity_guards("$1", "$2");
    format!(
        "UPDATE snapshots
            SET state = 'failed',
                failure_reason = COALESCE(failure_reason, $3)
          WHERE state = 'creating'
            AND type = 'full'
            AND source_node_id = $1
            {identity_guards}
          RETURNING id, sandbox_id"
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
pub(crate) fn adopt_remote_snapshot_on_gone_node_query() -> String {
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

/// `$3` for [`mark_sandbox_failed_on_node_query`]: the lifecycles the suspend
/// pipeline evacuates, as `text[]`.
///
/// **The suspend set, not [`super::EVACUABLE_LIFECYCLES`].** This flip answers
/// a suspend that sbx-mgr refused because the sandbox is already `failed` on
/// the node, so only a row the suspend path could have been evacuating may
/// match. The wider constant includes `stopped`, and a `stopped` row matching
/// here would be flipped to `failed` and have its execution deleted — erasing
/// a rootfs that is the sandbox's only copy — if a user stopped it between the
/// refused suspend and this write.
pub(crate) fn mark_sandbox_failed_on_node_lifecycles() -> Vec<&'static str> {
    SUSPEND_EVACUABLE_LIFECYCLES
        .iter()
        .map(|lc| lc.as_str())
        .collect()
}

/// `$1` = sandbox id, `$2` = source node id, `$3` = the suspend-evacuable
/// lifecycle strings ([`mark_sandbox_failed_on_node_lifecycles`]). Terminal
/// give-up flip for a
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
pub(crate) fn mark_sandbox_failed_on_node_query() -> &'static str {
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
/// [`pinned_local_only_sandboxes_query`] and the drain blocker count. The
/// placement reset is load-bearing: a stuck restore that placed a pod bumped
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
pub(crate) fn adopt_remote_snapshot_for_drain_query() -> String {
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

/// `$1` = node id, `$2` = age seconds (BIGINT), `$3` = the non-evacuable
/// resident transient lifecycle strings ([`NON_EVACUATABLE_RESIDENT_LIFECYCLES`]
/// as `text[]`). The abandon-stuck-transient sweep: flip every sandbox
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
pub(crate) fn abandon_stale_resident_sandboxes_query() -> String {
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
pub(crate) fn fail_resident_sandboxes_on_gone_node_query() -> String {
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

/// Does this suspend error mean the sandbox is already `failed` on the node
/// (the terminal divergence), as opposed to a transient or other-invalid-state
/// rejection that may still self-heal?
///
/// This is a substring match on [`SUSPEND_SANDBOX_FAILED_MARKER`] by necessity,
/// not by preference: sbx-mgr rejects a suspend from `failed` **and** from
/// `stopped` (and other non-running states) with the same gRPC
/// `FailedPrecondition` code, and the node-api → substrate adapter collapses
/// every suspend rejection into `SubstrateError::Failed` (the code is not
/// preserved). So the marker phrase — emitted only for the `failed` state — is
/// the load-bearing distinguisher. The shared constant (referenced by the
/// sbx-mgr producer too, never the literal) is what keeps a reworded producer
/// message from silently reclassifying this as "retry forever"; the
/// `suspend_failed_marker_classifies_failed_state_only` test pins both ends.
fn is_sandbox_failed_on_node_rejection(suspend_error: &str) -> bool {
    suspend_error.contains(SUSPEND_SANDBOX_FAILED_MARKER)
}

/// Does this reupload error mean the sandbox is wedged in `restoring` on the
/// node (the give-up condition for a permanently-refused local-snapshot
/// reupload), as opposed to a transient failure or a refusal from a state that
/// may hold newer user state (`started`/`paused`/`stopped`)?
///
/// A substring match on [`REUPLOAD_SANDBOX_RESTORING_MARKER`] by necessity: the
/// node-api → substrate adapter collapses the sbx-mgr `FailedPrecondition` into
/// `SubstrateError::Conflict` carrying the message verbatim but not the code,
/// so the marker phrase is the distinguisher. The marker is scoped to the
/// `restoring` state on purpose — only there is the snapshot canonical (the VM
/// is loading from it, holds no newer state); a running state's reupload
/// refusal renders a generic message that does NOT contain this marker, so the
/// give-up never abandons potentially-newer state. The shared constant
/// (referenced by the sbx-mgr producer too, never the literal) keeps a reworded
/// producer message from silently reclassifying this. The
/// `reupload_restoring_marker_classifies_restoring_refusals_only` test pins
/// both ends.
fn is_reupload_sandbox_restoring_rejection(reupload_error: &str) -> bool {
    reupload_error.contains(REUPLOAD_SANDBOX_RESTORING_MARKER)
}

/// Evacuation outcome for a single sandbox. Maps directly to
/// `node_drain.failures{phase}` tags on the error path.
///
/// A retryable substrate restore wait never reaches this type: restore lease
/// contention and every `SubstrateError::Timeout` on the cohort-restore path
/// — including the phase-tagged readiness-wait timeouts — are classified as
/// [`RestoreOutcome::Converging`] before the failure mapping, so the
/// readiness phase (`pod_never_ready` / `proxy_unreachable` /
/// `guest_not_ready`) is carried on `node_drain.restore_converging{phase}`
/// rather than here.
#[derive(Debug, Clone, thiserror::Error)]
pub enum EvacuateError {
    #[error("suspend failed: {0}")]
    Suspend(String),
    #[error("restore failed: {0}")]
    Restore(String),
    #[error("lookup failed: {0}")]
    Lookup(String),
    /// The suspend RPC was rejected because the sandbox's node-local state is
    /// already `failed` (the Firecracker guest died, but the cluster SSOT
    /// still reads `started`/`paused`). Classified off
    /// [`SUSPEND_SANDBOX_FAILED_MARKER`] in the suspend error. This is a
    /// **terminal** evacuation outcome, not a retryable one: the divergence
    /// never self-heals, so the reconciler routes the sandbox to the
    /// give-up path ([`NodeDrainSink::mark_sandbox_failed_on_node`]) instead
    /// of retrying suspend every tick forever. The claim has already been
    /// reverted to the pre-evacuation lifecycle when this is returned, so the
    /// row is in a clean `started`/`paused` resting state for the mark-failed
    /// CAS (and self-heals on the next tick if the mark-failed call is lost).
    #[error("sandbox failed on node: {0}")]
    SandboxFailedOnNode(String),
    /// A `stopped` evacuation could not get its disk artifact published to S3:
    /// the source node's endpoint would not resolve, the node-api was
    /// unreachable, sbx-mgr refused the backup, or the pin write failed.
    ///
    /// Always retryable, never a give-up — the artifact is minted on demand
    /// from a rootfs the node still holds, so a refusal says the node is not
    /// ready to publish, not that the data is gone. The node stays held.
    #[error("stopped-disk publish failed: {0}")]
    Publish(String),
    /// A `stopped` evacuation proved its bytes durable but could not release
    /// the node. The sandbox is safe either way — the S3 copy
    /// exists and the local one still does too — so this is a stalled drain,
    /// not a data risk. Retried on the next tick against the same claim.
    #[error("stopped-resident local teardown failed: {0}")]
    LocalTeardown(String),
}

impl EvacuateError {
    pub fn phase(&self) -> &'static str {
        match self {
            EvacuateError::Suspend(_) => "suspend",
            EvacuateError::Restore(_) => "restore",
            EvacuateError::Lookup(_) => "lookup",
            EvacuateError::SandboxFailedOnNode(_) => "sandbox_failed_on_node",
            EvacuateError::Publish(_) => "publish",
            EvacuateError::LocalTeardown(_) => "local_teardown",
        }
    }
}

/// Success outcome of asking the substrate to ensure a node is
/// unschedulable. `KubeNodeGone` is a success (there is nothing left to
/// cordon, so drain progress is not blocked) but is surfaced distinctly so
/// the reconciler can run the KubeNodeGone recovery for `draining` rows —
/// without it, a draining node whose kube Node was deleted retries its
/// futile per-sandbox work (backup enqueues that can never find a node-api
/// pod, suspends that can never reach the node) every tick forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CordonOutcome {
    /// The kube Node exists and is (now) cordoned.
    Cordoned,
    /// The kube Node object is gone (404). Nothing to cordon; the caller
    /// decides whether the row is recoverable (EC2 cross-check) or parked.
    KubeNodeGone,
}

/// Outcome of asking the substrate to ensure a node is unschedulable.
/// Errors are non-fatal at the reconciler level — the loop records
/// `drain_last_error` and retries next tick, except for
/// `StaleIdentity` which is silently deferred to the next tick
/// (the inventory reflector resets `drain_phase` shortly).
#[derive(Debug, thiserror::Error)]
pub enum CordonError {
    #[error("DB lookup for node_k8s row failed: {0}")]
    Db(String),
    #[error("node {0} has no node_k8s row; cannot resolve substrate node name")]
    Unregistered(NodeId),
    #[error("substrate cordon patch failed: {0}")]
    Substrate(String),
    /// The current kube Node's UID differs from the UID we recorded
    /// in `node_k8s`. A same-name replacement Node has appeared and
    /// patching it would leave fresh capacity stuck unschedulable
    /// after the inventory reflector clears `drain_phase`. The
    /// reconciler treats this as "defer; let the inventory reflector
    /// catch up" — no `drain_last_error` written.
    #[error("kube Node UID changed under us; deferring cordon to inventory reconcile")]
    StaleIdentity,
}

/// Production [`NodeDrainSink`] backed by Postgres and the
/// `KubernetesSubstrate`. The substrate already owns the suspend/restore
/// implementations; this sink just plumbs IDs through and runs the SQL
/// the reconciler depends on.
pub struct SubstrateSink {
    pool: PgPool,
    substrate: SubstrateSinkBackend,
    promotion_node_api_clients: tokio::sync::Mutex<HashMap<NodeId, CachedPromotionNodeApiClient>>,
    /// Lazily-established Postgres `LISTEN` connection for the
    /// `EVACUATION_RESTORE_READY_CHANNEL` snapshot-ready signal. Held across
    /// calls so notifications buffer on the connection between `recv`s; reset
    /// to `None` on error to force a reconnect. Only the run loop awaits it
    /// (one consumer), so the `tokio::sync::Mutex` is uncontended.
    restore_listener: tokio::sync::Mutex<Option<sqlx::postgres::PgListener>>,
    /// Evidence required before a row stops counting against its
    /// `active_node_id`. Held on the sink so the resident scan and the
    /// evacuation scan can't answer the question differently.
    residency_evidence: ResidencyEvidence,
    /// Whether the termination-blocking term is enabled. Held here because the
    /// completion-edge write has to honour it too — see [`GuardScope`]. The
    /// reconciler reads the same field from its own config; both come from
    /// `SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE`.
    termination_blocking_gate: bool,
    /// Whether `stopped` residents are evacuated. Already the resolved
    /// conjunction from [`super::ReconcilerConfig::evacuates_stopped_residents`],
    /// not the raw flag — the sink must not re-derive a policy the reconciler
    /// has already decided, or the listing and the gate could disagree about
    /// which rows exist.
    ///
    /// Held on the sink because it scopes two things that have to agree: which
    /// lifecycles [`NodeDrainSink::list_evacuatable_sandboxes_on_node`] offers,
    /// and whether [`NodeDrainSink::evacuate_stopped_sandbox`] will act at all.
    evacuate_stopped_residents: bool,
}

enum SubstrateSinkBackend {
    Kubernetes(Arc<KubernetesSubstrate>),
    #[cfg(test)]
    SuspendOnly(Arc<TestSuspendExecution>),
    /// The two node-api legs of a *stopped* evacuation, faked so the entry
    /// point can be driven end to end against a real Postgres.
    ///
    /// Worth a backend variant rather than a narrower assertion: the teardown
    /// is the one step of this path that cannot be undone, and the only thing
    /// standing in front of it is the authorization read. A test that exercises
    /// the read alone proves the *predicate* is right and says nothing about
    /// whether the destroy consults it — which is exactly the wiring a mutation
    /// that bypasses the match arm breaks, silently, with no other test failing.
    #[cfg(test)]
    StoppedPath(Arc<TestStoppedPath>),
}

/// Records what the faked stopped path was asked to do, so a test can assert on
/// the *absence* of a teardown as precisely as on its presence.
#[cfg(test)]
struct TestStoppedPath {
    publish: Box<dyn Fn(SandboxId) -> Result<EnqueuedArtifactBackup, EvacuateError> + Send + Sync>,
    publish_calls: std::sync::Mutex<u32>,
    /// What the node reports for the sandbox under test. Defaults to "present,
    /// stopped, pinless" — the only shape a fresh evacuation may open on.
    node_artifact: NodeReportedArtifact,
    /// What the node reports on the tick that would tear down. `None` models a
    /// node that agrees with the cluster: present, stopped, VM not alive, and
    /// pinned to exactly the artifact the cluster row records.
    teardown_node_artifact: Option<NodeReportedArtifact>,
    teardowns: std::sync::Mutex<Vec<SandboxId>>,
}

type SuspendExecutionResult = Result<Option<PublishedLocalArtifact>, SubstrateError>;

#[cfg(test)]
type TestSuspendExecutionFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = SuspendExecutionResult> + Send>>;

#[cfg(test)]
type TestSuspendExecution = dyn Fn(SandboxId) -> TestSuspendExecutionFuture + Send + Sync;

impl SubstrateSinkBackend {
    fn kubernetes(&self) -> &KubernetesSubstrate {
        match self {
            Self::Kubernetes(substrate) => substrate,
            #[cfg(test)]
            Self::SuspendOnly(_) => panic!("suspend-only test backend used outside evacuation"),
            #[cfg(test)]
            Self::StoppedPath(_) => {
                panic!("stopped-path test backend used outside a stopped evacuation")
            }
        }
    }

    async fn suspend_execution(&self, sandbox_id: SandboxId) -> SuspendExecutionResult {
        match self {
            Self::Kubernetes(substrate) => substrate.suspend_execution(sandbox_id).await,
            #[cfg(test)]
            Self::SuspendOnly(suspend) => suspend(sandbox_id).await,
            #[cfg(test)]
            Self::StoppedPath(_) => panic!("a stopped evacuation performs no suspend"),
        }
    }
}

#[derive(Clone)]
struct CachedPromotionNodeApiClient {
    endpoint_addr: String,
    client: Arc<tokio::sync::Mutex<NodeApiClient>>,
}

/// What one read of a stopped evacuation's row says about it.
///
/// Defaults to "no claim, nothing durable, nothing pinned" so a vanished row
/// and a drifted one read the same, which is the fail-closed answer for both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct StoppedEvacuationState {
    /// The row is still a `stopped`, marked resident of a node this drain is
    /// force-draining — every conjunct of the claim, re-asked.
    claim_holds: bool,
    /// The pinned disk artifact has a ready, non-orphaned backup that
    /// this evacuation could restore from.
    ///
    /// **Durability only — this says nothing about provenance.**
    /// `current_disk_artifact_backup_ready` answers "the pinned disk artifact is
    /// durably restorable", not "the pinned disk artifact describes the current
    /// rootfs"; its own doc says so in those terms. The provenance half is
    /// established upstream, by [`claim_stopped_evacuation`] refusing to open on
    /// a row the node already has pinned. Conflating the two here would let a
    /// durable artifact authorize giving up bytes it does not describe.
    ///
    /// Computed unconditionally, and only *acted on* when the claim held — so on
    /// the path where no claim ever held, this value is read and ignored.
    bytes_durable: bool,
    /// The artifact the cluster row currently pins, so the caller can skip the
    /// pin write when the node hands back the same identity.
    pinned_artifact: Option<LocalArtifactId>,
    /// A catalog row for the pinned artifact is in `creating` — sbx-mgr has an
    /// upload attempt in flight for exactly these bytes.
    ///
    /// Read so the per-tick publish can be skipped while the answer is already
    /// on its way. Without it, step 2 re-issues `EnqueueArtifactBackup` every
    /// tick, and that is only idempotent for two of the three shapes it can
    /// meet: sbx-mgr short-circuits an already-`ready` backup and joins an
    /// in-flight attempt, but `reusable_artifact_backup_attempt` deliberately
    /// declines to reuse a **failed** job, so a failed upload falls through to
    /// a fresh `BackupAttemptId` — and `BeginArtifactBackup` inserts one
    /// `snapshots` row per attempt id, with `ON CONFLICT` keyed on that id, so
    /// the clause dedupes retries of one attempt rather than attempts of one
    /// artifact. At a five-second tick that is twelve rows a minute for a
    /// sandbox whose upload keeps failing, in a table the retention sweeper
    /// filters to `type = 'full'` and therefore never prunes.
    ///
    /// The node layer's choice is right for a one-shot caller: it looks at the
    /// failed job and declines to reuse it, because a failed attempt should be
    /// retryable. It is the per-tick caller that turns that into a loop, so the
    /// fix belongs here.
    upload_in_flight: bool,
    /// The row carries *this* path's marker.
    ///
    /// Read separately from [`Self::claim_holds`] because it is what separates
    /// the two ways a claim can fail to hold. A pinned row with no marker was
    /// not this drain's to evacuate while it stays stopped — only a successful
    /// start clears a node-local pin — so for a sandbox left stopped the hold
    /// is permanent and worth naming. A pinned row
    /// *with* the marker is an evacuation this drain opened whose world moved
    /// under it — an `:undrain`, a placement change, a lifecycle drift — which
    /// is ordinary drift that resolves itself. Reporting the second as the first
    /// sends a responder to clear a pin on a sandbox that is fine.
    marked: bool,
}

/// The longest-waiting stopped evacuation, with enough identity to act on.
///
/// The gauge built from this is a cluster-wide age with no tags, because that is
/// what an operator can alert on without choosing a node in advance. But it is
/// documented as a series to page on, and an age by itself leaves
/// a responder with nothing to open: both wedge shapes log at `debug`, which
/// production does not emit. So the query hands back the row it measured and the
/// caller logs it, paced.
///
/// There is no "was it claimed" flag, and deliberately isn't one: this query
/// selects on the marker, so every row it can return is a claim. An earlier
/// revision covered a second shape — a refusal, which carries no marker — and
/// needed the flag to tell them apart; narrowing the query to claims left it a
/// SQL constant, and three documents describing a value it could never hold,
/// including the WARN a page routes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoppedEvacuationStall {
    pub age: Duration,
    pub sandbox_id: SandboxId,
    pub node_id: NodeId,
}

/// Whether the drain may release a stopped resident's node, and if
/// not, which of the two reasons applies.
///
/// A single boolean would collapse "still uploading" into "no longer ours",
/// and those call for opposite handling: one holds the node and waits, the
/// other is done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TeardownAuthorization {
    Authorized,
    AwaitingDurableDisk,
    ClaimLost,
}

/// How far one call to [`NodeDrainSink::evacuate_stopped_sandbox`] got.
///
/// A stopped evacuation is a three-step sequence — claim, publish, tear down —
/// whose middle step waits on an S3 upload that outlasts a reconciler tick.
/// Rather than hold a task (and a scarce per-node evacuation slot) across the
/// upload, each tick advances the sequence as far as the persisted state
/// allows and reports where it stopped. Only `Released` is terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoppedEvacuationOutcome {
    /// The disk artifact's backup is enqueued (or was already in flight) and
    /// the catalog does not yet carry a ready row for that exact artifact. The
    /// node is still held; the next tick re-tests.
    AwaitingDurableDisk { local_artifact_id: LocalArtifactId },
    /// The bytes are durable and the node's hold on the sandbox is released:
    /// the retired executions' pods are deleted and the placement is cleared.
    /// The row is `stopped` and unplaced, carrying the marker the cluster-wide
    /// restore sweep selects it by.
    ///
    /// **Nothing on the node is deleted.** The teardown sends sbx-mgr no
    /// request, so the rootfs and sbx-mgr's row stay on the node until the node
    /// itself is terminated — which this release is what allows. So this
    /// outcome is not evidence that the local copy is gone: after an
    /// `:undrain` it is intact on a healthy node, while the row stays unplaced.
    ///
    /// `local_artifact_id` is an `Option` rather than a nil sentinel: `None`
    /// means the pin was absent at the last read, which the authorization rules
    /// out, and a nil UUID would report that impossible state in a form
    /// indistinguishable from a real artifact id.
    ///
    /// Selecting it is not yet the same as placing it — see
    /// [`super::EVACUABLE_LIFECYCLES`] for the substrate refusals that keep the
    /// feature flag off.
    Released {
        local_artifact_id: Option<LocalArtifactId>,
    },
    /// The claim did not hold, so nothing was published and — the part that
    /// matters — nothing was destroyed. A user took the sandbox over, its
    /// placement drifted, or the node stopped being force-drained between the
    /// listing and this call.
    Deferred { reason: String },
    /// The sandbox already carried a disk-artifact pin this drain did not mint,
    /// so nothing can be attributed to the rootfs as it stands and the
    /// evacuation refuses to start. Nothing published, nothing destroyed.
    ///
    /// Distinct from [`Self::Deferred`] because it does not resolve on its own:
    /// only a successful start clears a node-local pin, and a forced drain
    /// refuses starts, so the node parks until the sandbox is deleted, the node
    /// side changes so the guard can accept a current pin, or an operator
    /// intervenes. Reported
    /// separately so that hold is countable rather than hidden among ordinary
    /// drift.
    ///
    /// `local_artifact_id` is `None` when the node reported a pin that could
    /// not be parsed: the refusal is the same, but there is no id to name, and a
    /// synthesized nil UUID would be indistinguishable from a real one in the
    /// outcome and the log.
    UnattributableArtifact {
        local_artifact_id: Option<LocalArtifactId>,
    },
}

/// Outcome of attempting the cohort restore for an evacuated sandbox.
pub enum RestoreOutcome {
    /// The restore ran and the row was finalized to its target lifecycle.
    Restored,
    /// The sandbox is no longer eligible for the cohort restore, so it was
    /// left untouched for a later sweep rather than treated as a failure.
    /// Either the durable S3 snapshot isn't `ready` yet (the upload is still
    /// in flight — the snapshot-register trigger will re-drive once it lands),
    /// or the `evacuation_restore_target` marker was cleared by a concurrent
    /// user takeover between the sweep's list and the eligibility re-check, or
    /// a control-plane statement lost a lock race (see
    /// [`converging_restore_outcome`]).
    Deferred,
    /// The blocking wake wait exhausted the API-side node-RPC budget while
    /// the node was still restoring. Not a failure: the substrate reports
    /// budget exhaustion as a non-revert `Timeout` precisely because the
    /// node-side restore keeps running, the row stays `restoring` with its
    /// evacuation marker set, and the next sweep re-drives the same sandbox
    /// (the evacuation restore claim accepts an already-`restoring` row)
    /// until a re-driven wake finds the pod ready — converge-by-retry is the
    /// designed path for restores slower than one wait budget. `detail`
    /// carries the substrate's timeout message for the caller's log line;
    /// `wait_phase` carries the readiness step the wait stalled in (`None`
    /// when the outer wake budget fired without entering a specific
    /// readiness wait), so `node_drain.restore_converging{phase}` points at
    /// the stuck step the same way `node_drain.failures{phase}` does for
    /// genuine failures.
    Converging {
        detail: String,
        wait_phase: Option<WaitReadyTimeoutPhase>,
    },
}

impl RestoreOutcome {
    /// `phase` tag for `node_drain.restore_converging`: the readiness step a
    /// [`RestoreOutcome::Converging`] wake stalled in, or `"budget"` when the
    /// outer wake budget fired without entering a specific readiness wait.
    pub fn converging_phase_tag(wait_phase: Option<WaitReadyTimeoutPhase>) -> &'static str {
        wait_phase.map_or("budget", WaitReadyTimeoutPhase::as_str)
    }
}

/// Retryable substrate ownership/wait outcomes that the cohort restore should
/// re-drive on the next sweep instead of reporting as a failed evacuation.
fn converging_restore_outcome(error: &SubstrateError) -> Option<RestoreOutcome> {
    match error {
        SubstrateError::RestoreInFlight { message, .. }
        | SubstrateError::RestoreSourcePending { message }
        | SubstrateError::SuspendInFlight { message } => Some(RestoreOutcome::Converging {
            detail: message.clone(),
            wait_phase: None,
        }),
        SubstrateError::Timeout { message, phase } => Some(RestoreOutcome::Converging {
            detail: message.clone(),
            wait_phase: *phase,
        }),
        // `Deferred`, not `Converging`: nothing is progressing on the node, so
        // labelling it a converging restore would point
        // `node_drain.restore_converging{phase}` at a readiness step that was
        // never entered. Deferring re-drives it on the next sweep, which is the
        // whole remedy for a lost lock race, and keeps it out of the evacuation
        // failure series — the same reasoning as the `PromoteError::Db` arm
        // below. Without this arm it takes `_ => None` and becomes a
        // `RestoreFailed` event plus exponential backoff; that is what a
        // contended statement did before it was classified, so this is a
        // correction rather than a regression, and the rate it would have
        // inflated is one the `lock_timeout` rollout watches.
        SubstrateError::LockContention { .. } => Some(RestoreOutcome::Deferred),
        _ => None,
    }
}

impl SubstrateSink {
    pub fn new(
        pool: PgPool,
        substrate: Arc<KubernetesSubstrate>,
        residency_evidence: ResidencyEvidence,
        termination_blocking_gate: bool,
        evacuate_stopped_residents: bool,
    ) -> Self {
        Self {
            pool,
            substrate: SubstrateSinkBackend::Kubernetes(substrate),
            promotion_node_api_clients: tokio::sync::Mutex::new(HashMap::new()),
            restore_listener: tokio::sync::Mutex::new(None),
            residency_evidence,
            termination_blocking_gate,
            evacuate_stopped_residents,
        }
    }

    #[cfg(test)]
    fn with_suspend_execution<F, Fut>(pool: PgPool, suspend: F) -> Self
    where
        F: Fn(SandboxId) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = SuspendExecutionResult> + Send + 'static,
    {
        Self {
            pool,
            substrate: SubstrateSinkBackend::SuspendOnly(Arc::new(move |sandbox_id| {
                Box::pin(suspend(sandbox_id))
            })),
            promotion_node_api_clients: tokio::sync::Mutex::new(HashMap::new()),
            restore_listener: tokio::sync::Mutex::new(None),
            residency_evidence: ResidencyEvidence::NodeCensusAware,
            termination_blocking_gate: true,
            evacuate_stopped_residents: true,
        }
    }

    /// A sink whose publish returns `artifact` and whose teardown only records
    /// that it was asked, so a Postgres-backed test can drive
    /// [`NodeDrainSink::evacuate_stopped_sandbox`] end to end.
    #[cfg(test)]
    fn with_stopped_path(pool: PgPool, artifact: EnqueuedArtifactBackup) -> Self {
        Self {
            pool,
            substrate: SubstrateSinkBackend::StoppedPath(Arc::new(TestStoppedPath {
                publish: Box::new(move |_| Ok(artifact)),
                publish_calls: std::sync::Mutex::new(0),
                node_artifact: NodeReportedArtifact {
                    present: true,
                    pinned_artifact: None,
                    reports_stopped: true,
                    ..NodeReportedArtifact::default()
                },
                teardown_node_artifact: None,
                teardowns: std::sync::Mutex::new(Vec::new()),
            })),
            promotion_node_api_clients: tokio::sync::Mutex::new(HashMap::new()),
            restore_listener: tokio::sync::Mutex::new(None),
            residency_evidence: ResidencyEvidence::NodeCensusAware,
            termination_blocking_gate: true,
            evacuate_stopped_residents: true,
        }
    }

    /// Same, but with the node reporting `node_artifact` — the shape a fresh
    /// evacuation's provenance check actually consults.
    #[cfg(test)]
    fn with_stopped_path_node_state(
        pool: PgPool,
        artifact: EnqueuedArtifactBackup,
        node_artifact: NodeReportedArtifact,
    ) -> Self {
        let mut sink = Self::with_stopped_path(pool, artifact);
        sink.substrate = SubstrateSinkBackend::StoppedPath(Arc::new(TestStoppedPath {
            publish: Box::new(move |_| Ok(artifact)),
            publish_calls: std::sync::Mutex::new(0),
            node_artifact,
            teardown_node_artifact: None,
            teardowns: std::sync::Mutex::new(Vec::new()),
        }));
        sink
    }

    /// Same as [`Self::with_stopped_path`], but with the node reporting
    /// `teardown_node_artifact` on the tick that would tear down.
    #[cfg(test)]
    fn with_stopped_path_teardown_node_state(
        pool: PgPool,
        artifact: EnqueuedArtifactBackup,
        teardown_node_artifact: NodeReportedArtifact,
    ) -> Self {
        let mut sink = Self::with_stopped_path(pool, artifact);
        sink.substrate = SubstrateSinkBackend::StoppedPath(Arc::new(TestStoppedPath {
            publish: Box::new(move |_| Ok(artifact)),
            publish_calls: std::sync::Mutex::new(0),
            node_artifact: NodeReportedArtifact {
                present: true,
                pinned_artifact: None,
                reports_stopped: true,
                ..NodeReportedArtifact::default()
            },
            teardown_node_artifact: Some(teardown_node_artifact),
            teardowns: std::sync::Mutex::new(Vec::new()),
        }));
        sink
    }

    /// How many times the faked publish was called. The observable that separates
    /// "asked again" from "waited", since both report `AwaitingDurableDisk`.
    #[cfg(test)]
    fn publish_calls(&self) -> u32 {
        match &self.substrate {
            SubstrateSinkBackend::StoppedPath(fake) => *fake
                .publish_calls
                .lock()
                .expect("stopped-path publish counter poisoned"),
            _ => panic!("publish_calls is only meaningful on the stopped-path backend"),
        }
    }

    /// The sandboxes the faked teardown was asked to release.
    #[cfg(test)]
    fn recorded_teardowns(&self) -> Vec<SandboxId> {
        match &self.substrate {
            SubstrateSinkBackend::StoppedPath(fake) => fake
                .teardowns
                .lock()
                .expect("stopped-path teardown recorder poisoned")
                .clone(),
            _ => panic!("recorded_teardowns is only meaningful on the stopped-path backend"),
        }
    }

    async fn promotion_node_api_client(
        &self,
        source_node_id: NodeId,
        target: &NodeApiTarget<'_>,
    ) -> Result<CachedPromotionNodeApiClient, SubstrateError> {
        let mut clients = self.promotion_node_api_clients.lock().await;
        if let Some(client) = clients.get(&source_node_id) {
            return Ok(client.clone());
        }

        let (endpoint_addr, client) = connect_retrying_node_api_client_for_target(
            self.substrate.kubernetes(),
            target,
            Duration::from_secs(30),
        )
        .await?;
        let client = CachedPromotionNodeApiClient {
            endpoint_addr,
            client: Arc::new(tokio::sync::Mutex::new(client)),
        };
        clients.insert(source_node_id, client.clone());
        Ok(client)
    }

    async fn evict_promotion_node_api_client(&self, source_node_id: NodeId) {
        self.promotion_node_api_clients
            .lock()
            .await
            .remove(&source_node_id);
    }

    /// Run one RPC against a source node's cached node-api client, and drop the
    /// cache entry when the failure is the kind that poisons it.
    ///
    /// The eviction is a correctness invariant, not tidiness: a cached client
    /// whose transport has died keeps failing every tick, and the drain path
    /// that owns it has no other way to notice. Both callers — the suspended
    /// artifact's promotion and the stopped-disk publish — need it on every
    /// error path, so it lives here rather than being re-derived per call site
    /// and maintained by convention. A third caller gets it for free.
    ///
    /// **Constraint a third caller needs to know.** This returns
    /// client-construction failures and RPC failures through the same
    /// `Result`, so a caller that classifies some errors as *terminal* — as
    /// [`Self::promote_local_snapshot`] does, marking a sandbox lost on sbx-mgr
    /// marker substrings — must be sure construction cannot forge one.
    /// Today it cannot: construction runs
    /// `resolve_node_api_endpoint` (which yields only `Unavailable` or
    /// `Failed`) and then a pure client build, while every marker-bearing
    /// classification keys on `SubstrateError::Conflict`, which only the RPC
    /// mappers produce. A future construction path that could return
    /// `Conflict` would silently widen those terminal branches.
    async fn with_promotion_node_api_client<T, F>(
        &self,
        source_node_id: NodeId,
        target: &NodeApiTarget<'_>,
        call: F,
    ) -> Result<T, SubstrateError>
    where
        F: AsyncFnOnce(&mut NodeApiClient, &str) -> Result<T, SubstrateError>,
    {
        let result = match self.promotion_node_api_client(source_node_id, target).await {
            Ok(cached) => {
                let mut client = cached.client.lock().await;
                call(&mut client, &cached.endpoint_addr).await
            }
            Err(err) => Err(err),
        };
        if let Err(err) = &result
            && should_evict_promotion_client(err)
        {
            self.evict_promotion_node_api_client(source_node_id).await;
        }
        result
    }

    /// Ask the source node to publish the stopped sandbox's disk artifact to
    /// S3, returning the artifact identity it published under.
    ///
    /// `(CurrentInstance, Disk)` against a `Stopped` sandbox is the request the
    /// Kubernetes substrate already makes — but only from
    /// `teardown_execution`'s `mode.sends_sandbox_stop()` branch, i.e. under
    /// `ExecutionTeardown::Retire` and never under `Delete`. That indirection
    /// matters beyond attribution: it is why a sandbox can arrive here already
    /// carrying a pin registered *before* its `stopped` transition — which is
    /// why [`claim_stopped_evacuation`] refuses to evacuate a pinned row at
    /// all, rather than trying to date the artifact. The difference here is
    /// that this caller then *waits* on the result rather than treating the
    /// enqueue as done.
    ///
    /// Every failure is retryable and none is a give-up. That is a deliberate
    /// asymmetry with [`Self::promote_local_snapshot`], which classifies two
    /// sbx-mgr refusals as terminal and marks the sandbox lost: those mean the
    /// suspend artifact's *files are gone*, so no copy exists anywhere. The
    /// stopped case has no equivalent — the artifact is minted on demand from a
    /// rootfs the node still has, so a refusal means the node is in some state
    /// this call cannot use yet, not that the data is unrecoverable. Giving up
    /// would trade a parked node (visible, recoverable) for a destroyed disk.
    async fn enqueue_stopped_disk_backup(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<EnqueuedArtifactBackup, EvacuateError> {
        #[cfg(test)]
        if let SubstrateSinkBackend::StoppedPath(fake) = &self.substrate {
            *fake
                .publish_calls
                .lock()
                .expect("stopped-path publish counter poisoned") += 1;
            return (fake.publish)(sandbox_id);
        }
        let node = node_api_row(&self.pool, source_node_id)
            .await
            .map_err(|err| {
                EvacuateError::Publish(format!("source node endpoint lookup failed: {err}"))
            })?
            .ok_or_else(|| {
                EvacuateError::Publish(format!("node {source_node_id} not found in nodes table"))
            })?;
        let target = node.as_target();
        self.with_promotion_node_api_client(source_node_id, &target, async |client, addr| {
            enqueue_artifact_backup_with_node_api(
                client,
                addr,
                sandbox_id,
                ArtifactKind::Disk,
                BackupSource::CurrentInstance,
            )
            .await
        })
        .await
        .map_err(|err| EvacuateError::Publish(err.to_string()))
    }

    /// What the **node** believes about this sandbox's current artifact.
    ///
    /// The provenance guard has to ask the node, not Postgres. sbx-mgr decides
    /// whether to mint a fresh disk artifact or reuse an existing one by
    /// matching on
    /// `(SandboxState::Stopped, current_local_artifact_id)` from its **own
    /// node-local SQLite row**, loaded by `lookup_sandbox` and matched in
    /// `service/grpc_lifecycle/artifact_backup.rs`.
    /// A pinless row mints `d-stop-{id}`; every other shape takes
    /// `(_, Some(id)) => id`, and short-circuits without uploading anything if
    /// that artifact's backup is already `ready`.
    ///
    /// Postgres' `sandboxes.current_local_artifact_id` is a *different column on
    /// a different machine*, and for a `stopped` sandbox nothing keeps the two
    /// in step. The node publishes its pin — `PublishedSandboxState` carries
    /// `current_local_artifact_id` — but the cluster projection that consumes it
    /// is gated to `status = 'suspended'`, and the retire that *causes* the
    /// stop-time mint discards the enqueue response outright (`Ok(_) => {}` in
    /// the Kubernetes adapter). So the cluster row is NULL for essentially every
    /// stopped sandbox, whatever the node holds.
    ///
    /// A cluster-side `IS NULL` check therefore fails **open**: it admits the
    /// row it was written to refuse. Reading the node's own answer over the same
    /// connection the publish uses is what makes the guard mean what it says.
    ///
    /// **The consequence is that it currently refuses nearly everything.** The
    /// execution retire enqueues a disk backup on every ordinary stop and a
    /// pinless row mints, so a stopped sandbox's node row is almost always
    /// pinned by the time a drain asks. That pin is usually current — a
    /// successful start clears it — but a start that fails after the guest ran
    /// keeps the older one. So this reports `unattributable_artifact` for the
    /// ordinary case and the exceptional one alike, and cannot distinguish
    /// them, because the difference is *when the pin was minted* and no
    /// cluster-visible clock carries that (see [`claim_stopped_evacuation`]).
    ///
    /// The fix that does *not* couple to this check is to expose the pinned
    /// artifact's capture time, so the refusal becomes "the pin predates the
    /// current stop" rather than "there is a pin". See
    /// [`claim_stopped_evacuation`] for why both clocks have to be the node's,
    /// and for the alternative that does couple.
    ///
    /// One RPC per tick that tries to *open* an evacuation, and one more on
    /// the tick that would release the node; a resumed claim in between needs
    /// none (see [`claim_stopped_evacuation`]), and the steady-state tick
    /// returns before reaching this. A row this refuses
    /// writes nothing, so it is listed and asked again on every tick until the
    /// refusal clears: the price of keeping the refusal stateless.
    ///
    /// The window between this read and the publish is benign, which is not
    /// obvious: anything that pins the node's row in it must itself have minted
    /// from a pinless row, so whatever it pinned describes the rootfs as it
    /// stands.
    async fn node_reported_artifact(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<NodeReportedArtifact, EvacuateError> {
        #[cfg(test)]
        if let SubstrateSinkBackend::StoppedPath(fake) = &self.substrate {
            return Ok(fake.node_artifact.clone());
        }
        let node = node_api_row(&self.pool, source_node_id)
            .await
            .map_err(|err| {
                EvacuateError::Lookup(format!("source node endpoint lookup failed: {err}"))
            })?
            .ok_or_else(|| {
                EvacuateError::Lookup(format!("node {source_node_id} not found in nodes table"))
            })?;
        let target = node.as_target();
        let response = self
            .with_promotion_node_api_client(source_node_id, &target, async |client, addr| {
                sandbox_state_with_node_api(client, addr, sandbox_id).await
            })
            .await
            .map_err(|err| EvacuateError::Lookup(err.to_string()))?;
        Ok(response)
    }

    /// May the drain release this sandbox's node right now?
    ///
    /// **One statement, because every conjunct is a precondition for an
    /// irreversible act.** The teardown is the only step of a stopped
    /// evacuation that cannot be undone. It deletes nothing on the node itself,
    /// but it clears the placement that holds the node, and once the node is
    /// released the drain terminates it, taking the rootfs with it — and nothing
    /// on the node objects, since a `stopped` row's erase-safety verdict is
    /// `Safe`. Every other step is either a guarded UPDATE that
    /// simply matches no rows when the world moved, or an idempotent RPC. So
    /// this one re-asserts the whole claim rather than only the half the
    /// caller last wrote:
    ///
    /// - `lifecycle = 'stopped'` and the marker — the evacuation is still
    ///   ours. A user takeover moves the lifecycle and the
    ///   `sandboxes_clear_evacuation_restore_target` trigger drops the marker,
    ///   so either alone would catch it; both are named because the trigger is
    ///   an invariant of another file.
    /// - `active_node_id = $2` — the copy about to be given up is on the node
    ///   we are draining, not one the sandbox has since moved to.
    /// - the node is still `draining` under a `forced` mode — an operator's
    ///   `:undrain` between the claim and here withdraws the authority to
    ///   disrupt this resident at all.
    /// - the bytes are durable, via the shared
    ///   [`current_disk_artifact_backup_ready`] fragment, so the gate that
    ///   authorizes the destruction and the selector that later resolves the
    ///   artifact to a restore URI cannot disagree.
    ///
    /// What it does **not** re-establish is that the artifact describes the
    /// rootfs about to be given up. That is provenance, not state, and it is
    /// settled once at the claim — see [`claim_stopped_evacuation`]. No query
    /// here could recover it: registration time is not capture time.
    ///
    /// Reading them together rather than in sequence is what makes the
    /// authorization a single point in time. It does not close the window
    /// between this statement and the teardown, which is a separate
    /// transaction.
    ///
    /// **What losing that race actually costs**, since the drain-state conjunct
    /// invites the reading that an `:undrain` is honoured and it is only
    /// honoured up to this statement. An `:undrain` committing inside the
    /// window lets the teardown proceed, leaving the row `stopped`, unplaced
    /// and marked on a cluster with no drain in flight. That is not a new
    /// orphan class: it is the *ordinary* post-evacuation state, owned by the
    /// cluster-wide restore sweep and counted by
    /// `node_drain.stopped_replacement_pending`, which carries no drain-phase
    /// term precisely so it keeps seeing rows whose drain has ended. The
    /// sandbox is then unreachable for exactly as long as every other evacuated
    /// stopped sandbox is — until the three substrate refusals lift — and not
    /// because of the undrain. What the operator loses is the undo they thought
    /// they had, which is why it is written down here rather than left to be
    /// inferred from the conjunct.
    ///
    /// What an `:undrain` must not cost is the sandbox's newer bytes, and that
    /// is the one race this statement cannot see: after the undrain, the
    /// owner's `start` gets through and creates an execution, and an unfenced
    /// teardown would delete that pod and its rootfs. The teardown is therefore
    /// fenced on "no active execution" (`delete_execution_for_teardown`), which
    /// re-proves the absence inside the lock execution creation takes, and
    /// reports `Deferred` with nothing deleted when the start won.
    ///
    /// The two halves are reported apart because they mean opposite things to
    /// the caller: an undurable-but-ours row is the normal mid-upload state and
    /// the node stays held for it, while a row that is no longer ours is
    /// finished with — reporting drift as "still uploading" would keep an
    /// evacuation alive in the telemetry after a user had taken the sandbox
    /// back. A vanished row reads as drift, which is the fail-closed direction
    /// for both.
    async fn stopped_teardown_authorization(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<TeardownAuthorization, EvacuateError> {
        Ok(
            match self
                .stopped_evacuation_state(sandbox_id, source_node_id)
                .await?
            {
                StoppedEvacuationState {
                    claim_holds: true,
                    bytes_durable: true,
                    ..
                } => TeardownAuthorization::Authorized,
                StoppedEvacuationState {
                    claim_holds: true, ..
                } => TeardownAuthorization::AwaitingDurableDisk,
                // A vanished row reads as drift, not as a pending upload: there
                // is no claim left to hold and nothing to wait for.
                StoppedEvacuationState {
                    claim_holds: false, ..
                } => TeardownAuthorization::ClaimLost,
            },
        )
    }

    /// The teardown's one irreversible call, behind a seam so a test can
    /// observe whether it happened at all. Production has exactly one arm.
    /// `Ok(false)` when an active execution appeared since the authorization
    /// read, in which case nothing was torn down.
    ///
    /// Fenced on "no active execution" rather than unfenced: a stopped sandbox
    /// has none, and one that has gained one since the read is being started by
    /// its owner — an `:undrain` lets a `start` through — so the copy on the
    /// node is live again and must not be deleted.
    async fn delete_execution_for_teardown(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        #[cfg(test)]
        if let SubstrateSinkBackend::StoppedPath(fake) = &self.substrate {
            // Mirror the real fence against the same database, so a test can
            // reach the refusal by seeding an execution rather than by a knob.
            let active: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active
                 )"
            ))
            .bind(sandbox_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| SubstrateError::Failed {
                message: err.to_string(),
            })?;
            if active {
                return Ok(false);
            }
            fake.teardowns
                .lock()
                .expect("stopped-path teardown recorder poisoned")
                .push(sandbox_id);
            return Ok(true);
        }
        self.substrate
            .kubernetes()
            .delete_execution_with_no_active_execution(sandbox_id)
            .await
    }

    /// What the node reports on the tick that would tear down. In production
    /// this is the same RPC as [`Self::node_reported_artifact`]; it is a
    /// separate method so the test fake can answer the two questions apart.
    async fn node_reported_artifact_for_teardown(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<NodeReportedArtifact, EvacuateError> {
        #[cfg(test)]
        if let SubstrateSinkBackend::StoppedPath(fake) = &self.substrate {
            if let Some(reported) = &fake.teardown_node_artifact {
                return Ok(reported.clone());
            }
            let pinned: Option<LocalArtifactId> = sqlx::query_scalar(sql!(
                "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| EvacuateError::Lookup(err.to_string()))?
            .flatten();
            return Ok(NodeReportedArtifact {
                present: true,
                pinned_artifact: pinned,
                reports_stopped: true,
                ..NodeReportedArtifact::default()
            });
        }
        self.node_reported_artifact(sandbox_id, source_node_id)
            .await
    }

    /// Release the node, after asking it one last time whether the copy it
    /// holds is still the one the durable artifact describes. The caller has
    /// already established that the claim holds and the bytes are durable; it
    /// owns that authorization, and this owns the node re-check, the events
    /// and the retry classification. Reached from two places — the tick that
    /// proves durability, and every later tick, which reads the same proof and
    /// comes straight here.
    ///
    /// The claim's provenance argument covers the row as it was when the
    /// evacuation opened, and a resumed claim does not re-ask the node. That
    /// leaves one gap a cluster-side read cannot close: a Kubernetes start of
    /// a `stopped` sandbox boots its pod while the cluster row still reads
    /// `stopped` (the lifecycle is written when the start succeeds), so a
    /// start that got in before the drain turned forced can boot the guest,
    /// let it write, and fail, and the row comes back unchanged — marker,
    /// placement and all. So this asks the node, over the connection the
    /// publish used, and proceeds only if it reports the sandbox present,
    /// stopped, with no live VM, and pinned to exactly this artifact. A node
    /// that drops the pin on such a rollback is then refused here; until the
    /// node does, the pin it keeps still matches, and only a capture-time
    /// comparison could tell the bytes apart.
    async fn tear_down_stopped_local_copy(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        node_label: &str,
        pinned_artifact: Option<LocalArtifactId>,
    ) -> Result<StoppedEvacuationOutcome, EvacuateError> {
        let node = self
            .node_reported_artifact_for_teardown(sandbox_id, source_node_id)
            .await?;
        let node_agrees = node.present
            && !node.state_degraded
            && node.reports_stopped
            && !node.vm_alive
            && !node.pin_unreadable
            && node.pinned_artifact.is_some()
            && node.pinned_artifact == pinned_artifact;
        if !node_agrees {
            tracing::debug!(
                sandbox_id = %sandbox_id,
                node_id = %node_label,
                node_present = node.present,
                node_reports_stopped = node.reports_stopped,
                node_vm_alive = node.vm_alive,
                node_state_degraded = node.state_degraded,
                node_pin = ?node.pinned_artifact,
                cluster_pin = ?pinned_artifact,
                "node_drain: not releasing a stopped resident whose node no longer reports \
                 the copy the durable artifact describes"
            );
            return Ok(StoppedEvacuationOutcome::Deferred {
                reason: format!(
                    "sandbox {sandbox_id}'s node does not report it stopped and pinned to the \
                     evacuated artifact; its copy was left alone"
                ),
            });
        }
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::CleanupStarted,
            "node_drain_stopped_evacuation",
            SandboxEventDetails::default()
                .with_from_state(SandboxLifecycle::Stopped.as_str())
                .source_node_id(node_label.to_string()),
        );
        let torn_down = match self.delete_execution_for_teardown(sandbox_id).await {
            Ok(torn_down) => torn_down,
            Err(err) => {
                emit_node_drain_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    "node_drain_stopped_evacuation",
                    SandboxEventDetails::default()
                        .source_node_id(node_label.to_string())
                        .error(err.to_string()),
                );
                // Retryable, and safe to retry: the bytes are already durable,
                // the claim is still held, and a partially-applied teardown
                // converges because the delete clears placement even when it
                // finds no pod left to remove.
                return Err(EvacuateError::LocalTeardown(err.to_string()));
            }
        };
        if !torn_down {
            return Ok(StoppedEvacuationOutcome::Deferred {
                reason: format!(
                    "sandbox {sandbox_id} gained an active execution after the teardown was \
                     authorized; its copy on the node is live again and was left alone"
                ),
            });
        }
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::CleanupCompleted,
            "node_drain_stopped_evacuation",
            SandboxEventDetails::default().source_node_id(node_label.to_string()),
        );

        // `None` only if the pin vanished between the authorization read and
        // here, which the authorization itself rules out — it requires a pinned
        // artifact with a ready backup. Warn rather than assert: a log line is
        // the right cost for an impossible state, and it has to be a line of
        // its own, because folding it into the outcome as a nil UUID would
        // report the impossible case in a form indistinguishable from a real
        // artifact id.
        if pinned_artifact.is_none() {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                node_id = %node_label,
                "node_drain: tore down a stopped resident whose artifact pin was absent at \
                 the last read; the teardown authorization requires one, so this is a \
                 concurrent write the guard did not model"
            );
        }
        Ok(StoppedEvacuationOutcome::Released {
            local_artifact_id: pinned_artifact,
        })
    }

    /// One read answering everything the entry point needs to decide which of
    /// this evacuation's writes are needed at all: does the claim still hold,
    /// are the bytes durable, and which artifact is pinned.
    ///
    /// Every step of a stopped evacuation is idempotent, which made it tempting
    /// to just re-run them each tick: re-claim, re-publish, re-pin. Two of
    /// those are `UPDATE`s that write byte-identical values from the second
    /// tick onward, for the length of a multi-gigabyte upload — and on
    /// `sandboxes` that is not free. Postgres writes a new row version
    /// regardless, five `BEFORE UPDATE` triggers run (one of them doing two
    /// whole-row `to_jsonb` conversions and a deep compare), and an update that
    /// misses HOT writes an entry in every index on `sandboxes` whose predicate
    /// the row matches. At 16 residents x 12 ticks/min x 2 statements that is
    /// up to 384 no-op row versions per minute per draining node, on the
    /// busiest table in the control plane, all of it feeding autovacuum.
    ///
    /// Reading first turns the steady-state tick into one indexed read plus the
    /// publish RPC, and the terminal tick into one read plus the teardown.
    async fn stopped_evacuation_state(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<StoppedEvacuationState, EvacuateError> {
        // **No freshness term, deliberately.** Neither the sandbox's
        // `lifecycle_changed_at` nor the node's `drain_started_at` can serve as
        // one, because `snapshots.timestamp` is registration time and a reused
        // pin decouples it from when the snapshot was taken. The property is
        // established upstream instead, by `claim_stopped_evacuation` refusing
        // to open an evacuation on a row that already carries a pin: everything
        // this query can see was minted by the evacuation asking about it.
        //
        // Re-adding one here would not add safety and would subtract some. A
        // `drain_started_at` comparison, for instance, is satisfied by
        // construction for a claimed row — the mint necessarily follows the
        // claim, which follows the drain — but returns NULL, and so blocks the
        // teardown permanently, for a draining node whose start time is unset.
        // A redundant conjunct that can only fail spuriously is a wedge waiting
        // to happen.
        let ready = current_disk_artifact_backup_ready("s");
        let node_force_draining = node_is_force_draining("$2");
        let pinned = snapshot_is_current_local_artifact("s", "sn");
        // `COALESCE(..., FALSE)`, because two of the conjuncts are nullable
        // columns and SQL's `TRUE AND NULL` is NULL, not FALSE. An unmarked row
        // (`evacuation_restore_target IS NULL`) and a torn-down one
        // (`active_node_id IS NULL`) are exactly the shapes this has to report
        // as "no claim" — and without the coalesce they decode as a NULL bool
        // and fail the query instead, turning a clean deferral into an error.
        let query = format!(
            "SELECT COALESCE((
                 s.lifecycle = 'stopped'
                 AND s.evacuation_restore_target = 'stopped'
                 AND s.active_node_id = $2
                 AND {node_force_draining}
             ), FALSE) AS claim_holds,
             {ready} AS bytes_durable,
             s.current_local_artifact_id,
             COALESCE(s.evacuation_restore_target = 'stopped', FALSE) AS marked,
             EXISTS (
                 SELECT 1
                   FROM snapshots sn
                  WHERE sn.sandbox_id = s.id
                    AND {pinned}
                    AND sn.state = 'creating'
             ) AS upload_in_flight
             FROM sandboxes s
             WHERE s.id = $1"
        );
        let row: Option<(bool, bool, Option<LocalArtifactId>, bool, bool)> =
            sqlx::query_as::<_, (bool, bool, Option<LocalArtifactId>, bool, bool)>(
                sqlx::AssertSqlSafe(sql!(query.as_str())),
            )
            .bind(sandbox_id)
            .bind(source_node_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| {
                // `Lookup`, not `Publish`: this is a Postgres read, and `Publish`
                // means the node would not publish — endpoint resolution,
                // node-api reachability, an sbx-mgr refusal. Conflating them sends
                // whoever reads `node_drain.failures{phase=publish}` to the
                // node-api for a database hiccup.
                EvacuateError::Lookup(format!("stopped evacuation state query failed: {err}"))
            })?;
        // A row that has vanished has no claim and no pin — the same shape as
        // a row that drifted, which is the fail-closed reading of both.
        Ok(row.map_or(
            StoppedEvacuationState::default(),
            |(claim_holds, bytes_durable, pinned_artifact, marked, upload_in_flight)| {
                StoppedEvacuationState {
                    claim_holds,
                    marked,
                    upload_in_flight,
                    bytes_durable,
                    pinned_artifact,
                }
            },
        ))
    }

    /// Restore an evacuated sandbox onto a target-cohort node and finalize
    /// it to `target_lifecycle`, clearing the `evacuation_restore_target`
    /// marker. Driven by [`NodeDrainSink::restore_evacuated_sandbox`] (the
    /// cluster-wide restore sweep + the snapshot-register trigger), so the
    /// eligibility gate, the substrate restore call, the CAS-guarded finalize,
    /// and the port-exposure reconcile live in one place.
    ///
    /// Returns [`RestoreOutcome::Deferred`] (not an error) when the sandbox is
    /// no longer eligible — the newest full snapshot isn't `ready` yet, or the
    /// marker was cleared by a concurrent user takeover. The caller leaves it
    /// for the next sweep. Keyed on `latest_full_snapshot_ready` (NOT placement
    /// freshness) so a failed restore's `placement_changed_at` bump can't drop
    /// a still-marked evacuation, while still waiting for the current upload
    /// rather than restoring a stale prior-cycle snapshot.
    async fn restore_evacuated_and_finalize(
        &self,
        sandbox_id: SandboxId,
        target_lifecycle: SandboxLifecycle,
        evacuate_timeout: PositiveDuration,
        placement: Option<EvacuationPlacement>,
    ) -> Result<RestoreOutcome, EvacuateError> {
        let sandbox_id_typed = sandbox_id;

        // Eligibility gate: the cohort restore needs (a) the newest full
        // snapshot to be `ready` and (b) the evacuation marker still set. (a)
        // is the "defer until the async S3 upload finalizes" path — and gating
        // on the *newest* snapshot (not just "any ready") is what keeps a
        // `wake → modify → evacuation-suspend` cycle from restoring a stale
        // prior-cycle snapshot while the current upload is still `creating`.
        // (b) closes the common window of the user-restore race: between the
        // sweep listing this sandbox and this call, a concurrent user restore
        // can clear the marker; if it has, this restore is no longer ours —
        // defer cleanly. The atomic backstop for the residual micro-window is
        // the marker-guarded `begin_restore_lifecycle` inside
        // `restore_evacuated_execution`. Uses `latest_full_snapshot_ready` (no
        // placement-freshness gate) to match `restore_pending_evacuations_query`
        // and stay robust to a failed restore's placement bump.
        //
        // A `stopped` target reads the *disk* artifact's readiness instead, for
        // the reason that query's own comment gives: a stopped evacuation
        // publishes no full snapshot, so the full-snapshot test is
        // unsatisfiable for it and would defer every attempt forever. Selecting
        // the predicate off `target_lifecycle` — the same value the sweep
        // selected the row by — is what keeps the two in step.
        let ready_exists = if target_lifecycle == SandboxLifecycle::Stopped {
            current_disk_artifact_backup_ready("s")
        } else {
            latest_full_snapshot_ready("s")
        };
        let eligible_query = format!(
            "SELECT (s.evacuation_restore_target IS NOT NULL AND {ready_exists}) \
             FROM sandboxes s WHERE s.id = $1"
        );
        let eligible: bool =
            sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(eligible_query.as_str())))
                .bind(sandbox_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| {
                    EvacuateError::Restore(format!("restore eligibility query failed: {err}"))
                })?
                .unwrap_or(false);
        if !eligible {
            return Ok(RestoreOutcome::Deferred);
        }
        let target_lifecycle_str = target_lifecycle.as_str();
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::RestoreRequested,
            "node_drain",
            SandboxEventDetails::default()
                .target_state(target_lifecycle_str)
                .placement(placement.as_ref().map_or_else(
                    || "default".to_string(),
                    |p| format!("{}={}", p.cohort_key, p.cohort_value),
                )),
        );

        // Restore via the evacuation-specific substrate entry point. Two
        // things happen on this path beyond the steady-state HTTP restore:
        //
        // a) The rebuilt pod drops source-node affinity (kube-scheduler
        //    picks any uncordoned node that tolerates the configured cohort
        //    taints) and carries the operator-configured cohort placement.
        // b) `target_state` controls the final lifecycle in one shot:
        //    `Started` runs the post-load resume; `Paused` skips it so the
        //    firecracker VM stays paused from snapshot-load — preserving the
        //    user-visible lifecycle without a separate post-restore pause
        //    RPC.
        let metric_identity = SandboxApiMetricIdentity::native("node_drain.evacuate");
        match self
            .substrate
            .kubernetes()
            .restore_evacuated_execution(
                metric_identity,
                sandbox_id_typed,
                ActivationOptions {
                    blocking: true,
                    ready_timeout: evacuate_timeout.get(),
                },
                placement.as_ref(),
                target_lifecycle,
            )
            .await
        {
            Ok(()) => {}
            // `EvacuationClaimLost` means the marker-atomic
            // `begin_restore_lifecycle(true)` inside `restore_evacuated_execution`
            // missed its CAS because a concurrent user restore cleared the
            // marker in the micro-window after the eligibility check above. That
            // is the same clean-deferral condition the eligibility gate returns
            // `Deferred` for — classify it identically so a drain-vs-user race
            // doesn't fire `node_drain.failures` or log a spurious
            // restore-failed warning.
            Err(SubstrateError::EvacuationClaimLost { .. }) => {
                return Ok(RestoreOutcome::Deferred);
            }
            // Retryable ownership contention and bounded readiness waits are
            // still converging. Re-drive them on the next sweep without a
            // restore-failed event, failure metric, or exponential backoff.
            Err(err) if let Some(outcome) = converging_restore_outcome(&err) => {
                return Ok(outcome);
            }
            // Every other error — including a genuine `Conflict` (e.g. a real
            // wake-RPC failure on the rebuilt pod, which `wake_execution_inner`
            // wraps as `Conflict`) — is a `Restore` failure so it remains
            // visible to drain dashboards/alerts.
            Err(err) => {
                emit_node_drain_event(
                    sandbox_id,
                    SandboxEventType::RestoreFailed,
                    "node_drain",
                    SandboxEventDetails::default()
                        .target_state(target_lifecycle_str)
                        .error(err.to_string()),
                );
                return Err(EvacuateError::Restore(err.to_string()));
            }
        }

        // Post-restore finalize: lifecycle -> target, placement -> on_node,
        // and clear the evacuation marker so the sandbox stops blocking the
        // source node's drain and is no longer re-selected by the
        // restore-pending scan. Guarded on the `'restoring'` state
        // `begin_restore_lifecycle` set inside the restore call.
        //
        // `rows_affected == 0` means the CAS missed — something else moved
        // the row out of `'restoring'` between the restore returning and
        // this UPDATE landing. The pod is up and sbx-mgr is at the target
        // state, but the row we *can* finalize is no longer ours. Treat this
        // as a Restore failure rather than silent success: the reconciler
        // re-evaluates on the next tick against whatever state landed.
        let finalize = sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = $2,
                    placement = 'on_node',
                    evacuation_restore_target = NULL
              WHERE id = $1 AND lifecycle = 'restoring'"
        ))
        .bind(sandbox_id)
        .bind(target_lifecycle.as_str())
        .execute(&self.pool)
        .await
        .map_err(|err| {
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::RestoreFailed,
                "node_drain",
                SandboxEventDetails::default()
                    .with_from_state("restoring")
                    .target_state(target_lifecycle_str)
                    .error(err.to_string()),
            );
            EvacuateError::Restore(format!("post-restore lifecycle update failed: {err}"))
        })?;
        if finalize.rows_affected() == 0 {
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::LifecycleTransitionCasLost,
                "node_drain_cas_lost",
                SandboxEventDetails::default()
                    .with_from_state("restoring")
                    .target_state(target_lifecycle_str),
            );
            return Err(EvacuateError::Restore(format!(
                "post-restore finalize CAS missed for sandbox {sandbox_id} (lifecycle drifted \
                 from 'restoring' before final UPDATE landed); next tick will re-evaluate"
            )));
        }

        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::RestoreCompleted,
            "node_drain",
            SandboxEventDetails::default()
                .with_from_state("restoring")
                .with_to_state(target_lifecycle_str)
                .target_state(target_lifecycle_str),
        );

        // Enqueue port-exposure reconciliation for the new placement.
        // `sandbox_port_exposures.last_applied_node_id` still points at the
        // source node post-evacuation; without this signal exposed ports
        // would stay bound to the old (now-terminated) node. Mirrors the
        // HTTP wake path's `reconcile_open_sandbox_port_exposures_best_effort`.
        // Best-effort: log on failure but don't fail the restore.
        if let Err(err) = enqueue_sandbox_reconcile(
            &self.pool,
            sandbox_id,
            &[ReconcileRef::sandbox_desired_state(sandbox_id_typed)],
        )
        .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "node_drain: failed to enqueue post-evacuation port-exposure reconcile; \
                 exposed ports may stay bound to the source node until next reconcile trigger"
            );
        }

        Ok(RestoreOutcome::Restored)
    }
}

impl NodeDrainSink for SubstrateSink {
    // Both scans are scoped to `managed_by = 'k8s'`. The `nodes` table
    // also holds `managed_by = 'space'` direct/admin rows; without the
    // scope filter, any such row with `drain_phase = 'draining'` /
    // `'terminating'` would be fed to a Kubernetes/ASG-specific pipeline
    // (kube Node lookups fail, last_error gets recorded,
    // set_drain_phase could wrongly advance it). This reconciler is the
    // EKS drain path only.
    async fn list_draining_nodes(&self) -> Result<Vec<DrainingNode>, sqlx::Error> {
        // Read the mode as a raw string, not as `Option<NodeDrainMode>`: a typed
        // decode would fail the whole row on a value this build does not
        // recognize, dropping the node out of the drain work list entirely.
        // `from_column_str` resolves both NULL and unrecognized to gentle.
        let rows: Vec<(NodeId, Option<String>)> = sqlx::query_as(draining_nodes_query())
            .bind(PHASE_DRAINING)
            .bind(DRAIN_RECONCILER_SUBSTRATE)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|(id, mode)| {
                // The one place a bad column value is worth logging: once per node
                // per tick, bounded by the draining-node count, and this is the
                // reader whose answer decides whether the node's sandboxes get
                // evacuated. `from_column_str` is silent because it also runs on
                // the per-request API gate.
                let mode = match NodeDrainMode::from_column_str_checked(mode.as_deref()) {
                    Ok(mode) => mode,
                    Err(err) => {
                        tracing::warn!(
                            node_id = %id,
                            error = %err,
                            "node_drain: nodes.drain_mode holds a value this build does not \
                             recognize; treating the node as a gentle drain (no proactive \
                             evacuation)"
                        );
                        NodeDrainMode::default()
                    }
                };
                DrainingNode { id, mode }
            })
            .collect())
    }

    async fn list_terminating_nodes(&self) -> Result<Vec<NodeId>, sqlx::Error> {
        sqlx::query_scalar::<_, NodeId>(terminating_nodes_query())
            .bind(PHASE_TERMINATING)
            .bind(DRAIN_RECONCILER_SUBSTRATE)
            .fetch_all(&self.pool)
            .await
    }

    async fn count_resident_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<ResidentSandboxCounts, sqlx::Error> {
        let query = resident_sandbox_counts_query(self.residency_evidence);
        let (occupying, total): (i64, i64) =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_id)
                .bind(resident_lifecycle_strs())
                .fetch_one(&self.pool)
                .await?;
        let occupying = occupying.unsigned_abs();
        // `occupying` is a FILTER over exactly the rows `total` counts, so it
        // cannot exceed it unless the two are edited into disagreeing scans.
        // Saturating rather than wrapping keeps that programmer error a
        // reported zero instead of a discount count near u64::MAX.
        let discounted = total.unsigned_abs().saturating_sub(occupying);
        Ok(ResidentSandboxCounts {
            occupying,
            discounted,
        })
    }

    async fn count_termination_blocking_residents_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<TerminationBlockingResidents, sqlx::Error> {
        let query = termination_blocking_residents_query(self.residency_evidence);
        let rows: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .fetch_all(&self.pool)
            .await?;
        let mut by_lifecycle: Vec<(String, u64)> = rows
            .into_iter()
            .map(|(lifecycle, count)| (lifecycle, count.unsigned_abs()))
            .collect();
        // Sorted here, not in SQL, so the WARN text is stable tick to tick
        // without asking Postgres to sort the scan.
        by_lifecycle.sort();
        Ok(TerminationBlockingResidents { by_lifecycle })
    }

    async fn list_evacuatable_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
        let rows = list_evacuatable_sandbox_rows(
            &self.pool,
            node_id,
            self.residency_evidence,
            self.evacuate_stopped_residents,
        )
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (id, lifecycle) in rows {
            if !is_evacuable_lifecycle(lifecycle) {
                tracing::warn!(
                    node_id = %node_id,
                    sandbox_id = %id,
                    lifecycle = %lifecycle,
                    "node_drain: SQL filter returned a non-evacuable lifecycle; skipping"
                );
                continue;
            }
            out.push((id, lifecycle));
        }
        Ok(out)
    }

    /// Suspended sandboxes still pinned to this node whose snapshots
    /// haven't been registered to S3 yet — the local-only work queue
    /// the reconciler's promotion step has to drain. Sandboxes with a
    /// fresh `state='ready'` snapshot are portable and drop out;
    /// sandboxes whose durable upload is still in flight from this node
    /// are also excluded (promoting them is racy — see
    /// [`pinned_local_only_sandboxes_query`]). `suspending`/`restoring`
    /// lifecycles do **not** appear here either — they aren't promotable
    /// (no snapshot exists yet) but they DO block drain completion,
    /// counted separately via
    /// [`Self::count_drain_blocking_sandboxes_on_node`].
    async fn list_pinned_local_only_sandboxes_on_node(
        &self,
        node_id: NodeId,
        exclude_legacy_rows: bool,
    ) -> Result<Vec<SandboxId>, sqlx::Error> {
        // `cap + 1`, so the caller can tell "exactly the cap" from "more than
        // the cap" — at `LIMIT cap` a full page is ambiguous between the two and
        // the truncation warning fires on a node whose gauge is in fact exact.
        // Same sentinel, for the same reason, as the admin
        // `:force-upload-local-snapshots` enumeration.
        let query = pinned_local_only_sandboxes_query(
            exclude_legacy_rows,
            PINNED_LOCAL_ONLY_ENUMERATION_CAP.saturating_add(1),
        );
        sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .fetch_all(&self.pool)
            .await
    }

    /// Count sandboxes that still block `draining → drained`.
    /// `suspending`/`restoring` lifecycles always block (the local-disk
    /// artifacts aren't promotable yet); `suspended` blocks only when
    /// no fresh `state='ready'` S3 snapshot exists; and any sandbox
    /// whose durable upload is still in flight from this node blocks
    /// regardless of placement (see [`drain_blocking_sandboxes_query`]).
    /// Routes the portability check through the shared
    /// `fresh_ready_snapshot_exists` predicate so this count agrees
    /// with `drained_node_terminator::count_local_snapshots` and the
    /// admin `:terminate` endpoint's
    /// `sandbox_occupancy_for_node_in_tx` guard — all three observe
    /// the same "is this snapshot portable?" answer.
    async fn count_drain_blocking_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<i64, sqlx::Error> {
        let query = drain_blocking_sandboxes_query();
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .fetch_one(&self.pool)
            .await
    }

    /// Back up one local-only suspended sandbox from its source node.
    /// Use the exact current artifact when known; otherwise let the manager
    /// validate and adopt the suspended instance through the same backup RPC.
    /// Once a matching ready backup is registered, the sandbox
    /// drops out of [`Self::list_pinned_local_only_sandboxes_on_node`] on
    /// the next tick.
    async fn promote_local_snapshot(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<(), PromoteError> {
        let node = node_api_row(&self.pool, source_node_id)
            .await
            .map_err(|err| PromoteError::Db(err.to_string()))?
            .ok_or_else(|| {
                PromoteError::EndpointUnresolved(format!(
                    "node {source_node_id} not found in nodes table"
                ))
            })?;
        let target = node.as_target();
        let local_artifact_id: Option<LocalArtifactId> = sandbox_store::get(&self.pool, sandbox_id)
            .await
            .map_err(|err| PromoteError::Db(err.to_string()))?
            .and_then(|record| record.current_local_artifact_id);
        let promotion = self
            .with_promotion_node_api_client(source_node_id, &target, async |client, addr| {
                let source = local_artifact_id.map_or(BackupSource::CurrentInstance, |id| {
                    BackupSource::LocalArtifact(id)
                });
                enqueue_artifact_backup_with_node_api(
                    client,
                    addr,
                    sandbox_id,
                    ArtifactKind::Full,
                    source,
                )
                .await
                .map(|_| ())
            })
            .await;
        match promotion {
            Ok(()) => Ok(()),
            // The on-node sbx-mgr surfaces two terminal failure modes
            // via `FailedPrecondition`, whose messages embed the shared
            // marker substrings. The substrate adapter wraps
            // `FailedPrecondition` into `SubstrateError::Conflict` with
            // the message verbatim. Both producer and consumer
            // reference the shared constants in
            // `pplx_space_common_core::node_drain_signals`, so a
            // reworded producer message cannot silently flip this
            // classifier from "terminal, mark sandbox failed" to "retry
            // forever".
            //
            // - `REUPLOAD_ARTIFACT_MISSING_MARKER`: on-disk snapshot
            //   files have been pruned. Reupload can never succeed.
            // - `REUPLOAD_SANDBOX_UNKNOWN_MARKER`: sbx-mgr has no
            //   record of the sandbox in its local sqlite. The sandbox
            //   is not on this node, so reupload can never succeed
            //   from here.
            Err(SubstrateError::Conflict { message })
                if message.contains(REUPLOAD_ARTIFACT_MISSING_MARKER)
                    || message.contains(REUPLOAD_SANDBOX_UNKNOWN_MARKER) =>
            {
                Err(PromoteError::ArtifactMissing(message))
            }
            // Current-instance backup for an unpinned suspended row refuses
            // when the sandbox is wedged in `restoring` on the
            // node while the DB reads `suspended`. The local artifacts aren't
            // gone (distinct from `REUPLOAD_ARTIFACT_MISSING_MARKER`), but the
            // reupload can never proceed from a stuck restore, so retrying it
            // every tick wedges the node's drain. Route to the give-up path,
            // which (after a settle window, gated on the newest ready remote
            // snapshot) adopts that snapshot so the node can finish
            // draining. Scoped to `restoring` only — a running state's refusal
            // carries no marker, since abandoning live state for an older
            // snapshot would lose work. Checked after the terminal artifact-
            // missing/unknown markers — a single message can't carry both — so
            // ordering is incidental.
            Err(SubstrateError::Conflict { message })
                if is_reupload_sandbox_restoring_rejection(&message) =>
            {
                Err(PromoteError::SandboxRestoringOnNode(message))
            }
            // A lost lock race is this controller's own database, not the
            // node's, so it takes `Db` rather than the `NodeApi` catch-all
            // below. Both are retried on the next drain tick, so the
            // disposition is unchanged — only the message the operator reads
            // when a promotion keeps failing.
            Err(SubstrateError::LockContention { message }) => Err(PromoteError::Db(message)),
            Err(
                SubstrateError::Unavailable { message }
                | SubstrateError::NotFound { message }
                | SubstrateError::Conflict { message }
                | SubstrateError::FailedPrecondition { message }
                | SubstrateError::Timeout { message, .. }
                | SubstrateError::RestoreContextUnavailable { message }
                | SubstrateError::RestoreSourcePending { message }
                | SubstrateError::RestoreInFlight { message, .. }
                | SubstrateError::SuspendInFlight { message }
                | SubstrateError::RetainedFilesystemOnlyCopy { message }
                | SubstrateError::ActivationInFlight { message, .. }
                | SubstrateError::AdmissionRejected { message, .. }
                | SubstrateError::Cancelled { message }
                | SubstrateError::Unsupported { message }
                | SubstrateError::NodeTombstoned { message }
                | SubstrateError::EvacuationClaimLost { message }
                | SubstrateError::Failed { message },
            ) => Err(PromoteError::NodeApi(message)),
        }
    }

    /// Abandon-stuck-transient sweep: tear down every sandbox wedged in a
    /// non-evacuable resident transient lifecycle (see
    /// [`NON_EVACUATABLE_RESIDENT_LIFECYCLES`]) on `node_id` for longer than
    /// `age`, returning the number of rows whose lifecycle this call flipped
    /// to `'failed'`. The age signal is `sandboxes.lifecycle_changed_at`
    /// (maintained by the `sandboxes_lifecycle_changed_at` trigger), so
    /// `NOW() - lifecycle_changed_at` is exactly the time-in-current-
    /// -lifecycle regardless of which transient state the row is wedged in.
    /// (For a `creating` row this equals `NOW() - created_at`, since the
    /// trigger only fires on a lifecycle *change* and the column defaults to
    /// `NOW()` at insert — so the original creating semantics are preserved.)
    ///
    /// Predicates gate the sweep and are re-checked atomically on the row
    /// UPDATE so a sandbox that drifted out from under the reconciler is
    /// left alone:
    ///
    /// - `active_node_id = $node_id OR target_node_id = $node_id` — sandbox
    ///   is still attached to this node, either as its resident
    ///   (`active_node_id`) or as the pinned restore target (`target_node_id`,
    ///   e.g. a user restore that began before the drain and had its
    ///   `active_node_id` cleared by `finish_suspended_execution`). Both pins
    ///   are matched so the sweep covers exactly the rows the drain-complete
    ///   gate counts as blockers.
    /// - `lifecycle = ANY($lifecycles)` — still in a non-evacuable transient
    ///   state (not already progressed to `started`/`failed`/`suspended`).
    /// - `(evacuation_restore_target IS NULL OR lifecycle = 'suspending')` —
    ///   exempts the drain's **own** evacuation *restore* (a marked
    ///   `restoring` row, re-driven by the cluster-wide restore sweep) but
    ///   **not** a marked `suspending` row: nothing else recovers a
    ///   crashed/hung evacuation suspend (the restore-pending scan covers only
    ///   `suspended`/`restoring`), and a `suspending` row always blocks the
    ///   drain, so leaving it exempt would wedge the drain forever. The marker
    ///   is set in [`SubstrateSink::evacuate_sandbox`]'s suspend claim and
    ///   preserved across the suspend → restore chain by the
    ///   `sandboxes_clear_evacuation_restore_target` trigger.
    /// - `EXISTS (SELECT 1 FROM nodes WHERE id = $node_id AND
    ///   drain_phase = 'draining')` — the node is still draining at the
    ///   moment of the UPDATE. Without this, an admin `:undrain` landing
    ///   between `list_draining_nodes()` returning the node id and this
    ///   UPDATE landing would let the sweep flip legitimate in-flight rows
    ///   on a node that is no longer being drained.
    ///
    /// **Events + substrate cleanup.** Flipping lifecycle alone isn't enough.
    /// First, each abandoned row is a logical sandbox state transition, so it
    /// emits a `space.sandbox` `LifecycleTransitionTimedOut` event (carrying
    /// the real pre-failure lifecycle as `from_state`) plus
    /// `CleanupStarted`/`CleanupCompleted`/`CleanupFailed` around the teardown
    /// — mirroring the cluster-wide transient sweeper so a sandbox's timeline
    /// is reconstructable regardless of which sweep fired. Second, `'failed'`
    /// is terminal for the orphaned-sandbox reconciler, so a backing pod left
    /// behind (a wedged transient typically has a Pending/Running pod the
    /// transition never finished tearing down/promoting) would leak
    /// indefinitely whenever the node lingers in `drained` (drain-keep
    /// annotation, terminator disabled, manual hold). The cleanup happens
    /// **after** the atomic claim: the data-modifying CTE flips lifecycle to
    /// `'failed'` under the guard and returns `(id, pre_failure_lifecycle)`.
    /// Iterating substrate cleanup on the returned set is then safe — those
    /// rows can no longer race a concurrent completion CAS (the per-lifecycle
    /// completion UPDATEs also guard on the pre-completion lifecycle, so once
    /// we've moved a row to `'failed'` the completing path fails its CAS and
    /// rolls back its own substrate state). Substrate errors are logged and
    /// swallowed: the cleanup is best-effort and the drain-unblock has higher
    /// priority than the pod-leak guard. The kube cascade on node termination
    /// will sweep any pods we didn't manage to delete here.
    async fn abandon_stale_resident_sandboxes(
        &self,
        node_id: NodeId,
        age: PositiveDuration,
    ) -> Result<u64, sqlx::Error> {
        // No `is_zero` guard anywhere on this path: `PositiveDuration`
        // carries the non-zero invariant at the type level (its smart
        // constructor rejects `Duration::ZERO`), and the disabled case
        // is filtered out at the call site via `Option<PositiveDuration>`.
        // The SQL boundary just unwraps to seconds.
        let age_secs_i64 = i64::try_from(age.get().as_secs()).unwrap_or(i64::MAX);
        // Single source of truth for the swept set; sqlx wants `&[&str]`
        // for the `text[]` parameter.
        let lifecycle_strs: Vec<&'static str> = NON_EVACUATABLE_RESIDENT_LIFECYCLES
            .iter()
            .map(|lc| lc.as_str())
            .collect();

        // Atomic claim: flip lifecycle to `failed` and return
        // `(id, pre_failure_lifecycle)` for the rows we won the CAS for. The
        // data-modifying CTE makes the SELECT + UPDATE a single statement, so a
        // concurrent transition out of the transient lifecycle either lands
        // before us (we observe the row in its new lifecycle and skip) or after
        // us (its CAS fails because we just flipped to `'failed'`, and its own
        // CAS-loss rollback path tears down substrate state). A read-then-write
        // shape would open a window where we could delete the pod of a sandbox
        // that subsequently completed its transition — leaving a legitimate row
        // with no backing pod.
        let query = abandon_stale_resident_sandboxes_query();
        let claimed: Vec<(SandboxId, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_id)
                .bind(age_secs_i64)
                .bind(&lifecycle_strs)
                .fetch_all(&self.pool)
                .await?;

        if claimed.is_empty() {
            return Ok(0);
        }

        // Emit the required `space.sandbox` state event for each abandoned row
        // (mirrors the cluster-wide transient sweeper), then best-effort
        // substrate teardown. The row is already `'failed'` in postgres, so a
        // cleanup failure here doesn't open a new race — it just risks a leaked
        // kube pod (the kube cascade on node termination eventually sweeps it),
        // surfaced via a `CleanupFailed` event + warn log.
        let node_label = node_id.to_string();
        for (sandbox_id, prev_lifecycle) in &claimed {
            emit_node_drain_event(
                *sandbox_id,
                SandboxEventType::LifecycleTransitionTimedOut,
                "drain_abandon_stale",
                SandboxEventDetails::default()
                    .with_from_state(prev_lifecycle)
                    .with_to_state("failed")
                    .source_node_id(node_label.clone()),
            );
            emit_node_drain_sandbox_failed_event(
                *sandbox_id,
                "drain_abandon_stale",
                SandboxEventDetails::default()
                    .with_from_state(prev_lifecycle)
                    .source_node_id(node_label.clone()),
            );
            emit_node_drain_event(
                *sandbox_id,
                SandboxEventType::CleanupStarted,
                "drain_abandon_stale",
                SandboxEventDetails::default()
                    .with_from_state(prev_lifecycle)
                    .source_node_id(node_label.clone()),
            );
            let sandbox_id_typed = *sandbox_id;
            match self
                .substrate
                .kubernetes()
                .delete_execution(sandbox_id_typed)
                .await
            {
                Ok(()) => {
                    emit_node_drain_event(
                        *sandbox_id,
                        SandboxEventType::CleanupCompleted,
                        "drain_abandon_stale",
                        SandboxEventDetails::default().source_node_id(node_label.clone()),
                    );
                }
                Err(err) => {
                    emit_node_drain_event(
                        *sandbox_id,
                        SandboxEventType::CleanupFailed,
                        "drain_abandon_stale",
                        SandboxEventDetails::default()
                            .source_node_id(node_label.clone())
                            .error(err.to_string())
                            .logged_by_caller(),
                    );
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "node_drain: best-effort substrate delete during abandon-stale \
                         failed after lifecycle flip; backing pod may persist until the \
                         node is terminated"
                    );
                }
            }
        }

        Ok(claimed.len() as u64)
    }

    /// KubeNodeGone bulk give-up: flip every resident sandbox on a draining
    /// node whose kube Node is gone and whose EC2 instance is confirmed
    /// terminated to `lifecycle = 'failed'`, under the drain-phase + kube-uid
    /// guards (see [`fail_resident_sandboxes_on_gone_node_query`]), and emit
    /// the required `space.sandbox` event per flipped row. Unlike the
    /// abandon-stale and failed-on-node give-ups there is no best-effort
    /// substrate teardown: the kube Node object is gone, so its pods are
    /// reaped by Kubernetes' pod garbage collector — issuing per-sandbox
    /// delete RPCs here would only burn API budget on 404s.
    async fn fail_resident_sandboxes_on_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        // Single source of truth for the resident set, rendered to its wire
        // forms at this boundary because the parameter is a `text[]`.
        let lifecycle_strs = resident_lifecycle_strs();
        let query = fail_resident_sandboxes_on_gone_node_query();
        let claimed: Vec<(SandboxId, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_id)
                .bind(&lifecycle_strs)
                .bind(expected_uid)
                .fetch_all(&self.pool)
                .await?;

        let node_label = node_id.to_string();
        for (sandbox_id, prev_lifecycle) in &claimed {
            emit_node_drain_event(
                *sandbox_id,
                SandboxEventType::LifecycleTransitionFailedOnNode,
                "drain_kube_node_gone",
                SandboxEventDetails::default()
                    .with_from_state(prev_lifecycle)
                    .with_to_state("failed")
                    .source_node_id(node_label.clone()),
            );
            emit_node_drain_sandbox_failed_event(
                *sandbox_id,
                "drain_kube_node_gone",
                SandboxEventDetails::default()
                    .with_from_state(prev_lifecycle)
                    .source_node_id(node_label.clone()),
            );
        }

        Ok(claimed.len() as u64)
    }

    /// Terminalize in-flight suspend-snapshot uploads sourced from a gone
    /// node (kube Node gone + EC2 instance confirmed terminated) and emit a
    /// per-snapshot `suspend_failed` event — the same event the
    /// stuck-snapshot reaper emits for its age-based flip of the same rows.
    async fn fail_inflight_snapshots_on_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        let query = fail_inflight_snapshots_on_gone_node_query();
        let flipped: Vec<(SnapshotId, SandboxId)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_id)
                .bind(expected_uid)
                .bind(GONE_NODE_UPLOAD_FAILURE_REASON)
                .fetch_all(&self.pool)
                .await?;

        let node_label = node_id.to_string();
        for (snapshot_id, sandbox_id) in &flipped {
            emit_node_drain_event(
                *sandbox_id,
                SandboxEventType::SuspendFailed,
                "drain_kube_node_gone_upload_lost",
                SandboxEventDetails::default()
                    .with_from_state("creating")
                    .target_state("failed")
                    .snapshot_id(snapshot_id.to_string())
                    .source_node_id(node_label.clone()),
            );
        }

        Ok(flipped.len() as u64)
    }

    /// Mark a sandbox as `lifecycle = 'failed'` after the reconciler
    /// determined its current local artifact is gone and no ready
    /// S3 snapshot exists. Source-node and lifecycle guards prevent
    /// stomping a row that has drifted since the promote attempt; the
    /// `NOT EXISTS` clause re-checks portability atomically with the
    /// UPDATE.
    ///
    /// The portability predicate matches the blocker-count predicate
    /// ([`Self::list_pinned_local_only_sandboxes_on_node`] /
    /// [`Self::count_drain_blocking_sandboxes_on_node`]) — all three
    /// route through the shared `fresh_ready_snapshot_exists` SQL
    /// fragment. The predicates must agree atomically, otherwise a
    /// sandbox with a stale-only S3 snapshot and artifacts gone would
    /// count as a blocker (no fresh snapshot) but be rejected from
    /// mark-failed (some ready snapshot exists) — drain would wedge
    /// forever. The UPDATE also refuses to fire while a durable upload
    /// is still in flight from the source node (see
    /// [`mark_sandbox_lost_query`]): the row is recoverable until that
    /// upload finalizes or fails.
    async fn mark_sandbox_lost(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<u64, sqlx::Error> {
        // Intentionally does NOT bump `placement_changed_at`: that
        // column tracks placement changes, not lifecycle changes, and
        // bumping it here would couple the two and could mask the
        // stale-fresh freshness check against pre-existing snapshots
        // if any future read paths still consult `failed` rows. The
        // lifecycle flip alone is the contract.
        let query = mark_sandbox_lost_query();
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(source_node_id)
            .execute(&self.pool)
            .await?;
        let flipped = result.rows_affected();
        if flipped > 0 {
            // The WHERE guard pins the pre-flip lifecycle to `suspended`, so
            // no data-modifying CTE is needed to recover the from_state.
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::SandboxLost,
                "drain_sandbox_lost",
                SandboxEventDetails::default()
                    .with_from_state("suspended")
                    .with_to_state("failed")
                    .source_node_id(source_node_id.to_string()),
            );
            emit_node_drain_sandbox_failed_event(
                sandbox_id,
                "drain_sandbox_lost",
                SandboxEventDetails::default()
                    .with_from_state("suspended")
                    .source_node_id(source_node_id.to_string()),
            );
        }
        Ok(flipped)
    }

    /// KubeNodeGone variant of [`Self::mark_sandbox_lost`]: terminal flip for
    /// a pinned local-only suspended sandbox on a node whose kube Node is gone
    /// and whose EC2 instance is confirmed terminated. Refuses while **any**
    /// ready full snapshot exists — stricter than `mark_sandbox_lost`'s
    /// freshness-anchored guard, because this path chains after the adopt CAS
    /// and must not race it into failing an adoptable row (see
    /// [`mark_sandbox_lost_on_gone_node_query`]) — and refuses once a
    /// same-name kube Node replacement bumped the observed uid or reset the
    /// drain phase (see [`gone_node_identity_guards`]).
    async fn mark_sandbox_lost_on_gone_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        let query = mark_sandbox_lost_on_gone_node_query();
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(source_node_id)
            .bind(expected_uid)
            .execute(&self.pool)
            .await?;
        let flipped = result.rows_affected();
        if flipped > 0 {
            // The WHERE guard pins the pre-flip lifecycle to `suspended`.
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::SandboxLost,
                "drain_kube_node_gone_lost",
                SandboxEventDetails::default()
                    .with_from_state("suspended")
                    .with_to_state("failed")
                    .source_node_id(source_node_id.to_string()),
            );
            emit_node_drain_sandbox_failed_event(
                sandbox_id,
                "drain_kube_node_gone_lost",
                SandboxEventDetails::default()
                    .with_from_state("suspended")
                    .source_node_id(source_node_id.to_string()),
            );
        }
        Ok(flipped)
    }

    /// KubeNodeGone variant of
    /// [`Self::make_sandbox_durable_from_remote_snapshot`]: adopts the remote
    /// snapshot for a pinned local-only suspended sandbox on a node whose
    /// kube Node is gone and whose EC2 instance is confirmed terminated.
    /// Adds the node-identity guards and the current-suspend-cycle anchor on
    /// top of the shared adopt CAS (see
    /// [`adopt_remote_snapshot_on_gone_node_query`]); emits the same
    /// `LocalSnapshotPromotionAbandoned` event with a gone-node reason.
    async fn make_sandbox_durable_on_gone_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        settle: PositiveDuration,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        let settle_secs = i64::try_from(settle.get().as_secs()).unwrap_or(i64::MAX);
        let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(
            adopt_remote_snapshot_on_gone_node_query(),
        )))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(expected_uid)
        .bind(settle_secs)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if flipped == 0 {
            return Ok(0);
        }

        // Not a lifecycle transition — the row stays `suspended`; mirrors
        // `make_sandbox_durable_from_remote_snapshot`'s event with a reason
        // that names the gone-node trigger.
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::LocalSnapshotPromotionAbandoned,
            "drain_kube_node_gone_adopt_remote_snapshot",
            SandboxEventDetails::default().source_node_id(source_node_id.to_string()),
        );

        Ok(flipped)
    }

    /// Terminal give-up for a sandbox the evacuation could not suspend because
    /// it is already `failed` on the node while the DB still reads it
    /// `started`/`paused`. Flips the row to `'failed'` (clearing any evacuation
    /// marker) under the `active_node_id` + evacuable-lifecycle CAS, emits the
    /// required `space.sandbox` state event with the real pre-flip lifecycle,
    /// then best-effort tears down the backing pod so the dead Firecracker
    /// process is reclaimed rather than leaking until the node terminates.
    ///
    /// Returns the number of rows flipped: `1` on success, `0` if the sandbox
    /// drifted out from under the CAS (woken elsewhere, already cleaned up) —
    /// in which case the next tick re-evaluates against whatever is current.
    /// The lifecycle flip is committed before the substrate cleanup, so a
    /// cleanup failure doesn't reopen a race; it only risks a leaked pod (the
    /// kube cascade on node termination eventually sweeps it), surfaced via a
    /// `CleanupFailed` event + warn log. Mirrors the abandon-stale sweep's
    /// claim-then-clean ordering.
    async fn mark_sandbox_failed_on_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<u64, sqlx::Error> {
        let claimed: Vec<String> = sqlx::query_scalar(mark_sandbox_failed_on_node_query())
            .bind(sandbox_id)
            .bind(source_node_id)
            .bind(mark_sandbox_failed_on_node_lifecycles())
            .fetch_all(&self.pool)
            .await?;
        let Some(prev_lifecycle) = claimed.into_iter().next() else {
            return Ok(0);
        };

        let node_label = source_node_id.to_string();
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::LifecycleTransitionFailedOnNode,
            "drain_sandbox_failed_on_node",
            SandboxEventDetails::default()
                .with_from_state(prev_lifecycle.as_str())
                .with_to_state("failed")
                .source_node_id(node_label.clone()),
        );
        emit_node_drain_sandbox_failed_event(
            sandbox_id,
            "drain_sandbox_failed_on_node",
            SandboxEventDetails::default()
                .with_from_state(prev_lifecycle.as_str())
                .source_node_id(node_label.clone()),
        );
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::CleanupStarted,
            "drain_sandbox_failed_on_node",
            SandboxEventDetails::default()
                .with_from_state(prev_lifecycle.as_str())
                .source_node_id(node_label.clone()),
        );
        let sandbox_id_typed = sandbox_id;
        match self
            .substrate
            .kubernetes()
            .delete_execution(sandbox_id_typed)
            .await
        {
            Ok(()) => {
                emit_node_drain_event(
                    sandbox_id,
                    SandboxEventType::CleanupCompleted,
                    "drain_sandbox_failed_on_node",
                    SandboxEventDetails::default().source_node_id(node_label.clone()),
                );
            }
            Err(err) => {
                emit_node_drain_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    "drain_sandbox_failed_on_node",
                    SandboxEventDetails::default()
                        .source_node_id(node_label.clone())
                        .error(err.to_string())
                        .logged_by_caller(),
                );
                tracing::warn!(
                    node_id = %source_node_id,
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "node_drain: best-effort substrate delete after marking node-failed \
                     sandbox failed did not complete; backing pod may persist until the \
                     node is terminated"
                );
            }
        }

        Ok(1)
    }

    /// Give-up for a `suspended` sandbox whose local-snapshot reupload is
    /// permanently refused (not `Suspended` on the node). Adopts an existing
    /// ready remote snapshot as the canonical recovery source via the
    /// [`adopt_remote_snapshot_for_drain_query`] CAS, then — only if the flip
    /// fired — emits the `LocalSnapshotPromotionAbandoned` sandbox event.
    ///
    /// Unlike [`Self::mark_sandbox_failed_on_node`], this is **not** terminal
    /// for the sandbox: the row stays `suspended` and recoverable from the
    /// adopted snapshot, so there is no pod teardown — the stuck restore pod is
    /// the underlying node-side bug's to reap. Returns the number of rows
    /// flipped: `1` on adoption, `0` if not yet eligible (still settling, no
    /// ready remote snapshot, an in-flight upload, or drifted out from under
    /// the CAS), in which case the next tick re-evaluates.
    async fn make_sandbox_durable_from_remote_snapshot(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        settle: PositiveDuration,
    ) -> Result<u64, sqlx::Error> {
        // `PositiveDuration` guarantees a non-zero settle, so the SQL never
        // sees a zero interval that would fire on a just-suspended row.
        let settle_secs = i64::try_from(settle.get().as_secs()).unwrap_or(i64::MAX);
        let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(
            adopt_remote_snapshot_for_drain_query()
        )))
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(settle_secs)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if flipped == 0 {
            return Ok(0);
        }

        // Not a lifecycle transition — the row stays `suspended`. This records
        // the durability handoff (local promotion abandoned in favor of the
        // existing ready remote snapshot), so it carries the source node, not a
        // from/to state pair that would imply a transition.
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::LocalSnapshotPromotionAbandoned,
            "drain_adopt_remote_snapshot",
            SandboxEventDetails::default().source_node_id(source_node_id.to_string()),
        );

        Ok(flipped)
    }

    /// Evacuate one sandbox off its source node by running the
    /// suspend → restore pipeline.
    ///
    /// **Lifecycle preservation.** `pre_lifecycle` is the user-visible
    /// lifecycle the reconciler observed at listing time. The pipeline
    /// rebuilds the pod via suspend → restore, and the restore RPC's
    /// `target_state` parameter controls the final lifecycle in one
    /// shot:
    /// - `Started`: post-restore UPDATE sets `lifecycle = 'started'`.
    /// - `Paused`: the restore is issued with `target_state = Paused`,
    ///   so sbx-mgr loads the snapshot and skips the post-load resume,
    ///   leaving the VM in the firecracker-paused state. The UPDATE
    ///   then sets `lifecycle = 'paused'` to match, with no extra RPC.
    ///
    /// **Concurrency invariant — yields to user lifecycle requests and
    /// to placement changes.** `list_evacuatable_sandboxes_on_node`
    /// selects sandboxes whose `lifecycle = $pre_lifecycle`, but the
    /// spawned evacuation task runs asynchronously and either (a) a
    /// user pause/resume/stop can land between selection and execution,
    /// changing the lifecycle, or (b) the sandbox can be moved to a
    /// different node via a wake of a previously-suspended sandbox,
    /// admin force-evacuate, etc. We CAS-claim the lifecycle
    /// (`$pre_lifecycle -> suspending`) **guarded on `active_node_id =
    /// source_node`** before calling sbx-mgr's suspend RPC; if the CAS
    /// doesn't match either predicate, we bail out without touching the
    /// node-side state. Each post-step DB write is similarly guarded
    /// with `WHERE lifecycle = <expected>` so we never overwrite a
    /// concurrent user-driven change later in the pipeline. On suspend
    /// failure we best-effort revert the claim back to `$pre_lifecycle`
    /// only while the active execution remains uncommitted. Once the substrate
    /// marks that exact execution suspended, `suspending` retains ownership so
    /// a later drain tick can retry pod retirement.
    async fn evacuate_sandbox(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        pre_lifecycle: SandboxLifecycle,
    ) -> Result<(), EvacuateError> {
        // Defend against a caller passing a non-evacuable lifecycle.
        // `is_suspend_evacuable_lifecycle`, not `is_evacuable_lifecycle`. The
        // wider constant is the source of truth for the
        // `evacuation_restore_target` CHECK, so it includes `Stopped` — and
        // `Stopped` is precisely what this path must refuse, because it CASes
        // into `'suspending'` and asks sbx-mgr to snapshot a VM that does not
        // exist. Reading the wide set here would have let a future refactor of
        // `spawn_evacuations`' dispatch park a stopped sandbox in the
        // `suspending` transient carrying the marker, which
        // `abandon_stale_resident_sandboxes` deliberately spares — a wedge with
        // a fabricated user-visible lifecycle.
        //
        // No longer unreachable, either: the listing returns `stopped` rows on
        // purpose now, and its in-flight disjunct returns them regardless of the
        // feature flag. This is a real guard, not the price of reusing
        // `SandboxLifecycle` (see the module doc-comment in
        // `node_drain/mod.rs`).
        if !is_suspend_evacuable_lifecycle(pre_lifecycle) {
            return Err(EvacuateError::Lookup(format!(
                "sandbox {sandbox_id} cannot be evacuated through the suspend path from \
                 lifecycle '{pre_lifecycle}' (not in SUSPEND_EVACUABLE_LIFECYCLES)"
            )));
        }
        let sandbox_id_typed = sandbox_id;
        let source_node_id_typed = source_node_id;
        let pre_lifecycle_str = pre_lifecycle.as_str();

        // 1. Claim: only proceed if the sandbox is still in the
        //    pre-evacuation lifecycle we listed it under, or resume the same
        //    marked `'suspending'` claim left by an earlier retirement
        //    attempt. The `active_node_id` guard catches the case where the
        //    sandbox was evacuated/restored elsewhere between list and claim.
        //    The same UPDATE persists `evacuation_restore_target =
        //    $pre_lifecycle`: the cross-node restore needs a ready backup of
        //    the local artifact published below, so restore is deferred and
        //    re-driven on a later tick. This marker records what to restore the
        //    sandbox back to (and that it's mid-evacuation, not plain
        //    user-suspended) so the re-drive — and a controller restart —
        //    preserve the original lifecycle.
        let claimed = claim_evacuation_suspend(
            &self.pool,
            sandbox_id_typed,
            source_node_id_typed,
            pre_lifecycle,
        )
        .await
        .map_err(|err| EvacuateError::Lookup(format!("lifecycle claim query failed: {err}")))?;
        if !claimed {
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::LifecycleTransitionCasLost,
                "node_drain_claim_lost",
                SandboxEventDetails::default()
                    .with_from_state(pre_lifecycle_str)
                    .target_state("suspending")
                    .source_node_id(source_node_id.to_string()),
            );
            return Err(EvacuateError::Lookup(format!(
                "sandbox {sandbox_id} no longer in lifecycle '{pre_lifecycle_str}' on node \
                {source_node_id} (lifecycle or placement drifted before evacuate claim)"
            )));
        }
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::SuspendRequested,
            "node_drain",
            SandboxEventDetails::default()
                .with_from_state(pre_lifecycle_str)
                .with_to_state("suspending")
                .target_state("suspended")
                .source_node_id(source_node_id.to_string()),
        );

        // 2. Suspend. On error before a durable execution commit, revert the
        //    claim so the user observes the pre-evacuation lifecycle again.
        //    `record_suspend_commit` atomically marks the active execution
        //    `suspended` and binds its exact artifact. Once that happens, pod
        //    retirement owns the retry, so the conditional revert leaves the
        //    sandbox `suspending`. The revert is best-effort — log its failure
        //    but still surface the original suspend error. sbx-mgr's `Suspend` accepts both
        //    started and paused VMs (auto-pauses started before
        //    persisting), so the suspend RPC is the same call for both
        //    pre-evacuation lifecycles.
        let published_artifact = match self.substrate.suspend_execution(sandbox_id_typed).await {
            Ok(artifact) => artifact,
            Err(err) => {
                let reverted = match revert_evacuation_suspend_claim(
                    &self.pool,
                    sandbox_id_typed,
                    pre_lifecycle,
                )
                .await
                {
                    Ok(reverted) => reverted,
                    Err(revert_err) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            pre_lifecycle = pre_lifecycle_str,
                            error = %revert_err,
                            "node_drain: failed to revert lifecycle after suspend error"
                        );
                        false
                    }
                };
                let mut failure_details = SandboxEventDetails::default()
                    .with_from_state("suspending")
                    .source_node_id(source_node_id.to_string())
                    .error(err.to_string());
                if reverted {
                    failure_details = failure_details.with_to_state(pre_lifecycle_str);
                }
                emit_node_drain_event(
                    sandbox_id,
                    SandboxEventType::SuspendFailed,
                    "node_drain",
                    failure_details,
                );
                // A suspend rejected because the sandbox is already `failed`
                // on the node is terminal, not retryable: the DB lifecycle
                // (`started`/`paused`) has diverged from the node-local
                // `failed` state, and that divergence never self-heals. The
                // claim has just been reverted to `pre_lifecycle`, so the row
                // rests in a clean evacuable lifecycle for the reconciler's
                // give-up path (`mark_sandbox_failed_on_node`). Classify via
                // the shared-marker predicate so a reworded producer message
                // can't silently turn this back into an infinite suspend retry.
                let err_msg = err.to_string();
                if is_sandbox_failed_on_node_rejection(&err_msg) {
                    return Err(EvacuateError::SandboxFailedOnNode(err_msg));
                }
                return Err(EvacuateError::Suspend(err_msg));
            }
        };
        let published_local_artifact_id = published_artifact
            .as_ref()
            .map_or_else(|| "none".to_string(), |artifact| artifact.id.to_string());

        // 3. Commit the suspend in the lifecycle column. `suspend_execution`
        //    on the K8s substrate touches `sandbox_k8s_executions` +
        //    placement but NOT `sandboxes.lifecycle`; the HTTP path does
        //    that via `maybe_update_sandbox_lifecycle`. Guarded so we
        //    don't stomp a concurrent change that somehow slipped past
        //    the `'suspending'` claim.
        if let Err(err) = sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'suspended'
             WHERE id = $1 AND lifecycle = 'suspending'"
        ))
        .bind(sandbox_id)
        .execute(&self.pool)
        .await
        {
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::SuspendFailed,
                "node_drain",
                SandboxEventDetails::default()
                    .with_from_state("suspending")
                    .source_node_id(source_node_id.to_string())
                    .error(err.to_string()),
            );
            return Err(EvacuateError::Suspend(format!(
                "post-suspend lifecycle update failed: {err}"
            )));
        }
        emit_node_drain_event(
            sandbox_id,
            SandboxEventType::SuspendCompleted,
            "node_drain",
            SandboxEventDetails::default()
                .with_from_state("suspending")
                .with_to_state("suspended")
                .target_state(pre_lifecycle_str)
                .source_node_id(source_node_id.to_string()),
        );

        // 4. Enqueue backup for the exact local artifact published by suspend.
        //    Failure here leaves a safe local-only suspension; the pinned-local
        //    retry pass will attempt the same artifact again on the next tick.
        match self
            .promote_local_snapshot(sandbox_id, source_node_id)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    local_artifact_id = %published_local_artifact_id,
                    "node_drain: local evacuation artifact backup enqueued"
                );
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    local_artifact_id = %published_local_artifact_id,
                    error = %err,
                    "node_drain: initial local artifact backup enqueue failed; \
                     pinned-local reconciliation will retry"
                );
            }
        }

        // 5. The cohort restore is decoupled from this suspend. Once the
        //    backup worker registers this artifact `ready`, the restore is driven
        //    independently — immediately via the snapshot-register trigger,
        //    and by the cluster-wide restore reconciler as the fallback (see
        //    `restore_evacuated_sandbox` / `restore_pending_evacuations_query`).
        //    The sandbox rests in `suspended` carrying the
        //    `evacuation_restore_target` marker (set in step 1) until then.
        //    The source node is free to finish draining once the backup
        //    completes — the data is durable in S3, so the restore no longer
        //    needs the source node alive.
        tracing::info!(
            sandbox_id = %sandbox_id,
            pre_lifecycle = pre_lifecycle_str,
            "node_drain: evacuation suspend complete; cohort restore will be driven on \
             backup-ready (trigger + reconciler fallback)"
        );

        Ok(())
    }

    /// Advance one `stopped` resident's evacuation by a single step.
    ///
    /// ## Why this is a sibling of [`Self::evacuate_sandbox`], not a branch in it
    ///
    /// That pipeline's three load-bearing steps are CAS `$pre_lifecycle →
    /// 'suspending'`, `suspend_execution`, commit `'suspended'`. A stopped
    /// sandbox shares none of them: there is no VM to snapshot, and its resting
    /// lifecycle before, during and after the move is `stopped`. What the two
    /// do share is the skeleton — a node-guarded claim, a publish, an
    /// event-emitting teardown, and the `evacuation_restore_target` marker the
    /// cluster-wide restore sweep selects by — and that is shared through the
    /// marker and the sweep rather than through a common function body.
    ///
    /// ## The sequence, and why it is re-entrant instead of long-running
    ///
    /// 1. **Claim** ([`claim_stopped_evacuation`]) — the marker, under
    ///    `active_node_id` + node-still-force-draining guards.
    /// 2. **Publish** — `EnqueueArtifactBackup(CurrentInstance, Disk)`. On a
    ///    `Stopped` sandbox with no pin, sbx-mgr mints a read-only `d-stop-{id}`
    ///    btrfs snapshot and publishes it as the current disk artifact; with a
    ///    pin it reuses it, joins any in-flight upload, and short-circuits on an
    ///    already-ready backup. That makes the call idempotent, which is what
    ///    lets this run once per tick.
    /// 3. **Mirror the pin** ([`record_stopped_evacuation_artifact`]) — the
    ///    identity every later step resolves the artifact by.
    /// 4. **Prove durability** — a ready, non-orphaned catalog row for that
    ///    exact artifact.
    /// 5. **Release the node** — `delete_execution_for_teardown`, which deletes
    ///    the retired executions' pods and clears the cluster row's placement,
    ///    refusing if an execution has appeared. It sends sbx-mgr nothing: the
    ///    rootfs and sbx-mgr's row stay on the node until the node is
    ///    terminated, which is what releasing it allows.
    ///
    /// Steps 2–4 span an S3 upload of a multi-GB rootfs, which outlasts a tick
    /// by orders of magnitude. Holding a task across it would pin one of the 16
    /// per-node evacuation slots for the whole upload and lose all progress to a
    /// controller restart. Instead every step is idempotent and keyed on
    /// persisted state, so the reconciler re-lists the row each tick and this
    /// resumes wherever the database says it got to.
    ///
    /// ## What releases the node — and what does not yet re-place the sandbox
    ///
    /// Step 5 is where this path's own work ends. Placing the row back on a
    /// node belongs to the restore sweep, and **the substrate cannot serve that
    /// request today**: `begin_restore_lifecycle`'s marker-required claim
    /// admits only `('suspended','restoring')`,
    /// `sandbox_lifecycle_to_wake_target_proto` accepts only `Started`/`Paused`,
    /// and the evacuation restore's source selector wants a `full` snapshot (the
    /// disk-only one serves only a user `start` of a `stopped` row). The sweep
    /// therefore fails, backs off, and escalates. That is the reason the
    /// feature ships disabled, and the reason the sweep's stopped arm is here
    /// at all rather than withheld: once those refusals lift, every
    /// already-evacuated sandbox is picked up automatically instead of needing
    /// a further deploy to notice it.
    ///
    ///
    /// Step 5's placement clear sets `placement = 'unplaced'`,
    /// `active_node_id = NULL`, `target_node_id = NULL`. `active_node_id = NULL`
    /// is the release: both gates that hold a node for a `stopped` resident —
    /// the drain's fourth term and `drained_node_terminator`'s guard — are
    /// `sandbox_residency::active_resident_predicate`, whose first conjunct is
    /// `active_node_id = <node>`. The row also drops out of the drain's own
    /// evacuatable listing on the next tick for the same reason, which is what
    /// hands it over to the cluster-wide restore sweep.
    ///
    /// The node's census keeps reporting the sandbox, since its local row is
    /// untouched; the release does not depend on the census at all, and nothing
    /// here depends on [`ResidencyEvidence::NodeCensusAware`] being enabled.
    ///
    /// ## The durability gate is the only guard on the user's disk
    ///
    /// Once step 5 releases the node, the drain terminates it, and the rootfs
    /// goes with it. Nothing on the node guards against that the way sbx-mgr's
    /// erase-safety check guards a suspended sandbox's only copy:
    /// `sandbox_erase_safety` returns `Safe` for a `Stopped` row. Step 4 is
    /// what stands between the drain and destroying a user's disk, and it is
    /// why the publish is a *blocking* gate rather than the fire-and-forget
    /// enqueue `retire_execution` already performs.
    async fn evacuate_stopped_sandbox(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<StoppedEvacuationOutcome, EvacuateError> {
        let node_label = source_node_id.to_string();

        // 0. Read the row once, and let it decide which of the steps below are
        //    needed. On the tick that finishes the evacuation it is followed only
        //    by the node re-check and the teardown; on every tick in between it
        //    is the only query at all. See `stopped_evacuation_state` for why re-running
        //    the idempotent writes instead is not free.
        let state = self
            .stopped_evacuation_state(sandbox_id, source_node_id)
            .await?;
        if state.claim_holds && state.bytes_durable {
            return self
                .tear_down_stopped_local_copy(
                    sandbox_id,
                    source_node_id,
                    &node_label,
                    state.pinned_artifact,
                )
                .await;
        }

        // 0b/0c. The provenance checks, and they share one gate on purpose.
        //
        // Both only apply when this tick would open a *fresh* evacuation, which
        // is what `!marked` says: the row carries no evacuation marker, so
        // nobody's in-flight evacuation owns it.
        //
        // `!claim_holds` is redundant and kept for legibility, not for effect —
        // `claim_holds` includes the marker conjunct, so `!marked` already
        // implies it. Don't hunt for a case that distinguishes them; there
        // isn't one. Reverting to `!marked` alone changes no test, which is what
        // a redundant conjunct predicts.
        //
        // What *is* load-bearing is that 0b and 0c share this binding: two
        // separate conditions can drift, one gaining the marker conjunct and the
        // other not, which reinstates the misclassification the conjunct
        // prevents.
        //
        // What the gate excludes is ordinary drift on an evacuation this drain
        // *did* open: an `:undrain` between the listing and this read, a
        // placement change, a lifecycle move. Each leaves `claim_holds` false
        // with a pin present — and the pin is the drain's own. Reporting that as
        // unattributable sends a responder to clear a pin on a healthy sandbox,
        // on an outcome the metrics catalog documents as never self-resolving.
        // Those fall through to the claim below, which refuses them as
        // `Deferred`.
        if !state.claim_holds && !state.marked {
            // 0b. The cluster row's own pin. `claim_stopped_evacuation` enforces
            //     this too — it is the atomic guard — but repeating it here is
            //     what makes the refusal *reportable*: the claim's `UPDATE`
            //     cannot say why it matched nothing.
            if let Some(pinned) = state.pinned_artifact {
                return Ok(StoppedEvacuationOutcome::UnattributableArtifact {
                    local_artifact_id: Some(pinned),
                });
            }

            // 0c. The *node's* pin, which is the one that decides whether the
            //     next publish mints or reuses. Everything cluster-side is a
            //     backstop: the mint arm reads sbx-mgr's own SQLite row, and for
            //     a `stopped` sandbox nothing keeps that in step with Postgres —
            //     the projection that would carry it up is gated to
            //     `status = 'suspended'`, and the retire that causes the
            //     stop-time mint throws the enqueue response away. So the
            //     cluster column is NULL for essentially every stopped sandbox
            //     and 0b alone admits exactly the row it exists to refuse. See
            //     [`Self::node_reported_artifact`].
            //
            //     Only an unclaimed row reaches this. A resumed claim is
            //     trusted by the marker-plus-lifecycle conjunction (see
            //     [`claim_stopped_evacuation`]) and returns above, so an
            //     evacuation in progress costs no RPC per tick; a row refused
            //     here is asked again on every tick.
            let node = self
                .node_reported_artifact(sandbox_id, source_node_id)
                .await?;
            if node.state_degraded {
                // The node published a stub: its raw lifecycle with the pin and
                // liveness at their defaults. Read as an answer, that is
                // exactly the pinless, stopped, not-alive shape this step
                // admits, so it is drift to wait out rather than a yes.
                return Ok(StoppedEvacuationOutcome::Deferred {
                    reason: format!(
                        "node {source_node_id} could only publish a degraded state for \
                         sandbox {sandbox_id}, so this drain cannot open an evacuation for it yet"
                    ),
                });
            }
            if node.pin_unreadable {
                // Unreadable is refused exactly as a readable pin is — the
                // point is that the bytes cannot be attributed either way — but
                // it carries no artifact id, because inventing one would put a
                // nil UUID in the outcome and the log where a real id belongs.
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    node_id = %node_label,
                    node_stopped_since = ?node.stopped_since,
                    "node_drain: refusing to open a stopped evacuation because the node \
                     reported an artifact pin that could not be read"
                );
                return Ok(StoppedEvacuationOutcome::UnattributableArtifact {
                    local_artifact_id: None,
                });
            }
            if let Some(pinned) = node.pinned_artifact {
                // `debug`: this refusal is today the expected outcome for
                // nearly every stopped resident, and it repeats every tick for
                // as long as the sandbox sits there, so an `info` line would be
                // a line per sandbox per five seconds. The counter is the
                // signal. `stopped_since` is the node's own entry time into
                // `stopped`; the artifact's capture time is the value that
                // would make this refusal precise instead of universal, and the
                // node does not expose it yet.
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    node_id = %node_label,
                    local_artifact_id = %pinned.as_uuid(),
                    node_stopped_since = ?node.stopped_since,
                    "node_drain: refusing to open a stopped evacuation on an artifact this \
                     drain did not mint; the node would reuse it rather than capture the \
                     rootfs as it stands"
                );
                return Ok(StoppedEvacuationOutcome::UnattributableArtifact {
                    local_artifact_id: Some(pinned),
                });
            }
            if !node.present || !node.reports_stopped || node.vm_alive {
                // Not a provenance problem and not permanent, so none of these
                // may be reported as unattributable. Three shapes:
                //
                // * no row on the node — nothing there to publish or tear down;
                // * the node believes the sandbox is running, so the cluster's
                //   `stopped` is the stale view;
                // * the node's row says `stopped` but its **Firecracker process
                //   is alive**. That is not a contradiction the node resolves
                //   for us: its status classifier only overrides a persisted
                //   state when it can say `paused`, so a live VM on a `stopped`
                //   row reports `"stopped"`. Publishing here would snapshot a
                //   rootfs a guest is still writing to and the teardown would
                //   then erase it — strictly worse than the stale-pin case this
                //   whole path exists to prevent, because the sandbox is live.
                //   `sandbox_states_with_client` already treats `VmAlive` as
                //   outranking both status and artifact; this matches it.
                return Ok(StoppedEvacuationOutcome::Deferred {
                    reason: format!(
                        "node {source_node_id} reports sandbox {sandbox_id} as \
                         present={} stopped={} vm_alive={}, so this drain cannot open \
                         an evacuation for it yet",
                        node.present, node.reports_stopped, node.vm_alive
                    ),
                });
            }
        }

        // 1. Claim.
        // The flag lands on the claim rather than here, so that a disabled
        // cluster stops *opening* stopped evacuations without abandoning the
        // ones already in flight. A refusal at this level would strand a
        // sandbox whose disk is published and whose local copy is not yet gone.
        //    Skipped entirely when the read above already proved every conjunct
        //    of it: re-claiming would write the marker it just read.
        let claim = if state.claim_holds {
            StoppedEvacuationClaim::Resumed
        } else {
            claim_stopped_evacuation(
                &self.pool,
                sandbox_id,
                source_node_id,
                self.evacuate_stopped_residents,
            )
            .await
            .map_err(|err| {
                EvacuateError::Lookup(format!("stopped evacuation claim query failed: {err}"))
            })?
        };
        if !claim.holds() {
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::LifecycleTransitionCasLost,
                "node_drain_stopped_claim_lost",
                SandboxEventDetails::default()
                    .with_from_state(SandboxLifecycle::Stopped.as_str())
                    .target_state(SandboxLifecycle::Stopped.as_str())
                    .source_node_id(node_label.clone()),
            );
            return Ok(StoppedEvacuationOutcome::Deferred {
                reason: format!(
                    "sandbox {sandbox_id} is no longer a stopped resident of force-draining node \
                     {source_node_id} (lifecycle, placement, or drain mode drifted before the \
                     evacuation claim)"
                ),
            });
        }

        // 1b. An attempt for exactly these bytes is already in flight — so the
        //     answer is on its way and asking again cannot change it. Return
        //     before step 2.
        //
        //     This is not just a saved RPC. Step 2 is *not* idempotent for all
        //     three shapes it can meet: sbx-mgr joins an in-flight attempt and
        //     short-circuits an already-`ready` backup, but it deliberately
        //     declines to reuse a **failed** job, so a failed upload falls
        //     through to a fresh `BackupAttemptId` — and one `snapshots` row is
        //     inserted per attempt id. Re-issuing every tick therefore leaks a
        //     row every five seconds for a sandbox whose upload keeps failing,
        //     into a table the retention sweeper never prunes for disk rows.
        //     Skipping while an attempt is live is what keeps the *retry* rate
        //     tied to attempts rather than to ticks.
        //
        //     The residual is deliberate: a genuinely failed attempt is retried
        //     on the next tick, once, and if it fails again there is one new row
        //     per failure rather than per tick — **provided the node's pin
        //     matches this row's**. It does by construction on the happy path,
        //     because the claim proves the row pinless and step 3 writes back
        //     whatever the node returned. If they ever diverge, this gate keys
        //     on the row's artifact while step 2 enqueues the node's, so the
        //     gate cannot see the in-flight attempt and the leak is per tick
        //     again — until step 3's pin write refuses the mismatch and defers.
        //     Fail-closed and self-announcing, but not the bound stated above.
        //     Widening this `EXISTS` to any `creating` disk row for the sandbox
        //     is the mirror failure priced below: it would wedge on a stale row
        //     for a different artifact. Bounding that further wants a
        //     backoff keyed on the attempt count, which is the same shape as the
        //     restore side's and wants the same columns — see
        //     [`StoppedEvacuationState::upload_in_flight`] for why it is not
        //     here.
        //
        //     **And this gate moves a cost rather than removing one, so both
        //     ends are worth naming.** Without it, a wedged upload leaked a
        //     catalog row every tick, unbounded, into a table nothing prunes.
        //     With it, a `creating` row that never finalizes blocks the
        //     re-publish for as long as it persists — and nothing terminalizes a
        //     stale *disk* row: `stuck_snapshot_reaper` is scoped `type =
        //     'full'`, and the retention sweeper filters the same way. So a
        //     finalize that never lands (its RPC failing permanently for that
        //     attempt, say) parks the evacuation instead of retrying it.
        //
        //     That scope was not an oversight when it was written — it mirrored
        //     the suspend side's `inflight_suspend_upload_exists`, which was the
        //     only in-flight gate at the time. This gate is the second, and its
        //     own comment there has been corrected to say so, because it read as
        //     an assurance that excluding disk rows was still sufficient.
        //
        //     That is the better end of the trade, and not by much: it fails
        //     *stuck* rather than open — nothing is destroyed, the claim holds,
        //     the node stays held — and unlike the leak it is visible, because a
        //     marked claim on a force-draining node is exactly what
        //     `node_drain.stopped_evacuation_stall_secs` selects, and the age it
        //     reports comes from that same stale row's registration. The alert
        //     built for a wedged evacuation fires on it. The leak had no such
        //     signal.
        if state.claim_holds
            && state.upload_in_flight
            && let Some(pinned) = state.pinned_artifact
        {
            return Ok(StoppedEvacuationOutcome::AwaitingDurableDisk {
                local_artifact_id: pinned,
            });
        }

        // 2. Publish. Idempotent on the node for two of three shapes: an
        //    existing pin is reused, an in-flight upload is joined, and an
        //    already-ready backup returns without re-uploading. A *failed*
        //    attempt is the exception — see step 1b.
        let published = self
            .enqueue_stopped_disk_backup(sandbox_id, source_node_id)
            .await?;
        let local_artifact_id = published.artifact.id;
        // Only on the tick that opened the evacuation. Every later tick
        // re-issues the same request against the same artifact — sbx-mgr joins
        // the in-flight attempt rather than starting one — so a per-tick event
        // would report a queue that was only ever entered once.
        if claim == StoppedEvacuationClaim::Claimed {
            emit_node_drain_event(
                sandbox_id,
                SandboxEventType::ArtifactBackupQueued,
                "node_drain_stopped_evacuation",
                SandboxEventDetails::default()
                    .with_from_state(SandboxLifecycle::Stopped.as_str())
                    .source_node_id(node_label.clone()),
            );
        }

        // 3. Mirror the pin into the cluster row — only when the node handed
        //    back an identity the row does not already carry. A miss means the
        //    row drifted between the claim and here; stop rather than proceed
        //    toward a teardown on stale evidence.
        //
        //    When the pin is unchanged and step 0 had *already* proved the claim,
        //    there is nothing new to learn: that read consulted durability and
        //    found it false, and the same artifact cannot have become durable
        //    between two statements of the same tick. Return, and let the next
        //    tick's step 0 pick it up five seconds later.
        //
        //    The `claim_holds` half of that condition is load-bearing, not
        //    belt-and-braces. Step 0 computes `bytes_durable` unconditionally
        //    but the entry point only *acts* on it when the claim held, so on
        //    the path where this tick opened the claim, durability was read and
        //    ignored — and may well have been true, for an artifact this drain
        //    minted before a controller restart. Taking the shortcut there
        //    would defer a teardown the evidence already authorized, and the
        //    comment above would be asserting a premise this branch lacks.
        if state.claim_holds && state.pinned_artifact == Some(local_artifact_id) {
            return Ok(StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id });
        }
        let recorded = record_stopped_evacuation_artifact(
            &self.pool,
            sandbox_id,
            source_node_id,
            local_artifact_id,
        )
        .await
        .map_err(|err| {
            EvacuateError::Publish(format!(
                "stopped evacuation artifact pin write failed: {err}"
            ))
        })?;
        if !recorded {
            return Ok(StoppedEvacuationOutcome::Deferred {
                reason: format!(
                    "sandbox {sandbox_id} drifted out of its stopped evacuation claim before its \
                     disk artifact pin could be recorded"
                ),
            });
        }

        // 4. Durability. Re-read only because the pin just changed: the
        //    artifact may already have a ready backup from an earlier attempt,
        //    and waiting a tick to notice would be a gratuitous five seconds on
        //    every crash recovery.
        match self
            .stopped_teardown_authorization(sandbox_id, source_node_id)
            .await?
        {
            TeardownAuthorization::Authorized => {}
            TeardownAuthorization::AwaitingDurableDisk => {
                return Ok(StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id });
            }
            TeardownAuthorization::ClaimLost => {
                return Ok(StoppedEvacuationOutcome::Deferred {
                    reason: format!(
                        "sandbox {sandbox_id} drifted out of its stopped evacuation claim \
                         before the teardown was authorized"
                    ),
                });
            }
        }

        // 5. Teardown. Everything before this point is reversible; this is not.
        self.tear_down_stopped_local_copy(
            sandbox_id,
            source_node_id,
            &node_label,
            Some(local_artifact_id),
        )
        .await
    }

    async fn list_restore_pending_evacuations(
        &self,
        honor_backoff_windows: bool,
    ) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
        let query = restore_pending_evacuations_query(honor_backoff_windows);
        let rows: Vec<(SandboxId, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .fetch_all(&self.pool)
                .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (id, target_str) in rows {
            // The CHECK on `evacuation_restore_target` restricts the column
            // to `'started'`/`'paused'`/`'stopped'`, so both guards are
            // unreachable in production — the price for reusing
            // `SandboxLifecycle` instead of a closed-subset enum (mirrors
            // `list_evacuatable_sandboxes_on_node`). Log + skip rather than
            // feed a wrong target into the restore.
            //
            // Membership is tested against the whole of `EVACUABLE_LIFECYCLES`,
            // never the per-environment enabled subset. A marker written while
            // stopped-resident evacuation was on has to keep being honoured
            // after it is turned off, or the flip strands a sandbox whose node is
            // already released with no reconciler willing to place it.
            let target = match target_str.parse::<SandboxLifecycle>() {
                Ok(lc) if is_evacuable_lifecycle(lc) => lc,
                Ok(lc) => {
                    tracing::warn!(
                        sandbox_id = %id,
                        lifecycle = %lc,
                        "node_drain: restore-pending marker is a non-evacuable lifecycle; skipping"
                    );
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %id,
                        error = %err,
                        "node_drain: unparseable evacuation_restore_target; skipping"
                    );
                    continue;
                }
            };
            out.push((id, target));
        }
        Ok(out)
    }

    async fn stopped_evacuation_stall(
        &self,
    ) -> Result<Option<StoppedEvacuationStall>, sqlx::Error> {
        let query = stopped_evacuation_stall_secs_query();
        let row: Option<(i64, SandboxId, NodeId)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .fetch_optional(&self.pool)
                .await?;
        Ok(
            row.map(|(secs, sandbox_id, node_id)| StoppedEvacuationStall {
                age: Duration::from_secs(secs.max(0).cast_unsigned()),
                sandbox_id,
                node_id,
            }),
        )
    }

    async fn count_stopped_replacements_pending(&self) -> Result<u64, sqlx::Error> {
        let pending: i64 = sqlx::query_scalar(stopped_replacements_pending_query())
            .fetch_one(&self.pool)
            .await?;
        Ok(pending.max(0).cast_unsigned())
    }

    async fn schedule_restore_retry(
        &self,
        sandbox_id: SandboxId,
        backoff_base: PositiveDuration,
        backoff_cap: PositiveDuration,
    ) -> Result<Option<i32>, sqlx::Error> {
        // `i64::MAX` seconds overflows Postgres interval arithmetic long after
        // any sane config; saturate rather than wrap so an absurd env value
        // degrades to "interval out of range" instead of a negative interval
        // that would silently disable the backoff.
        let base_secs = i64::try_from(backoff_base.get().as_secs()).unwrap_or(i64::MAX);
        let cap_secs = i64::try_from(backoff_cap.get().as_secs()).unwrap_or(i64::MAX);
        sqlx::query_scalar::<_, i32>(schedule_restore_retry_query())
            .bind(sandbox_id)
            .bind(base_secs)
            .bind(cap_secs)
            .fetch_optional(&self.pool)
            .await
    }

    async fn redrive_stranded_suspending_evacuations(&self) -> Result<Vec<SandboxId>, sqlx::Error> {
        let query = redrive_stranded_suspending_query();
        let advanced: Vec<SandboxId> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .fetch_all(&self.pool)
                .await?;
        // Emit the missing `suspended` transition the crashed controller never
        // committed, so a sandbox timeline reconstructed from the event stream
        // shows the recovery rather than an unexplained gap between
        // `suspend_requested` and the later restore.
        for sandbox_id in &advanced {
            emit_node_drain_event(
                *sandbox_id,
                SandboxEventType::SuspendCompleted,
                "node_drain_redrive_stranded_suspend",
                SandboxEventDetails::default()
                    .with_from_state("suspending")
                    .with_to_state("suspended"),
            );
        }
        Ok(advanced)
    }

    async fn restore_evacuated_sandbox(
        &self,
        sandbox_id: SandboxId,
        target_lifecycle: SandboxLifecycle,
        evacuate_timeout: PositiveDuration,
        placement: Option<EvacuationPlacement>,
    ) -> Result<RestoreOutcome, EvacuateError> {
        // Defense in depth: the restore-pending list query only surfaces
        // sandboxes whose marker is a valid evacuable lifecycle, but reuse
        // of `SandboxLifecycle` means a non-evacuable value is representable.
        if !is_evacuable_lifecycle(target_lifecycle) {
            return Err(EvacuateError::Lookup(format!(
                "sandbox {sandbox_id} restore target '{target_lifecycle}' is not in \
                 EVACUABLE_LIFECYCLES"
            )));
        }
        let outcome = self
            .restore_evacuated_and_finalize(
                sandbox_id,
                target_lifecycle,
                evacuate_timeout,
                placement,
            )
            .await?;
        if matches!(outcome, RestoreOutcome::Deferred) {
            // `list_restore_pending_evacuations` already confirmed the newest
            // full snapshot is ready + the marker is set, so a Deferred here
            // means the marker raced out from under us: a concurrent user
            // restore (allowed once the snapshot is portable) cleared it
            // between the sweep's list and this call. The ready snapshot can't
            // disappear, and the readiness gate (`latest_full_snapshot_ready`)
            // references no placement timestamp, so a `placement_changed_at`
            // bump can't defer us. The caller leaves it for the next sweep and
            // — crucially — does not record it as a restore (the outcome is
            // returned so the restore-latency metric only counts real
            // restores).
            tracing::info!(
                sandbox_id = %sandbox_id,
                "node_drain: cohort restore no longer eligible (evacuation marker \
                 cleared by a user takeover); will re-evaluate on next sweep"
            );
        }
        Ok(outcome)
    }

    async fn set_drain_phase(
        &self,
        node_id: NodeId,
        expected_phase: &str,
        new_phase: &str,
        last_error: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        // Only the drain-completion edge carries the residency guard; see
        // `set_drain_phase_query`. Every other production call passes
        // `expected == new` (cordon convergence and the terminating-phase error
        // writes), so none of them is this edge. `undrain` is not reachable here
        // at all — it writes NULL, and `new_phase` is a `&str`.
        let completing_drain = is_drain_completion_edge(expected_phase, new_phase);
        // The scope follows the flag, not just the edge. See [`GuardScope`]: a
        // wide guard here with the gate off would enforce the very term the flag
        // is meant to withhold, silently and without the WARN that explains it.
        let scope = match (completing_drain, self.termination_blocking_gate) {
            (false, _) => GuardScope::None,
            (true, false) => GuardScope::Resident,
            (true, true) => GuardScope::Wide,
        };
        let query = set_drain_phase_query(self.residency_evidence, scope);
        let result = if completing_drain {
            // Serialized against *one* interleaving of concurrent sandbox
            // attachment, and only one. The guard is a
            // `NOT EXISTS` over `sandboxes`, and under READ COMMITTED an UPDATE
            // that blocks on a row lock re-checks only the row it is updating —
            // Postgres documents that it "does not see effects of those commands
            // on other rows in the database". So the guard alone would still
            // miss a placement that commits while the statement runs, which is
            // the whole park this gate exists to prevent.
            //
            // Taking the `nodes` row lock *first*, in its own statement, fixes
            // that for the pod-placement writer without touching its path:
            // `kubernetes::store`'s placement transaction upserts this same
            // `nodes` row (`INSERT … ON CONFLICT (id) DO UPDATE`) *before* it
            // writes `sandboxes`, so it already holds this lock. Once the lock
            // is ours that transaction has committed, and the UPDATE below takes
            // a fresh statement snapshot including its rows.
            //
            // **The lock orders the two transactions; it does not decide the
            // outcome.** If this write wins the lock instead, its guard sees an
            // empty node, commits `drained`, and the blocked placement then
            // attaches a `started` sandbox to a `drained` node —
            // `persist_pod_node_placement` never re-checks `drain_phase`. So this
            // halves the window rather than closing it, and the paragraph below
            // about NULL `active_node_id` is a *second*, independent open case,
            // not the same one restated.
            //
            // Both routes into `terminating` must guard residency:
            // `mark_terminating_query` and the admin `POST :terminate` driven
            // by `scripts/space-rollout-mng.py`. Each checks active residents
            // and local snapshots with the same census policy, so a node
            // gaining a resident in either window parks instead of terminating.
            //
            // **Scoped to that one writer, deliberately narrowly.** There is no
            // FK from `sandboxes.active_node_id` to `nodes(id)`, so attaching a
            // sandbox takes no lock on the node row and nothing here can be
            // inferred from the write alone. The pooled post-wake, post-create
            // placement, pooled create, and direct-substrate restore writers all
            // set `active_node_id` without touching `nodes`, and stay
            // pre-check-only. What normally keeps them off a draining node is
            // the API drain guard (`drain_phase.is_some()`), which is itself an
            // unlocked read of this row and carries the same window — so it is
            // a mitigation, not a closure.
            //
            // The reverse order is **not** closed, and it does not need a lost
            // cordon to reach. A pod scheduled before the drain started keeps
            // running; cordon only stops new scheduling. Until its placement is
            // persisted the `sandboxes` row carries a NULL `active_node_id` and
            // no `target_node_id`, so it is invisible to every term of this
            // gate — the resident scan, this guard, the snapshot blockers, and
            // the in-flight evacuation set alike. The drain can complete and the
            // placement then attaches, with the same outcome as above: a park
            // at both routes into `terminating`, visible to an operator, rather
            // than a destroyed sandbox.
            //
            // That sequence predates this guard (the attach lands as `started`,
            // which the terminator's resident guard has always refused), and
            // closing it belongs to whichever writer can see a scheduled-but-
            // unattached pod. Counting `sandbox_k8s_executions` here is the
            // obvious move and the wrong one: historical execution rows can
            // outnumber real residents and hold drains open indefinitely.
            // Re-checking `drain_phase` in the attach path instead strands a
            // running pod with no sandbox row. Both need a separate design.
            // Two costs, and the second is not what the previous shape had.
            //
            // *Waiting*: bounded by the `lock_timeout` set below. `run_tick`
            // iterates draining nodes sequentially, so an unbounded wait here
            // would stall every node behind this one in the list; the timeout
            // turns that into a one-tick deferral for the contended node alone.
            //
            // *Holding*: wider than before, not by one round trip. A guarded
            // single statement evaluates its qual **before** taking the row
            // lock; this transaction takes the lock first and then holds it
            // across a client round trip, the guarded UPDATE — whose `NOT
            // EXISTS` walks the node's `active_node_id` rows and evaluates
            // census subqueries per row — and the COMMIT round trip.
            // Everything that writes this `nodes` row blocks for that
            // whole span: pod placement, the kube inventory reflector, the
            // node-bootstrap upsert, and the admin drain endpoints. Index-backed
            // and once per node at drain completion, so tens to low hundreds of
            // milliseconds rather than seconds — but not one round trip.
            let mut tx = self.pool.begin().await?;
            // Transaction-local bound on the lock wait, from a typed value
            // ([`DRAIN_COMPLETION_LOCK_TIMEOUT`]) rather than a SQL literal.
            // `sandbox_delete`'s bounded lock is the pattern, and its contract is
            // two halves — bound the wait *and* classify the resulting `55P03` as
            // retryable. The caller does the second via [`is_lock_timeout`].
            sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
                .bind(DRAIN_COMPLETION_LOCK_TIMEOUT.as_millis().to_string())
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
                .bind(node_id)
                .execute(&mut *tx)
                .await?;
            let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_id)
                .bind(new_phase)
                .bind(last_error)
                .bind(expected_phase)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            result
        } else {
            sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(node_id)
                .bind(new_phase)
                .bind(last_error)
                .bind(expected_phase)
                .execute(&self.pool)
                .await?
        };
        let updated = result.rows_affected() > 0;
        if updated && completing_drain {
            self.evict_promotion_node_api_client(node_id).await;
        }
        Ok(updated)
    }

    async fn delete_terminating_node(&self, node_id: NodeId) -> Result<bool, sqlx::Error> {
        let deleted = delete_terminating_node_and_record_tombstone(&self.pool, node_id).await?;
        if deleted {
            self.evict_promotion_node_api_client(node_id).await;
        }
        Ok(deleted)
    }

    /// Re-assert `spec.unschedulable=true` on the kube Node backing this
    /// SPACE node id. Idempotent: if the Node is already cordoned the
    /// patch is a no-op.
    ///
    /// **Why on every tick:** the admin `:drain` endpoint commits
    /// `drain_phase='draining'` in Phase 1 and patches `unschedulable`
    /// in Phase 2 as a best-effort; if Phase 2 fails it persists
    /// `drain_last_error` and returns 502 to the caller, relying on this
    /// reconciler to converge. Without this seam, a node can sit in
    /// `draining` while still admitting new sandbox pods, then advance
    /// to `drained` once incidental residents leave — a node we'd then
    /// terminate while it had fresh placements landing.
    ///
    /// **Identity guard:** we resolve `k8s_node_name` from `node_k8s`
    /// and verify the live kube Node's UID matches the UID stored in
    /// `node_k8s` *before* patching. Without this guard a same-name
    /// replacement Node that the inventory reflector hasn't yet
    /// observed would get cordoned, and after the next inventory tick
    /// clears `drain_phase` for the new identity nothing would
    /// uncordon it — fresh capacity stuck unschedulable indefinitely.
    async fn ensure_node_unschedulable(
        &self,
        node_id: NodeId,
    ) -> Result<CordonOutcome, CordonError> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(sql!(
            "SELECT k8s_node_name, k8s_node_uid FROM node_k8s WHERE node_id = $1"
        ))
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| CordonError::Db(err.to_string()))?;
        let (node_name, stored_uid) = row.ok_or(CordonError::Unregistered(node_id))?;

        // UID guard: fetch the current kube Node and verify its UID
        // matches the one we previously persisted. On mismatch we
        // defer — the inventory reflector will clear drain_phase
        // shortly. On 404 there is nothing to cordon (the Node was
        // already removed); surfaced as `KubeNodeGone` so the caller
        // can run the KubeNodeGone recovery rather than retrying
        // node-bound work that can never reach a deleted node.
        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.substrate.kubernetes().k8s.client.clone());
        match nodes.get(&node_name).await {
            Ok(node) => {
                if let (Some(stored), Some(current)) =
                    (stored_uid.as_deref(), node.metadata.uid.as_deref())
                    && stored != current
                {
                    return Err(CordonError::StaleIdentity);
                }
            }
            Err(kube::Error::Api(resp)) if resp.code == 404 => {
                return Ok(CordonOutcome::KubeNodeGone);
            }
            Err(err) => return Err(CordonError::Substrate(err.to_string())),
        }

        self.substrate
            .kubernetes()
            .set_node_schedulability(
                NodeSchedulabilityRequest {
                    substrate_node_id: &node_name,
                },
                NodeSchedulability::Unschedulable,
            )
            .await
            .map_err(|err| CordonError::Substrate(err.to_string()))?;
        Ok(CordonOutcome::Cordoned)
    }

    async fn node_phase_counts(&self) -> Result<HashMap<String, u64>, sqlx::Error> {
        // Only non-NULL drain_phase values, since the metric is the
        // drain-phase distribution; NULL (non-draining nodes) is
        // implicit.
        let rows: Vec<(Option<String>, i64)> = sqlx::query_as(sql!(
            "SELECT drain_phase, COUNT(*) FROM nodes
             WHERE drain_phase IS NOT NULL
             GROUP BY drain_phase"
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(phase, n)| phase.map(|s| (s, n.max(0).cast_unsigned())))
            .collect())
    }

    async fn wait_for_restore_signal(&self) {
        // Backoff applied (outside the listener lock) whenever the listener
        // can't be established, drops, or errors, so the run loop's `select!`
        // doesn't busy-spin on this branch. The periodic tick is the
        // correctness fallback, so returning early on error is safe — the
        // caller just runs a (cheap, idempotent) sweep.
        const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

        let mut guard = self.restore_listener.lock().await;
        if guard.is_none() {
            match connect_restore_listener(&self.pool).await {
                Ok(listener) => *guard = Some(listener),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        channel = EVACUATION_RESTORE_READY_CHANNEL,
                        "node_drain: failed to establish snapshot-ready listener; \
                         falling back to periodic sweep"
                    );
                    drop(guard);
                    tokio::time::sleep(RECONNECT_BACKOFF).await;
                    return;
                }
            }
        }

        // Safe: just established or was already `Some`.
        //
        // `try_recv` (not `recv`) so a dropped connection surfaces here as
        // `Ok(None)` instead of `recv` silently looping onto a sqlx-reconnected,
        // *unhardened* backend (see `connect_restore_listener` — reconnect
        // replays only the LISTEN, not the keepalive `SET`s). On any drop we
        // reset the listener so the next call reconnects through
        // `connect_restore_listener`, re-applying the hardening.
        let listener = guard.as_mut().expect("restore listener present");
        match listener.try_recv().await {
            Ok(Some(_)) => {
                // A snapshot-ready notification arrived — return so the run
                // loop drives the restore sweep. The payload (sandbox id) is
                // advisory; the sweep re-derives the full restore-pending set.
            }
            Ok(None) => {
                // Connection lost (eager_reconnect is off, so sqlx did not
                // silently re-establish). Drop the listener so we reconnect
                // through the hardened path; the periodic tick covers any signal
                // missed in the gap.
                *guard = None;
                drop(guard);
                tokio::time::sleep(RECONNECT_BACKOFF).await;
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "node_drain: snapshot-ready listener recv failed; resetting listener"
                );
                *guard = None;
                drop(guard);
                tokio::time::sleep(RECONNECT_BACKOFF).await;
            }
        }
    }
}

/// TCP keepalive + `tcp_user_timeout` GUCs applied to the dedicated
/// snapshot-ready `LISTEN` session so Postgres reaps a dead or unreachable
/// cluster-controller promptly instead of letting its backend linger for hours.
///
/// This listener connection is detached from the pool and then parks in
/// `recv()`, so it is exempt from the pool's idle-timeout recycling. If this
/// process dies ungracefully (leaving a half-open TCP connection) or the
/// backend wedges in `ClientWrite` because we stopped reading it, Postgres
/// would otherwise keep that backend — and the snapshot it holds — alive until
/// its own multi-hour TCP timeout. A backend held that long pins the cluster's
/// xmin horizon, which starves autovacuum on the high-churn
/// `node_reconcile_queue` / `node_reconcile_triggers` tables until they bloat
/// enough that sequential scans over the dead tuples dominate RDS CPU.
/// `tcp_user_timeout` bounds how long unacknowledged sends wait before the
/// connection is dropped, and the keepalive probes detect a silently-gone peer,
/// so the backend (and its xmin) is released within ~1 minute.
const LISTENER_KEEPALIVE_SQL: &str = sql!(
    "SET tcp_keepalives_idle = 30; \
     SET tcp_keepalives_interval = 10; \
     SET tcp_keepalives_count = 3; \
     SET tcp_user_timeout = 30000"
);

/// Establish the dedicated `EVACUATION_RESTORE_READY_CHANNEL` listener, apply
/// the keepalive hardening above, then start listening. Kept as a free function
/// (rather than inlined in `wait_for_restore_signal`) so the connection
/// hardening is unit-testable without constructing the full substrate.
///
/// `eager_reconnect` is disabled deliberately: sqlx's transparent reconnect
/// replays only the tracked `LISTEN` channels, not the ad-hoc session `SET`s
/// above, so a silent reconnect would park the listener on a fresh *unhardened*
/// backend and re-expose the xmin-pinning defect. With it off, a dropped
/// connection surfaces to the caller as `try_recv() -> Ok(None)`;
/// `wait_for_restore_signal` then resets and reconnects through this function,
/// re-applying the hardening on every reconnect.
async fn connect_restore_listener(
    pool: &PgPool,
) -> Result<sqlx::postgres::PgListener, sqlx::Error> {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    listener.eager_reconnect(false);
    sqlx::raw_sql(LISTENER_KEEPALIVE_SQL)
        .execute(&mut listener)
        .await?;
    listener.listen(EVACUATION_RESTORE_READY_CHANNEL).await?;
    Ok(listener)
}

/// Delete a k8s node row in `drain_phase = 'terminating'` and record a
/// tombstone in the same transaction.
///
/// The tombstone outlives the `node_k8s` CASCADE so the
/// `bootstrap_k8s_origin_node` path can refuse heartbeats from a daemon
/// running on the same physical instance during AWS `shutting-down` (the
/// `m5d.metal` shutdown is slow and the daemon keeps publishing) as well
/// as a recycled-IP fresh EC2 that joins under the same hostname before
/// an operator clears the tombstone.
pub(crate) async fn delete_terminating_node_and_record_tombstone(
    pool: &PgPool,
    node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let snapshot: Option<(String, Option<String>)> = sqlx::query_as(sql!(
        "SELECT n.role, nk.k8s_node_name
         FROM nodes n
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE n.id = $1
           AND n.drain_phase = 'terminating'
           AND n.managed_by = $2"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((role, k8s_node_name)) = snapshot else {
        return Ok(false);
    };
    let result = sqlx::query(sql!(
        "DELETE FROM nodes
         WHERE id = $1
           AND drain_phase = 'terminating'
           AND managed_by = $2"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(&mut *tx)
    .await?;
    if result.rows_affected() == 0 {
        return Ok(false);
    }
    sqlx::query(sql!(
        "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role, reason)
         VALUES ($1, $2, $3, 'admin-terminate')
         ON CONFLICT (node_id) DO UPDATE
         SET terminated_at = NOW(),
             k8s_node_name = EXCLUDED.k8s_node_name,
             role          = EXCLUDED.role,
             reason        = EXCLUDED.reason"
    ))
    .bind(node_id)
    .bind(k8s_node_name.unwrap_or_default())
    .bind(&role)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod lifecycle_writer_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tombstone_tests;

/// Postgres-backed regression tests for the durable-upload drain gate: a node
/// must not be reclaimed (nor its sandbox marked `failed`) while a
/// suspend-snapshot upload is still in flight *from* that node
/// (`snapshots.state='creating'`, `source_node_id = node`). The race these
/// pin: a two-MNG-rollout smoke flipped two sandboxes to `failed` ~330 ms
/// before their durable uploads finalized `ready`, then drained and
/// terminated the node, stranding both.
#[cfg(test)]
mod durable_upload_gate_tests;

/// Scale regression for the pinned local-only work queue: the shape of the
/// statement, not the rows it returns. `durable_upload_gate_tests` covers the
/// rows; this covers how the work of finding them grows with a node's suspend
/// history, so large histories cannot make a forced drain time out.
#[cfg(test)]
mod pinned_local_only_scale_tests;

/// Postgres-backed coverage for how the reconciler reads and re-checks
/// `nodes.drain_mode`: the draining listing resolves NULL and unrecognized
/// values to gentle rather than dropping the row, and the evacuation claim
/// re-asserts the node's phase and mode inside its own UPDATE so a mid-tick
/// mode change cannot be acted on.
#[cfg(test)]
mod drain_mode_tests;

/// Postgres-backed coverage for the node-census residency discount: which
/// rows stop counting against a draining node, and — the half that matters —
/// which keep counting when the node's own report cannot be trusted.
#[cfg(test)]
mod residency_tests;

/// Postgres-backed coverage for the gate term that keeps a node in `draining`
/// when the drained-node terminator's resident guard would refuse it. Runs both
/// gates over one fixture, since the claim is that they agree — not merely that
/// each answers something plausible.
#[cfg(test)]
mod termination_gate_tests;

/// Postgres-backed coverage for evacuating a `stopped` resident: the claim the
/// marker-clearing trigger must not eat, the durability gate that authorizes
/// destroying the user's rootfs, and the re-placement sweep's second arm.
#[cfg(test)]
mod stopped_evacuation_tests;
