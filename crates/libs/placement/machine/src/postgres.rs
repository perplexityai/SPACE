//! The Postgres [`PlacementStore`] over the current schema columns.
//!
//! Every operation is one transaction whose precondition is re-checked inside
//! the mutating statement's own WHERE — a raced row matches zero rows and
//! nothing is written. Guard clauses are adapted from the legacy writers each
//! operation absorbs at cutover; each statement cites its source.

use std::sync::LazyLock;
use std::time::Duration;

use jiff::Timestamp;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, SandboxId, SandboxLifecycle, SandboxLifecycleParseError,
};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::lifecycle::StoredSandboxLifecycle;
use crate::model::{ArtifactRef, Attempt, ClaimOrigin, LeaseToken, PlacementFence, PlacementProof};
use crate::outcome::{
    AbandonOutcome, BindOutcome, BlockedReason, ClaimKind, ClaimOutcome, Created, DeleteOutcome,
    EvacuationOutcome, LossClassification, NodeLossOutcome, OverrideOutcome, PlacementOutcome,
    QuarantineOutcome, SuspendOutcome,
};
use crate::request::{
    AdminOverride, CreateSandbox, EvacuationStep, NodeIncarnation, PodEvidence, QuarantineHold,
};
use crate::store::{PlacementError, PlacementEvidence, PlacementStore};

/// Lease TTL for a claim: the node-RPC timeout, so a live restore cannot be
/// re-driven underneath its own in-flight node request. The same
/// `restore_ownership` constant the api substrate reads — a timeout bump
/// moves both or neither.
const RECOVERY_LEASE_TTL: Duration = pplx_space_common_core::restore_ownership::NODE_RPC_TIMEOUT;

/// How long a fresh active execution keeps ownership past the lease deadline,
/// so an already-issued Kubernetes/worker request cannot overlap a
/// replacement owner. `restore_ownership` carries the compile assert that
/// this outlives the RPC timeout (and with it the lease).
const RESTORE_EXECUTION_OWNERSHIP_TTL: Duration =
    pplx_space_common_core::restore_ownership::RESTORE_EXECUTION_OWNERSHIP_TTL;

pub(crate) fn recovery_lease_ttl_secs() -> i64 {
    i64::try_from(RECOVERY_LEASE_TTL.as_secs()).expect("recovery lease TTL seconds fit i64")
}

pub(crate) fn restore_execution_ownership_ttl_secs() -> i64 {
    i64::try_from(RESTORE_EXECUTION_OWNERSHIP_TTL.as_secs())
        .expect("restore execution ownership TTL seconds fit i64")
}

/// Lifecycles a placement completion must never overwrite: teardown/terminal
/// states applied while substrate work was in flight. Byte-parity with the
/// post-wake/post-restore CAS list in the api's `lifecycle.rs`; a render test
/// pins the SQL form.
pub(crate) const PLACEMENT_COMPLETION_BLOCKING_LIFECYCLES: [StoredSandboxLifecycle; 7] = [
    StoredSandboxLifecycle::Live(SandboxLifecycle::Failed),
    StoredSandboxLifecycle::Live(SandboxLifecycle::Quarantined),
    StoredSandboxLifecycle::Live(SandboxLifecycle::Stopped),
    StoredSandboxLifecycle::Live(SandboxLifecycle::Stopping),
    StoredSandboxLifecycle::Live(SandboxLifecycle::Deleting),
    StoredSandboxLifecycle::Deleted,
    StoredSandboxLifecycle::CleanupRequired,
];

/// Lifecycles that *supersede* a placement completion: a newer suspend claim owns
/// the row. Kept separate from the blocking set because the disposition
/// differs — blocked is a loss to surface, superseded is a handoff
/// ([`PlacementOutcome::SupersededBySuspend`]): the node work succeeded, but the
/// stale completion must not replace `suspending` with `started` and break
/// the suspend commit's generation fence.
pub(crate) const PLACEMENT_SUPERSEDING_SUSPEND_LIFECYCLES: [StoredSandboxLifecycle; 2] = [
    StoredSandboxLifecycle::Live(SandboxLifecycle::Suspending),
    StoredSandboxLifecycle::Live(SandboxLifecycle::Suspended),
];

fn lifecycles_sql_list(lifecycles: &[StoredSandboxLifecycle]) -> String {
    lifecycles
        .iter()
        .map(|lifecycle| format!("'{}'", lifecycle.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One-time render of the blocking set as a SQL `IN` list.
pub(crate) static BLOCKING_LIFECYCLES_SQL_LIST: LazyLock<String> =
    LazyLock::new(|| lifecycles_sql_list(&PLACEMENT_COMPLETION_BLOCKING_LIFECYCLES));

/// One-time render of the superseding set as a SQL `IN` list.
pub(crate) static SUPERSEDING_LIFECYCLES_SQL_LIST: LazyLock<String> =
    LazyLock::new(|| lifecycles_sql_list(&PLACEMENT_SUPERSEDING_SUSPEND_LIFECYCLES));

/// The one completion statement both [`PlacementProof`] arms render: the
/// shared finalization SET plus the worker-scoped `placed` join. Only the
/// proof-specific pieces are parameters — the generation pin inside `placed`
/// (the fenced arm pins the fence's generation; the lapsed adoption reads
/// the active execution) and the ownership predicate the proof verifies —
/// so the two arms cannot drift.
fn completion_sql(generation_pin: Option<&str>, ownership_predicate: &str) -> String {
    let generation_pin = generation_pin
        .map(|predicate| format!("\n                               AND {predicate}"))
        .unwrap_or_default();
    format!(
        "UPDATE sandboxes
                        SET lifecycle = COALESCE(sandboxes.evacuation_restore_target, 'started'),
                            placement = 'on_node',
                            evacuation_restore_target = NULL,
                            restore_lease_token = NULL,
                            restore_lease_expires_at = NULL,
                            active_node_id = placed.node_id,
                            target_node_id = placed.node_id,
                            placement_changed_at = NOW()
                       FROM (
                            SELECT e.sandbox_id, k.node_id
                              FROM sandbox_k8s_executions e
                              JOIN node_k8s k ON k.k8s_node_name = e.k8s_node_name
                              JOIN nodes n ON n.id = k.node_id AND n.role = 'worker'
                             WHERE e.sandbox_id = $1
                               AND e.active{generation_pin}
                               AND e.state = 'running'
                               AND e.pod_ip IS NOT NULL
                       ) placed
                      WHERE sandboxes.id = placed.sandbox_id
                        AND sandboxes.evacuation_restore_target IS DISTINCT FROM 'stopped'
                        AND {ownership_predicate}"
    )
}

/// User-path claim: `suspended -> restoring`, minting the lease half of the
/// fence. Candidate WHERE byte-identical to the user branch of the api
/// substrate's `begin_restore_lifecycle`; each guard is commented in the SQL.
/// A marked `suspended` row is a clean user takeover — the claim clears the
/// marker, so the drain's marker-required claim then loses.
const USER_RESTORE_CLAIM_SQL: &str = include_str!("postgres/sql/user_restore_claim.sql");

/// Evacuation-path claim, atomic on the node-drain `evacuation_restore_target`
/// marker: `suspended` (or a released stopped evacuation's `stopped`) ->
/// `restoring` only while the marker is set. Adapted from
/// the marker-required branch of `begin_restore_lifecycle`; guards commented
/// in the SQL.
///
/// Deliberate delta from the legacy (tokenless) writer: every claim kind
/// mints the same fence, and that one lease is what resolves the
/// evacuation-versus-user-wake race to exactly one owner. Minting forces a
/// second delta — the claim takes a marked row only once its lease has
/// lapsed, because an ungated re-drive would replace a live owner's token
/// and cost that owner its completion.
const EVACUATION_RESTORE_CLAIM_SQL: &str =
    include_str!("postgres/sql/evacuation_restore_claim.sql");

/// Post-miss snapshot of the restore gate, read to classify a claim that
/// matched zero rows. Adapted from `fetch_restore_ownership_snapshot` in the
/// api substrate store.
const CLAIM_MISS_SNAPSHOT_SQL: &str = include_str!("postgres/sql/claim_miss_snapshot.sql");

/// The claimed row's fence ingredients, as returned by the claim statements:
/// the minted lease deadline, the attempt count, the prior target node, and
/// the candidate's claimed-from lifecycle (the fresh-versus-redrive origin).
type ClaimedRow = (i64, i32, Option<Uuid>, String);

/// The only production [`PlacementStore`]: guarded transactions over the
/// current cluster Postgres schema. `namespace` is stamped onto execution
/// rows this store allocates, mirroring the api substrate's pod namespace.
#[derive(Clone)]
pub struct PostgresPlacementStore {
    pool: PgPool,
    namespace: String,
}

impl PostgresPlacementStore {
    pub fn new(pool: PgPool, namespace: String) -> Self {
        Self { pool, namespace }
    }

    /// Ensure an active execution row exists and return its generation — the
    /// generation half of the fence. Adapted from the api substrate's
    /// `ensure_active_execution_claim_in`: reuse the active row, else insert
    /// `MAX(generation) + 1` as `creating`. Callers hold the claim's
    /// `FOR UPDATE` sandbox lock, which serializes allocation.
    async fn ensure_active_execution_generation(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox: SandboxId,
    ) -> Result<ExecutionGeneration, PlacementError> {
        let existing = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox)
        .fetch_optional(&mut **tx)
        .await?;

        let generation = match existing {
            Some(generation) => generation,
            None => {
                let generation = sqlx::query_scalar::<_, i64>(sql!(
                    "SELECT COALESCE(MAX(generation), 0) + 1
                     FROM sandbox_k8s_executions
                     WHERE sandbox_id = $1"
                ))
                .bind(sandbox)
                .fetch_one(&mut **tx)
                .await?;
                let pod_name = format!("space-sbx-{}-g{}", sandbox.as_uuid().simple(), generation);
                let inserted = sqlx::query_scalar::<_, i64>(sql!(
                    "INSERT INTO sandbox_k8s_executions (
                         sandbox_id, generation, pod_name, pod_namespace, state, active
                     )
                     VALUES ($1, $2, $3, $4, 'creating', TRUE)
                     ON CONFLICT DO NOTHING
                     RETURNING generation"
                ))
                .bind(sandbox)
                .bind(generation)
                .bind(&pod_name)
                .bind(&self.namespace)
                .fetch_optional(&mut **tx)
                .await?;
                match inserted {
                    Some(generation) => generation,
                    // Insert conflicted: another writer landed an active row
                    // despite the sandbox lock. Trust the winner's row; a
                    // missing one here is a broken invariant surfaced as the
                    // row-not-found database error.
                    None => {
                        sqlx::query_scalar::<_, i64>(sql!(
                            "SELECT generation
                             FROM sandbox_k8s_executions
                             WHERE sandbox_id = $1 AND active"
                        ))
                        .bind(sandbox)
                        .fetch_one(&mut **tx)
                        .await?
                    }
                }
            }
        };

        ExecutionGeneration::new(generation).map_err(|err| PlacementError::UndecodableRow {
            sandbox,
            column: "sandbox_k8s_executions.generation",
            value: generation.to_string(),
            reason: err.to_string(),
        })
    }

    /// Classify a claim whose guarded UPDATE matched zero rows. Inside the
    /// kind's claimable window, a miss means another owner holds the row —
    /// [`ClaimOutcome::AlreadyOwned`] with the ownership deadline. Outside
    /// it, nothing may place the sandbox — [`ClaimOutcome::Blocked`]. Two
    /// evacuation-only cases: a gone marker can never succeed again
    /// ([`BlockedReason::EvacuationSuperseded`], drop it), and the cohort
    /// backoff window reports [`ClaimOutcome::Backoff`]. Each kind's window
    /// mirrors its own claim statement's candidate WHERE.
    ///
    /// A missing row is [`PlacementError::SandboxNotFound`], deliberately
    /// diverging from [`Self::classify_completion_cas_miss`]'s CAS-loss
    /// mapping: the classifier must read the row to classify the miss, so a
    /// missing row means the caller named a sandbox that does not exist.
    async fn classify_missed_claim(
        &self,
        sandbox: SandboxId,
        kind: ClaimKind,
    ) -> Result<ClaimOutcome, PlacementError> {
        let snapshot = sqlx::query_as::<_, (String, i64, i64, Option<i64>, bool, bool, bool)>(
            sqlx::AssertSqlSafe(sql!(CLAIM_MISS_SNAPSHOT_SQL)),
        )
        .bind(sandbox)
        .bind(restore_execution_ownership_ttl_secs())
        .fetch_optional(&self.pool)
        .await?;
        let Some((
            lifecycle_raw,
            lease_expires_at_ms,
            execution_owned_until_ms,
            backoff_until_ms,
            marker_present,
            stopped_marker,
            resident,
        )) = snapshot
        else {
            return Err(PlacementError::SandboxNotFound(sandbox));
        };
        let lifecycle = parse_stored_lifecycle(sandbox, &lifecycle_raw)?;

        // A marked `restoring` row is drain-owned: no user re-claim can
        // succeed until the drain finalizes or abandons, so this is Blocked,
        // not a deadline inviting a hopeless retry.
        if kind == ClaimKind::UserRestore
            && lifecycle.live() == Some(SandboxLifecycle::Restoring)
            && marker_present
        {
            return Ok(ClaimOutcome::Blocked(BlockedReason::Lifecycle(lifecycle)));
        }

        let owned_until_ms = match lifecycle.live() {
            // The deadline mirrors the missed kind's own gates: the user
            // claim also defers to a fresh active execution; the evacuation
            // claim gates on the lease alone, and reporting the longer
            // window would tell a re-drive to wait past its eligibility.
            Some(SandboxLifecycle::Restoring | SandboxLifecycle::Suspended) => {
                if kind == ClaimKind::EvacuationRestore {
                    lease_expires_at_ms
                } else {
                    lease_expires_at_ms.max(execution_owned_until_ms)
                }
            }
            // `stopped` is in the evacuation claim's window only. A markerless
            // row reaches the superseded check below; a stopped evacuation's
            // own marker on a row still on its node is a teardown in progress.
            Some(SandboxLifecycle::Stopped) if kind == ClaimKind::EvacuationRestore => {
                match (marker_present, stopped_marker, resident) {
                    (true, true, true) => {
                        return Ok(ClaimOutcome::Blocked(
                            BlockedReason::StoppedEvacuationNotReleased,
                        ));
                    }
                    (true, false, _) => {
                        return Ok(ClaimOutcome::Blocked(BlockedReason::Lifecycle(lifecycle)));
                    }
                    _ => {}
                }
                lease_expires_at_ms
            }
            _ => return Ok(ClaimOutcome::Blocked(BlockedReason::Lifecycle(lifecycle))),
        };
        // Once the marker is gone no evacuation re-claim can ever succeed;
        // a deadline here would be a hot loop.
        if kind == ClaimKind::EvacuationRestore && !marker_present {
            return Ok(ClaimOutcome::Blocked(BlockedReason::EvacuationSuperseded));
        }
        // Only the evacuation claim gates on the restore backoff (user
        // demand outranks cohort throttling); the later deadline binds.
        if kind == ClaimKind::EvacuationRestore
            && let Some(backoff_until_ms) = backoff_until_ms
            && backoff_until_ms > owned_until_ms
        {
            let next_attempt_at =
                timestamp_from_ms(sandbox, "next_restore_attempt_at", backoff_until_ms)?;
            return Ok(ClaimOutcome::Backoff { next_attempt_at });
        }
        let lease_expires_at =
            timestamp_from_ms(sandbox, "restore_lease_expires_at", owned_until_ms)?;
        Ok(ClaimOutcome::AlreadyOwned { lease_expires_at })
    }

    fn claimed_attempt(
        sandbox: SandboxId,
        lease: LeaseToken,
        generation: ExecutionGeneration,
        row: ClaimedRow,
    ) -> Result<Attempt, PlacementError> {
        let (lease_expires_at_ms, attempt_count, target_node_id, claimed_from_raw) = row;
        let lease_expires_at =
            timestamp_from_ms(sandbox, "restore_lease_expires_at", lease_expires_at_ms)?;
        let attempt =
            u32::try_from(attempt_count).map_err(|err| PlacementError::UndecodableRow {
                sandbox,
                column: "restore_attempt_count",
                value: attempt_count.to_string(),
                reason: err.to_string(),
            })?;
        // The candidate WHEREs admit exactly these three shapes (`stopped`
        // through the evacuation claim only), so anything else decoding here
        // means a guard and this vocabulary have drifted.
        let origin = match parse_stored_lifecycle(sandbox, claimed_from_raw.trim())?.live() {
            Some(SandboxLifecycle::Suspended) => ClaimOrigin::FreshFromSuspended,
            Some(SandboxLifecycle::Stopped) => ClaimOrigin::FreshFromStopped,
            Some(SandboxLifecycle::Restoring) => ClaimOrigin::RedroveRestoring,
            _ => {
                return Err(PlacementError::UndecodableRow {
                    sandbox,
                    column: "lifecycle",
                    value: claimed_from_raw,
                    reason: "claimed-from lifecycle outside the claimable window".to_string(),
                });
            }
        };
        Ok(Attempt {
            target_node: target_node_id.map(NodeId::from_uuid),
            fence: PlacementFence {
                sandbox,
                generation,
                lease,
            },
            lease_expires_at,
            attempt,
            origin,
        })
    }

    /// Classify a placement-completion CAS miss: a newer suspend owner is a
    /// handoff, everything else is a loss to surface. The read is advisory —
    /// the CAS already refused to write — so a race here can only change the
    /// report, never fabricate a write.
    ///
    /// A missing row maps to [`PlacementOutcome::CasLost`], deliberately
    /// diverging from [`Self::classify_missed_claim`]'s not-found error:
    /// this read runs after a refused write, and a row that vanished lost
    /// the race to a hard delete — which *is* a CAS loss to report, not a
    /// caller naming error.
    async fn classify_completion_cas_miss(
        &self,
        sandbox: SandboxId,
    ) -> Result<PlacementOutcome, PlacementError> {
        let sql = sql!(format!(
            "SELECT LOWER(TRIM(lifecycle)) IN ({superseding})
               FROM sandboxes
              WHERE id = $1",
            superseding = *SUPERSEDING_LIFECYCLES_SQL_LIST
        ));
        let superseded = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql))
            .bind(sandbox)
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(false);
        Ok(if superseded {
            PlacementOutcome::SupersededBySuspend
        } else {
            PlacementOutcome::CasLost
        })
    }
}

impl PlacementStore for PostgresPlacementStore {
    async fn claim_placement(
        &self,
        sandbox: SandboxId,
        kind: ClaimKind,
    ) -> Result<ClaimOutcome, PlacementError> {
        // The user claim's guard takes the fresh-execution ownership window
        // as an extra parameter; the evacuation claim's marker guard does not.
        // Each arm applies its own `sql!` so the two claim kinds carry
        // distinct locator comments in pg_stat_activity and DBM samples.
        let (claim_sql, ownership_ttl_secs) = match kind {
            ClaimKind::UserRestore => (
                sql!(USER_RESTORE_CLAIM_SQL),
                Some(restore_execution_ownership_ttl_secs()),
            ),
            ClaimKind::EvacuationRestore => (sql!(EVACUATION_RESTORE_CLAIM_SQL), None),
            ClaimKind::OnDemandCreate | ClaimKind::PooledClaim => {
                return Err(PlacementError::UnfencedClaimKind(kind));
            }
        };

        let lease = LeaseToken::mint();
        let mut tx = self.pool.begin().await?;
        let mut claim = sqlx::query_as::<_, ClaimedRow>(sqlx::AssertSqlSafe(claim_sql))
            .bind(sandbox)
            .bind(recovery_lease_ttl_secs())
            .bind(lease.as_uuid());
        if let Some(ownership_ttl_secs) = ownership_ttl_secs {
            claim = claim.bind(ownership_ttl_secs);
        }
        let claimed = claim.fetch_optional(&mut *tx).await?;

        let Some(row) = claimed else {
            // Nothing was written; release the row lock before the
            // classification snapshot reads.
            tx.rollback().await?;
            return self.classify_missed_claim(sandbox, kind).await;
        };

        let generation = self
            .ensure_active_execution_generation(&mut tx, sandbox)
            .await?;
        // Decode inside the transaction: a boundary failure (an undecodable
        // returned value, e.g. a negative attempt count — the column has no
        // non-negative CHECK) must roll the minted lease/lifecycle/execution
        // writes back, not stamp a live owner while the caller sees an error.
        let attempt = Self::claimed_attempt(sandbox, lease, generation, row)?;
        tx.commit().await?;

        Ok(ClaimOutcome::Claimed(attempt))
    }

    /// The one placement-completion CAS. The blocking set is byte-identical to
    /// the api's post-wake/post-restore CAS; a miss against a suspend owner
    /// is [`PlacementOutcome::SupersededBySuspend`] (handoff), everything else a
    /// loss. Each arm verifies its proof's guards inside the UPDATE's own
    /// WHERE ([`PlacementProof`] docs) and consumes the lease pair in its
    /// own SET — a consumed proof must never survive its completion, and the
    /// explicit clear keeps the CAS correct under either definition of the
    /// lease-clear trigger's window: today's trigger clears on any write
    /// that leaves the row non-`restoring` (an evacuation finalizing into
    /// `paused` included), while the widened claim-window trigger keeps the
    /// pair on `restoring`/`creating`/`paused` rows — under which that same
    /// finalize stays inside the window and the trigger would *not* clear.
    ///
    /// Ownership is not evidence: whichever proof is presented, the
    /// completing execution row must record routable evidence (`running` +
    /// pod IP) at flip time, or the flip would expose a `started` row with
    /// nothing routable behind it — the connect-wedge shape. What the CAS
    /// proves is that the row records that evidence, not that the pod is
    /// live: the evidence is as fresh as the substrate's last write, and
    /// there is deliberately no `updated_at` freshness bound on it — the
    /// lapsed-claim arm exists to converge attempts up to a full lease TTL
    /// plus a publish cadence after the evidence was last touched, and a
    /// freshness TTL would refuse exactly those completions. The fenced arm
    /// pins the fence's generation; the adoption reads the active one.
    ///
    /// Completion is also the placement write: the CAS derives the SPACE
    /// node from the execution's pod through the worker-scoped `node_k8s`
    /// join and stamps `active_node_id`/`target_node_id`. The role scope
    /// matters — one Kubernetes host registers worker *and* builder rows
    /// under the same `k8s_node_name`, and a name-only lookup could stamp
    /// the builder sibling. The join being inside the CAS means an
    /// execution on an unregistered or non-worker node cannot complete.
    ///
    /// Evacuation completions are lifecycle-preserving: the completion
    /// stamps back the `evacuation_restore_target` marker and consumes it.
    /// The user claim cleared the marker, so a user completion finalizes
    /// `started`. A stopped evacuation (marker `'stopped'`) can't complete
    /// here: its re-placement has no finalization yet, and stamping the
    /// marker back over a `running` execution would leave a running pod on a
    /// `stopped` sandbox. The CAS excludes the marker, so such an attempt
    /// writes nothing and reports [`PlacementOutcome::CasLost`].
    async fn complete_placement(
        &self,
        proof: PlacementProof<'_>,
    ) -> Result<PlacementOutcome, PlacementError> {
        let (sandbox, result) = match proof {
            PlacementProof::Fence(fence) => {
                let sandbox = fence.sandbox;
                let ownership = format!(
                    "LOWER(TRIM(sandboxes.lifecycle)) NOT IN ({blocking})
                        AND LOWER(TRIM(sandboxes.lifecycle)) NOT IN ({superseding})
                        AND sandboxes.restore_lease_token = $2",
                    blocking = *BLOCKING_LIFECYCLES_SQL_LIST,
                    superseding = *SUPERSEDING_LIFECYCLES_SQL_LIST
                );
                let sql = sql!(completion_sql(Some("e.generation = $3"), &ownership));
                let result = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(sandbox)
                    .bind(fence.lease.as_uuid())
                    .bind(fence.generation.get())
                    .execute(&self.pool)
                    .await?;
                (sandbox, result)
            }
            // The adopted token must not survive completion for a later
            // caller to adopt again.
            PlacementProof::LapsedClaim { sandbox } => {
                let sql = sql!(completion_sql(
                    None,
                    "LOWER(TRIM(sandboxes.lifecycle)) = 'restoring'
                        AND sandboxes.restore_lease_token IS NOT NULL
                        AND sandboxes.restore_lease_expires_at IS NOT NULL
                        AND sandboxes.restore_lease_expires_at <= NOW()"
                ));
                let result = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(sandbox)
                    .execute(&self.pool)
                    .await?;
                (sandbox, result)
            }
        };
        if result.rows_affected() == 0 {
            return self.classify_completion_cas_miss(sandbox).await;
        }
        Ok(PlacementOutcome::Completed)
    }

    // Everything below is declared ahead of its port and refuses with the
    // typed error until its port PR implements it.

    async fn create(&self, _req: CreateSandbox) -> Result<Created, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "create",
        })
    }

    async fn begin_suspend(
        &self,
        _sandbox: SandboxId,
        _generation: ExecutionGeneration,
    ) -> Result<SuspendOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "begin_suspend",
        })
    }

    async fn complete_suspend(
        &self,
        _sandbox: SandboxId,
        _generation: ExecutionGeneration,
        _artifact: ArtifactRef,
    ) -> Result<SuspendOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "complete_suspend",
        })
    }

    async fn abandon(
        &self,
        _fence: &PlacementFence,
        _loss: LossClassification,
    ) -> Result<AbandonOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "abandon",
        })
    }

    async fn evacuate(
        &self,
        _sandbox: SandboxId,
        _step: EvacuationStep,
    ) -> Result<EvacuationOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "evacuate",
        })
    }

    async fn node_loss(
        &self,
        _node: NodeId,
        _observed_incarnation: NodeIncarnation,
    ) -> Result<Vec<Result<NodeLossOutcome, PlacementError>>, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "node_loss",
        })
    }

    async fn quarantine(
        &self,
        _sandbox: SandboxId,
        _hold: QuarantineHold,
    ) -> Result<QuarantineOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "quarantine",
        })
    }

    async fn finalize_delete(&self, _sandbox: SandboxId) -> Result<DeleteOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "finalize_delete",
        })
    }

    async fn admin_override(&self, _op: AdminOverride) -> Result<OverrideOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "admin_override",
        })
    }
}

impl PlacementEvidence for PostgresPlacementStore {
    async fn bind_pod(
        &self,
        _fence: &PlacementFence,
        _pod: PodEvidence,
    ) -> Result<BindOutcome, PlacementError> {
        Err(PlacementError::UnimplementedOperation {
            operation: "bind_pod",
        })
    }
}

fn parse_stored_lifecycle(
    sandbox: SandboxId,
    raw: &str,
) -> Result<StoredSandboxLifecycle, PlacementError> {
    raw.parse().map_err(
        |err: SandboxLifecycleParseError| PlacementError::UndecodableRow {
            sandbox,
            column: "lifecycle",
            value: raw.to_string(),
            reason: err.to_string(),
        },
    )
}

fn timestamp_from_ms(
    sandbox: SandboxId,
    column: &'static str,
    millis: i64,
) -> Result<Timestamp, PlacementError> {
    Timestamp::from_millisecond(millis).map_err(|err| PlacementError::UndecodableRow {
        sandbox,
        column,
        value: millis.to_string(),
        reason: err.to_string(),
    })
}

#[cfg(test)]
mod tests;
