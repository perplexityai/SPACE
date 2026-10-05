//! Service-side executor for the cold-sandbox eviction loop
//! (`crate::sandbox_eviction`). The loop module owns policy (which
//! sandbox, why); this file owns the privileged teardown, reusing the
//! exact machinery the Delete RPC drives so eviction cannot diverge from
//! normal deletion semantics.

use pplx_space_common_core::types::{LocalArtifactId, SandboxId};

use std::time::Duration;
use tonic::Status;

use super::cleanup_support::{CleanupCompletion, CleanupLockTryResult};

use super::cleanup::{CleanupAdmission, SynchronousCleanupRequest};
use super::*;
use crate::node_state_sync::mark_sandbox_dirty;

/// Local quiescence floor for cluster-deleted evictions. A row that
/// transitioned more recently than this is refused (`FailedPrecondition`) so
/// the eviction can never act on state the caller read mid-transition; the
/// next facet sync retries once the row has settled.
const CLUSTER_DELETED_EVICTION_QUIESCENCE_SECS: u64 = 600;

/// Which local row shape an eviction claims, and the identity it re-asserts
/// in the CAS.
#[derive(Debug, Clone)]
pub(crate) enum EvictionClaim {
    /// `suspended → deleting`, re-asserting the **proven identity**: the
    /// exact pinned artifact the eviction decision proved backed, or `None`
    /// asserting the row is still unpinned.
    SuspendedPinned(Option<LocalArtifactId>),
    /// `restoring → deleting` for a row a failed cross-node restore retained
    /// "for retry" (`failure_reason` persisted, state left `restoring`).
    /// Only meaningful once the cluster reports the sandbox deleted — a
    /// retry can never come — and only with `expected_updated_at`, so any
    /// writer advancing the row under the caller fails the claim.
    ///
    /// Safety does not rest on the marker meaning "no restore is live": a retry
    /// of a failed cross-node restore reuses the row without clearing
    /// `failure_reason`, so a live retry carries the stale marker for its whole
    /// duration. It rests on the cluster's absent-row report (a `deleting`
    /// cluster row is still present and is never reported), the 600 s
    /// quiescence on `updated_at`, and the `DeleteRow` cleanup taking the
    /// sandbox's staging lock, which serializes with an in-flight pull — worst
    /// case that pull's fenced phase advance fails with a lost transition and
    /// the cleanup cancels its scope. The marker only distinguishes the retained
    /// shape from a first-attempt restore that has not failed.
    FailedRestoring,
    /// `quarantined → deleting` only after the cluster reports the row absent.
    /// Quarantine preserves evidence for an existing sandbox; it must not
    /// prevent completion of a cluster deletion. The row-version CAS and
    /// staging lock prevent racing recovery, and DeleteRow owns full teardown
    /// even when the quarantined sandbox still has a resident runtime.
    Quarantined,
    /// `suspending → deleting` for a row whose teardown suspend staged its
    /// artifact locally and then had the durable backup permanently refused
    /// by the cluster
    /// ([`pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON`]).
    ///
    /// This shape exists because a sandbox deleted while one of its
    /// executions was still starting leaves no reachable runtime for the
    /// cluster's teardown to address, so no execution delete receipt ever
    /// reaches the node. When the activation then finishes, the pod teardown
    /// drives a durable teardown suspend whose backup the cluster refuses
    /// forever — there is no sandbox row left to hang a snapshot on. Nothing
    /// else reclaims the result: the periodic suspend-publication recovery
    /// skips a row whose artifact is already staged, and ordinary delete is
    /// refused while a suspend awaits publication.
    ///
    /// Unlike the other two shapes the runtime may still be resident here
    /// (the capture happens before the suspend releases it), so the
    /// `DeleteRow` cleanup this claim hands to does the full teardown rather
    /// than a bytes-only reclaim.
    ///
    /// That resident runtime is also why this shape measures time and
    /// identity differently from the other two. A `suspended` or `restoring`
    /// row has no resident runtime, so the net-gateway probe never reaches
    /// it, but it is not unwritten: the auto-lifecycle scheduler skips only
    /// `deleting` rows and can still record on these. Every such write moves
    /// `updated_at`, which can only restart the window or fail the claim —
    /// the eviction waits longer, never acts sooner — so both the quiescence
    /// window and the claim CAS can key on it there. A stranded `suspending`
    /// row is different in kind: its VM is still up and its auto-lifecycle
    /// policy keeps failing the same way, so
    /// `update_network_counters` (every 30 s, `MAX()`-guarded, so an idle
    /// sandbox's counters never move) and `update_auto_lifecycle_state`
    /// (every scheduler tick once a stage falls due and keeps failing the
    /// same way) re-stamp `updated_at` indefinitely with values identical to
    /// what is already stored. Keying on `updated_at` there does not make the
    /// eviction safer, it makes it impossible: the window never opens and the
    /// CAS can always lose. Both writes are made conditional in `db` so an
    /// unchanged row stops being touched at all, but correctness may not rest
    /// on having found every toucher.
    ///
    /// The window here is therefore time since the row entered `suspending`,
    /// not time since the backup was refused: stamping the marker does not
    /// move `state_changed_at`, so a refusal discovered after the window has
    /// already elapsed is eligible at once, and no post-failure wait is
    /// intended. The window only keeps the eviction off a suspend that began
    /// moments ago. Safety rests on the rest: the cluster's absent-row report;
    /// the terminal marker, written only once the backup of that exact staged
    /// artifact has failed permanently; a claim CAS that re-asserts the row's
    /// identity (state, the exact marker, the exact staged artifact, and the
    /// `state_changed_at` read) rather than its version, so a row that left
    /// `suspending` or re-staged in the meantime is refused; and the fenced
    /// `DeleteRow` cleanup the claim hands to.
    TeardownSuspendPermanentlyFailed {
        /// The artifact the refused backup staged, as read at decision time.
        /// Re-asserted by the claim: a re-stage means the row moved on.
        staged_artifact: LocalArtifactId,
        /// The row's `state_changed_at` witness, verbatim. Both the
        /// quiescence window and the claim CAS read it.
        state_changed_at: String,
    },
}

impl SandboxManagerService {
    /// Remove a sandbox's entire local presence: claim the row with the
    /// CAS `claim` describes (see [`EvictionClaim`]), then run the standard `DeleteRow` cleanup to
    /// completion (node-mgr removes the cache directory including the
    /// `instance/` subvolume; the SQLite rows cascade), and mark the
    /// sandbox dirty so the inventory facet sync reports it absent. The
    /// cluster-level `sandboxes` row is untouched: for a pinned-suspended
    /// claim the sandbox stays `suspended` and restorable from its remote
    /// snapshot; for a quarantined, failed-restoring or terminal-teardown-suspend
    /// claim the cluster row is already gone. The `DeleteRow` completion also
    /// records the execution delete receipt, which is what lets a stranded
    /// `PrepareTeardownSuspend` poller finally be answered `DELETED`.
    ///
    /// Errors:
    /// - `Aborted` — a failed-restoring or quarantined claim found a staging pull
    ///   live on the row (the per-sandbox staging lock was held); nothing
    ///   was changed and the caller should retry on its next pass rather
    ///   than back off.
    /// - `FailedPrecondition` — the row no longer matched the claim at
    ///   claim time: not `suspended` with the proven pin, not `restoring`
    ///   with a persisted failure or `quarantined` at the version read, or not `suspending`
    ///   with the terminal teardown-backup marker, the same staged artifact
    ///   and the same `state_changed_at` (a concurrent resume / re-suspend /
    ///   retry won the race); nothing was changed.
    /// - other statuses — the claim succeeded but cleanup failed; the
    ///   row stays in `deleting` and the cleanup machinery converges on
    ///   retry (the same contract a failed Delete RPC has).
    pub(crate) async fn evict_local_sandbox(
        &self,
        sandbox_id: &str,
        claim: EvictionClaim,
        expected_updated_at: Option<&str>,
    ) -> Result<(), Status> {
        let writer = self.state_writer_lease("EvictLocalSandbox")?;
        // Shutdown can cancel the waiter after cleanup persistence is submitted.
        // Scope admission so blocking storage retains ownership until it finishes.
        super::drain::with_state_writer(
            writer,
            self.evict_local_sandbox_owned(sandbox_id, claim, expected_updated_at),
        )
        .await
    }

    async fn evict_local_sandbox_owned(
        &self,
        sandbox_id: &str,
        claim: EvictionClaim,
        expected_updated_at: Option<&str>,
    ) -> Result<(), Status> {
        let CleanupLockTryResult::Acquired(fence) = self.try_cleanup_fence(sandbox_id) else {
            return Err(Status::unavailable(
                "sandbox cleanup is active; retry eviction",
            ));
        };
        let row = {
            let db = self.db_conn()?;
            crate::db::get_sandbox(&db, sandbox_id)
                .map_err(|e| Status::internal(format!("db sandbox lookup failed: {e}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?
        };

        // Exclude live staging for failed-restoring and quarantined rows.
        // For the failed-restoring shape: the marker only says
        // a staging attempt failed once, and a retry reuses the row without
        // clearing it. A retry holds the per-sandbox staging lock for its
        // whole pull (`recovery_cross_node`), and nothing writes the row
        // mid-pull, so the quiescence check alone can go stale under a
        // multi-GB transfer. Taking the lock non-blocking here proves no
        // pull is live at claim time. Refusal is `Aborted`, not
        // `FailedPrecondition`: node-ctl retries it on its next sync rather
        // than paying the flat cluster-deleted back-off, because a pull ends
        // on its own timescale. The guard is released right after the claim
        // commits — the `DeleteRow` cleanup below takes the same lock
        // blocking — and a retry cannot restart in that gap because the row
        // is already `deleting`.
        let staging_guard = match claim {
            EvictionClaim::FailedRestoring | EvictionClaim::Quarantined => Some(
                self.auto_lifecycle
                    .try_staging_guard(sandbox_id)
                    .ok_or_else(|| {
                        Status::aborted(format!(
                            "sandbox {sandbox_id} has a restore staging pull in flight; retry \
                             the cluster-deleted eviction on the next sync"
                        ))
                    })?,
            ),
            EvictionClaim::SuspendedPinned(_)
            | EvictionClaim::TeardownSuspendPermanentlyFailed { .. } => None,
        };
        let claimed = {
            let db = self.db_conn()?;
            match &claim {
                EvictionClaim::SuspendedPinned(proven_pin) => {
                    let pin = proven_pin.map(|id| id.to_string());
                    crate::db::claim_suspended_sandbox_for_eviction(
                        &db,
                        sandbox_id,
                        pin.as_deref(),
                        expected_updated_at,
                    )
                }
                EvictionClaim::FailedRestoring => {
                    let expected_updated_at = expected_updated_at.ok_or_else(|| {
                        Status::internal(
                            "a failed-restoring eviction claim requires the row version it \
                             was decided against",
                        )
                    })?;
                    crate::db::claim_failed_restoring_sandbox_for_eviction(
                        &db,
                        sandbox_id,
                        expected_updated_at,
                    )
                }
                EvictionClaim::Quarantined => {
                    let expected_updated_at = expected_updated_at.ok_or_else(|| {
                        Status::internal("a quarantined eviction claim requires the row version")
                    })?;
                    crate::db::claim_quarantined_sandbox_for_eviction(
                        &db,
                        sandbox_id,
                        expected_updated_at,
                    )
                }
                EvictionClaim::TeardownSuspendPermanentlyFailed {
                    staged_artifact,
                    state_changed_at,
                } => crate::db::claim_teardown_suspend_failed_sandbox_for_eviction(
                    &db,
                    sandbox_id,
                    *staged_artifact,
                    state_changed_at,
                ),
            }
            .map_err(|e| Status::internal(format!("eviction claim failed: {e}")))?
        };
        if !claimed {
            return Err(Status::failed_precondition(match &claim {
                EvictionClaim::SuspendedPinned(_) => format!(
                    "sandbox {sandbox_id} is no longer suspended with the proven artifact pinned"
                ),
                EvictionClaim::FailedRestoring => format!(
                    "sandbox {sandbox_id} is no longer a failed cross-node restore at the \
                     version the eviction was decided against"
                ),
                EvictionClaim::Quarantined => format!(
                    "sandbox {sandbox_id} is no longer quarantined at the version the eviction was decided against"
                ),
                EvictionClaim::TeardownSuspendPermanentlyFailed { .. } => format!(
                    "sandbox {sandbox_id} is no longer a terminally failed teardown suspend \
                     with the staged artifact the eviction was decided against"
                ),
            }));
        }
        // The row is `deleting` now; release the staging lock before the
        // cleanup re-acquires it.
        drop(staging_guard);

        self.watchdog.remove_sandbox(sandbox_id).await;
        self.snapshot_scheduler.remove_sandbox(sandbox_id).await;

        let (typed_id, uuid8) = Self::cleanup_identity(sandbox_id, Some(&row.uuid8))?;
        let admission = CleanupAdmission::persisted(
            typed_id,
            row.space_api_route_identity.execution_generation,
            None,
            fence,
        );
        self.run_admitted_cleanup_to_completion(
            SynchronousCleanupRequest {
                sandbox_id,
                uuid8: uuid8.as_str(),
                keep_filesystem: false,
                context: "eviction cleanup failed",
                completion: CleanupCompletion::DeleteRow,
                published_artifact: None,
                immediate_kill: Self::cleanup_immediate_kill(CleanupCompletion::DeleteRow),
                guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            },
            admission,
        )
        .await?;
        self.auto_lifecycle.remove_sandbox(sandbox_id);
        mark_sandbox_dirty(&self.db, sandbox_id);
        Ok(())
    }

    /// Evict the local presence of a sandbox the cluster authoritatively
    /// reports deleted (no `sandboxes` row at all — rows are hard-deleted
    /// on delete). Unlike the disk-pressure eviction loop this requires no
    /// backup proof: with the cluster row gone the sandbox can never be
    /// restored, so the local bytes are unreclaimable by any other path.
    /// Guards, in order:
    /// - the UDS peer must be in [`ALLOW_SANDBOX_CLUSTER_DELETED_EVICT`]
    ///   (node-ctl, root, or the local operator role — the callers that
    ///   can carry the authoritative cluster signal);
    /// - the local row must still be `quarantined`, `suspended`, or `restoring`
    ///   with a persisted cross-node staging failure — the row a failed cross-node
    ///   restore keeps on the target "for retry", which no other path
    ///   reclaims once the cluster row is gone — or `suspending` with the
    ///   permanently-refused-teardown-backup marker, the row a teardown
    ///   suspend leaves behind when the cluster can no longer accept its
    ///   backup. A `restoring` row without its marker is a first-attempt
    ///   restore and is refused, as is a `suspending` row without its own:
    ///   both are still-live shapes. Neither marker proves nothing is in
    ///   flight on its own — see [`EvictionClaim::FailedRestoring`] and
    ///   [`EvictionClaim::TeardownSuspendPermanentlyFailed`] for what
    ///   actually makes each teardown safe;
    /// - the owner must be a platform user (`user:<uuid>`): "no cluster
    ///   row" is only evidence of deletion for rows the cluster minted.
    ///   Local-provenance sandboxes (operator `space-sbxmgr create`,
    ///   template-manager base-snapshot builds, orphan-adopted rows) never
    ///   had one, and pool slots (`pool_template` set) get theirs only at
    ///   claim — all are refused;
    /// - it must have been quiescent for the configured window so the
    ///   eviction cannot race a transition in flight between the caller's
    ///   read and this call — read on `updated_at` for `suspended`, `restoring`
    ///   and `quarantined`, and on `state_changed_at` (time since entering
    ///   `suspending`, not since the backup was refused) for the terminal
    ///   teardown suspend, whose row is touched by live loops without
    ///   changing (see [`EvictionClaim::TeardownSuspendPermanentlyFailed`]);
    /// - the teardown CAS (`suspended → deleting` with the pinned-artifact
    ///   identity re-asserted, `restoring/quarantined → deleting` at the exact row
    ///   version read, or `suspending → deleting` with the marker, staged
    ///   artifact and `state_changed_at` re-asserted) closes the remaining
    ///   window.
    ///
    /// The teardown itself runs on a detached task: a caller that gives up
    /// (timeout, facet-budget cancellation) cannot strand a claimed row in
    /// `deleting` with its bytes half-removed — the task runs to its own
    /// completion and emits the terminal metrics/event either way. A crash
    /// mid-teardown converges through the existing startup recovery of
    /// `deleting` rows.
    pub(crate) async fn evict_cluster_deleted_sandbox(
        &self,
        request: Request<crate::proto::EvictClusterDeletedSandboxRequest>,
    ) -> Result<Response<crate::proto::EvictClusterDeletedSandboxResponse>, Status> {
        self.authorize_peer(
            &request,
            "EvictClusterDeletedSandbox",
            ALLOW_SANDBOX_CLUSTER_DELETED_EVICT,
        )?;
        let sandbox_id = request
            .into_inner()
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| {
                Status::invalid_argument(format!("sandbox_id is not a valid sandbox id: {error}"))
            })?
            .to_string();
        // Both reads come off the same connection back to back, and every
        // claim re-asserts what it decided against, so a row that moves
        // between them fails its CAS rather than being acted on.
        let (row, state_changed_at) = {
            let db = self.db_conn()?;
            let row = crate::db::get_sandbox(&db, &sandbox_id)
                .map_err(|e| Status::internal(format!("db sandbox lookup failed: {e}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?;
            let state_changed_at = crate::db::sandbox_state_changed_at_witness(&db, &sandbox_id)
                .map_err(|e| Status::internal(format!("db sandbox lookup failed: {e}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?;
            (row, state_changed_at)
        };
        let claim = match row.state {
            crate::state::SandboxState::Quarantined => EvictionClaim::Quarantined,
            crate::state::SandboxState::Suspended => {
                EvictionClaim::SuspendedPinned(row.current_local_artifact_id)
            }
            crate::state::SandboxState::Restoring
                if row
                    .failure_reason
                    .as_deref()
                    .is_some_and(|reason| !reason.trim().is_empty()) =>
            {
                EvictionClaim::FailedRestoring
            }
            crate::state::SandboxState::Restoring => {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id} is restoring with no persisted staging failure; \
                     refusing cluster-deleted eviction"
                )));
            }
            crate::state::SandboxState::Suspending
                if row.failure_reason.as_deref()
                    == Some(
                        pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON,
                    ) =>
            {
                // The marker is written only together with the artifact the
                // refused backup staged, so a marked row without one has had
                // its pin cleared by something else and is no longer this
                // shape.
                let staged_artifact = row.current_local_artifact_id.ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "sandbox {sandbox_id} carries the terminal teardown-backup marker with \
                         no staged artifact pinned; refusing cluster-deleted eviction"
                    ))
                })?;
                EvictionClaim::TeardownSuspendPermanentlyFailed {
                    staged_artifact,
                    state_changed_at: state_changed_at.clone(),
                }
            }
            crate::state::SandboxState::Suspending => {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id} is suspending with no permanently refused teardown \
                     backup; refusing cluster-deleted eviction"
                )));
            }
            state => {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id} is {state}, not suspended, quarantined, a failed cross-node \
                     restore, or a terminally failed teardown suspend; refusing \
                     cluster-deleted eviction"
                )));
            }
        };
        if !row.user.is_platform_user() {
            return Err(Status::failed_precondition(format!(
                "sandbox {sandbox_id} is owned by {owner}, not a platform user; the cluster \
                 never owned this row, so its absence there does not authorize teardown",
                owner = row.user
            )));
        }
        if row.pool_template.is_some() {
            return Err(Status::failed_precondition(format!(
                "sandbox {sandbox_id} is an unclaimed pool slot; pool slots have no cluster \
                 row until claimed and must not be evicted as cluster-deleted"
            )));
        }
        // Which clock the window runs on depends on the shape, because what
        // "quiet" can mean depends on the shape. A `suspended` or `restoring`
        // row has no resident runtime; the auto-lifecycle scheduler can still
        // write it, but every such write only restarts the window, so
        // `updated_at` errs toward waiting. Quarantined rows use the same
        // conservative window; teardown still releases any resident runtime.
        // A stranded `suspending` row is
        // still being written every few seconds by loops that change nothing
        // (see [`EvictionClaim::TeardownSuspendPermanentlyFailed`]), so it is
        // read on `state_changed_at`: time since the row entered `suspending`,
        // which moves only when the suspend actually ends. It is not time
        // since the backup was refused, so a refusal found after the window
        // has elapsed is eligible at once; the marker, the identity claim and
        // the fenced cleanup carry the safety, not an extra wait.
        let (quiescence_column, quiescence_since) = match &claim {
            EvictionClaim::TeardownSuspendPermanentlyFailed { .. } => {
                ("state_changed_at", state_changed_at.as_str())
            }
            EvictionClaim::SuspendedPinned(_)
            | EvictionClaim::FailedRestoring
            | EvictionClaim::Quarantined => ("updated_at", row.updated_at.as_str()),
        };
        let quiescent_since_secs = crate::db::timestamps::EpochSeconds::parse(quiescence_since)
            .map_err(|_| {
                Status::failed_precondition(format!(
                    "sandbox {sandbox_id} has malformed {quiescence_column} \
                     {quiescence_since:?}; operator repair required"
                ))
            })?;
        let quiescence_secs = CLUSTER_DELETED_EVICTION_QUIESCENCE_SECS;
        let idle_secs =
            crate::sandbox_eviction::now_secs().saturating_sub(quiescent_since_secs.unsigned());
        if idle_secs < quiescence_secs {
            // Still `FailedPrecondition` — the row is fine, only too fresh —
            // but the refusal says WHEN it stops being too fresh, so node-ctl
            // can retry at quiescence end instead of applying the flat
            // per-id back-off it keeps for refusals with no such horizon.
            let remaining = Duration::from_secs(quiescence_secs.saturating_sub(idle_secs));
            return Err(pplx_space_common_grpc::with_retry_after(
                Status::failed_precondition(format!(
                    "sandbox {sandbox_id} last moved {idle_secs}s ago by {quiescence_column}; \
                     refusing eviction inside the {quiescence_secs}s quiescence window \
                     (retry_after_ms={})",
                    remaining.as_millis()
                )),
                remaining,
            ));
        }

        // Detached (and tracked) so a dropped RPC future cannot abandon
        // the teardown between the CAS claim and cleanup completion;
        // terminal telemetry, and the pre-delete size estimate whose
        // directory walk can outlive the caller's deadline, live inside
        // the task for the same reason. Shutdown aborts cooperatively:
        // a row already claimed into `deleting` converges through the
        // startup recovery of `deleting` rows, the same contract as a
        // crash mid-teardown.
        let cache_dir = crate::sandbox_eviction::sandbox_cache_dir(&sandbox_id);
        let service = self.clone();
        let shutdown = self.shutdown_token.clone();
        let state_before = row.state;
        let checked_updated_at = row.updated_at.clone();
        let teardown = self.task_tracker.spawn(async move {
            let reason = crate::sandbox_eviction::EvictReason::ClusterRowAbsent;
            // Size estimate before the directory disappears; log/event
            // only, so it races shutdown too — service drain waits on
            // this tracked task and must not be held up by a slow
            // directory walk (the detached blocking thread finishes on
            // its own, its result discarded).
            let dir_for_size = cache_dir.clone();
            let size_walk = tokio::task::spawn_blocking(move || {
                crate::snapshot_gc::dir_size_bytes(std::path::Path::new(&dir_for_size))
            });
            let bytes = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        "shutdown during cluster-deleted eviction size estimate; nothing \
                         claimed, retry on successor",
                    );
                    return Err(Status::unavailable(
                        "sandbox manager is shutting down; retry on successor",
                    ));
                }
                joined = size_walk => joined.unwrap_or(0),
            };
            let result = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        "shutdown during cluster-deleted eviction; any claimed row converges \
                         via startup recovery of deleting rows",
                    );
                    return Err(Status::unavailable(
                        "sandbox manager is shutting down; retry on successor",
                    ));
                }
                result = service.evict_local_sandbox(
                    &sandbox_id,
                    claim,
                    // The owner/pool/quiescence proof above was checked
                    // against this exact row version; for the `suspended`
                    // `restoring` and `quarantined` shapes any write in the size-walk gap
                    // bumps updated_at and fails the claim. The terminal
                    // teardown-suspend shape ignores this and re-asserts
                    // its own identity instead, because on that row
                    // updated_at moves without the row changing.
                    Some(&checked_updated_at),
                ) => result,
            };
            match &result {
                Ok(()) => {
                    pplx_space_metrics::sandbox_eviction::evicted(reason.as_str());
                    pplx_space_metrics::sandbox_eviction::logical_bytes_freed(
                        reason.as_str(),
                        bytes,
                    );
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        reason = reason.as_str(),
                        state_before = %state_before,
                        bytes,
                        idle_secs,
                        "cluster-deleted sandbox: local presence evicted",
                    );
                    crate::sandbox_eviction::emit_eviction_event(
                        &sandbox_id,
                        pplx_space_events::SandboxEventType::LocalEvictionCompleted,
                        pplx_space_events::SandboxEventDetails::default()
                            .reason(reason.as_str())
                            .with_from_state(state_before.as_str())
                            .local_path(cache_dir),
                    );
                }
                // The CAS lost a race (resume/restore/delete claimed the
                // row first); nothing was changed and nothing to report.
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::FailedPrecondition | tonic::Code::NotFound
                    ) => {}
                Err(status) => {
                    pplx_space_metrics::sandbox_eviction::failed();
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        code = %status.code(),
                        message = %status.message(),
                        "cluster-deleted eviction: local delete failed; row left in deleting \
                         for cleanup retry",
                    );
                    crate::sandbox_eviction::emit_eviction_event(
                        &sandbox_id,
                        pplx_space_events::SandboxEventType::LocalEvictionFailed,
                        pplx_space_events::SandboxEventDetails::default()
                            .reason(reason.as_str())
                            .with_from_state(state_before.as_str())
                            .error(status.message().to_string()),
                    );
                }
            }
            result
        });
        teardown.await.map_err(|join_error| {
            Status::internal(format!("eviction task failed: {join_error}"))
        })??;

        Ok(Response::new(
            crate::proto::EvictClusterDeletedSandboxResponse {},
        ))
    }

    /// Spawn the cold-sandbox eviction loop. No-op when
    /// `SPACE_SBX_EVICT_ENABLED` is unset. Wired alongside the other
    /// reconcilers in main.rs.
    pub fn start_sandbox_eviction(
        &self,
        shutdown_token: tokio_util::sync::CancellationToken,
        task_tracker: &tokio_util::task::TaskTracker,
    ) {
        let service = self.clone();
        let cfg = crate::sandbox_eviction::EvictionConfig::from_env();
        task_tracker.spawn(async move {
            crate::sandbox_eviction::run_loop(service, cfg, shutdown_token).await;
        });
    }
}
