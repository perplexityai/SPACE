//! Replacing a stale suspended copy before an upgrade stages its checkpoint.
//!
//! A resume-time runtime upgrade boots the checkpoint it was decided from,
//! which the row must pin ([`super::require_decided_checkpoint`]). A restore
//! placed on a node that still holds an older suspended copy of the sandbox
//! finds another pin there, and staging lays a checkpoint down only for a
//! sandbox absent from the node. So the copy is evicted first, through the
//! eviction executor and only on the proof eviction acts on: a ready backup of
//! exactly the artifact it pins. Without that proof the copy may be the only
//! one of something, and the upgrade goes on refusing it.
//!
//! A newer local copy is never replaced by an older decided checkpoint,
//! whatever the minting nodes' clocks say or which backup registered last.
//! Each checkpoint is captured by one execution, and executions' generations
//! only grow: the copy is replaced only when the cluster shows the decided
//! checkpoint's ready backup was captured by a later execution than the
//! copy's. Without a capture generation on both backups nothing orders them,
//! and the copy is refused, as every copy pinning another checkpoint was
//! before replacement existed. An encrypted copy is left too, since an
//! encrypted checkpoint staged here can't be cold-booted.

use pplx_space_common_core::types::LocalArtifactId;
use tonic::Status;

use crate::db::RuntimeUpgradeOperationId;
use crate::sandbox_eviction::LocalEvidence;
use crate::state::{CleanupState, SandboxState, SandboxStatus};

#[cfg(test)]
mod tests;

fn unproven(sandbox_id: &str) -> Status {
    Status::failed_precondition(format!(
        "sandbox {sandbox_id}'s checkpoint on this node isn't the one its runtime upgrade was \
         decided from, and nothing proves it backed up; refusing to replace it"
    ))
}

impl crate::service::SandboxManagerService {
    /// Evict this node's suspended copy of `sandbox_id` when it pins an
    /// artifact other than `decided`, the checkpoint upgrade `operation_id`
    /// was decided from, so the caller's staging lays that checkpoint down in
    /// its place. Everything else is left to the upgrade's own checks: an
    /// absent sandbox, one in another state, a copy of the decided
    /// checkpoint, a request that names no checkpoint, and a sandbox this
    /// operation already ran on, whose record decides what a replay means.
    ///
    /// An upgrade that doesn't authorize the memory loss a suspended
    /// sandbox's upgrade means is left to the upgrade too, which refuses it.
    ///
    /// A copy that isn't replaced is refused before any node work:
    /// `FailedPrecondition` when nothing proves it backed, when nothing shows
    /// the decided checkpoint was captured after it, when it's encrypted, or
    /// when the sandbox holds a user snapshot no local artifact records,
    /// and `Unavailable` when the proof couldn't be had or another upgrade
    /// still owns the copy. So is a request whose stage would be refused for
    /// its form or route identity, and a retry of an activation the row
    /// rolled back. A copy that changed under the eviction's claim, or one
    /// with an artifact upload pending, running or cancelling, is refused by
    /// the claim as `Aborted` until that settles. Each is safe to retry. A
    /// cleanup that fails after the claim leaves the row's cleanup failed, its
    /// delete intent kept, for the cleanup machinery to finish; once it has, the sandbox is absent and a
    /// retry stages it as it would any absent sandbox.
    pub(in crate::service) async fn evict_stale_checkpoint_copy(
        &self,
        sandbox_id: &str,
        operation_id: RuntimeUpgradeOperationId,
        decided: Option<LocalArtifactId>,
        memory_loss_authorized: bool,
        staging: &crate::proto::StartRequest,
    ) -> Result<(), Status> {
        let Some(decided) = decided else {
            return Ok(());
        };
        if !memory_loss_authorized {
            return Ok(());
        }
        // Only a Kubernetes execution's captures register the generation that
        // orders them; nothing orders a Direct sandbox's, so its copy is left
        // to the upgrade, which refuses it as it did before replacement
        // existed, without the cluster being asked anything. The request's
        // identity names the substrate: a suspended row keeps none.
        if crate::service::activation_support::space_api_route_identity_from_start_request(staging)?
            .effective_substrate_kind()
            != Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes)
        {
            return Ok(());
        }
        // One hold of the node's only database connection, which staging
        // writes through too: no stage can repin the row between reading its
        // pin and reading the evidence the proof names. After it, the
        // eviction's claim re-asserts the proven pin and the activation
        // generations read here, so a generation an activation retains after
        // the checks below fails the claim.
        let (pinned, generations) = {
            let db = self.db_conn()?;
            let sandbox = crate::db::read::sandbox(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("db sandbox lookup failed: {error}")))?;
            let Some(sandbox) = sandbox else {
                return Ok(());
            };
            if !sandbox.state.is_unowned(SandboxState::Suspended)
                || sandbox.current_local_artifact_id == Some(decided)
            {
                return Ok(());
            }
            let recorded = crate::db::read::runtime_upgrade_report(&db, operation_id)
                .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))?;
            if recorded.is_some() {
                return Ok(());
            }
            // An interrupted upgrade's preserved disk, or a checkpoint whose
            // retirement is still owed, lives in the copy, and the recovery
            // loop finishes it.
            super::refuse_runtime_upgrade_fence_in(&db, sandbox_id)?;
            super::refuse_unretired_runtime_upgrade_in(&db, sandbox_id)?;
            if sandbox.volume_encryption {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id}'s checkpoint on this node isn't the one its runtime \
                     upgrade was decided from, and an encrypted checkpoint staged in its place \
                     can't be cold-booted; refusing to replace it"
                )));
            }
            let evidence = crate::sandbox_eviction::gather_local_evidence(&db, sandbox_id)
                .map_err(|error| {
                    Status::unavailable(format!(
                        "read sandbox {sandbox_id}'s pinned artifact: {error}"
                    ))
                })?;
            let pinned = match evidence {
                Some((LocalEvidence::Pinned(pinned), _)) => pinned,
                // Unpinned, or pinning an artifact row it doesn't own: no
                // proof names these bytes.
                _ => return Err(unproven(sandbox_id)),
            };
            let generations = crate::db::read::activation_generations(&db, sandbox_id)
                .map_err(|error| {
                    Status::unavailable(format!(
                        "read sandbox {sandbox_id}'s activation generations: {error}"
                    ))
                })?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?;
            (pinned, generations)
        };
        // A stage that would be refused stages nothing in the copy's place,
        // and an activation the row would refuse, such as one at a generation
        // it already retained, must not get past it by removing the row.
        let activation_claim = self.validate_staging_start(staging)?;
        // A retry of an activation the row rolled back matches the claim the
        // row kept open for it, which is how every later attempt of that
        // activation is asked whether its execution was retired. Removing the
        // row closes the claim, so a retry after the copy is gone would match
        // nothing and go unasked; such a retry keeps the copy, as it did before
        // replacement existed, until a new activation replaces it.
        if let Some(claim) = &activation_claim {
            let db = self.db_conn()?;
            let retried = crate::db::read::sandbox_view(&db, sandbox_id)
                .route_claim()
                .matches(claim)
                .map_err(|error| Status::internal(format!("read activation claim: {error}")))?;
            if retried {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id}'s copy on this node holds the open claim of the \
                     activation this request retries, which replacing the copy would close; \
                     refusing to replace it for a retry"
                )));
            }
        }
        match crate::sandbox_eviction::prove_pinned_backup(self, sandbox_id, &pinned).await {
            Ok(true) => {}
            Ok(false) => return Err(unproven(sandbox_id)),
            Err(reason) => {
                return Err(Status::unavailable(format!(
                    "couldn't prove sandbox {sandbox_id}'s copy on this node backed up ({}); \
                     retry",
                    reason.as_str()
                )));
            }
        }
        // Asked after the copy's backup is proven ready, so the cluster
        // compares two ready backups' capture generations.
        match crate::sandbox_eviction::prove_captured_after(
            sandbox_id,
            (decided, pplx_space_common_core::types::ArtifactKind::Full),
            (pinned.artifact_id, pinned.artifact_kind),
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(Status::failed_precondition(format!(
                    "nothing shows the checkpoint sandbox {sandbox_id}'s runtime upgrade was \
                     decided from was captured after its copy on this node; refusing to replace \
                     the copy"
                )));
            }
            Err(reason) => {
                return Err(Status::unavailable(format!(
                    "couldn't order sandbox {sandbox_id}'s checkpoints ({}); retry",
                    reason.as_str()
                )));
            }
        }
        tracing::info!(
            sandbox_id,
            %operation_id,
            stale_local_artifact_id = %pinned.artifact_id,
            decided_local_artifact_id = %decided,
            "evicting a backed-up stale suspended copy so the runtime upgrade can stage its \
             checkpoint"
        );
        // The eviction deletes every local artifact of the sandbox, not only
        // the copy: a named snapshot nothing backed up would be lost. Each one
        // the cluster shows backed is stamped, and the claim refuses while any
        // other stays unstamped.
        let awaiting = self.db_conn().and_then(|db| {
            crate::db::read::sandbox_view(&db, sandbox_id)
                .artifacts()
                .awaiting_backup_besides(Some(pinned.artifact_id))
                .map_err(|error| Status::unavailable(format!("local artifact lookup: {error}")))
        })?;
        match crate::sandbox_eviction::prove_local_artifact_backups(self, sandbox_id, &awaiting)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(Status::failed_precondition(format!(
                    "another local artifact of sandbox {sandbox_id} isn't backed up, and \
                     replacing its copy would delete it; refusing to replace the copy"
                )));
            }
            Err(reason) => {
                return Err(Status::unavailable(format!(
                    "couldn't prove sandbox {sandbox_id}'s other local artifacts backed up ({}); \
                     retry",
                    reason.as_str()
                )));
            }
        }
        let result = self
            .evict_local_sandbox(
                sandbox_id,
                crate::service::EvictionClaim::StaleUpgradeCopy {
                    pin: pinned.artifact_id,
                    generations,
                },
                None,
            )
            .await;
        let reported = reported_eviction_result(
            &result,
            self.lookup_sandbox(sandbox_id).ok().map(|row| row.state),
        );
        crate::sandbox_eviction::report_eviction_outcome(
            sandbox_id,
            crate::sandbox_eviction::EvictReason::StaleUpgradeCopy,
            pplx_space_events::SandboxEventDetails::default()
                .local_artifact_id(pinned.artifact_id.to_string()),
            &reported,
            "stale suspended copy eviction for a runtime upgrade failed",
        );
        // The claim refuses a row that changed or is held as
        // `FailedPrecondition`, and a cleanup it handed off can fail with the
        // same code; a retry finds which. A row another eviction removed
        // during the proof is absent, as staging needs it to be.
        // A snapshot no local artifact records stays until someone acts on
        // it, so that refusal keeps its code rather than inviting a retry.
        result.or_else(|status| match status.code() {
            tonic::Code::NotFound => Ok(()),
            tonic::Code::FailedPrecondition
                if crate::service::is_unrecorded_snapshot_refusal(&status) =>
            {
                Err(status)
            }
            tonic::Code::FailedPrecondition => Err({
                Status::aborted(format!(
                    "the runtime upgrade couldn't replace sandbox {sandbox_id}'s copy on this \
                     node: {}; retry",
                    status.message()
                ))
            }),
            _ => Err(status),
        })
    }
}

/// What an eviction attempt reports, given the row's state after it. A
/// `FailedPrecondition` after which the row's cleanup failed came from the
/// cleanup the claim handed off, not from the claim, and is reported as the
/// failure it is. Any other `FailedPrecondition` is the claim's: the row
/// changed, or another eviction claimed it first and the row is `deleting`
/// under that one, a race lost and not a failure.
fn reported_eviction_result(
    result: &Result<(), Status>,
    row_state: Option<SandboxStatus>,
) -> Result<(), Status> {
    match result {
        Err(status)
            if status.code() == tonic::Code::FailedPrecondition
                && row_state.and_then(SandboxStatus::cleanup) == Some(CleanupState::Failed) =>
        {
            Err(Status::internal(status.message().to_string()))
        }
        other => other.clone(),
    }
}
