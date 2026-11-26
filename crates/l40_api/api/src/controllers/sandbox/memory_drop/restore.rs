//! Inline restore-time memory drop under an already-claimed restore lease.
//!
//! When a restore arrives for a sandbox whose recorded environment has retired
//! its checkpoint memory before a campaign reached the row, the restore runs
//! the same server-side rootfs copy and disk-snapshot registration under the
//! restore lease it already owns, then atomically re-pins the sandbox to the
//! disk artifact and parks it back in `suspended`.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::fork_source_lifecycle::{
    active_fork_source_operation_exists, lock_fork_source_lifecycle,
};
use pplx_space_sandbox_store::memory_drop::restore::{
    RestoreDropRefusal, candidate_for_restore, commit_for_restore, refusal_for_restore,
    renew_for_restore,
};
use pplx_space_sandbox_store::memory_drop::{CandidateRead, MemoryDropCandidate};
use sqlx::PgPool;
use uuid::Uuid;

use crate::controllers::ControllerError;

use super::{
    CommitOutcome, Copied, CopyPlan, EarlierDisk, MemoryDrop, MemoryDropIneligible,
    MemoryDropOutcome, MemoryDropped, Sealed, db_error, insert_disk_snapshot,
};

impl MemoryDrop {
    /// Drop a `restoring` sandbox's checkpoint memory inline under
    /// `restore_lease_token`, committing the disk pin, clearing the restore
    /// lease and parking the row back in `suspended`.
    pub async fn drop_for_restore(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<MemoryDropOutcome, ControllerError> {
        let candidate = match self
            .read_for_restore(pool, sandbox_id, restore_lease_token)
            .await?
        {
            Sealed::Candidate(candidate) => candidate,
            Sealed::Outcome(outcome) => return Ok(outcome),
        };
        let Some(plan) = CopyPlan::new(sandbox_id, &candidate) else {
            tracing::warn!(
                %sandbox_id,
                snapshot_id = %candidate.full_snapshot_id,
                "restore-time memory drop refused: the suspend artifact's location isn't a per-attempt prefix"
            );
            return Ok(MemoryDropOutcome::NotEligible(
                MemoryDropIneligible::UnsupportedLocation,
            ));
        };
        let copied = match self.earlier_disk(pool, &plan).await? {
            EarlierDisk::None => {
                self.copy_disk_for_restore(pool, sandbox_id, restore_lease_token, &candidate, &plan)
                    .await?
            }
            EarlierDisk::Ready { rootfs_size } => Copied::Size(rootfs_size),
            EarlierDisk::Retiring => {
                return Err(ControllerError::service_unavailable(
                    "an earlier drop's disk for this snapshot is still being deleted; retry",
                ));
            }
        };
        let rootfs_size = match copied {
            Copied::Size(rootfs_size) => rootfs_size,
            Copied::Outcome(outcome) => return Ok(outcome),
        };

        match self
            .commit_restore_drop(
                pool,
                sandbox_id,
                restore_lease_token,
                &candidate,
                &plan,
                rootfs_size,
            )
            .await?
        {
            CommitOutcome::Committed => Ok(MemoryDropOutcome::Dropped(MemoryDropped {
                disk_snapshot_id: plan.disk_snapshot_id,
                disk_artifact_id: plan.disk_artifact_id,
                retained_snapshot_id: candidate.full_snapshot_id,
                retained_artifact_id: candidate.pinned_artifact_id,
            })),
            CommitOutcome::ForkActive => Ok(MemoryDropOutcome::NotEligible(
                MemoryDropIneligible::ForkActive,
            )),
            CommitOutcome::DiskRetiring => Err(ControllerError::service_unavailable(
                "an earlier drop's disk for this snapshot is still being deleted; retry",
            )),
            CommitOutcome::Lost => {
                self.release_lost_prefix(pool, sandbox_id, &candidate, &plan)
                    .await?;
                Err(ControllerError::service_unavailable(
                    "the sandbox changed while its disk was being written; retry",
                ))
            }
        }
    }

    async fn copy_disk_for_restore(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
        candidate: &MemoryDropCandidate,
        plan: &CopyPlan,
    ) -> Result<Copied, ControllerError> {
        let written = self
            .renewing(
                self.write_disk(sandbox_id, candidate, plan),
                |ttl| async move {
                    renew_for_restore(pool, sandbox_id, restore_lease_token, ttl)
                        .await
                        .map_err(|error| {
                            db_error("renew the restore-time memory drop lease", &error)
                        })
                },
            )
            .await?;
        match written {
            Some(Ok(Ok(rootfs_size))) => Ok(Copied::Size(Some(rootfs_size))),
            Some(Ok(Err(why))) => Ok(Copied::Outcome(MemoryDropOutcome::NotEligible(
                MemoryDropIneligible::Metadata(why),
            ))),
            Some(Err(error)) => {
                tracing::warn!(
                    %sandbox_id,
                    snapshot_id = %candidate.full_snapshot_id,
                    %error,
                    "restore-time memory drop failed: writing the disk artifact failed"
                );
                Err(ControllerError::service_unavailable(format!(
                    "writing the disk artifact failed; retry: {error}"
                )))
            }
            None => {
                self.release_lost_prefix(pool, sandbox_id, candidate, plan)
                    .await?;
                Err(ControllerError::service_unavailable(
                    "the restore lost its lease while its disk was being written; retry",
                ))
            }
        }
    }

    async fn read_for_restore(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
    ) -> Result<Sealed, ControllerError> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|error| db_error("begin the restore-time memory drop check", &error))?;
        lock_fork_source_lifecycle(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("lock the fork source lifecycle", &error))?;
        if active_fork_source_operation_exists(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("check for an active fork", &error))?
        {
            return Ok(Sealed::Outcome(MemoryDropOutcome::NotEligible(
                MemoryDropIneligible::ForkActive,
            )));
        }
        let candidate = candidate_for_restore(
            &mut *tx,
            sandbox_id,
            restore_lease_token,
            CandidateRead::Locked,
        )
        .await
        .map_err(|error| db_error("read the restore-time memory drop candidate", &error))?;
        let Some(candidate) = candidate else {
            drop(tx);
            return match refusal_for_restore(pool, sandbox_id, restore_lease_token)
                .await
                .map_err(|error| db_error("read the restore-time memory drop refusal", &error))?
            {
                Some(RestoreDropRefusal::AlreadyDropped) => {
                    Ok(Sealed::Outcome(MemoryDropOutcome::AlreadyDropped))
                }
                Some(RestoreDropRefusal::LostLease) => Err(ControllerError::service_unavailable(
                    "the restore lost its lease before its memory drop started; retry",
                )),
                Some(RestoreDropRefusal::Refused(refusal)) => Ok(Sealed::Outcome(
                    MemoryDropOutcome::NotEligible(MemoryDropIneligible::Row(refusal)),
                )),
                None => Err(ControllerError::service_unavailable(
                    "the sandbox changed while its restore-time memory drop was being checked; retry",
                )),
            };
        };
        tx.commit()
            .await
            .map_err(|error| db_error("commit the restore-time memory drop check", &error))?;
        Ok(Sealed::Candidate(candidate))
    }

    async fn commit_restore_drop(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        restore_lease_token: Uuid,
        candidate: &MemoryDropCandidate,
        plan: &CopyPlan,
        rootfs_size: Option<u64>,
    ) -> Result<CommitOutcome, ControllerError> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|error| db_error("begin the restore-time memory drop commit", &error))?;
        lock_fork_source_lifecycle(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("lock the fork source lifecycle", &error))?;
        if active_fork_source_operation_exists(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("check for an active fork", &error))?
        {
            return Ok(CommitOutcome::ForkActive);
        }
        if !insert_disk_snapshot(&mut tx, sandbox_id, candidate, plan, rootfs_size).await? {
            return Ok(CommitOutcome::DiskRetiring);
        }
        if !commit_for_restore(
            &mut *tx,
            sandbox_id,
            restore_lease_token,
            candidate,
            plan.disk_artifact_id,
        )
        .await
        .map_err(|error| db_error("re-pin the restoring sandbox", &error))?
        {
            return Ok(CommitOutcome::Lost);
        }
        tx.commit()
            .await
            .map_err(|error| db_error("commit the restore-time memory drop", &error))?;
        Ok(CommitOutcome::Committed)
    }
}
