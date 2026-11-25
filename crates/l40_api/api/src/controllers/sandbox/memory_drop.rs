//! Dropping the memory of a suspended Kubernetes sandbox whose node is gone,
//! without a node.
//!
//! A suspended Kubernetes sandbox's only copy is its durable full suspend
//! artifact. Its memory and VM state load only on the runtime that captured
//! them; its rootfs doesn't depend on that runtime, and is already the disk a
//! stop would publish. This seals the row, copies that rootfs object
//! server-side into a new disk prefix with a projected `metadata.json`, then
//! in one transaction registers the disk snapshot row, re-pins the sandbox to
//! it and lifts the seal. The row stays `suspended` and its lifecycle clock
//! doesn't move. The full artifact's snapshot rows and prefixes stay as they
//! were, so an operator can point the row back at them
//! (`pplx_space_sandbox_store::memory_drop` says why retention keeps them).
//!
//! The seal is the row's restore lease, taken in a transaction that holds the
//! fork-source lock a delete takes and the row's `FOR UPDATE` lock, and
//! refused while a fork of the sandbox is queued or running: the fork would
//! copy the full artifact's memory into a new sandbox. The copy runs after
//! that transaction commits, so no row lock is held across object-store
//! calls; the lease is renewed while it runs. A user restore refuses a live
//! lease, and fork admission, the delete and the drain's restore claim refuse
//! a sealed row with a retryable error, so nothing else takes the row before
//! the commit, which requires the seal's token and the pin unchanged.
//!
//! The disk ids are derived from the pinned artifact alone, so a retried
//! attempt copies into the same prefix instead of leaking a new one each
//! time, even when it reads a newer backup of the same pin. The disk row
//! takes the full snapshot's `timestamp`, so it orders with the artifact it
//! was copied from rather than with the drop.
//!
//! The cluster controller runs it as the `drop_sandbox_memory` operation an
//! admin route enqueues; it's the only process with the object store.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use pplx_space_artifact_model::{ExactStorageUri, SnapshotArtifact as SnapshotFile, StorageUri};
use pplx_space_execution_spec::SnapshotExecutionMetadata;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::fork_source_lifecycle::{
    active_fork_source_operation_exists, lock_fork_source_lifecycle,
};
use pplx_space_sandbox_store::memory_drop::{self, MemoryDropCandidate, MemoryDropRefusal};
use pplx_space_snapshot_model::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SnapshotId, SnapshotKind, SnapshotState,
};
use pplx_space_snapshot_store::{NewSnapshot, SnapshotArtifact};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::controllers::ControllerError;

/// The switch, read by the API (whether its route accepts a drop) and the
/// cluster controller (whether its executor runs one). Off by default.
pub const MEMORY_DROP_ENABLED_VAR: &str = "SPACE_MEMORY_DROP_ENABLED";

/// How long a seal lasts without renewal. A drop that dies mid-copy leaves
/// the row sealed this long before a retry can take it over.
const SEAL_TTL: Duration = Duration::from_secs(60);

/// How often the copy renews its seal: a third of [`SEAL_TTL`], so two
/// renewals can stall before the seal lapses. A renewal that fails ends the
/// drop, and the seal lapses. While a drop is live it holds the seal for the
/// whole copy, so a delete or restore of the row answers a retryable 503 until
/// the copy ends.
const SEAL_RENEW_EVERY: Duration = Duration::from_secs(20);

/// The `metadata.json` schema version whose `execution` block the projection
/// carries over. Older artifacts aren't dropped.
const SNAPSHOT_METADATA_VERSION: u64 = 2;

/// The `rootfs_diff_format` that names the `rootfs.btrfs-send` object.
const ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1: &str = "btrfs-send-v1";

/// An object-store operation failed.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MemoryDropObjectsError(pub String);

/// The four object-store operations the memory drop needs, on the files of
/// one snapshot prefix.
///
/// A trait object rather than a generic so [`SandboxController`] stays one
/// concrete type: the cluster controller plugs in S3, tests plug in a map.
/// The methods return [`BoxFuture`] because `async fn` in a trait isn't
/// usable through `dyn` yet; the `'a` lifetime ties each future to the
/// borrowed arguments it reads.
///
/// [`SandboxController`]: super::SandboxController
pub trait MemoryDropObjects: Send + Sync + 'static {
    /// The stored size of `file` under `prefix`, or `None` when it's absent.
    fn size<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
    ) -> BoxFuture<'a, Result<Option<u64>, MemoryDropObjectsError>>;

    /// The decoded bytes of a small `file` under `prefix`. A node may have
    /// zstd-compressed it on upload; the implementation undoes that.
    fn read<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
    ) -> BoxFuture<'a, Result<Vec<u8>, MemoryDropObjectsError>>;

    /// Copy `file` from `source` to `dest` server-side; `size` is its stored
    /// size from [`Self::size`].
    fn copy<'a>(
        &'a self,
        source: &'a StorageUri,
        dest: &'a StorageUri,
        file: SnapshotFile,
        size: u64,
    ) -> BoxFuture<'a, Result<(), MemoryDropObjectsError>>;

    /// Write `body` as `file` under `prefix`, replacing any object there.
    fn write<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), MemoryDropObjectsError>>;
}

/// The object key of `file` under `prefix`.
pub fn object_key(prefix: &StorageUri, file: SnapshotFile) -> String {
    if prefix.key_prefix.is_empty() {
        file.file_name().to_string()
    } else {
        format!("{}/{}", prefix.key_prefix, file.file_name())
    }
}

/// The memory drop and its object store.
#[derive(Clone)]
pub struct MemoryDrop {
    // `Arc<dyn ...>` so cloning shares one store instead of requiring the
    // store itself to be `Clone`.
    objects: Arc<dyn MemoryDropObjects>,
    seal: SealTiming,
}

/// How long a seal lasts and how often the copy renews it. Tests shorten
/// both to exercise renewal and expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SealTiming {
    ttl: Duration,
    renew_every: Duration,
}

/// The ids a committed drop wrote and kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryDropped {
    /// The new `disk` `stop` snapshot row the sandbox is pinned to.
    pub disk_snapshot_id: SnapshotId,
    pub disk_artifact_id: LocalArtifactId,
    /// The full suspend snapshot the sandbox was pinned to, retained.
    pub retained_snapshot_id: SnapshotId,
    pub retained_artifact_id: LocalArtifactId,
}

/// Why a sandbox's memory can't be dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryDropIneligible {
    /// The row fails a condition `memory_drop::candidate` checks.
    Row(MemoryDropRefusal),
    /// A fork of the sandbox is queued or running.
    ForkActive,
    /// The suspend artifact's location isn't a per-attempt prefix.
    UnsupportedLocation,
    /// The suspend artifact's `metadata.json` can't project to a disk.
    Metadata(&'static str),
}

impl MemoryDropIneligible {
    /// A stable snake_case reason for the API and the audit record.
    pub fn reason(self) -> String {
        match self {
            Self::Row(refusal) => refusal.as_str().to_string(),
            Self::ForkActive => "fork_active".to_string(),
            Self::UnsupportedLocation => "unsupported_location".to_string(),
            Self::Metadata(why) => format!("metadata: {why}"),
        }
    }
}

/// What [`MemoryDrop::drop_memory`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryDropOutcome {
    /// The row is `suspended`, pinned to the new disk artifact.
    Dropped(MemoryDropped),
    /// The row was already dropped: a retry after a commit that this attempt
    /// didn't see.
    AlreadyDropped,
    /// Nothing was committed, though a fork that started mid-copy leaves the
    /// copied bytes under the disk prefix.
    NotEligible(MemoryDropIneligible),
}

impl MemoryDrop {
    pub fn new(objects: Arc<dyn MemoryDropObjects>) -> Self {
        Self {
            objects,
            seal: SealTiming {
                ttl: SEAL_TTL,
                renew_every: SEAL_RENEW_EVERY,
            },
        }
    }

    #[cfg(test)]
    fn with_seal_timing(mut self, ttl: Duration, renew_every: Duration) -> Self {
        self.seal = SealTiming { ttl, renew_every };
        self
    }

    /// Why the sandbox's memory can't be dropped now, or `None` when it can.
    /// A plain read for a caller deciding whether to ask; the drop itself
    /// re-checks everything.
    pub async fn eligibility(
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> Result<Option<MemoryDropIneligible>, ControllerError> {
        if let Some(refusal) = memory_drop::refusal(pool, sandbox_id)
            .await
            .map_err(|error| db_error("read the memory drop eligibility", &error))?
        {
            return Ok(Some(MemoryDropIneligible::Row(refusal)));
        }
        if fork_active(pool, sandbox_id).await? {
            return Ok(Some(MemoryDropIneligible::ForkActive));
        }
        Ok(None)
    }

    /// Drop the sandbox's memory. `Err` is retryable: a database or
    /// object-store failure, another drop holding the row's seal, or a row
    /// that moved during the copy.
    pub async fn drop_memory(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> Result<MemoryDropOutcome, ControllerError> {
        // UUIDv7, like the restore claims' tokens: unique per attempt, so a
        // token-guarded write matches only the seal this attempt took.
        let token = Uuid::now_v7();
        let candidate = match self.seal(pool, sandbox_id, token).await? {
            Sealed::Candidate(candidate) => candidate,
            Sealed::Outcome(outcome) => return Ok(outcome),
        };
        let Some(plan) = CopyPlan::new(sandbox_id, &candidate) else {
            tracing::warn!(
                %sandbox_id,
                snapshot_id = %candidate.full_snapshot_id,
                "memory drop refused: the suspend artifact's location isn't a per-attempt prefix"
            );
            self.release(pool, sandbox_id, token).await;
            return Ok(MemoryDropOutcome::NotEligible(
                MemoryDropIneligible::UnsupportedLocation,
            ));
        };
        let copied = match self.earlier_disk(pool, &plan).await? {
            EarlierDisk::None => {
                self.copy_disk(pool, sandbox_id, token, &candidate, &plan)
                    .await?
            }
            // An earlier drop of this pin copied and registered these exact
            // bytes, and an operator has since pointed the row back at its
            // full artifact. Copying again would rewrite objects retention may
            // claim at any time; reusing them leaves nothing to race. The
            // commit refuses if retention claimed the disk in the meantime.
            EarlierDisk::Ready { rootfs_size } => Copied::Size(rootfs_size),
            // The bytes under the prefix are being deleted, so a copy written
            // there now could lose objects to that delete after it registers.
            // Once the delete finishes its row is gone, and a retry copies.
            EarlierDisk::Retiring => {
                self.release(pool, sandbox_id, token).await;
                return Err(ControllerError::service_unavailable(
                    "an earlier drop's disk for this snapshot is still being deleted; retry the memory drop",
                ));
            }
        };
        let rootfs_size = match copied {
            Copied::Size(rootfs_size) => rootfs_size,
            Copied::Outcome(outcome) => return Ok(outcome),
        };

        match self
            .commit(pool, sandbox_id, token, &candidate, &plan, rootfs_size)
            .await?
        {
            CommitOutcome::Committed => {
                return Ok(MemoryDropOutcome::Dropped(MemoryDropped {
                    disk_snapshot_id: plan.disk_snapshot_id,
                    disk_artifact_id: plan.disk_artifact_id,
                    retained_snapshot_id: candidate.full_snapshot_id,
                    retained_artifact_id: candidate.pinned_artifact_id,
                }));
            }
            // A fork started while this copied, which only an API release that
            // predates the seal admits: this release's fork admission refuses a
            // sealed row. The copy stays under the disk prefix,
            // as after an object-store failure above.
            CommitOutcome::ForkActive => {
                self.release(pool, sandbox_id, token).await;
                return Ok(MemoryDropOutcome::NotEligible(
                    MemoryDropIneligible::ForkActive,
                ));
            }
            // The seal is gone, or the pin moved under it. In the second case
            // the seal is still this attempt's, so lift it rather than leave
            // the row sealed until it expires.
            CommitOutcome::Lost => self.release(pool, sandbox_id, token).await,
            // Retention owns the earlier disk's bytes, so leave the prefix to
            // it; once it has deleted the row, a retry registers a fresh one.
            CommitOutcome::DiskRetiring => {
                self.release(pool, sandbox_id, token).await;
                return Err(ControllerError::service_unavailable(
                    "an earlier drop's disk for this snapshot is still being deleted; retry the memory drop",
                ));
            }
        }
        self.release_lost_prefix(pool, sandbox_id, &candidate, &plan)
            .await?;
        Err(ControllerError::service_unavailable(
            "the sandbox changed while its disk was being written; retry the memory drop",
        ))
    }

    /// What an earlier drop of the same pin left under the disk ids, read
    /// after the seal so no other drop of this row is copying.
    ///
    /// Retention claims a disk (`ready` to `deleting`), deletes its prefix,
    /// and only then deletes its rows; the storage sweeper deletes an
    /// orphaned prefix before its row. So no row and no pending orphan means
    /// nothing is deleting the prefix, and nothing can start to: retention
    /// claims only a registered row, and only a drop of this pin, which the
    /// seal excludes, orphans this prefix.
    async fn earlier_disk(
        &self,
        pool: &PgPool,
        plan: &CopyPlan,
    ) -> Result<EarlierDisk, ControllerError> {
        let registered = pplx_space_snapshot_store::get(pool, plan.disk_snapshot_id)
            .await
            .map_err(|error| {
                ControllerError::internal(format!(
                    "failed to load an earlier drop's disk snapshot: {error}"
                ))
            })?;
        if let Some(registered) = registered {
            let usable = registered.state().is_ready()
                && registered
                    .storage_location()
                    .is_some_and(|location| location.orphaned_at().is_none());
            return Ok(if usable {
                EarlierDisk::Ready {
                    rootfs_size: registered
                        .size_bytes()
                        .and_then(|size| u64::try_from(size).ok()),
                }
            } else {
                EarlierDisk::Retiring
            });
        }
        let pending = pplx_space_snapshot_store::orphan::orphan_pending(pool, &plan.dest)
            .await
            .map_err(|error| db_error("check for an unswept disk prefix", &error))?;
        Ok(if pending {
            EarlierDisk::Retiring
        } else {
            EarlierDisk::None
        })
    }

    /// Copy the disk under a renewing seal. Returns its rootfs size, or the
    /// outcome when the copy can't go ahead; every path but the size lifts
    /// or has lost the seal.
    async fn copy_disk(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        token: Uuid,
        candidate: &MemoryDropCandidate,
        plan: &CopyPlan,
    ) -> Result<Copied, ControllerError> {
        let written = self
            .renewing_seal(
                pool,
                sandbox_id,
                token,
                self.write_disk(sandbox_id, candidate, plan),
            )
            .await?;
        match written {
            Some(Ok(Ok(rootfs_size))) => Ok(Copied::Size(Some(rootfs_size))),
            Some(Ok(Err(why))) => {
                self.release(pool, sandbox_id, token).await;
                Ok(Copied::Outcome(MemoryDropOutcome::NotEligible(
                    MemoryDropIneligible::Metadata(why),
                )))
            }
            Some(Err(error)) => {
                // The row stays on its full pin, so a retry derives the same
                // disk prefix and rewrites it. Nothing registers the copied
                // bytes for the sweeper while another attempt may still own
                // them: the sweeper doesn't check whether a live snapshot row
                // shares a prefix before deleting it, so an orphan registered
                // now could delete a concurrent winner's disk. If the pin
                // moves some other way, such as a wake, nothing reuses the
                // prefix and it leaks: at most one per pinned artifact.
                tracing::warn!(
                    %sandbox_id,
                    snapshot_id = %candidate.full_snapshot_id,
                    %error,
                    "memory drop failed: writing the disk artifact failed"
                );
                self.release(pool, sandbox_id, token).await;
                Err(ControllerError::service_unavailable(format!(
                    "writing the disk artifact failed; retry: {error}"
                )))
            }
            // The seal was lost mid-copy: it expired and something took the
            // row, or a write moved its lifecycle.
            None => {
                self.release_lost_prefix(pool, sandbox_id, candidate, plan)
                    .await?;
                Err(ControllerError::service_unavailable(
                    "the memory drop lost its seal while its disk was being written; retry",
                ))
            }
        }
    }

    /// Seal the row with `token`, under the fork-source lock and the row
    /// lock, and commit the seal. Returns the candidate to copy, or the
    /// outcome when the row can't be sealed.
    ///
    /// The fork-source lock comes first, then the row lock, the order a
    /// delete takes them in (`begin_delete_against_active_fork`). A fork
    /// queued or running copies the full artifact, memory included, so the
    /// drop refuses rather than race it.
    async fn seal(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        token: Uuid,
    ) -> Result<Sealed, ControllerError> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|error| db_error("begin the memory drop seal", &error))?;
        lock_fork_source_lifecycle(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("lock the fork source lifecycle", &error))?;
        if active_fork_source_operation_exists(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("check for an active fork", &error))?
        {
            // `tx` is dropped unwritten on return, which rolls it back and
            // releases its transaction-scoped locks.
            return Ok(Sealed::Outcome(MemoryDropOutcome::NotEligible(
                MemoryDropIneligible::ForkActive,
            )));
        }
        let sealed = memory_drop::seal(&mut tx, sandbox_id, token, self.seal.ttl)
            .await
            .map_err(|error| db_error("seal the sandbox", &error))?;
        let Some(candidate) = sealed else {
            drop(tx);
            return match memory_drop::refusal(pool, sandbox_id)
                .await
                .map_err(|error| db_error("read the memory drop eligibility", &error))?
            {
                Some(MemoryDropRefusal::AlreadyDropped) => {
                    Ok(Sealed::Outcome(MemoryDropOutcome::AlreadyDropped))
                }
                // Another attempt is copying, or died mid-copy and its seal
                // hasn't expired yet.
                Some(MemoryDropRefusal::Sealed) => Err(ControllerError::service_unavailable(
                    "another memory drop of the sandbox is in progress; retry",
                )),
                Some(refusal) => Ok(Sealed::Outcome(MemoryDropOutcome::NotEligible(
                    MemoryDropIneligible::Row(refusal),
                ))),
                // It became eligible between the two reads.
                None => Err(ControllerError::service_unavailable(
                    "the sandbox changed while its memory drop was being checked; retry",
                )),
            };
        };
        tx.commit()
            .await
            .map_err(|error| db_error("commit the memory drop seal", &error))?;
        Ok(Sealed::Candidate(candidate))
    }

    /// Run `work` while renewing the seal every `renew_every`. Returns its
    /// output, or `None` when a renewal finds the seal gone; `work` is then
    /// dropped, which cancels whatever object-store call it was awaiting.
    ///
    /// `tokio::pin!` is needed because `select!` polls `work` through a
    /// `&mut` on every iteration: polling a future requires it to stay at one
    /// address (`Pin`), and pinning it on the stack here promises that
    /// without a heap allocation.
    async fn renewing_seal<T>(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        token: Uuid,
        work: impl Future<Output = T>,
    ) -> Result<Option<T>, ControllerError> {
        tokio::pin!(work);
        let mut renewals = tokio::time::interval(self.seal.renew_every);
        // The first tick completes at once; the seal was just taken.
        renewals.tick().await;
        loop {
            tokio::select! {
                output = &mut work => return Ok(Some(output)),
                _ = renewals.tick() => {
                    let renewed = memory_drop::renew(pool, sandbox_id, token, self.seal.ttl)
                        .await
                        .map_err(|error| db_error("renew the memory drop seal", &error))?;
                    if !renewed {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Lift the seal after a refusal or a failed copy. Best effort: a seal
    /// that can't be lifted expires after its TTL, and a retry takes it over.
    async fn release(&self, pool: &PgPool, sandbox_id: SandboxId, token: Uuid) {
        if let Err(error) = memory_drop::release(pool, sandbox_id, token).await {
            tracing::warn!(
                %sandbox_id,
                %error,
                "memory drop: lifting the seal failed; it lapses when it expires"
            );
        }
    }

    /// Copy the rootfs into the disk prefix and write its `metadata.json`
    /// last, as the publication marker. Returns the rootfs's logical size, or
    /// `Ok(Err(why))` when the full artifact's metadata can't project to a
    /// disk.
    async fn write_disk(
        &self,
        sandbox_id: SandboxId,
        candidate: &MemoryDropCandidate,
        plan: &CopyPlan,
    ) -> Result<Result<u64, &'static str>, MemoryDropObjectsError> {
        let metadata = self
            .objects
            .read(plan.source.parsed(), SnapshotFile::Metadata)
            .await?;
        let projection = match project_disk_metadata(
            &metadata,
            sandbox_id,
            candidate,
            plan.disk_artifact_id,
        ) {
            Ok(projection) => projection,
            Err(reason) => {
                tracing::warn!(
                    %sandbox_id,
                    snapshot_id = %candidate.full_snapshot_id,
                    reason,
                    "memory drop refused: the suspend artifact's metadata can't project to a disk"
                );
                return Ok(Err(reason));
            }
        };
        let size = self
            .objects
            .size(plan.source.parsed(), projection.rootfs)
            .await?
            .ok_or_else(|| {
                MemoryDropObjectsError(format!("no {} object", projection.rootfs.file_name()))
            })?;
        self.objects
            .copy(
                plan.source.parsed(),
                plan.dest.parsed(),
                projection.rootfs,
                size,
            )
            .await?;
        self.objects
            .write(
                plan.dest.parsed(),
                SnapshotFile::Metadata,
                // Moved, not cloned: `projection` isn't used for its bytes
                // again, only for the `Copy` size read after this.
                projection.metadata,
            )
            .await?;
        Ok(Ok(projection.rootfs_size))
    }

    /// Register the disk snapshot, re-pin the row to it and lift the seal, if
    /// the row still holds the seal's `token` and `candidate`'s pin and no
    /// fork depends on the full artifact. Anything but
    /// [`CommitOutcome::Committed`] rolls everything back.
    ///
    /// The fork check repeats the seal's for a release that predates the
    /// seal: its fork admission doesn't refuse a sealed row, so it can queue a
    /// fork after the seal was taken.
    async fn commit(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        token: Uuid,
        candidate: &MemoryDropCandidate,
        plan: &CopyPlan,
        rootfs_size: Option<u64>,
    ) -> Result<CommitOutcome, ControllerError> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|error| db_error("begin the memory drop", &error))?;
        lock_fork_source_lifecycle(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("lock the fork source lifecycle", &error))?;
        if active_fork_source_operation_exists(&mut tx, sandbox_id)
            .await
            .map_err(|error| db_error("check for an active fork", &error))?
        {
            return Ok(CommitOutcome::ForkActive);
        }
        let full = pplx_space_snapshot_store::get(&mut *tx, candidate.full_snapshot_id)
            .await
            .map_err(|error| {
                ControllerError::internal(format!("failed to load the suspend snapshot: {error}"))
            })?
            .ok_or_else(|| {
                ControllerError::internal("the pinned suspend snapshot disappeared".to_string())
            })?;
        let artifact = SnapshotArtifact::new(
            Some(plan.disk_artifact_id),
            ArtifactKind::Disk,
            ArtifactOrigin::Stop,
            None,
        )
        .map_err(|error| ControllerError::internal(format!("disk artifact identity: {error}")))?;
        let mut snapshot = NewSnapshot::new(
            plan.disk_snapshot_id,
            sandbox_id,
            SnapshotKind::Disk,
            SnapshotState::Ready,
            candidate.org_id,
            plan.dest.clone(),
        )
        .with_resources(full.vcpu(), full.memory_mib())
        .with_size_bytes(rootfs_size.and_then(|size| i64::try_from(size).ok()))
        .with_template(full.template_id().cloned(), full.content_hash().cloned())
        .with_artifact(artifact)
        .with_generation(full.generation())
        // The bytes are the full snapshot's, so the row orders with it.
        .with_timestamp(full.timestamp())
        // The control plane wrote this disk, not a node: no node holds a local
        // copy of it.
        .written_off_node();
        if let Some(execution) = full.execution_metadata() {
            // A disk carries no vCPU state, so no capture CPU constrains where
            // it boots.
            snapshot = snapshot.with_execution_metadata(execution.clone().with_host_cpu(None));
        }
        let (registered, _) = pplx_space_snapshot_store::insert(&mut tx, &snapshot)
            .await
            .map_err(|error| {
                ControllerError::internal(format!("failed to register the disk snapshot: {error}"))
            })?;
        // The ids are derived from the pin, so an earlier drop of this pin
        // registered the same row. If the row was repointed back to the full
        // since, retention may already be deleting that disk: re-pinning to it
        // would leave the row on bytes being deleted.
        if !registered.state().is_ready()
            || registered
                .storage_location()
                .is_none_or(|location| location.orphaned_at().is_some())
        {
            return Ok(CommitOutcome::DiskRetiring);
        }
        if !memory_drop::commit(
            &mut *tx,
            sandbox_id,
            token,
            candidate,
            plan.disk_artifact_id,
        )
        .await
        .map_err(|error| db_error("re-pin the sandbox", &error))?
        {
            return Ok(CommitOutcome::Lost);
        }
        tx.commit()
            .await
            .map_err(|error| db_error("commit the memory drop", &error))?;
        Ok(CommitOutcome::Committed)
    }

    /// After a lost commit, give the disk prefix to the storage sweeper unless
    /// another attempt may still use it.
    ///
    /// Two cases keep it. While the row is still pinned to the same artifact,
    /// a later attempt derives the same prefix and copies into it again. And
    /// a concurrent attempt that won registered a snapshot row for it, which
    /// [`pplx_space_snapshot_store::orphan::orphan_unowned_location`] checks
    /// in the same statement that registers the orphan.
    async fn release_lost_prefix(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        candidate: &MemoryDropCandidate,
        plan: &CopyPlan,
    ) -> Result<(), ControllerError> {
        let pin_held = memory_drop::pin_held(pool, sandbox_id, candidate)
            .await
            .map_err(|error| db_error("re-read the pin", &error))?;
        if pin_held {
            return Ok(());
        }
        pplx_space_snapshot_store::orphan::orphan_unowned_location(
            pool,
            plan.disk_snapshot_id,
            &plan.dest,
            candidate.org_id,
        )
        .await
        .map_err(|error| db_error("hand the disk prefix to the sweeper", &error))?;
        Ok(())
    }
}

/// What [`MemoryDrop::seal`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Sealed {
    /// The row is sealed; copy this.
    Candidate(MemoryDropCandidate),
    /// The row wasn't sealed, and this is the drop's answer.
    Outcome(MemoryDropOutcome),
}

/// What [`MemoryDrop::commit`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitOutcome {
    Committed,
    /// The row lost the seal or moved off the pin since it was sealed.
    Lost,
    /// A fork of the sandbox is queued or running.
    ForkActive,
    /// An earlier drop of the same pin registered this disk, and it's no
    /// longer `ready` or its location was given to the sweeper.
    DiskRetiring,
}

/// What an earlier drop of the same pin left under the disk ids
/// ([`MemoryDrop::earlier_disk`]).
enum EarlierDisk {
    /// No row and no orphan: the drop copies.
    None,
    /// A `ready` disk with an attached location. Its bytes are complete, so
    /// the drop re-pins to it without copying; `rootfs_size` is what that
    /// drop recorded.
    Ready { rootfs_size: Option<u64> },
    /// Retention or the storage sweeper is deleting the prefix.
    Retiring,
}

/// The result of [`MemoryDrop::copy_disk`].
enum Copied {
    /// The disk's bytes are in place, with the rootfs's logical size when
    /// known.
    Size(Option<u64>),
    /// The drop ends here with this outcome.
    Outcome(MemoryDropOutcome),
}

/// Where the copy reads and writes, and the ids it registers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyPlan {
    source: ExactStorageUri,
    dest: ExactStorageUri,
    disk_snapshot_id: SnapshotId,
    disk_artifact_id: LocalArtifactId,
}

impl CopyPlan {
    /// `None` when the candidate's location isn't the
    /// `.../<sandbox_id>/<snapshot_id>` prefix a node uploads each attempt
    /// under. The destination swaps the last segment for the disk snapshot
    /// id, so a location of any other shape could address another artifact.
    fn new(sandbox_id: SandboxId, candidate: &MemoryDropCandidate) -> Option<Self> {
        let source = ExactStorageUri::parse(&candidate.storage_connection_string).ok()?;
        let attempt_suffix = format!("{sandbox_id}/{}", candidate.full_snapshot_id);
        let key_prefix = source.parsed().key_prefix.as_str();
        let parent = if key_prefix == attempt_suffix {
            ""
        } else {
            key_prefix.strip_suffix(&format!("/{attempt_suffix}"))?
        };
        let disk_snapshot_id = SnapshotId::from_uuid(derived_id(candidate, b"snapshot"));
        let disk_artifact_id = LocalArtifactId::from_uuid(derived_id(candidate, b"artifact"));
        let disk_suffix = format!("{sandbox_id}/{disk_snapshot_id}");
        let mut dest = source.parsed().clone();
        dest.key_prefix = if parent.is_empty() {
            disk_suffix
        } else {
            format!("{parent}/{disk_suffix}")
        };
        let dest = ExactStorageUri::parse(&dest.to_string()).ok()?;
        Some(Self {
            source,
            dest,
            disk_snapshot_id,
            disk_artifact_id,
        })
    }
}

/// A UUID that only drops of this candidate's pinned artifact derive:
/// version 5, namespaced by the pinned artifact id, named by `kind`.
///
/// A pin is dropped at most once: a committed drop moves the pin to the
/// disk, and nothing pins that full artifact again but an operator pointing
/// the row back at it, after which a second drop finds the same ids already
/// registered and reuses them. Not by the full snapshot id: a retried backup
/// of the pinned artifact mints a new snapshot row, and an attempt that read
/// the older row would otherwise leave its copy under a prefix no later
/// attempt derives. Every backup of one pin carries that one artifact's
/// rootfs, so two attempts copying into one prefix write the same disk.
fn derived_id(candidate: &MemoryDropCandidate, kind: &[u8]) -> Uuid {
    let mut name = b"memory-drop/".to_vec();
    name.extend_from_slice(kind);
    Uuid::new_v5(&candidate.pinned_artifact_id.as_uuid(), &name)
}

/// The disk artifact's `metadata.json` and the rootfs object it describes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DiskProjection {
    metadata: Vec<u8>,
    rootfs: SnapshotFile,
    rootfs_size: u64,
}

/// Project the full artifact's `metadata.json` onto the disk artifact, the
/// way a fork projects a disk: same rootfs and environment, `disk` `stop`
/// identity, sizes of the rootfs alone, and nothing that names the full
/// artifact's node-local bundle. Fields this doesn't know are kept.
///
/// `Err` names why the artifact can't be projected; the drop then refuses.
fn project_disk_metadata(
    bytes: &[u8],
    sandbox_id: SandboxId,
    candidate: &MemoryDropCandidate,
    disk_artifact_id: LocalArtifactId,
) -> Result<DiskProjection, &'static str> {
    let mut metadata: Value = serde_json::from_slice(bytes).map_err(|_| "not JSON")?;
    let object = metadata.as_object_mut().ok_or("not a JSON object")?;
    if object.get("version").and_then(Value::as_u64) != Some(SNAPSHOT_METADATA_VERSION) {
        return Err("unsupported metadata version");
    }
    if object.get("sandbox_id").and_then(Value::as_str) != Some(sandbox_id.to_string().as_str()) {
        return Err("metadata names another sandbox");
    }
    if object.get("encrypted").and_then(Value::as_bool) == Some(true) {
        return Err("encrypted artifact");
    }
    if object.get("artifact_kind").and_then(Value::as_str) != Some(ArtifactKind::Full.as_str()) {
        return Err("not a full artifact");
    }
    let pinned = object
        .get("local_artifact_id")
        .and_then(Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok());
    if pinned != Some(candidate.pinned_artifact_id.as_uuid()) {
        return Err("metadata names another local artifact");
    }
    let rootfs_size = object
        .get("rootfs_size_bytes")
        .and_then(Value::as_u64)
        .ok_or("rootfs size unknown")?;
    let rootfs = match object.get("rootfs_diff_format") {
        None | Some(Value::Null) => SnapshotFile::Rootfs,
        Some(Value::String(format)) if format == ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1 => {
            SnapshotFile::RootfsBtrfsSend
        }
        Some(_) => return Err("unknown rootfs format"),
    };
    let execution: SnapshotExecutionMetadata = object
        .get("execution")
        .cloned()
        .map(serde_json::from_value)
        .ok_or("no execution metadata")?
        .map_err(|_| "invalid execution metadata")?;
    let execution = serde_json::to_value(execution.with_host_cpu(None))
        .map_err(|_| "invalid execution metadata")?;

    object.insert("execution".to_string(), execution);
    object.insert("size_bytes".to_string(), rootfs_size.into());
    object.insert(
        "artifact_kind".to_string(),
        ArtifactKind::Disk.as_str().into(),
    );
    object.insert(
        "artifact_origin".to_string(),
        ArtifactOrigin::Stop.as_str().into(),
    );
    object.insert(
        "local_artifact_id".to_string(),
        disk_artifact_id.to_string().into(),
    );
    // The disk has the stream, never the full artifact's node-local bundle.
    object.remove("rootfs_send_source_id");
    object.remove("artifact_snapshot_name");
    let metadata = serde_json::to_vec_pretty(&metadata).map_err(|_| "unserializable")?;
    Ok(DiskProjection {
        metadata,
        rootfs,
        rootfs_size,
    })
}

/// Whether a queued or running fork of the sandbox may read its full
/// artifact, for [`MemoryDrop::eligibility`]; the seal checks again under the
/// lock.
async fn fork_active(pool: &PgPool, sandbox_id: SandboxId) -> Result<bool, ControllerError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| db_error("begin the memory drop fork check", &error))?;
    lock_fork_source_lifecycle(&mut tx, sandbox_id)
        .await
        .map_err(|error| db_error("lock the fork source lifecycle", &error))?;
    // `tx` is dropped unwritten at the end of this function, which rolls it
    // back and releases the transaction-scoped advisory lock.
    active_fork_source_operation_exists(&mut tx, sandbox_id)
        .await
        .map_err(|error| db_error("check for an active fork", &error))
}

fn db_error(what: &str, error: &sqlx::Error) -> ControllerError {
    crate::controllers::db_contention_error(&format!("failed to {what}"), error)
}

#[cfg(test)]
mod tests;
