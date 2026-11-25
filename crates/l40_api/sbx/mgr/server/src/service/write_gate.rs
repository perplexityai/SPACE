//! The node's one admission point. Every node-local writer of a sandbox's
//! artifacts is admitted here, naming its class ([`NodeWriter`]), and the
//! activation seal ([`crate::activation_seal`]) decides it by the class's
//! rule. Every VM activation is admitted here too
//! ([`SandboxManagerService::admit_vm_activation`]). Nothing else asks the
//! seal: its admissions take a [`GateKey`], which only this module makes.
//!
//! A sandbox's artifact lock serializes the publication and consumption of
//! its immutable local artifacts: live full-artifact capture takes it after
//! the sandbox's wake lock, and disk publication, direct restore and backup
//! admission take it directly. The order is the wake lock, then this lock,
//! then the seal's state lock, then the database's mutex: a writer whose rule
//! needs its sandbox's row reads it under the seal's state lock. The registry
//! of these locks is the gate's own, and each entry point takes only the
//! classes it serves: a writer decided under the lock by a [`LockedWriter`],
//! one admitted without it by an [`UnlockedWriter`], a writer a drain is
//! compatible with by its [`DrainCompatible`] reason, an activation's lock by
//! the activation's kind, and a reader by its [`Reader`]. A raw artifact lock
//! taken outside the gate doesn't compile. The gate can't tell a reader from
//! a writer or check a drain-compatible reason; a call site's class is
//! checked in review.
//!
//! A drain relies on one invariant: every node-local writer that changes a
//! sandbox's artifacts, disk or recorded state while the sandbox has no VM
//! takes the lock through the gate, is decided under it, and holds it until
//! its last write commits. A drain finds a sandbox evacuated only under the
//! same lock, reading its row again there
//! (`SandboxManagerService::confirm_finished_rows`), so a writer admitted
//! before the seal either finishes before that read or keeps the row
//! pending, and one that asks after the seal is refused by its class. Three
//! writers fall outside what the lock shows a drain: the idle auto-stop,
//! which takes no artifact lock; a sandbox import, which has no row until it
//! reserves one; and a runtime upgrade's stage of a sandbox absent from the
//! node, which has no row until it adopts one. Each counts as an artifact
//! producer in flight until it ends, which keeps a drain pending.
//!
//! Each class's rule is the one its call site applied before the gate. A
//! node whose seal is unknown admits every writer, as a node never sealed
//! does; only a VM activation is refused then. A class that is always
//! admitted reads neither the seal nor the database, and a rule that needs
//! the sandbox's row reads it only on a node known sealed, so a node never
//! sealed admits as it did before the gate.

use std::sync::Arc;

use pplx_space_snapshot_model::ArtifactKind;
use tonic::Status;

use super::SandboxManagerService;
use super::start_support::{ActivationAdmissionKind, ActivationAdmissionPermit};
use crate::activation_seal::{
    ActivationInFlightToken, ActivationSeal, ArtifactProducerToken, SealReading, SealRefusal,
};
use crate::auto_lifecycle::{AutoLifecycleCoordinator, LifecycleLocks, LifecycleMutexGuard};
use crate::db::SharedConnection;
use crate::state::{SandboxState, SandboxStatus};
use pplx_space_sandbox_metrics::admission::AdmissionPermitOutcome;

/// The gate's proof that it is the one asking the seal: only this module
/// makes one, so no other code can ask the seal to admit anything.
pub(crate) struct GateKey(());

/// The key the gate asks the seal with.
const KEY: GateKey = GateKey(());

#[cfg(test)]
impl GateKey {
    /// A key for the seal's own tests, which ask it directly.
    pub(crate) const FOR_TEST: Self = Self(());
}

/// Every class of node-local writer the gate admits, and the rule the seal
/// applies to each. A call site names its class through the entry point
/// that serves it ([`LockedWriter`], [`UnlockedWriter`], [`DrainCompatible`],
/// an activation's kind), so what the seal does to it is read here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeWriter {
    /// Leaves an artifact, or rewrites a disk, that a drain must make
    /// durable. A node known sealed refuses one for a sandbox without a VM:
    /// a drain may already have found everything there durable. A sandbox
    /// with a VM keeps a drain pending until its suspend, which waits for
    /// the same lock, so its producer is admitted sealed too. Counted in
    /// flight until its guard drops.
    Producer(Producer),
    /// The idle auto-stop. It leaves the sandbox stopped on the node, which
    /// a v2 drain doesn't move, so a node known sealed refuses it whether or
    /// not the sandbox has a VM. It takes no artifact lock and is counted in
    /// flight until its state write commits.
    IdleStop,
    /// A runtime upgrade's stage of a sandbox absent from the node from its
    /// suspend checkpoint, which leaves the sandbox suspended there. A node
    /// known sealed refuses it: the upgrade lands on another node once the
    /// drain has made the sandbox durable. It takes no artifact lock, and
    /// has no row until it adopts one, so it is counted in flight until the
    /// stage returns. The upgrade's fence decides again
    /// ([`Self::RuntimeUpgrade`]).
    UpgradeStage,
    /// A runtime upgrade's fence, under the artifact lock, before it
    /// finishes and preserves the disk. A node known sealed refuses one
    /// while the sandbox has no VM and isn't stopped: the upgrade lands on
    /// another node once the drain has made the sandbox durable. A started
    /// or paused sandbox's is admitted, as is a stopped sandbox's, which a
    /// v2 drain doesn't move. Not counted: its boot is an activation.
    RuntimeUpgrade,
    /// The lock a VM activation takes before
    /// [`SandboxManagerService::admit_vm_activation`] admits it, or
    /// [`NodeWriteGate::check_activation`] refuses it first: the lock itself
    /// is always admitted.
    Activation(ActivationAdmissionKind),
    /// Work a drain wants done, or that leaves it nothing new to make
    /// durable: always admitted.
    DrainCompatible(DrainCompatible),
}

/// The writers decided under the artifact lock the gate takes for them
/// ([`NodeWriteGate::lock_for_write`], [`NodeWriteGate::lock_for_publication`])
/// or that they already hold ([`ArtifactReadGuard::admit`],
/// [`ArtifactWriteGuard::admit`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LockedWriter {
    /// [`NodeWriter::Producer`].
    Producer(Producer),
    /// [`NodeWriter::RuntimeUpgrade`].
    RuntimeUpgrade,
}

impl From<LockedWriter> for NodeWriter {
    fn from(writer: LockedWriter) -> Self {
        match writer {
            LockedWriter::Producer(producer) => Self::Producer(producer),
            LockedWriter::RuntimeUpgrade => Self::RuntimeUpgrade,
        }
    }
}

/// The writers admitted without an artifact lock ([`NodeWriteGate::admit`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnlockedWriter {
    /// [`NodeWriter::IdleStop`].
    IdleStop,
    /// [`NodeWriter::UpgradeStage`].
    UpgradeStage,
}

impl From<UnlockedWriter> for NodeWriter {
    fn from(writer: UnlockedWriter) -> Self {
        match writer {
            UnlockedWriter::IdleStop => Self::IdleStop,
            UnlockedWriter::UpgradeStage => Self::UpgradeStage,
        }
    }
}

/// The node-local producers, by what they leave.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Producer {
    /// `CreateSnapshot` without a creation identity: a new named snapshot.
    Snapshot,
    /// `CreateSnapshot` with a creation identity, once its retry of one that
    /// already completed has been answered with the earlier result.
    OperationSnapshot,
    /// `ImportSnapshot` into a sandbox already on the node.
    SnapshotImport,
    /// `ImportSandbox`: a new sandbox, which has no VM.
    SandboxImport,
    /// `EnqueueArtifactBackup`'s publication of an artifact it adopts: a
    /// named snapshot without a row, a suspended sandbox's unpinned capture,
    /// a stopped sandbox's disk.
    BackupPublication,
    /// The scheduler's checkpoint.
    ScheduledCheckpoint,
    /// The scheduler's disk snapshot.
    ScheduledDiskSnapshot,
    /// `Rollback` without a restart: it rewrites the disk, which a drain may
    /// already have reported durable, and leaves the sandbox stopped. One
    /// that restarts is an [`NodeWriter::Activation`].
    Rollback,
}

impl Producer {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 8] = [
        Self::Snapshot,
        Self::OperationSnapshot,
        Self::SnapshotImport,
        Self::SandboxImport,
        Self::BackupPublication,
        Self::ScheduledCheckpoint,
        Self::ScheduledDiskSnapshot,
        Self::Rollback,
    ];

    /// The producer as a refusal names it.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot | Self::OperationSnapshot => "snapshot",
            Self::SnapshotImport => "snapshot import",
            Self::SandboxImport => "sandbox import",
            Self::BackupPublication => "artifact backup's publication",
            Self::ScheduledCheckpoint => "scheduled checkpoint",
            Self::ScheduledDiskSnapshot => "scheduled disk snapshot",
            Self::Rollback => "rollback",
        }
    }
}

impl std::fmt::Display for Producer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The writers a drain is compatible with, each with why.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainCompatible {
    /// `Suspend`: makes the sandbox durable, which a drain wants.
    Suspend,
    /// The idle auto-suspend, as [`Self::Suspend`].
    IdleSuspend,
    /// The drain's own suspend.
    DrainSuspend,
    /// A Kubernetes teardown's suspend, as [`Self::Suspend`].
    TeardownSuspend,
    /// Recovery finishing an interrupted suspend's capture or publication.
    SuspendRecovery,
    /// `Pause`: the VM stays, and a drain still suspends it.
    Pause,
    /// Guest maintenance of a running VM, which a drain's suspend waits for
    /// and settles under this lock.
    GuestMaintenance,
    /// `EnqueueArtifactBackup` of an artifact already published, which it
    /// makes durable. Its publication of one it adopts is a
    /// [`Producer::BackupPublication`], admitted under the same lock.
    BackupUpload,
    /// The drain's own publication and upload of a suspended sandbox's
    /// snapshot.
    DrainPublication,
    /// A v1 drain's reupload of a suspended sandbox's snapshot, which makes
    /// it durable.
    SuspendReupload,
    /// `DeleteSnapshots`: a deleted snapshot leaves nothing to make durable.
    SnapshotDelete,
    /// A cleanup's admission of a discard and its finalization.
    Cleanup,
    /// The eviction of a sandbox whose artifacts have a durable copy, or
    /// whose cluster row is gone.
    Eviction,
    /// Garbage collection of evictable or superseded artifacts: the reaper
    /// and the scheduler's retention.
    GarbageCollection,
    /// A verified runtime upgrade's retirement of the checkpoint it
    /// superseded.
    CheckpointRetirement,
    /// A runtime upgrade's resume of a stage interrupted after it adopted
    /// the row, which a drain already waits on: the row stays pending until
    /// the stage settles, and refused, the drain would wait on it until the
    /// undrain. The resume launches nothing.
    StageResume,
    /// The deletion of staging no artifact owns: an abandoned suspend's or
    /// snapshot import's.
    StagingCleanup,
    /// The watchdog's recovery of a pause-failed quarantine whose VM it saw
    /// running.
    WatchdogRecovery,
    /// A failed runtime upgrade's restore of the disk it preserved: refused,
    /// it would leave the disk the failed boot wrote.
    UpgradeRollback,
}

#[cfg(test)]
impl DrainCompatible {
    pub(crate) const ALL: [Self; 19] = [
        Self::Suspend,
        Self::IdleSuspend,
        Self::DrainSuspend,
        Self::TeardownSuspend,
        Self::SuspendRecovery,
        Self::Pause,
        Self::GuestMaintenance,
        Self::BackupUpload,
        Self::DrainPublication,
        Self::SuspendReupload,
        Self::SnapshotDelete,
        Self::Cleanup,
        Self::Eviction,
        Self::GarbageCollection,
        Self::CheckpointRetirement,
        Self::StageResume,
        Self::StagingCleanup,
        Self::WatchdogRecovery,
        Self::UpgradeRollback,
    ];
}

/// Why a call site takes a sandbox's artifact lock without writing under
/// it. Every reader is admitted; the reason names the site.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reader {
    /// A drain's confirmation that a sandbox is evacuated
    /// (`SandboxManagerService::confirm_finished_rows`).
    DrainConfirm,
    /// `ExportSandbox` and `ExportSnapshot`, which stream what the lock
    /// keeps still.
    Export,
    /// `ListSnapshots`'s read of a sandbox's snapshots.
    Listing,
    /// A writer's reads before it knows its class, as a snapshot's and an
    /// import's are. It writes only once [`ArtifactReadGuard::admit`] has
    /// admitted it.
    BeforeAdmission,
    /// A test holding the lock as another holder would.
    #[cfg(test)]
    Test,
}

/// What the seal does to a class of writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SealRule {
    /// Always admitted; neither the seal nor the row is read.
    Admit,
    /// Refused, as [`Refused`] says, while the node is known sealed and the
    /// sandbox has no VM.
    RefuseSealedWithoutVm(Refused),
    /// Refused while the node is known sealed and the sandbox has no VM and
    /// isn't stopped.
    RefuseSealedWithoutVmUnlessStopped(Refused),
    /// Refused while the node is known sealed; the row isn't read.
    RefuseSealed(Refused),
}

/// How the seal refuses a writer: the work it names, and the refusal that
/// tells its caller what to do next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Refused {
    /// A runtime upgrade's step ([`SealRefusal::RuntimeUpgrade`]).
    RuntimeUpgrade(&'static str),
    /// Another writer's work ([`SealRefusal::Writer`]).
    Writer(&'static str),
}

impl Refused {
    fn sealed(self, sandbox_id: &str, generation: u64) -> SealRefusal {
        let sandbox_id = sandbox_id.to_owned();
        match self {
            Self::RuntimeUpgrade(step) => SealRefusal::RuntimeUpgrade {
                step,
                sandbox_id,
                generation,
            },
            Self::Writer(work) => SealRefusal::Writer {
                work,
                sandbox_id,
                generation,
            },
        }
    }

    fn row_unreadable(self, sandbox_id: &str, error: String) -> SealRefusal {
        let (Self::RuntimeUpgrade(work) | Self::Writer(work)) = self;
        SealRefusal::RowUnreadable {
            work,
            sandbox_id: sandbox_id.to_owned(),
            error,
        }
    }
}

impl NodeWriter {
    const fn rule(self) -> SealRule {
        match self {
            Self::Producer(producer) => {
                SealRule::RefuseSealedWithoutVm(Refused::Writer(producer.as_str()))
            }
            Self::IdleStop => SealRule::RefuseSealed(Refused::Writer("idle auto-stop")),
            Self::UpgradeStage => {
                SealRule::RefuseSealed(Refused::RuntimeUpgrade("runtime upgrade's stage"))
            }
            Self::RuntimeUpgrade => SealRule::RefuseSealedWithoutVmUnlessStopped(
                Refused::RuntimeUpgrade("runtime upgrade"),
            ),
            Self::Activation(_) | Self::DrainCompatible(_) => SealRule::Admit,
        }
    }

    /// Whether an admitted writer of this class counts in flight as an
    /// artifact producer, which a drain reports pending.
    const fn counted(self) -> bool {
        matches!(
            self,
            Self::Producer(_) | Self::IdleStop | Self::UpgradeStage
        )
    }
}

/// What a rule reads of a sandbox's row, and only on a node known sealed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Residency {
    /// `started` or `paused`, no cleanup owning the row.
    WithVm,
    /// `stopped`, no cleanup owning the row.
    Stopped,
    /// Anything else, a missing row included.
    WithoutVm,
}

impl Residency {
    fn of(status: Option<SandboxStatus>) -> Self {
        match status.and_then(SandboxStatus::unowned) {
            Some(SandboxState::Started | SandboxState::Paused) => Self::WithVm,
            Some(SandboxState::Stopped) => Self::Stopped,
            _ => Self::WithoutVm,
        }
    }
}

/// `writer`'s admission for `sandbox_id` on the seal as `reading` stands,
/// reading the sandbox's row through `residency` only when the node is
/// known sealed and the rule needs it: whether it counts in flight, or its
/// refusal. A row that can't be read refuses it as retryable.
fn decide(
    writer: NodeWriter,
    sandbox_id: &str,
    reading: SealReading,
    residency: impl FnOnce() -> Result<Residency, String>,
) -> Result<bool, SealRefusal> {
    let SealReading::Sealed { generation } = reading else {
        return Ok(writer.counted());
    };
    let refused = match writer.rule() {
        SealRule::Admit => return Ok(writer.counted()),
        SealRule::RefuseSealed(refused) => refused,
        SealRule::RefuseSealedWithoutVm(refused) => match residency() {
            Ok(Residency::WithVm) => return Ok(writer.counted()),
            Ok(Residency::Stopped | Residency::WithoutVm) => refused,
            Err(error) => return Err(refused.row_unreadable(sandbox_id, error)),
        },
        SealRule::RefuseSealedWithoutVmUnlessStopped(refused) => match residency() {
            Ok(Residency::WithVm | Residency::Stopped) => return Ok(writer.counted()),
            Ok(Residency::WithoutVm) => refused,
            Err(error) => return Err(refused.row_unreadable(sandbox_id, error)),
        },
    };
    Err(refused.sealed(sandbox_id, generation))
}

/// What deciding a writer reads: the seal, and the sandbox's row on a node
/// known sealed.
#[derive(Clone)]
struct Admission {
    seal: Arc<ActivationSeal>,
    db: SharedConnection,
}

impl Admission {
    fn admit(
        &self,
        sandbox_id: &str,
        writer: NodeWriter,
    ) -> Result<Option<ArtifactProducerToken>, SealRefusal> {
        if writer.rule() == SealRule::Admit && !writer.counted() {
            return Ok(None);
        }
        self.seal.admit_writer(&KEY, sandbox_id, |reading| {
            decide(writer, sandbox_id, reading, || self.residency(sandbox_id))
        })
    }

    /// The sandbox's row as a rule reads it, or why it couldn't be read.
    fn residency(&self, sandbox_id: &str) -> Result<Residency, String> {
        let conn = self
            .db
            .lock()
            .map_err(|_| "the sandbox database mutex is poisoned".to_owned())?;
        crate::db::read::sandbox(&conn, sandbox_id)
            .map(|row| Residency::of(row.map(|row| row.state)))
            .map_err(|error| error.to_string())
    }
}

/// A sandbox's artifact lock, held for a writer the gate admitted. Dropping
/// it releases the writer's producer token, then the lock.
#[must_use = "dropping the guard releases the sandbox's artifact lock"]
pub(crate) struct ArtifactWriteGuard {
    producer: Option<ArtifactProducerToken>,
    _lock: LifecycleMutexGuard,
    writer: NodeWriter,
    sandbox_id: String,
    admission: Admission,
}

impl ArtifactWriteGuard {
    /// Admit `writer` too, under the lock this guard holds: a writer whose
    /// class is decided only once it holds the lock, as a backup's
    /// publication of what it adopts is. One that counts is counted until
    /// the guard drops.
    pub(crate) fn admit(&mut self, writer: LockedWriter) -> Result<(), SealRefusal> {
        let writer = NodeWriter::from(writer);
        if let Some(token) = self.admission.admit(&self.sandbox_id, writer)? {
            self.producer = Some(token);
        }
        self.writer = writer;
        Ok(())
    }
}

impl std::fmt::Debug for ArtifactWriteGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactWriteGuard")
            .field("sandbox_id", &self.sandbox_id)
            .field("writer", &self.writer)
            .field("counted", &self.producer.is_some())
            .finish_non_exhaustive()
    }
}

/// A sandbox's artifact lock, held to read: no writer is admitted, and what
/// the lock guards stays as it is until the guard drops.
#[must_use = "dropping the guard releases the sandbox's artifact lock"]
pub(crate) struct ArtifactReadGuard {
    lock: LifecycleMutexGuard,
    reader: Reader,
    sandbox_id: String,
    admission: Admission,
}

impl ArtifactReadGuard {
    /// Become a writer of class `writer` under the lock already held: a
    /// writer that reads under the lock before it knows it writes, as a
    /// retried operation snapshot does. A refusal releases the lock.
    pub(crate) fn admit(self, writer: LockedWriter) -> Result<ArtifactWriteGuard, SealRefusal> {
        let writer = NodeWriter::from(writer);
        let producer = self.admission.admit(&self.sandbox_id, writer)?;
        Ok(ArtifactWriteGuard {
            producer,
            _lock: self.lock,
            writer,
            sandbox_id: self.sandbox_id,
            admission: self.admission,
        })
    }
}

impl std::fmt::Debug for ArtifactReadGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactReadGuard")
            .field("sandbox_id", &self.sandbox_id)
            .field("reader", &self.reader)
            .finish_non_exhaustive()
    }
}

/// A writer admitted without the artifact lock ([`NodeWriteGate::admit`]),
/// counted in flight until dropped if its class counts.
#[derive(Debug)]
#[must_use = "dropping the admission ends the writer's in-flight count"]
pub(crate) struct WriteAdmission {
    _producer: Option<ArtifactProducerToken>,
}

/// The node's write gate: the artifact locks, and the seal that decides
/// each writer under its lock. Cloned handles share both.
#[derive(Clone)]
pub(crate) struct NodeWriteGate {
    admission: Admission,
    artifact_locks: LifecycleLocks,
    /// The wake locks a full publication takes before the artifact lock.
    coordinator: AutoLifecycleCoordinator,
}

impl NodeWriteGate {
    /// The gate of a node whose seal is `seal`, reading rows from `db`, with
    /// the wake locks of `coordinator`.
    pub(crate) fn new(
        seal: Arc<ActivationSeal>,
        db: SharedConnection,
        coordinator: AutoLifecycleCoordinator,
    ) -> Self {
        Self {
            admission: Admission { seal, db },
            artifact_locks: LifecycleLocks::default(),
            coordinator,
        }
    }

    /// A gate on a node never sealed, for code under test that has no
    /// service, with the wake locks of `coordinator`.
    #[cfg(test)]
    pub(crate) fn unsealed_for_test(coordinator: AutoLifecycleCoordinator) -> Self {
        Self::new(
            Arc::new(ActivationSeal::unsealed()),
            SharedConnection::new(crate::db::test_conn()),
            coordinator,
        )
    }

    /// The seal this gate decides by.
    #[cfg(test)]
    pub(crate) fn seal(&self) -> &Arc<ActivationSeal> {
        &self.admission.seal
    }

    /// How many handles keep `sandbox_id`'s artifact lock alive: its holder
    /// and each caller waiting for it, so a test can tell one has started
    /// waiting.
    #[cfg(test)]
    pub(crate) fn artifact_lock_handles(&self, sandbox_id: &str) -> usize {
        Arc::strong_count(&self.artifact_locks.get(sandbox_id)) - 1
    }

    fn held(
        &self,
        lock: LifecycleMutexGuard,
        sandbox_id: &str,
        reader: Reader,
    ) -> ArtifactReadGuard {
        ArtifactReadGuard {
            lock,
            reader,
            sandbox_id: sandbox_id.to_owned(),
            admission: self.admission.clone(),
        }
    }

    /// An always-admitted writer's guard over `lock`.
    fn admitted(
        &self,
        lock: LifecycleMutexGuard,
        sandbox_id: &str,
        writer: NodeWriter,
    ) -> ArtifactWriteGuard {
        ArtifactWriteGuard {
            producer: None,
            _lock: lock,
            writer,
            sandbox_id: sandbox_id.to_owned(),
            admission: self.admission.clone(),
        }
    }

    /// Wait for `sandbox_id`'s artifact lock, then admit `writer` under it.
    /// A refusal releases the lock.
    pub(crate) async fn lock_for_write(
        &self,
        sandbox_id: &str,
        writer: LockedWriter,
    ) -> Result<ArtifactWriteGuard, SealRefusal> {
        self.lock_for_read(sandbox_id, Reader::BeforeAdmission)
            .await
            .admit(writer)
    }

    /// Wait for `sandbox_id`'s artifact lock for `writer`, which a drain is
    /// compatible with.
    pub(crate) async fn lock(
        &self,
        sandbox_id: &str,
        writer: DrainCompatible,
    ) -> ArtifactWriteGuard {
        let lock = self.artifact_locks.get(sandbox_id).lock_owned().await;
        self.admitted(lock, sandbox_id, NodeWriter::DrainCompatible(writer))
    }

    /// [`Self::lock`] for a capture, which asks optional maintenance waiting
    /// on the lock to yield first.
    pub(crate) async fn lock_for_capture(
        &self,
        sandbox_id: &str,
        writer: DrainCompatible,
    ) -> ArtifactWriteGuard {
        let lock = self.artifact_locks.get(sandbox_id).lock_for_capture().await;
        self.admitted(lock, sandbox_id, NodeWriter::DrainCompatible(writer))
    }

    /// [`Self::lock`] without waiting: `None` while another holds the lock.
    pub(crate) fn try_lock(
        &self,
        sandbox_id: &str,
        writer: DrainCompatible,
    ) -> Option<ArtifactWriteGuard> {
        let lock = self.artifact_locks.get(sandbox_id).try_lock_owned().ok()?;
        Some(self.admitted(lock, sandbox_id, NodeWriter::DrainCompatible(writer)))
    }

    /// [`Self::try_lock`] for optional maintenance, with the capture
    /// requests made from before the try on, so it can yield to a capture
    /// that asks while it holds the lock.
    pub(crate) fn try_lock_yielding_to_capture(
        &self,
        sandbox_id: &str,
        writer: DrainCompatible,
    ) -> Option<(ArtifactWriteGuard, tokio::sync::watch::Receiver<()>)> {
        let artifact = self.artifact_locks.get(sandbox_id);
        let capture_requests = artifact.subscribe_capture_requests();
        let lock = artifact.try_lock_owned().ok()?;
        Some((
            self.admitted(lock, sandbox_id, NodeWriter::DrainCompatible(writer)),
            capture_requests,
        ))
    }

    /// Wait for `sandbox_id`'s artifact lock for a VM activation of `kind`,
    /// which its own admission decides.
    pub(crate) async fn lock_for_activation(
        &self,
        sandbox_id: &str,
        kind: ActivationAdmissionKind,
    ) -> ArtifactWriteGuard {
        let lock = self.artifact_locks.get(sandbox_id).lock_owned().await;
        self.admitted(lock, sandbox_id, NodeWriter::Activation(kind))
    }

    /// The locks a publication of an `artifact_kind` artifact takes: for a
    /// full one the wake lock, then the artifact lock as a capture; for a
    /// disk one the artifact lock alone. Then `writer` is admitted under
    /// them; a refusal releases both.
    pub(crate) async fn lock_for_publication(
        &self,
        sandbox_id: &str,
        artifact_kind: ArtifactKind,
        writer: LockedWriter,
    ) -> Result<(Option<LifecycleMutexGuard>, ArtifactWriteGuard), SealRefusal> {
        let (wake, held) = self
            .lock_publication_for_read(sandbox_id, artifact_kind, Reader::BeforeAdmission)
            .await;
        Ok((wake, held.admit(writer)?))
    }

    /// [`Self::lock_for_publication`]'s locks for `reader`, admitting no
    /// writer yet.
    pub(crate) async fn lock_publication_for_read(
        &self,
        sandbox_id: &str,
        artifact_kind: ArtifactKind,
        reader: Reader,
    ) -> (Option<LifecycleMutexGuard>, ArtifactReadGuard) {
        let full = artifact_kind == ArtifactKind::Full;
        let wake = if full {
            Some(self.coordinator.wake_lock(sandbox_id).lock_owned().await)
        } else {
            None
        };
        let artifact = self.artifact_locks.get(sandbox_id);
        let lock = if full {
            artifact.lock_for_capture().await
        } else {
            artifact.lock_owned().await
        };
        (wake, self.held(lock, sandbox_id, reader))
    }

    /// Admit `writer` without the artifact lock: the idle auto-stop, and a
    /// runtime upgrade's stage of a sandbox absent from the node.
    pub(crate) fn admit(
        &self,
        sandbox_id: &str,
        writer: UnlockedWriter,
    ) -> Result<WriteAdmission, SealRefusal> {
        Ok(WriteAdmission {
            _producer: self.admission.admit(sandbox_id, writer.into())?,
        })
    }

    /// Wait for `sandbox_id`'s artifact lock for `reader`, to read what it
    /// guards.
    pub(crate) async fn lock_for_read(
        &self,
        sandbox_id: &str,
        reader: Reader,
    ) -> ArtifactReadGuard {
        let lock = self.artifact_locks.get(sandbox_id).lock_owned().await;
        self.held(lock, sandbox_id, reader)
    }

    /// [`Self::lock_for_read`] without waiting: `None` while another holds
    /// the lock.
    pub(crate) fn try_lock_for_read(
        &self,
        sandbox_id: &str,
        reader: Reader,
    ) -> Option<ArtifactReadGuard> {
        let lock = self.artifact_locks.get(sandbox_id).try_lock_owned().ok()?;
        Some(self.held(lock, sandbox_id, reader))
    }

    /// The capture requests made on `sandbox_id`'s artifact lock from now
    /// on, for maintenance running under the lock its caller holds.
    pub(crate) fn subscribe_capture_requests(
        &self,
        sandbox_id: &str,
    ) -> tokio::sync::watch::Receiver<()> {
        self.artifact_locks
            .get(sandbox_id)
            .subscribe_capture_requests()
    }

    /// Refuse a VM activation of `kind` while the node is sealed or its seal
    /// unknown, before the work that leads to it: it is admitted, and
    /// counted, only by [`SandboxManagerService::admit_vm_activation`].
    pub(crate) fn check_activation(
        &self,
        kind: ActivationAdmissionKind,
        sandbox_id: &str,
    ) -> Result<(), SealRefusal> {
        self.admission.seal.check(&KEY, kind.as_str(), sandbox_id)
    }

    fn admit_activation(
        &self,
        kind: ActivationAdmissionKind,
        sandbox_id: &str,
    ) -> Result<ActivationInFlightToken, SealRefusal> {
        self.admission.seal.admit(&KEY, kind.as_str(), sandbox_id)
    }
}

impl SandboxManagerService {
    /// The launch-time admission for a cold boot, a snapshot restore and the
    /// resume of a resident paused VM. Each path calls this before it sends
    /// node-manager `LaunchSandbox` (cold boot, restore) or `ResumeSandbox`
    /// (resident resume), and holds the permit across that request. A
    /// restore's permit ends before the restore loads the snapshot and resumes
    /// the VM. Two resumes do not pass here: a checkpoint's resume of the VM it
    /// paused, and a stop's resume of a paused VM so its guest can flush. The
    /// seal refuses neither, and a drain need not count them in flight: each
    /// runs while the sandbox's row is in a state that keeps the drain from
    /// being done.
    ///
    /// Cold boots and restores wait for
    /// [`super::start_support::ActivationAdmissionLimiter`] capacity and are
    /// refused when it stays exhausted. A resident resume takes an
    /// [`ActivationAdmissionPermit::Unlimited`] permit at once.
    ///
    /// While the node's activations are sealed, every kind is refused with the
    /// seal's marker. The seal decides here rather than when a request takes
    /// its state-writer lease, so a request that got past its lease or the wake
    /// lock before the seal landed is still refused, and so is nested work
    /// running under a caller's lease. A restore takes the activation lock
    /// before it gets here, so one waiting on that lock is refused too. A cold
    /// boot is admitted before it takes the activation lock, so one waiting
    /// there was already admitted and counts as in flight. The seal decides
    /// again after any limiter wait, so an activation queued for capacity when
    /// the seal lands is refused too.
    pub(super) async fn admit_vm_activation(
        &self,
        kind: ActivationAdmissionKind,
        sandbox_id: &str,
    ) -> Result<VmActivationAdmission, Status> {
        #[cfg(test)]
        if let Ok(admitted) = super::start_support::REQUESTED_VM_ACTIVATIONS.try_with(Arc::clone) {
            admitted.lock().unwrap().push(kind);
        }
        // Refuse before any limiter wait, which a sealed node has no use for.
        self.write_gate.check_activation(kind, sandbox_id)?;
        let permit = match kind.limited() {
            Some(limited) => {
                self.activation_admission_limiter
                    .acquire(limited, sandbox_id)
                    .await?
            }
            None => ActivationAdmissionPermit::Unlimited,
        };
        match self.write_gate.admit_activation(kind, sandbox_id) {
            Ok(in_flight) => Ok(VmActivationAdmission { permit, in_flight }),
            Err(refusal) => {
                permit.finish(AdmissionPermitOutcome::Cancelled);
                Err(refusal.into())
            }
        }
    }
}

/// What [`SandboxManagerService::admit_vm_activation`] admits an activation
/// with: the limiter's permit, held across the node-manager request that
/// launches or resumes the VM, and the in-flight token, held until the
/// activation's last step finishes or fails.
#[derive(Debug)]
#[must_use = "dropping the admission ends the activation's permit and in-flight count"]
pub(super) struct VmActivationAdmission {
    permit: ActivationAdmissionPermit,
    in_flight: ActivationInFlightToken,
}

impl VmActivationAdmission {
    #[cfg(test)]
    pub(super) fn permit(&self) -> &ActivationAdmissionPermit {
        &self.permit
    }

    /// End an activation that stops before it launches anything: the permit
    /// with `outcome`, and the in-flight token with it.
    pub(super) fn finish(self, outcome: AdmissionPermitOutcome) {
        self.permit.finish(outcome);
    }

    pub(super) fn into_parts(self) -> (ActivationAdmissionPermit, ActivationInFlightToken) {
        (self.permit, self.in_flight)
    }
}

#[cfg(test)]
mod tests;
