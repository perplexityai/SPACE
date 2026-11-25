//! Node-local runtime upgrades (`UpgradeRuntime`).
//!
//! An upgrade moves a stopped or suspended sandbox to another execution
//! environment by cold-booting its disk under that environment. It follows the
//! rules `docs/execution-environments.md` sets for an incompatible upgrade:
//!
//! 1. **Fence.** A `sandbox_runtime_upgrades` row, inserted only while the
//!    sandbox is stopped or suspended and owned by nothing else, refuses every
//!    other activation until the boot is verified or the old pair is back.
//! 2. **Preserve.** A read-only copy of the instance subvolume (the rootfs,
//!    and for encrypted sandboxes `cow.luks` and the suspend volume; a staged
//!    encrypted checkpoint's holds only `cow.luks`) is kept under an internal
//!    snapshot name. The checkpoint directory is never touched.
//! 3. **Select the consumer.** The target must keep the disk's guest,
//!    kernel and components; its runtime and snapshot format may differ, since
//!    no memory is loaded. It is saved as the sandbox's consumer. The
//!    checkpoint's producer evidence is never rewritten.
//! 4. **Boot.** The ordinary blocking cold start, which reads the saved
//!    consumer. Guest readiness is the verification.
//! 5. **Settle.** A verified boot retires the old checkpoint durably and drops
//!    the preserved copy. A failed boot restores the preserved copy, restores
//!    the previous consumer and releases the fence, leaving the sandbox
//!    restorable on its old environment.
//!
//! Every phase is persisted, so a retry of the same operation, or the recovery
//! loop after a restart, resumes from the row: an interrupted upgrade rolls
//! back, and a verified one finishes its retirement.
//!
//! Each call runs as an attempt (`attempt_id`): one the caller names and
//! keeps across its retries, or a fresh one the node generates. A Kubernetes
//! substrate derives it from kubelet's pod UID, so kubelet's retries of a
//! failed create are one attempt. With attempt finality on
//! ([`ATTEMPT_FINALITY_ENABLED_ENV`], off by default), a rollback, or a
//! [`Refusal::Final`], is final for the attempt it was recorded for: a retry
//! of that attempt gets the recorded failure back and never fences or boots
//! again, even after a later attempt's outcome replaces it as the operation's
//! reported one, and only a new attempt starts over. A [`Refusal::Retry`] is
//! left unrecorded, so a retry of the same attempt tries again. A caller
//! settles on such an outcome only when it names the attempt it's waiting on,
//! then gives that attempt up, so its own retry is a new one. An upgraded or
//! already-on-target outcome is answered to any attempt. When a started
//! target's activation, or a fence that found the operation already on its
//! target, is refused with a [`Refusal::Final`] after that, the refusal is
//! recorded as a refused activation for that attempt, final for it the same
//! way, unless the sandbox may still hold the pod's VM (see `rpc`).
//!
//! A rollback is final for its attempt from the transaction that makes it
//! reportable, before its disk discard runs, so a caller can't read a
//! rollback that an interrupted discard and a copy eviction then let the
//! attempt's retry run past. A final refusal is recorded only while the
//! sandbox is still as the refusal read it. Each final outcome keeps what its
//! operation asked for (sandbox, target environment and state, authorization),
//! so the operation id still names one request after the sandbox's row is
//! gone.
//!
//! The switch decides which attempts acquire finality, not which recorded
//! outcomes are honored: with it off, no new outcome is final, and every call
//! runs as an unnamed one did before attempts existed, except that a retry of
//! an attempt that's already final gets its outcome back. Turning the switch
//! on later makes nothing recorded while it was off final.

use std::time::Duration;

use pplx_space_execution_spec::{EnvironmentReference, ExecutionMetadata};
use pplx_space_sandbox_model::SandboxActivationAttemptId;
use pplx_space_snapshot_model::LocalArtifactId;
use tonic::Status;

use crate::db::{
    BeginActivationError, BeginRuntimeUpgrade, NewRuntimeUpgrade, RuntimeUpgradeAuthorization,
    RuntimeUpgradeOperationId, RuntimeUpgradePhase, RuntimeUpgradeRecord,
    RuntimeUpgradeTargetState, SandboxRow,
};
use crate::proto::{StartRequest, StartResponse};
use crate::state::{SandboxState, SandboxStatus};

mod node_effects;
mod rpc;
mod staged_disk;
mod stale_copy;
/// The refusal a call for `operation_id` naming `sandbox_id` gets once the
/// control plane abandoned the operation on this node, if it did: a call
/// naming the abandonment's sandbox gets `FAILED_PRECONDITION`, and one naming
/// another gets `INVALID_ARGUMENT`, as a report of it does, since an operation
/// id names one sandbox.
fn abandoned_refusal(
    conn: &crate::db::Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
) -> Result<Option<Status>, Status> {
    let abandoned_for = crate::db::read::runtime_upgrade_abandoned_sandbox(conn, operation_id)
        .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))?;
    Ok(abandoned_for.map(|abandoned_for| {
        if abandoned_for == sandbox_id {
            Status::failed_precondition(format!(
                "runtime upgrade {operation_id} was abandoned on this node; a new decision \
                 names a new operation"
            ))
        } else {
            Status::invalid_argument(format!(
                "runtime upgrade {operation_id} was abandoned on this node for another sandbox"
            ))
        }
    }))
}

#[cfg(test)]
mod tests;

pub(super) use node_effects::NodeRuntimeUpgradeEffects;

/// Turns attempt finality on (off by default): an attempt's rollback, final
/// refusal or refused activation is recorded as final for it. The switch
/// decides which attempts acquire finality, not which recorded finality is
/// honored: a retry of an attempt recorded as final is answered from its
/// record, and `GetRuntimeUpgrade` names it, whatever the switch says now.
/// Off, the node records nothing final and writes no `refused` outcome, so a
/// binary rollback releases nothing. Once a node has recorded a final outcome,
/// a binary rollback releases it unless its target honors recorded finality,
/// as this release does (an earlier one fences and boots the attempt's retry
/// again), and the target must parse `refused`. So turn it on only once every
/// release a node may roll back to does both.
pub(super) const ATTEMPT_FINALITY_ENABLED_ENV: &str =
    "SPACE_SBXMGR_RUNTIME_UPGRADE_ATTEMPT_FINALITY_ENABLED";

/// The preserved instance copy's name. The internal operation prefix keeps it
/// out of users' snapshot listing, deletion and rollback.
pub(super) fn preserved_disk_name(operation_id: RuntimeUpgradeOperationId) -> String {
    let prefix = pplx_space_host_layout::paths::INTERNAL_OPERATION_SNAPSHOT_PREFIX;
    format!("{prefix}runtime-upgrade-{operation_id}")
}

/// Refuse a mutation of a sandbox's disk while an upgrade fences it, with the
/// status a fenced activation gets.
pub(super) fn refuse_runtime_upgrade_fence_in(
    db: &crate::db::Connection,
    sandbox_id: &str,
) -> Result<(), Status> {
    match crate::db::read::runtime_upgrade_fence(db, sandbox_id)
        .map_err(|error| Status::unavailable(format!("read runtime upgrade fence: {error}")))?
    {
        Some(operation_id) => Err(begin_activation_status(
            crate::db::RuntimeUpgradeFenceError::Fenced { operation_id }.into(),
            "runtime upgrade fence",
        )),
        None => Ok(()),
    }
}

/// [`super::SandboxManagerService::refuse_unretired_runtime_upgrade`] on a
/// held connection.
pub(super) fn refuse_unretired_runtime_upgrade_in(
    db: &crate::db::Connection,
    sandbox_id: &str,
) -> Result<(), Status> {
    match crate::db::read::unretired_runtime_upgrade(db, sandbox_id)
        .map_err(|error| Status::unavailable(format!("read runtime upgrade: {error}")))?
    {
        Some(operation_id) => Err(Status::unavailable(format!(
            "runtime upgrade {operation_id} is retiring this sandbox's old checkpoint; retry"
        ))),
        None => Ok(()),
    }
}

/// The runtime upgrade of `sandbox_id` that must leave it `stopped`, while
/// its boot's execution may yet run it: a fencing one, or a verified one
/// whose boot is the sandbox's current execution, while the sandbox is started
/// or paused on it. Once verified, until the
/// upgrade stops that execution, the sandbox runs unfenced; a suspend in that window
/// would leave it `suspended`, which the upgrade never reports and the stopped
/// disk backup that follows it refuses. Once the execution is gone, by the
/// upgrade's stop or any other, the upgrade never stops a later one, so a
/// later execution suspends as usual even while the upgrade stays unretired.
pub(super) fn stay_stopped_upgrade_awaiting_its_stop(
    db: &crate::db::Connection,
    sandbox_id: &str,
) -> Result<Option<RuntimeUpgradeOperationId>, Status> {
    let read_failed =
        |error: crate::db::Error| Status::unavailable(format!("read runtime upgrade: {error}"));
    crate::db::read::stay_stopped_runtime_upgrade_awaiting_its_stop(db, sandbox_id)
        .map_err(read_failed)
}

/// The status an activation returns when it can't claim the sandbox.
pub(super) fn begin_activation_status(error: BeginActivationError, context: &str) -> Status {
    match error {
        BeginActivationError::RuntimeUpgrade(crate::db::RuntimeUpgradeFenceError::Fenced {
            operation_id,
        }) => Status::unavailable(format!(
            "runtime upgrade {operation_id} is moving this sandbox; retry"
        )),
        error @ BeginActivationError::RuntimeUpgrade(
            crate::db::RuntimeUpgradeFenceError::NotFencing { .. },
        ) => Status::aborted(error.to_string()),
        error @ BeginActivationError::ExecutionChanged => {
            Status::failed_precondition(error.to_string())
        }
        error => Status::internal(format!("{context}: {error}")),
    }
}

/// What an upgrade reads from the sandbox's files before fencing it.
#[derive(Clone, Debug)]
pub(super) struct LocalDisk {
    /// The execution the disk's metadata records.
    pub(super) execution: ExecutionMetadata,
    /// The suspended checkpoint's producer, for a suspended sandbox.
    pub(super) checkpoint_execution: Option<ExecutionMetadata>,
    /// Filesystem identity of the checkpoint directory, for a suspended
    /// sandbox that has one.
    pub(super) checkpoint_dir_identity: Option<String>,
}

/// The node and lifecycle work an upgrade performs between its phases.
#[tonic::async_trait]
pub(super) trait RuntimeUpgradeEffects: Send + Sync {
    /// Finish a suspended checkpoint staged on this node into the disk a copy
    /// suspended here has ([`staged_disk`]). The caller excludes disk
    /// mutations. It refuses an absent unencrypted rootfs as
    /// [`Self::inspect_local_disk`] would.
    async fn complete_staged_disk(&self, sandbox: &SandboxRow) -> Result<(), Refusal>;
    /// The producer of a suspended sandbox's checkpoint, which staging lays
    /// down with it, read before [`Self::complete_staged_disk`] so the
    /// upgrade refuses what it can before anything on the node changes. Its
    /// metadata is classified as [`Self::inspect_local_disk`] classifies it.
    async fn checkpoint_execution(
        &self,
        sandbox: &SandboxRow,
    ) -> Result<ExecutionMetadata, Refusal>;
    /// Refuse a suspended sandbox whose checkpoint here isn't the one its row
    /// pins, by the checkpoint's own sidecar. Asked before an upgrade records
    /// that it found the sandbox on its target, which no later attempt of the
    /// operation revisits ([`RuntimeUpgrader::refuse_after_finding_its_target`]).
    async fn require_pinned_checkpoint(&self, sandbox: &SandboxRow) -> Result<(), Status>;
    /// Read the local disk, refusing a sandbox without one: a cold start
    /// would otherwise boot a fresh template clone.
    async fn inspect_local_disk(&self, sandbox: &SandboxRow) -> Result<LocalDisk, Refusal>;
    /// Replace any copy named `name` with a read-only copy of the instance.
    async fn preserve_disk(&self, sandbox_id: &str, name: &str) -> Result<(), Status>;
    /// Replace the instance with the copy named `name`.
    async fn restore_disk(&self, sandbox_id: &str, name: &str) -> Result<(), Status>;
    /// Delete the copy named `name`; an absent copy is already deleted.
    async fn discard_disk(&self, sandbox_id: &str, name: &str) -> Result<(), Status>;
    /// The complete execution of `target` on this node.
    async fn resolve_target(
        &self,
        target: EnvironmentReference,
    ) -> Result<ExecutionMetadata, Status>;
    /// Run the upgrade's cold start to guest readiness.
    async fn cold_boot(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        start: StartRequest,
    ) -> Result<StartResponse, Status>;
    /// Stop the sandbox and wait until it is stopped, but only while it still
    /// runs the node execution `execution` began.
    async fn stop(
        &self,
        sandbox_id: &str,
        execution: SandboxActivationAttemptId,
    ) -> Result<(), Status>;
    /// Hold off every other writer of the instance disk until the returned
    /// guard drops, for a new upgrade's fence: a node known sealed refuses it
    /// while the sandbox has no VM and isn't stopped
    /// ([`super::write_gate::NodeWriter::RuntimeUpgrade`]).
    async fn exclude_disk_mutations(&self, sandbox_id: &str) -> Result<Box<dyn Send>, Status>;
    /// [`Self::exclude_disk_mutations`] for a failed boot's restore of a
    /// suspended sandbox's preserved disk, which the seal never refuses. A
    /// wait that runs out answers `Unavailable`, which the next pass retries.
    async fn exclude_disk_mutations_to_restore(
        &self,
        sandbox_id: &str,
    ) -> Result<Box<dyn Send>, Status>;
    /// Retire the checkpoint the upgrade superseded, if it still exists.
    async fn retire_checkpoint(&self, record: &RuntimeUpgradeRecord) -> Result<(), Status>;
}

pub(super) struct RuntimeUpgradeRequest {
    pub(super) operation_id: RuntimeUpgradeOperationId,
    pub(super) sandbox_id: String,
    pub(super) target: EnvironmentReference,
    pub(super) target_state: RuntimeUpgradeTargetState,
    pub(super) authorization: RuntimeUpgradeAuthorization,
    pub(super) start: StartRequest,
    /// For a suspended sandbox, the checkpoint the upgrade was decided from;
    /// see [`require_decided_checkpoint`].
    pub(super) checkpoint_local_artifact_id: Option<LocalArtifactId>,
    /// The attempt this call runs as: the request's `attempt_id`, or one the
    /// node generated for a request that named none. The store records
    /// outcomes per `(operation_id, attempt_id)`.
    pub(super) attempt_id: String,
}

impl RuntimeUpgradeRequest {
    fn identity(&self) -> crate::db::RuntimeUpgradeRequestIdentity {
        crate::db::RuntimeUpgradeRequestIdentity {
            operation_id: self.operation_id,
            sandbox_id: self.sandbox_id.clone(),
            target_environment: self.target,
            target_state: self.target_state,
            authorization_id: self.authorization.id.clone(),
        }
    }

    fn names_another(&self) -> Status {
        Status::invalid_argument(format!(
            "runtime upgrade {} names a different sandbox, target or authorization",
            self.operation_id
        ))
    }
}

#[derive(Debug)]
pub(super) enum RuntimeUpgradeOutcome {
    /// This call booted the sandbox under the target.
    Upgraded,
    /// A replay of an upgrade that already retired. It never touches the
    /// sandbox's lifecycle again, so it has no start to report.
    Replayed,
    AlreadyOnTarget,
}

/// The boot runs on this node's copy, so for a suspended sandbox it must be
/// the checkpoint the upgrade was decided from: the row's pinned local
/// artifact must be the one the caller names. Staging pins the staged
/// checkpoint, so the check holds across retries. An upgrade of a suspended
/// sandbox that names no checkpoint has nothing to check the copy against.
pub(super) fn require_decided_checkpoint(
    sandbox: &SandboxRow,
    decided: Option<LocalArtifactId>,
) -> Result<(), Status> {
    if sandbox.state.is_unowned(SandboxState::Suspended)
        && (decided.is_none() || sandbox.current_local_artifact_id != decided)
    {
        return Err(Status::failed_precondition(format!(
            "sandbox {}'s checkpoint on this node isn't the one its runtime upgrade was decided \
             from; refusing to boot it",
            sandbox.id
        )));
    }
    Ok(())
}

/// A refusal before the fence, and whether it's final for the attempt that sent
/// it, decided where it's produced rather than read back from its code. The
/// status is what the caller gets either way.
pub(super) enum Refusal {
    /// Every retry of the same request is answered the same way: it
    /// contradicts what the node recorded or holds (the checkpoint, the edge,
    /// the authorization, a disk that's missing or unreadable as written), or
    /// names another operation. Recorded for the attempt, so its retries get it
    /// back.
    Final(Status),
    /// [`Refusal::Final`] while the sandbox stays as the row it was decided
    /// from: its state, pinned artifact and saved consumer, and for a refused
    /// activation its runtime label, execution and route identity. It's
    /// recorded only in a transaction that finds the row unchanged, so a
    /// start, stop or suspend that moved the sandbox since (none takes the
    /// upgrade lock) leaves it for a retry to decide again.
    FinalOn(Status, Box<SandboxRow>),
    /// The node was busy or failing, or the sandbox may yet change, so a
    /// retry may be answered differently. Left unrecorded.
    Retry(Status),
}

/// `SandboxRow` doesn't implement `Debug`, so a refusal shows only the id of
/// the sandbox it was decided on.
impl std::fmt::Debug for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Final(status) => f.debug_tuple("Final").field(status).finish(),
            Refusal::FinalOn(status, sandbox) => f
                .debug_struct("FinalOn")
                .field("status", status)
                .field("sandbox_id", &sandbox.id)
                .finish(),
            Refusal::Retry(status) => f.debug_tuple("Retry").field(status).finish(),
        }
    }
}

impl Refusal {
    /// A refusal final for the attempt while `sandbox` stays as read.
    pub(super) fn final_on(sandbox: &SandboxRow, status: Status) -> Self {
        Refusal::FinalOn(status, Box::new(sandbox.clone()))
    }

    /// The status of a refusal final for the attempt, with the sandbox row it
    /// was decided from when it depends on one; `None` for a retry.
    pub(super) fn as_final(&self) -> Option<(&Status, Option<&SandboxRow>)> {
        match self {
            Refusal::Final(status) => Some((status, None)),
            Refusal::FinalOn(status, sandbox) => Some((status, Some(sandbox))),
            Refusal::Retry(_) => None,
        }
    }

    pub(super) fn into_status(self) -> Status {
        match self {
            Refusal::Final(status) | Refusal::FinalOn(status, _) | Refusal::Retry(status) => status,
        }
    }
}

/// An error nothing classified, a database read say, is one a retry may get
/// past, so `?` leaves it unrecorded.
impl From<Status> for Refusal {
    fn from(status: Status) -> Self {
        Refusal::Retry(status)
    }
}

/// What [`RuntimeUpgrader::fence`] decided.
enum Fence {
    /// The sandbox is fenced for the boot, with what its disk recorded.
    Fenced(Box<(RuntimeUpgradeRecord, LocalDisk)>),
    /// The sandbox is already on the target; nothing was fenced.
    AlreadyOnTarget,
}

/// How [`RuntimeUpgrader::settle`] left an upgrade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Settled {
    Retired,
    RolledBack,
}

/// How long a rollback waits for a failed boot's cleanup to return the
/// sandbox to its old state. Past it the fence stays, and a retry or the
/// recovery loop finishes the rollback.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
const SETTLE_POLL: Duration = Duration::from_millis(250);

/// Refuse a move that isn't a certified cold-boot upgrade edge from the
/// compiled environment `source` ran under.
/// The edge is a property of the node's compiled catalog, so its absence is
/// final. A catalog that can't be loaded is this binary's fault, not the
/// request's: the failure is cached until sbx-manager restarts, which may load
/// it, so it's left retryable.
fn require_certified_upgrade_edge(
    source: &ExecutionMetadata,
    target: EnvironmentReference,
) -> Result<(), Refusal> {
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let catalog = CompiledEnvironment::cached_for_architecture(source.guest().architecture)
        .map_err(|error| {
            Refusal::Retry(Status::failed_precondition(format!(
                "compiled catalog: {error}"
            )))
        })?;
    let from = CompiledEnvironment::of_execution(catalog, source).ok_or_else(|| {
        Refusal::Final(Status::failed_precondition(
            "the sandbox's execution belongs to no compiled environment, so no upgrade edge leaves it",
        ))
    })?;
    if from.upgrade_targets().contains(&target) {
        Ok(())
    } else {
        Err(Refusal::Final(Status::failed_precondition(format!(
            "{} -> {target} is not a certified upgrade edge",
            from.definition().reference()
        ))))
    }
}

/// What the execution a verified boot started is doing now.
#[derive(Debug, PartialEq, Eq)]
enum BootActivity {
    /// Started or paused on the boot's execution: the upgrade's to stop.
    Running,
    /// At rest, or running another execution: nothing of the upgrade's to stop.
    Done,
    /// Between states on the boot's execution, such as checkpointing, pausing
    /// or quarantined; it may come back running, so the stop waits for a retry.
    Changing(SandboxStatus),
}

fn boot_activity(
    state: impl Into<SandboxStatus>,
    current: Option<SandboxActivationAttemptId>,
    boot: SandboxActivationAttemptId,
) -> BootActivity {
    let state = state.into();
    // Quarantine completes a wait but can recover to started on the same
    // execution.
    if current != Some(boot)
        || (state.completes_wait() && !state.is_unowned(SandboxState::Quarantined))
    {
        BootActivity::Done
    } else if matches!(
        state.unowned(),
        Some(SandboxState::Started | SandboxState::Paused)
    ) {
        BootActivity::Running
    } else {
        BootActivity::Changing(state)
    }
}

/// A stop the cleanup claim refused: if the boot's execution is at rest or
/// another one runs, the upgrade has nothing to stop (`Ok(false)`). Otherwise
/// the boot's own execution is still the upgrade's to stop on a retry.
fn moved_on_or_retry(
    state: impl Into<SandboxStatus>,
    current: Option<SandboxActivationAttemptId>,
    execution: SandboxActivationAttemptId,
    refusal: &Status,
) -> Result<bool, Status> {
    match boot_activity(state, current, execution) {
        BootActivity::Done => Ok(false),
        BootActivity::Running | BootActivity::Changing(_) => Err(Status::unavailable(format!(
            "stop of the upgrade's boot was refused; retry: {}",
            refusal.message()
        ))),
    }
}

fn sandbox_not_found(sandbox_id: &str) -> Status {
    Status::not_found(format!("sandbox {sandbox_id} not found"))
}

/// The environment of the sandbox's saved consumer, if it's bound to one.
fn saved_environment(sandbox: &SandboxRow) -> Option<EnvironmentReference> {
    sandbox
        .execution_metadata
        .as_ref()
        .and_then(ExecutionMetadata::environment)
}

pub(super) struct RuntimeUpgrader<E> {
    db: crate::db::SharedConnection,
    effects: E,
    settle_timeout: Duration,
    settle_poll: Duration,
    /// Whether attempts are final ([`ATTEMPT_FINALITY_ENABLED_ENV`]); off
    /// unless [`Self::with_attempt_finality`] turns it on.
    attempt_finality: bool,
}

impl<E: RuntimeUpgradeEffects> RuntimeUpgrader<E> {
    pub(super) fn new(db: crate::db::SharedConnection, effects: E) -> Self {
        Self {
            db,
            effects,
            settle_timeout: SETTLE_TIMEOUT,
            settle_poll: SETTLE_POLL,
            attempt_finality: false,
        }
    }

    /// Record and answer final outcomes per attempt, as
    /// [`ATTEMPT_FINALITY_ENABLED_ENV`] says.
    pub(super) fn with_attempt_finality(mut self, enabled: bool) -> Self {
        self.attempt_finality = enabled;
        self
    }

    /// For the recovery loop: a sandbox that hasn't settled is skipped until
    /// the next pass rather than waited for, and retirement's wait for a busy
    /// sandbox's artifacts is bounded. Node-manager calls keep their own
    /// deadlines.
    pub(super) fn without_settle_wait(db: crate::db::SharedConnection, effects: E) -> Self {
        Self {
            db,
            effects,
            settle_timeout: Duration::ZERO,
            settle_poll: SETTLE_POLL,
            attempt_finality: false,
        }
    }

    #[cfg(test)]
    fn with_settle_timing(mut self, timeout: Duration, poll: Duration) -> Self {
        self.settle_timeout = timeout;
        self.settle_poll = poll;
        self
    }

    fn db(&self) -> Result<crate::db::ConnectionGuard<'_>, Status> {
        self.db
            .lock()
            .map_err(|_| Status::internal("sandbox database mutex poisoned"))
    }

    fn record(
        &self,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<RuntimeUpgradeRecord>, Status> {
        crate::db::read::runtime_upgrade(&*self.db()?, operation_id)
            .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))
    }

    fn phase(
        &self,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<RuntimeUpgradePhase>, Status> {
        Ok(self.record(operation_id)?.map(|record| record.phase))
    }

    fn sandbox(&self, sandbox_id: &str) -> Result<Option<SandboxRow>, Status> {
        crate::db::read::sandbox(&*self.db()?, sandbox_id)
            .map_err(|error| Status::internal(format!("read sandbox: {error}")))
    }

    /// Whether a cleanup owns `sandbox`: its row's cleanup intent or
    /// cleanup-owned state, or an outstanding cleanup episode.
    fn cleanup_owns(&self, sandbox: &SandboxRow) -> Result<bool, Status> {
        Self::cleanup_owns_in(&*self.db()?, sandbox)
    }

    fn cleanup_owns_in(db: &crate::db::Connection, sandbox: &SandboxRow) -> Result<bool, Status> {
        Ok(sandbox.cleanup_completion.is_some()
            || sandbox.state.is_cleanup_owned()
            || crate::db::read::sandbox_view(db, &sandbox.id)
                .cleanup_episode()
                .present()
                .map_err(|error| Status::internal(format!("read cleanup episode: {error}")))?)
    }

    fn existing_sandbox(&self, sandbox_id: &str) -> Result<SandboxRow, Status> {
        self.sandbox(sandbox_id)?
            .ok_or_else(|| sandbox_not_found(sandbox_id))
    }

    /// `refusal`, final only while `sandbox` is still as `fence` read it. A
    /// cold `Start` takes neither the upgrade lock nor the artifact lock
    /// `fence` holds, and nothing is fenced until `begin_runtime_upgrade`, so
    /// a stopped sandbox may have started or rebound its execution while the
    /// disk was inspected, or may do so before the refusal is recorded; a
    /// refusal decided on the earlier read is then one a retry decides again.
    /// [`crate::db::record_runtime_upgrade_refusal`] compares the row in the
    /// transaction that records it, on what `begin_runtime_upgrade` compares
    /// to answer `SandboxChanged` and on the row's last activation, which a
    /// start that ran and stopped again in between replaces.
    fn final_unless_changed(sandbox: &SandboxRow, refusal: Refusal) -> Refusal {
        match refusal {
            Refusal::Final(status) => Refusal::final_on(sandbox, status),
            refusal => refusal,
        }
    }

    /// Move `record`'s upgrade from `from` to `to`, only while the
    /// operation's row is still `record`'s sandbox's: the caller holds that
    /// sandbox's upgrade lock, not the one of a sandbox that reused the
    /// operation id since.
    fn advance(
        &self,
        record: &RuntimeUpgradeRecord,
        from: RuntimeUpgradePhase,
        to: RuntimeUpgradePhase,
    ) -> Result<(), Status> {
        let operation_id = record.operation_id;
        let advanced = crate::db::advance_runtime_upgrade_for_sandbox(
            &*self.db()?,
            operation_id,
            &record.sandbox_id,
            from,
            to,
        )
        .map_err(|error| Status::internal(format!("record runtime upgrade {to}: {error}")))?;
        if advanced {
            Ok(())
        } else {
            Err(Status::aborted(format!(
                "runtime upgrade {operation_id} left {from} before it could reach {to}"
            )))
        }
    }

    /// Refuse a request that names another sandbox, target or authorization
    /// than its operation's live row or recorded outcome does, or than its
    /// attempt's final outcome does: a retry may not turn an operation into
    /// another one.
    pub(super) fn refuse_another_request(
        &self,
        request: &RuntimeUpgradeRequest,
    ) -> Result<(), Status> {
        // All three under one lock of the connection every write takes: a
        // rollback records its outcome and deletes the live row in one
        // commit, which would otherwise fall between the reads and hide the
        // operation from both.
        let (recorded, retained, live) = {
            let db = self.db()?;
            let read = |error| Status::internal(format!("read runtime upgrade outcome: {error}"));
            let operation_id = request.operation_id;
            (
                crate::db::read::recorded_runtime_upgrade_request(&db, operation_id)
                    .map_err(read)?,
                crate::db::read::retained_runtime_upgrade_requests(&db, operation_id)
                    .map_err(read)?,
                crate::db::read::runtime_upgrade(&db, operation_id)
                    .map_err(|error| Status::internal(format!("read runtime upgrade: {error}")))?
                    .map(|record| record.identity()),
            )
        };
        // Its attempts' final outcomes outlive the other two when the
        // sandbox's row is deleted, and still say what the operation asked
        // for: the sandbox, target, target state and authorization. A request
        // that differs, under any attempt id, is told so rather than given an
        // attempt's outcome or started as new work.
        if recorded
            .into_iter()
            .chain(live)
            .chain(retained)
            .any(|identity| identity != request.identity())
        {
            return Err(request.names_another());
        }
        Ok(())
    }

    /// Run, resume or settle one upgrade.
    pub(super) async fn upgrade(
        &self,
        request: RuntimeUpgradeRequest,
    ) -> Result<RuntimeUpgradeOutcome, Status> {
        self.upgrade_classified(request)
            .await
            .map_err(Refusal::into_status)
    }

    /// [`Self::upgrade`], keeping whether its refusal is final for the
    /// attempt, which decides whether a started target's caller records it
    /// as the attempt's refused activation. A refusal answered from the
    /// attempt's record, and every failure after the fence (whose rollback is
    /// the attempt's outcome), is a [`Refusal::Retry`] here: neither is a new
    /// final refusal to record. So is a request that the operation's recorded
    /// request already rules out ([`Self::refuse_another_request`]), which
    /// every retry gets anyway. One the fence finds naming another
    /// operation's request is a [`Refusal::Final`], and the store declines to
    /// record it for the attempt: the operation's retained request isn't the
    /// one it names.
    pub(super) async fn upgrade_classified(
        &self,
        request: RuntimeUpgradeRequest,
    ) -> Result<RuntimeUpgradeOutcome, Refusal> {
        self.refuse_another_request(&request)?;
        // An attempt whose rollback or refusal is final gets it back before it
        // settles anything: a later attempt's live record isn't its own to
        // settle, and a replay of that attempt mustn't answer it.
        self.refuse_settled_for_attempt(&request)?;
        if let Some(record) = self.record(request.operation_id)? {
            // The per-sandbox lock doesn't serialize two sandboxes, so another
            // sandbox's call may have begun this operation since the check
            // above; its record isn't this call's to settle.
            if record.identity() != request.identity() {
                return Err(Refusal::Final(request.names_another()));
            }
            // A retired upgrade already reached its lifecycle; the sandbox may
            // have moved on since, and a replay must not move it back.
            match self.settle(&record, true).await? {
                Settled::Retired => return Ok(RuntimeUpgradeOutcome::Replayed),
                // An interrupted attempt is back on its old pair. A retry
                // of the attempt it ran for gets its rollback back below; any
                // other starts over.
                Settled::RolledBack => {}
            }
            // Settling may have just rolled back this attempt's own boot.
            self.refuse_settled_for_attempt(&request)?;
        }
        let (record, disk) = match self.fence(&request).await {
            Ok(Fence::Fenced(fenced)) => *fenced,
            Ok(Fence::AlreadyOnTarget) => return Ok(RuntimeUpgradeOutcome::AlreadyOnTarget),
            Err(refusal) => return Err(refusal),
        };
        self.run(&record, &disk, request.start).await?;
        Ok(RuntimeUpgradeOutcome::Upgraded)
    }

    /// Answer a retry of an attempt with the final outcome recorded for it,
    /// rather than activating, staging, fencing or booting again: its
    /// rollback, its refusal before the fence, or its activation refused
    /// after the operation reached its target. Its caller may already have
    /// settled on that outcome and released its execution. The attempt's own
    /// history decides, not the operation's latest outcome, which a later
    /// attempt replaces. An attempt with no final outcome, a fresh one
    /// included, starts over. This holds whatever the attempt finality
    /// switch says now: it governs which attempts get a final outcome, and
    /// turning it off doesn't take back one already given.
    pub(super) fn refuse_settled_for_attempt(
        &self,
        request: &RuntimeUpgradeRequest,
    ) -> Result<(), Status> {
        let attempt_id = &request.attempt_id;
        let Some((settled, recorded_for)) =
            crate::db::read::runtime_upgrade_attempt_outcome_with_request(
                &*self.db()?,
                request.operation_id,
                attempt_id,
            )
            .map_err(|error| Status::internal(format!("read runtime upgrade outcome: {error}")))?
        else {
            return Ok(());
        };
        // The per-sandbox lock doesn't serialize two sandboxes, so a request
        // reusing the operation id for another sandbox may have recorded this
        // outcome since `refuse_another_request` checked; it isn't this
        // request's to replay.
        if recorded_for != request.identity() {
            return Err(request.names_another());
        }
        let operation_id = request.operation_id;
        let failure = settled.failure;
        let failure = failure.as_deref().unwrap_or("no reason recorded");
        tracing::info!(
            sandbox_id = %request.sandbox_id,
            %operation_id,
            attempt_id,
            outcome = settled.outcome.as_str(),
            failure,
            "runtime upgrade retry answered from its attempt's final outcome"
        );
        Err(Status::failed_precondition(match settled.outcome {
            crate::db::RecordedAttemptOutcome::RolledBack => format!(
                "runtime upgrade {operation_id} rolled back for attempt {attempt_id}: \
                 {failure}; only a new attempt retries it"
            ),
            crate::db::RecordedAttemptOutcome::Refused => format!(
                "runtime upgrade {operation_id} was refused for attempt {attempt_id}: \
                 {failure}; only a new attempt retries it"
            ),
            crate::db::RecordedAttemptOutcome::ActivationRefused => format!(
                "runtime upgrade {operation_id} reached its target but refused attempt \
                 {attempt_id}'s activation: {failure}; only a new attempt retries it"
            ),
        }))
    }

    /// Record a [`Refusal::Final`] that came before anything was fenced for
    /// the request's attempt, so it's final for that attempt as a rollback
    /// is. A [`Refusal::Retry`] (cleanup owns the sandbox, another operation
    /// fences it, the sandbox changed while it was inspected or is in a state
    /// an upgrade can't start from, a disk that can't be read, a node-manager
    /// call that failed before the fence) stays unrecorded, so a retry of the
    /// same attempt tries again. Nothing is recorded with attempt finality
    /// off. Best effort: an unrecorded refusal leaves the attempt's report as
    /// it was, which its caller doesn't settle on, and its retry decides
    /// again.
    pub(super) fn record_refusal(&self, request: &RuntimeUpgradeRequest, refusal: &Refusal) {
        let Some((refusal, decided_on)) = refusal.as_final() else {
            return;
        };
        if !self.attempt_finality {
            return;
        }
        let recorded = self.db().and_then(|db| {
            crate::db::record_runtime_upgrade_refusal(
                &db,
                &request.identity(),
                &request.attempt_id,
                refusal.message(),
                decided_on.map(crate::db::ObservedSandbox::of),
            )
            .map_err(|error| Status::internal(error.to_string()))
        });
        match recorded {
            Ok(true) => tracing::info!(
                sandbox_id = %request.sandbox_id,
                operation_id = %request.operation_id,
                attempt_id = %request.attempt_id,
                refusal = refusal.message(),
                "runtime upgrade refused before the fence; final for the attempt"
            ),
            // The store's guards: the sandbox is gone or moved since the
            // refusal read it, a live row, an outcome a refusal may not
            // replace or another request's final outcome holds the
            // operation, or the attempt already has its final outcome. The refusal is still
            // the answer; a retry decides it again.
            Ok(false) => tracing::info!(
                sandbox_id = %request.sandbox_id,
                operation_id = %request.operation_id,
                attempt_id = %request.attempt_id,
                refusal = refusal.message(),
                "runtime upgrade refusal not recorded: the operation's record keeps its outcome"
            ),
            Err(error) => tracing::warn!(
                sandbox_id = %request.sandbox_id,
                operation_id = %request.operation_id,
                attempt_id = %request.attempt_id,
                %error,
                "could not record the runtime upgrade's refusal"
            ),
        }
    }

    /// Everything before the boot: decide whether the sandbox is already on
    /// the target, check the edge, and fence it. Every error here leaves
    /// nothing fenced for this operation, and a final one is recorded
    /// ([`Self::record_refusal`]) before the disk exclusion is released.
    /// Staging the sandbox again takes the same exclusion, so a cleanup
    /// admitted meanwhile is either still on the row when the refusal is
    /// recorded, where the store checks for it, or, if it deleted the row,
    /// leaves no row for the store's comparison to match: the sandbox can't
    /// have been staged again with the same columns in between.
    async fn fence(&self, request: &RuntimeUpgradeRequest) -> Result<Fence, Refusal> {
        // Snapshot rollback checks the fence under the same exclusion before
        // it rewrites the instance, so it either finishes before the fence or
        // sees it.
        // A node known sealed refuses a new upgrade while the sandbox has no
        // VM and isn't stopped, so the refusal is left unrecorded for the
        // attempt. A seal landing between the stage's admission and this
        // fence can leave the staged restore behind, which the drain then
        // waits on as pending upload until its backup is confirmed.
        let disk_mutations = self
            .effects
            .exclude_disk_mutations(&request.sandbox_id)
            .await
            .map_err(Refusal::Retry)?;
        let fenced = self.fence_excluded(request).await;
        if let Err(refusal) = &fenced {
            self.record_refusal(request, refusal);
        }
        drop(disk_mutations);
        fenced
    }

    /// [`Self::fence`], under its disk exclusion.
    async fn fence_excluded(&self, request: &RuntimeUpgradeRequest) -> Result<Fence, Refusal> {
        // Another upgrade's booting row has already saved its target as the
        // consumer, so "already on target" can't be decided while it fences.
        if let Some(other) =
            crate::db::read::runtime_upgrade_fence(&*self.db()?, &request.sandbox_id)
                .map_err(|error| Status::internal(format!("read runtime upgrade fence: {error}")))?
            && other != request.operation_id
        {
            return Err(Refusal::Retry(Status::aborted(format!(
                "runtime upgrade {other} already owns this sandbox"
            ))));
        }
        let sandbox = self.existing_sandbox(&request.sandbox_id)?;
        // A cleanup may be removing the copy the checkpoint, target and disk
        // checks would read, so nothing they'd answer is final for the
        // attempt, already on target included; the
        // store checks again where it records a refusal, for a cleanup
        // admitted meanwhile.
        if self.cleanup_owns(&sandbox)? {
            return Err(Refusal::Retry(Status::unavailable(
                "cleanup owns this sandbox; retry the runtime upgrade",
            )));
        }
        require_decided_checkpoint(&sandbox, request.checkpoint_local_artifact_id)
            .map_err(|status| Refusal::final_on(&sandbox, status))?;
        if !sandbox.state.is_unowned(SandboxState::Suspended)
            && saved_environment(&sandbox) == Some(request.target)
        {
            return self
                .already_on_target(request, &sandbox)
                .map(|()| Fence::AlreadyOnTarget);
        }
        // Every refusal that doesn't need the finished disk comes before
        // finishing it, so a refused upgrade leaves the node as it found it.
        // The same checks run again below against what the inspection reads.
        // Pinning the checkpoint, or reading it, is retryable as the disk's
        // would be; metadata that reads but doesn't parse is final, as the
        // inspection would find it.
        if sandbox.state.is_unowned(SandboxState::Suspended) {
            let checkpoint = self
                .effects
                .checkpoint_execution(&sandbox)
                .await
                .map_err(|refusal| Self::final_unless_changed(&sandbox, refusal))?;
            if checkpoint.environment() == Some(request.target) {
                self.effects.require_pinned_checkpoint(&sandbox).await?;
                return self
                    .already_on_target(request, &sandbox)
                    .map(|()| Fence::AlreadyOnTarget);
            }
            self.refuse_after_finding_its_target(request, &sandbox)?;
            require_certified_upgrade_edge(&checkpoint, request.target)
                .map_err(|refusal| Self::final_unless_changed(&sandbox, refusal))?;
            if !request.authorization.memory_loss {
                return Err(Refusal::final_on(&sandbox, memory_loss_unauthorized()));
            }
        }
        // The fence's transaction still decides this; reading it again only
        // keeps a cleanup admitted since the check above from following a
        // finished disk.
        if self.cleanup_owns(&sandbox)? {
            return Err(Refusal::Retry(cleanup_active()));
        }
        // Only a suspended sandbox's disk is finished, and only its absent
        // unencrypted rootfs is final, as the inspection would find it.
        self.effects
            .complete_staged_disk(&sandbox)
            .await
            .map_err(|refusal| Self::final_unless_changed(&sandbox, refusal))?;
        let disk = match self.effects.inspect_local_disk(&sandbox).await {
            Ok(disk) => disk,
            // A sandbox that isn't stopped or suspended may still settle, its
            // disk written by the activation or stop that moves it, so no
            // refusal of its disk is final for the attempt; a retry inspects it
            // again.
            Err(refusal)
                if !matches!(
                    sandbox.state.unowned(),
                    Some(SandboxState::Stopped | SandboxState::Suspended)
                ) =>
            {
                return Err(Refusal::Retry(refusal.into_status()));
            }
            Err(refusal) => return Err(Self::final_unless_changed(&sandbox, refusal)),
        };
        if sandbox.state.is_unowned(SandboxState::Suspended)
            && disk
                .checkpoint_execution
                .as_ref()
                .and_then(ExecutionMetadata::environment)
                == Some(request.target)
        {
            self.effects.require_pinned_checkpoint(&sandbox).await?;
            return self
                .already_on_target(request, &sandbox)
                .map(|()| Fence::AlreadyOnTarget);
        }
        self.refuse_after_finding_its_target(request, &sandbox)?;
        // What the sandbox ran under last: its checkpoint's producer when
        // suspended, otherwise its saved consumer or, unbound, its disk's.
        let source = match sandbox.state.unowned() {
            Some(SandboxState::Suspended) => disk.checkpoint_execution.as_ref(),
            _ => sandbox.execution_metadata.as_ref(),
        }
        .unwrap_or(&disk.execution);
        // A sandbox that isn't stopped or suspended may still settle under
        // another execution, so its edge refusal isn't final for the attempt.
        require_certified_upgrade_edge(source, request.target).map_err(|refusal| {
            if matches!(
                sandbox.state.unowned(),
                Some(SandboxState::Stopped | SandboxState::Suspended)
            ) {
                Self::final_unless_changed(&sandbox, refusal)
            } else {
                Refusal::Retry(refusal.into_status())
            }
        })?;

        let begun = crate::db::begin_runtime_upgrade(
            &*self.db()?,
            NewRuntimeUpgrade {
                operation_id: request.operation_id,
                sandbox_id: &request.sandbox_id,
                observed: crate::db::ObservedSandbox::of(&sandbox),
                target_environment: request.target,
                target_state: request.target_state,
                authorization: &request.authorization,
                checkpoint_dir_identity: disk.checkpoint_dir_identity.as_deref(),
                previous_execution: sandbox
                    .execution_metadata
                    .as_ref()
                    .unwrap_or(&disk.execution),
                // An upgrade begun with finality off runs as no attempt, so
                // neither its rollback nor its report ever names one, even
                // once the switch is on.
                attempt_id: self.attempt_finality.then_some(request.attempt_id.as_str()),
            },
        )
        .map_err(|error| Status::internal(format!("fence runtime upgrade: {error}")))?;
        let record = match begun {
            BeginRuntimeUpgrade::Fenced(record) => *record,
            BeginRuntimeUpgrade::SandboxMissing => {
                return Err(Refusal::Retry(sandbox_not_found(&request.sandbox_id)));
            }
            // The sandbox may still settle into a state an upgrade starts
            // from, so a retry may succeed and the refusal isn't final.
            BeginRuntimeUpgrade::UnsupportedState(state) => {
                return Err(Refusal::Retry(Status::failed_precondition(format!(
                    "a runtime upgrade needs a stopped or suspended sandbox, not {state}"
                ))));
            }
            BeginRuntimeUpgrade::CleanupActive => return Err(Refusal::Retry(cleanup_active())),
            BeginRuntimeUpgrade::OtherOperation(other) => {
                return Err(Refusal::Retry(Status::aborted(format!(
                    "runtime upgrade {other} already owns this sandbox"
                ))));
            }
            BeginRuntimeUpgrade::OperationNamesAnother => {
                return Err(Refusal::Final(request.names_another()));
            }
            BeginRuntimeUpgrade::OperationExists => {
                return Err(Refusal::Retry(Status::aborted(format!(
                    "runtime upgrade {} began concurrently",
                    request.operation_id
                ))));
            }
            BeginRuntimeUpgrade::MemoryLossUnauthorized => {
                return Err(Refusal::final_on(&sandbox, memory_loss_unauthorized()));
            }
            // Every later call is refused too, but by the abandonment, not
            // by an outcome: recorded, this refusal would report a refused
            // create where the operation was abandoned, so it stays
            // unrecorded, like the check before the fence.
            BeginRuntimeUpgrade::Abandoned => {
                let refusal =
                    abandoned_refusal(&*self.db()?, request.operation_id, &request.sandbox_id)?
                        .unwrap_or_else(|| {
                            Status::failed_precondition(format!(
                                "runtime upgrade {} was abandoned on this node",
                                request.operation_id
                            ))
                        });
                return Err(Refusal::Retry(refusal));
            }
            BeginRuntimeUpgrade::SandboxChanged => {
                return Err(Refusal::Retry(Status::aborted(
                    "the sandbox changed while the runtime upgrade inspected it; retry",
                )));
            }
        };
        Ok(Fence::Fenced(Box::new((record, disk))))
    }

    /// An operation that already found its sandbox on the target never
    /// boots: a replay after the sandbox moved to another environment would
    /// otherwise undo that move.
    fn refuse_after_finding_its_target(
        &self,
        request: &RuntimeUpgradeRequest,
        sandbox: &SandboxRow,
    ) -> Result<(), Refusal> {
        let found_its_target =
            crate::db::read::runtime_upgrade_report(&*self.db()?, request.operation_id)
                .map_err(|error| {
                    Status::internal(format!("read runtime upgrade outcome: {error}"))
                })?
                .is_some_and(|report| {
                    matches!(
                        report.status,
                        crate::db::RuntimeUpgradeStatus::AlreadyOnTarget { .. }
                    )
                });
        if found_its_target {
            let refusal = Status::failed_precondition(format!(
                "runtime upgrade {} found sandbox {} on its target before, and the sandbox \
                 moved since; moving it from where it is now is another operation",
                request.operation_id, request.sandbox_id
            ));
            // Final only for a sandbox at rest, as the edge and disk refusals
            // are: one that isn't may still hold a VM for the caller's pod.
            return Err(
                if matches!(
                    sandbox.state.unowned(),
                    Some(SandboxState::Stopped | SandboxState::Suspended)
                ) {
                    Refusal::final_on(sandbox, refusal)
                } else {
                    Refusal::Retry(refusal)
                },
            );
        }
        Ok(())
    }

    /// Record that the upgrade fenced nothing, so a caller that lost this
    /// answer reads it from `GetRuntimeUpgrade`. Decided from `sandbox` as
    /// [`Self::fence`] read it, so it's recorded only while the row is still
    /// as read and no cleanup owns the sandbox, which a disk inspection can
    /// await long enough to admit; otherwise it's a retryable refusal.
    fn already_on_target(
        &self,
        request: &RuntimeUpgradeRequest,
        sandbox: &SandboxRow,
    ) -> Result<(), Refusal> {
        // Recorded only if no other request reused the operation id first;
        // the per-sandbox upgrade lock doesn't serialize two sandboxes. With
        // attempt finality off it names no attempt, as a node that predates
        // attempt ids records it, so turning the switch on later doesn't
        // make it name one.
        let recorded = crate::db::record_runtime_upgrade_already_on_target_observed(
            &*self.db()?,
            &request.identity(),
            self.attempt_finality.then_some(request.attempt_id.as_str()),
            crate::db::ObservedSandbox::of(sandbox),
        )
        .map_err(|error| Status::internal(format!("record runtime upgrade outcome: {error}")))?;
        if recorded == crate::db::AlreadyOnTargetRecord::SandboxMoved {
            return Err(Refusal::Retry(Status::unavailable(
                "cleanup owns this sandbox or it changed while the runtime upgrade inspected it; \
                 retry the runtime upgrade",
            )));
        }
        if recorded == crate::db::AlreadyOnTargetRecord::NotRecorded {
            // An abandon naming another sandbox can land after this call's
            // own check, under that sandbox's lock: the write refuses it as
            // the fence does, and so does the answer, unrecorded. That's
            // checked first, in the RPC's order.
            if let Some(refusal) =
                abandoned_refusal(&*self.db()?, request.operation_id, &request.sandbox_id)?
            {
                return Err(Refusal::Retry(refusal));
            }
            // The attempt's own final outcome, recorded since this call
            // checked, is its answer; otherwise another request holds the
            // operation.
            self.refuse_settled_for_attempt(request)?;
            return Err(Refusal::Final(request.names_another()));
        }
        Ok(())
    }

    async fn run(
        &self,
        record: &RuntimeUpgradeRecord,
        disk: &LocalDisk,
        start: StartRequest,
    ) -> Result<StartResponse, Status> {
        let operation_id = record.operation_id;
        let sandbox_id = &record.sandbox_id;
        let preserved = preserved_disk_name(operation_id);
        if let Err(error) = self.effects.preserve_disk(sandbox_id, &preserved).await {
            return Err(self
                .fail(
                    record,
                    RuntimeUpgradePhase::Fenced,
                    "preserve the disk",
                    error,
                )
                .await);
        }
        self.advance(
            record,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved,
        )?;

        let consumer = match self.consumer(record.target_environment, disk).await {
            Ok(consumer) => consumer,
            Err(error) => {
                return Err(self
                    .fail(
                        record,
                        RuntimeUpgradePhase::Preserved,
                        "select the consumer",
                        error,
                    )
                    .await);
            }
        };
        let bound = crate::db::bind_runtime_upgrade_consumer(
            &*self.db()?,
            operation_id,
            &record.sandbox_id,
            &consumer,
        )
        .map_err(|error| Status::internal(format!("save the upgrade consumer: {error}")))?;
        if !bound {
            return Err(Status::aborted(format!(
                "runtime upgrade {operation_id} moved before its consumer was saved"
            )));
        }

        match self.effects.cold_boot(operation_id, start).await {
            Ok(response) => {
                self.verify(record, true).await?;
                Ok(response)
            }
            // The start can fail after its guest became ready, and the
            // transaction that marked it started also verified the upgrade.
            Err(error) if self.phase(operation_id)? == Some(RuntimeUpgradePhase::Verified) => {
                self.finish_verified(record, true).await?;
                Err(Status::new(
                    error.code(),
                    format!(
                        "runtime upgrade completed and retired the old checkpoint, but its start \
                         reported: {}; retry the operation to confirm",
                        error.message()
                    ),
                ))
            }
            Err(error) => Err(self
                .fail(record, RuntimeUpgradePhase::Booting, "cold boot", error)
                .await),
        }
    }

    /// Resolve the target and check that the disk can boot under it: the
    /// same runtime kind, guest platform and components. The runtime version
    /// and snapshot format may differ, since a cold start loads no memory.
    async fn consumer(
        &self,
        target: EnvironmentReference,
        disk: &LocalDisk,
    ) -> Result<ExecutionMetadata, Status> {
        let consumer = self.effects.resolve_target(target).await?;
        if consumer.environment() != Some(target) {
            return Err(Status::failed_precondition(
                "node resolved a different environment than the upgrade target",
            ));
        }
        super::create_support::bind_disk_execution(Some(&consumer), disk.execution.clone())
    }

    /// Roll back after `step` failed in `phase`, and describe the outcome.
    async fn fail(
        &self,
        record: &RuntimeUpgradeRecord,
        phase: RuntimeUpgradePhase,
        step: &str,
        error: Status,
    ) -> Status {
        let failure = format!("{step} failed: {}", error.message());
        tracing::warn!(
            sandbox_id = %record.sandbox_id,
            operation_id = %record.operation_id,
            attempt_id = record.attempt_id.as_deref(),
            %phase,
            failure,
            "runtime upgrade failed; restoring the old disk/checkpoint pair"
        );
        match self.roll_back(record, phase, &failure).await {
            Ok(()) => Status::new(
                error.code(),
                format!("runtime upgrade rolled back to the old disk/checkpoint pair: {failure}"),
            ),
            Err(_) if matches!(self.sandbox(&record.sandbox_id), Ok(None)) => Status::aborted(
                format!("runtime upgrade {failure}; the sandbox was deleted during the upgrade"),
            ),
            Err(rollback) => Status::unavailable(format!(
                "runtime upgrade {failure}; the old pair is restored once the sandbox settles: {}",
                rollback.message()
            )),
        }
    }

    /// Return a fencing upgrade to its old pair: restore the disk if a boot
    /// may have written it, then restore the consumer and release the fence
    /// together, then drop the copy.
    async fn roll_back(
        &self,
        record: &RuntimeUpgradeRecord,
        phase: RuntimeUpgradePhase,
        failure: &str,
    ) -> Result<(), Status> {
        let preserved = preserved_disk_name(record.operation_id);
        // A suspended sandbox's disk is restored, and its consumer with it,
        // under the artifact lock a drain finds the sandbox evacuated under,
        // so the drain re-checks it until the rollback commits. A stopped
        // sandbox's, which a v2 drain doesn't move, as before.
        let mut disk_mutations = None;
        if phase == RuntimeUpgradePhase::Booting {
            self.wait_until_settled(record).await?;
            if record.source_state == SandboxState::Suspended {
                disk_mutations = Some(
                    self.effects
                        .exclude_disk_mutations_to_restore(&record.sandbox_id)
                        .await?,
                );
            }
            self.effects
                .restore_disk(&record.sandbox_id, &preserved)
                .await?;
        }
        let rolled_back = crate::db::roll_back_runtime_upgrade_for_sandbox(
            &*self.db()?,
            record.operation_id,
            &record.sandbox_id,
            failure,
            self.attempt_finality,
        )
        .map_err(|error| Status::internal(format!("record runtime upgrade rollback: {error}")))?;
        drop(disk_mutations);
        if !rolled_back {
            return Err(Status::aborted(format!(
                "runtime upgrade {} left {phase} during its rollback",
                record.operation_id
            )));
        }
        self.finish_rollback(record).await
    }

    async fn finish_rollback(&self, record: &RuntimeUpgradeRecord) -> Result<(), Status> {
        self.effects
            .discard_disk(
                &record.sandbox_id,
                &preserved_disk_name(record.operation_id),
            )
            .await?;
        let finished = crate::db::finish_runtime_upgrade_rollback_for_sandbox(
            &*self.db()?,
            record.operation_id,
            &record.sandbox_id,
        )
        .map_err(|error| Status::internal(format!("finish runtime upgrade rollback: {error}")))?;
        // False is a row already gone, finished by another settle, or one
        // that's now another sandbox's, which its own driver settles.
        if !finished
            && let Some(current) = self.record(record.operation_id)?
            && current.sandbox_id != record.sandbox_id
        {
            return Err(Status::aborted(format!(
                "runtime upgrade {} now names sandbox {}, not {}; its own driver settles it",
                record.operation_id, current.sandbox_id, record.sandbox_id
            )));
        }
        Ok(())
    }

    /// Wait until a failed boot's cleanup has returned the sandbox to the
    /// state its old pair belongs to, with no cleanup in progress. Nothing
    /// may replace the instance while a runtime could still be using it.
    async fn wait_until_settled(&self, record: &RuntimeUpgradeRecord) -> Result<(), Status> {
        let deadline = tokio::time::Instant::now() + self.settle_timeout;
        loop {
            let (sandbox, cleaning) = {
                let db = self.db()?;
                let sandbox = crate::db::read::sandbox(&db, &record.sandbox_id)
                    .map_err(|error| Status::internal(format!("read sandbox: {error}")))?
                    .ok_or_else(|| sandbox_not_found(&record.sandbox_id))?;
                let cleaning = Self::cleanup_owns_in(&db, &sandbox)?;
                (sandbox, cleaning)
            };
            // Auto-lifecycle may stop a suspended sandbox meanwhile: stopped
            // keeps only the disk, which the rollback restores too.
            let settled = sandbox.state.is_unowned(record.source_state)
                || (record.source_state == SandboxState::Suspended
                    && sandbox.state.is_unowned(SandboxState::Stopped));
            if settled && !cleaning {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::unavailable(format!(
                    "sandbox {} is {} and hasn't returned to {}",
                    record.sandbox_id, sandbox.state, record.source_state
                )));
            }
            tokio::time::sleep(self.settle_poll).await;
        }
    }

    /// Finish a verified boot. Marking the boot started normally verified the
    /// row already; one that isn't verified yet is recorded here.
    async fn verify(
        &self,
        record: &RuntimeUpgradeRecord,
        reach_target: bool,
    ) -> Result<(), Status> {
        if self.phase(record.operation_id)? != Some(RuntimeUpgradePhase::Verified) {
            self.advance(
                record,
                RuntimeUpgradePhase::Booting,
                RuntimeUpgradePhase::Verified,
            )?;
        }
        self.finish_verified(record, reach_target).await
    }

    /// Reach the requested lifecycle, when `reach_target`, before retiring
    /// what the boot superseded: once retired, the upgrade never touches the
    /// sandbox's lifecycle again.
    async fn finish_verified(
        &self,
        record: &RuntimeUpgradeRecord,
        reach_target: bool,
    ) -> Result<(), Status> {
        // Retire first: once stopped, the sandbox must not find the superseded
        // checkpoint where a warm start or restore would load it.
        self.effects.retire_checkpoint(record).await?;
        self.effects
            .discard_disk(
                &record.sandbox_id,
                &preserved_disk_name(record.operation_id),
            )
            .await?;
        if record.target_state == RuntimeUpgradeTargetState::Stopped
            && let Some(boot) = self.boot_still_running(record.operation_id)?
        {
            if reach_target {
                self.effects.stop(&record.sandbox_id, boot).await?;
            } else {
                // Marking it retired now would lose the stop the caller asked
                // for: a retired upgrade never touches the lifecycle again.
                return Err(Status::unavailable(format!(
                    "runtime upgrade {} waits for its caller to stop the sandbox",
                    record.operation_id
                )));
            }
        }
        self.advance(
            record,
            RuntimeUpgradePhase::Verified,
            RuntimeUpgradePhase::Retired,
        )
    }

    /// Whether the execution the verified boot started is still the one
    /// running. A sandbox stopped and started again since runs someone else's
    /// execution, which the upgrade must leave alone. One between states on
    /// the boot's execution is refused for a retry: a checkpoint or pause may
    /// leave it running. The stop itself rechecks the execution in its claim.
    fn boot_still_running(
        &self,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<SandboxActivationAttemptId>, Status> {
        let Some(record) = self.record(operation_id)? else {
            return Ok(None);
        };
        let Some(boot) = record.boot_execution_id else {
            return Ok(None);
        };
        let Some(sandbox) = self.sandbox(&record.sandbox_id)? else {
            return Ok(None);
        };
        match boot_activity(sandbox.state, sandbox.node_execution_id, boot) {
            BootActivity::Running => Ok(Some(boot)),
            BootActivity::Done => Ok(None),
            BootActivity::Changing(state) => Err(Status::unavailable(format!(
                "runtime upgrade {operation_id} waits for sandbox {} to leave {state} before \
                 stopping it",
                record.sandbox_id
            ))),
        }
    }

    /// Finish whatever an upgrade owes without booting anything: roll back
    /// an interrupted one, retire a verified one, finish a rollback. A boot
    /// that reached a started sandbox on the target before its verification
    /// was recorded counts as verified, since a blocking start only marks the
    /// sandbox started once its guest is ready. Only a caller that can stop
    /// the sandbox passes `reach_target`. Without it, an upgrade that must end
    /// stopped stays verified while its sandbox runs.
    pub(super) async fn settle(
        &self,
        record: &RuntimeUpgradeRecord,
        reach_target: bool,
    ) -> Result<Settled, Status> {
        // The caller's copy may predate a boot verified since it was read.
        let Some(current) = self.record(record.operation_id)? else {
            return Ok(Settled::RolledBack);
        };
        // The caller holds `record`'s sandbox's upgrade lock. Once that row
        // is gone, another sandbox may begin the same operation id under its
        // own lock, and its row is its own driver's to settle; each
        // transition below checks the sandbox again.
        if current.sandbox_id != record.sandbox_id {
            return Err(Status::aborted(format!(
                "runtime upgrade {} now names sandbox {}, not {}; its own driver settles it",
                record.operation_id, current.sandbox_id, record.sandbox_id
            )));
        }
        let record = &current;
        match record.phase {
            RuntimeUpgradePhase::Fenced | RuntimeUpgradePhase::Preserved => {
                self.roll_back(record, record.phase, "interrupted before the boot")
                    .await?;
                Ok(Settled::RolledBack)
            }
            RuntimeUpgradePhase::Booting => {
                let booted = self.sandbox(&record.sandbox_id)?.is_some_and(|sandbox| {
                    sandbox.state.is_unowned(SandboxState::Started)
                        && saved_environment(&sandbox) == Some(record.target_environment)
                });
                if booted {
                    self.verify(record, reach_target).await?;
                    Ok(Settled::Retired)
                } else {
                    self.roll_back(record, record.phase, "interrupted during the boot")
                        .await?;
                    Ok(Settled::RolledBack)
                }
            }
            RuntimeUpgradePhase::Verified => {
                self.finish_verified(record, reach_target).await?;
                Ok(Settled::Retired)
            }
            RuntimeUpgradePhase::RolledBack => {
                self.finish_rollback(record).await?;
                Ok(Settled::RolledBack)
            }
            RuntimeUpgradePhase::Retired => Ok(Settled::Retired),
        }
    }
}

const AUTHORIZATION_ID_MAX_BYTES: usize = 256;
const AUTHORIZATION_REASON_MAX_BYTES: usize = 1024;
/// How long an `UpgradeRuntime` waits for another driver of the same
/// sandbox's upgrade before answering with a retryable error.
const LOCK_WAIT: Duration = Duration::from_secs(5);
const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

fn required_text(value: &str, field: &str, max_bytes: usize) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() || value.len() > max_bytes {
        return Err(Status::invalid_argument(format!(
            "{field} must be 1 to {max_bytes} bytes"
        )));
    }
    Ok(value.to_string())
}

fn parse_sandbox_id(sandbox_id: &str) -> Result<String, Status> {
    sandbox_id
        .parse::<pplx_space_sandbox_model::SandboxId>()
        .map(|id| id.to_string())
        .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))
}

fn parse_operation_id(operation_id: &str) -> Result<RuntimeUpgradeOperationId, Status> {
    operation_id
        .parse()
        .map_err(|error| Status::invalid_argument(format!("invalid operation_id: {error}")))
}

fn parse_checkpoint_local_artifact_id(
    checkpoint_local_artifact_id: Option<&str>,
) -> Result<Option<LocalArtifactId>, Status> {
    checkpoint_local_artifact_id
        .map(str::parse)
        .transpose()
        .map_err(|error| {
            Status::invalid_argument(format!("invalid checkpoint_local_artifact_id: {error}"))
        })
}

/// The operation, target and authorization an upgrade names, parsed the same
/// way whether `UpgradeRuntime` or a Wake carries them.
fn parse_upgrade_identity(
    operation_id: &str,
    target_environment: &str,
    authorization: Option<&crate::proto::RuntimeUpgradeAuthorization>,
) -> Result<
    (
        RuntimeUpgradeOperationId,
        EnvironmentReference,
        RuntimeUpgradeAuthorization,
    ),
    Status,
> {
    let operation_id = parse_operation_id(operation_id)?;
    let target = target_environment
        .parse::<EnvironmentReference>()
        .map_err(|error| {
            Status::invalid_argument(format!("invalid target_environment: {error}"))
        })?;
    let authorization = authorization
        .ok_or_else(|| Status::invalid_argument("a runtime upgrade requires an authorization"))?;
    let authorization = RuntimeUpgradeAuthorization {
        id: required_text(
            &authorization.authorization_id,
            "authorization_id",
            AUTHORIZATION_ID_MAX_BYTES,
        )?,
        reason: required_text(
            &authorization.reason,
            "authorization reason",
            AUTHORIZATION_REASON_MAX_BYTES,
        )?,
        memory_loss: authorization.memory_loss,
    };
    Ok((operation_id, target, authorization))
}

/// Refuse an upgrade Start other than the plain blocking cold start the
/// upgrade runs: readiness is its verification.
fn require_cold_upgrade_start(start: &StartRequest, sandbox_id: &str) -> Result<(), Status> {
    use crate::proto::WakeTargetState;
    if start.sandbox_id != sandbox_id {
        return Err(Status::invalid_argument(
            "the upgrade's Start must name the upgraded sandbox",
        ));
    }
    if !start.blocking {
        return Err(Status::invalid_argument(
            "the upgrade's Start must block until guest readiness, which verifies the boot",
        ));
    }
    let cold = start.checkpoint.as_deref().is_none_or(str::is_empty)
        && !start.from_warm
        && start.restore_artifact_as_new.is_none()
        && start.suspend_restore_storage_uri.is_none()
        && start.suspend_restore_source_node.is_none()
        && start.expected_local_artifact_id.is_none()
        && matches!(
            WakeTargetState::try_from(start.target_state),
            Ok(WakeTargetState::Unspecified | WakeTargetState::Started)
        );
    if !cold {
        return Err(Status::invalid_argument(
            "the upgrade's Start must be a plain cold start into the started lifecycle",
        ));
    }
    Ok(())
}

/// Parse an `UpgradeRuntime` request.
fn parse_request(
    request: crate::proto::UpgradeRuntimeRequest,
) -> Result<RuntimeUpgradeRequest, Status> {
    use crate::proto::WakeTargetState;
    let sandbox_id = parse_sandbox_id(&request.sandbox_id)?;
    let (operation_id, target, authorization) = parse_upgrade_identity(
        &request.operation_id,
        &request.target_environment,
        request.authorization.as_ref(),
    )?;
    let target_state = match WakeTargetState::try_from(request.target_state) {
        Ok(WakeTargetState::Unspecified | WakeTargetState::Started) => {
            RuntimeUpgradeTargetState::Started
        }
        Ok(WakeTargetState::Stopped) => RuntimeUpgradeTargetState::Stopped,
        _ => {
            return Err(Status::invalid_argument(
                "a runtime upgrade ends started or stopped",
            ));
        }
    };
    let start = request
        .start
        .ok_or_else(|| Status::invalid_argument("a runtime upgrade requires its Start"))?;
    require_cold_upgrade_start(&start, &sandbox_id)?;
    Ok(RuntimeUpgradeRequest {
        operation_id,
        sandbox_id,
        target,
        target_state,
        authorization,
        start,
        checkpoint_local_artifact_id: parse_checkpoint_local_artifact_id(
            request.checkpoint_local_artifact_id.as_deref(),
        )?,
        attempt_id: parse_attempt_id(request.attempt_id)?,
    })
}

/// The longest attempt id the node records. The id is stored with each of
/// the attempt's records, so a caller-chosen one is bounded; a Kubernetes
/// pod UID or a UUID fits many times over.
const MAX_ATTEMPT_ID_BYTES: usize = 256;

/// The attempt an `UpgradeRuntime` request runs as: the id it names, or a
/// fresh one the node generates when it names none. An empty id names no
/// attempt a caller could reuse, so it's refused rather than read as omitted.
/// `GetRuntimeUpgrade` holds a set id to the same rule.
fn parse_attempt_id(attempt_id: Option<String>) -> Result<String, Status> {
    match attempt_id {
        None => Ok(crate::db::new_runtime_upgrade_attempt_id()),
        Some(id) if id.trim().is_empty() => Err(Status::invalid_argument(
            "a runtime upgrade's attempt_id is set but empty; name an attempt or leave it unset",
        )),
        Some(id) if id.len() > MAX_ATTEMPT_ID_BYTES => Err(Status::invalid_argument(format!(
            "a runtime upgrade's attempt_id is longer than {MAX_ATTEMPT_ID_BYTES} bytes"
        ))),
        Some(id) => Ok(id),
    }
}

/// The upgrade a Wake carries (`WakeRequest.runtime_upgrade`), parsed before
/// the Wake acts, with the caller its cold start runs as.
#[derive(Clone, Debug)]
pub(super) struct WakeRuntimeUpgrade {
    operation_id: RuntimeUpgradeOperationId,
    target: EnvironmentReference,
    authorization: RuntimeUpgradeAuthorization,
    /// The checkpoint the control plane decided the upgrade from, by its
    /// local artifact (`WakeRuntimeUpgrade.checkpoint_local_artifact_id`).
    checkpoint_local_artifact_id: Option<LocalArtifactId>,
    /// The Wake's caller, whom the upgrade's cold start runs as.
    metadata: tonic::metadata::MetadataMap,
    extensions: tonic::Extensions,
}

impl WakeRuntimeUpgrade {
    pub(super) const fn operation_id(&self) -> RuntimeUpgradeOperationId {
        self.operation_id
    }

    pub(super) const fn checkpoint_local_artifact_id(&self) -> Option<LocalArtifactId> {
        self.checkpoint_local_artifact_id
    }

    pub(super) fn parse(
        upgrade: &crate::proto::WakeRuntimeUpgrade,
        metadata: tonic::metadata::MetadataMap,
        extensions: tonic::Extensions,
    ) -> Result<Self, Status> {
        let (operation_id, target, authorization) = parse_upgrade_identity(
            &upgrade.operation_id,
            &upgrade.target_environment,
            upgrade.authorization.as_ref(),
        )?;
        let checkpoint_local_artifact_id =
            parse_checkpoint_local_artifact_id(upgrade.checkpoint_local_artifact_id.as_deref())?;
        Ok(Self {
            operation_id,
            target,
            authorization,
            checkpoint_local_artifact_id,
            metadata,
            extensions,
        })
    }
}

impl super::SandboxManagerService {
    /// Refuse a suspend of `sandbox_id`, retryably, while an upgrade that must
    /// leave it `stopped` may run it on its boot's execution; see
    /// [`stay_stopped_upgrade_awaiting_its_stop`]. Every suspend takes this
    /// under its wake and artifact locks, before it captures anything.
    pub(super) fn refuse_suspend_during_stay_stopped_upgrade(
        &self,
        sandbox_id: &str,
    ) -> Result<(), Status> {
        let stay_stopped_upgrade = {
            let db = self.db_conn()?;
            stay_stopped_upgrade_awaiting_its_stop(&db, sandbox_id)?
        };
        match stay_stopped_upgrade {
            Some(operation_id) => Err(Status::unavailable(format!(
                "runtime upgrade {operation_id} must stop this sandbox first; retry the suspend \
                 once it has"
            ))),
            None => Ok(()),
        }
    }

    /// Run the upgrade a Wake carries, booting `start`: the cold start the
    /// Wake built for this sandbox. The caller holds the Wake's lock.
    pub(super) async fn wake_runtime_upgrade(
        &self,
        upgrade: &WakeRuntimeUpgrade,
        start: StartRequest,
    ) -> Result<RuntimeUpgradeOutcome, Status> {
        // The Wake built `start` as its cold start, and the upgrade's boot
        // refuses anything else.
        let sandbox_id = start.sandbox_id.clone();
        self.run_runtime_upgrade(
            upgrade.metadata.clone(),
            upgrade.extensions.clone(),
            RuntimeUpgradeRequest {
                operation_id: upgrade.operation_id,
                sandbox_id,
                target: upgrade.target,
                target_state: RuntimeUpgradeTargetState::Started,
                authorization: upgrade.authorization.clone(),
                start,
                checkpoint_local_artifact_id: upgrade.checkpoint_local_artifact_id,
                // A Wake names no attempt, so its upgrade is a fresh one.
                attempt_id: crate::db::new_runtime_upgrade_attempt_id(),
            },
        )
        .await
    }

    async fn run_runtime_upgrade(
        &self,
        metadata: tonic::metadata::MetadataMap,
        extensions: tonic::Extensions,
        parsed: RuntimeUpgradeRequest,
    ) -> Result<RuntimeUpgradeOutcome, Status> {
        let sandbox_id = parsed.sandbox_id.clone();
        let _guard = self.runtime_upgrade_guard(&sandbox_id).await?;
        let upgrader = self.caller_upgrader(metadata, extensions);
        // The Wake's path to the upgrade, which never passes the RPC's
        // checks, so it makes them here in the RPC's order: a request that
        // names another identity than the operation's is refused as one, and
        // an operation already abandoned when this check runs does nothing
        // more, not even replay a retired upgrade or find the sandbox on its
        // target and let the Wake start it. A Wake already past this check
        // when an abandon lands still starts the sandbox (see the docs).
        upgrader.refuse_another_request(&parsed)?;
        self.refuse_abandoned_runtime_upgrade(parsed.operation_id, &parsed.sandbox_id)?;
        let outcome = upgrader.upgrade(parsed).await;
        crate::node_state_sync::mark_sandbox_dirty(&self.db, &sandbox_id);
        outcome
    }

    /// An upgrader whose boot and stop run as the caller of `metadata` and
    /// `extensions`.
    fn caller_upgrader(
        &self,
        metadata: tonic::metadata::MetadataMap,
        extensions: tonic::Extensions,
    ) -> RuntimeUpgrader<NodeRuntimeUpgradeEffects> {
        RuntimeUpgrader::new(
            self.db.clone(),
            NodeRuntimeUpgradeEffects::for_caller(self.clone(), metadata, extensions),
        )
        .with_attempt_finality(self.runtime_upgrade_attempt_finality)
    }

    /// Hold `sandbox_id`'s upgrade lock, waiting at most [`LOCK_WAIT`] for
    /// another driver of its upgrade.
    async fn runtime_upgrade_guard(
        &self,
        sandbox_id: &str,
    ) -> Result<crate::auto_lifecycle::LifecycleMutexGuard, Status> {
        let lock = self.auto_lifecycle.runtime_upgrade_lock(sandbox_id);
        super::restart_admission::wait_for_activation_admission(
            sandbox_id,
            tokio::time::timeout(LOCK_WAIT, lock.lock_owned()),
        )
        .await?
        .map_err(|_| Status::unavailable("another runtime upgrade step is running; retry"))
    }

    /// Refuse a call for an operation the control plane abandoned: it may
    /// already have captured the sandbox's disk, which nothing for this
    /// operation may change since.
    pub(super) fn refuse_abandoned_runtime_upgrade(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        sandbox_id: &str,
    ) -> Result<(), Status> {
        match self.abandoned_runtime_upgrade_refusal(operation_id, sandbox_id)? {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        }
    }

    /// The refusal a call for `operation_id` naming `sandbox_id` gets because
    /// the control plane abandoned the operation, if it did; an error is a
    /// read that failed, not an abandonment. See [`abandoned_refusal`].
    pub(super) fn abandoned_runtime_upgrade_refusal(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        sandbox_id: &str,
    ) -> Result<Option<Status>, Status> {
        abandoned_refusal(&*self.db_conn()?, operation_id, sandbox_id)
    }

    /// Refuse to stage or load a checkpoint while an upgrade still owes the
    /// retirement of the one it superseded: until then the canonical
    /// checkpoint may be the old runtime's, over a disk the new boot wrote.
    pub(super) fn refuse_unretired_runtime_upgrade(&self, sandbox_id: &str) -> Result<(), Status> {
        refuse_unretired_runtime_upgrade_in(&*self.db_conn()?, sandbox_id)
    }

    /// Gracefully stop `sandbox_id` and wait for it, but only while it runs the
    /// node execution `execution` began. The cleanup claim checks the
    /// execution under the fence every activation claims under, so a stop and
    /// restart in between makes this a no-op rather than stopping the new
    /// execution. Returns whether it stopped anything.
    pub(super) async fn stop_activation(
        &self,
        sandbox_id: &str,
        execution: SandboxActivationAttemptId,
    ) -> Result<bool, Status> {
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let (typed_id, runtime_label) = Self::cleanup_identity(sandbox_id, Some(&sandbox.uuid8))?;
        let admission = match self
            .admit_stop_for_activation(typed_id, sandbox.state, execution)
            .await
        {
            Ok(admission) => admission,
            Err(super::cleanup::CleanupAdmissionError::Status(status))
                if status.code() == tonic::Code::Aborted =>
            {
                let current = self.lookup_sandbox(sandbox_id)?;
                return moved_on_or_retry(
                    current.state,
                    current.node_execution_id,
                    execution,
                    &status,
                )
                .inspect(|_| {
                    tracing::info!(
                        sandbox_id,
                        %execution,
                        reason = status.message(),
                        "not stopping a sandbox that moved on from the upgrade's boot"
                    );
                });
            }
            Err(error) => return Err(error.into_status()),
        };
        self.watchdog.remove_sandbox(sandbox_id).await;
        self.snapshot_scheduler.remove_sandbox(sandbox_id).await;
        let request = super::cleanup::SynchronousCleanupRequest {
            sandbox_id,
            uuid8: runtime_label.as_str(),
            keep_filesystem: true,
            context: "cleanup after runtime upgrade stop",
            completion: crate::db::CleanupCompletion::MarkStopped,
            published_artifact: None,
            immediate_kill: false,
            // Admission persisted the flush decision; this asks nothing.
            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
        };
        if admission.is_unpersisted_for_sandbox(typed_id)? {
            if let Some(error) = self
                .run_cleanup_after_persistence_failure(request, admission)
                .await?
            {
                return Err(Status::internal(format!(
                    "runtime cleanup completed but the stopping transition could not be persisted for sandbox {sandbox_id}: {error}"
                )));
            }
        } else {
            self.run_admitted_stop_to_completion(request, admission)
                .await?;
        }
        crate::node_state_sync::mark_sandbox_dirty(&self.db, sandbox_id);
        Ok(true)
    }

    /// [`refuse_runtime_upgrade_fence_in`] on a connection of its own.
    pub(super) fn refuse_runtime_upgrade_fence(&self, sandbox_id: &str) -> Result<(), Status> {
        refuse_runtime_upgrade_fence_in(&*self.db_conn()?, sandbox_id)
    }

    /// Settle every unfinished upgrade once now and then on an interval:
    /// interrupted ones roll back, verified ones retire their checkpoint.
    pub fn start_runtime_upgrade_recovery(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
        tracker: &tokio_util::task::TaskTracker,
    ) {
        let service = self.clone();
        tracker.spawn(async move {
            loop {
                service.recover_runtime_upgrades().await;
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(RECOVERY_INTERVAL) => {}
                }
            }
        });
    }

    async fn recover_runtime_upgrades(&self) {
        let records = match self.db_conn().and_then(|db| {
            crate::db::scan::unsettled_runtime_upgrades(&db)
                .map_err(|error| Status::internal(error.to_string()))
        }) {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(%error, "runtime upgrade recovery could not list upgrades");
                return;
            }
        };
        for record in records {
            let lock = self.auto_lifecycle.runtime_upgrade_lock(&record.sandbox_id);
            // An RPC is driving this one.
            let Ok(_guard) = lock.try_lock_owned() else {
                continue;
            };
            let Ok(_writer) = self.state_writer_lease("RuntimeUpgradeRecovery") else {
                return;
            };
            let upgrader = RuntimeUpgrader::without_settle_wait(
                self.db.clone(),
                NodeRuntimeUpgradeEffects::for_recovery(self.clone()),
            )
            // Recovery's rollback of an interrupted attempt is final for that
            // attempt exactly as an RPC's would be.
            .with_attempt_finality(self.runtime_upgrade_attempt_finality);
            match upgrader.settle(&record, false).await {
                Ok(settled) => {
                    tracing::info!(
                        sandbox_id = %record.sandbox_id,
                        operation_id = %record.operation_id,
                        phase = %record.phase,
                        ?settled,
                        "settled runtime upgrade"
                    );
                    crate::node_state_sync::mark_sandbox_dirty(&self.db, &record.sandbox_id);
                }
                Err(error) => tracing::warn!(
                    sandbox_id = %record.sandbox_id,
                    operation_id = %record.operation_id,
                    phase = %record.phase,
                    %error,
                    "runtime upgrade recovery will retry"
                ),
            }
        }
    }
}

fn memory_loss_unauthorized() -> Status {
    Status::failed_precondition(
        "upgrading a suspended sandbox discards its memory; the authorization must accept memory loss",
    )
}

fn cleanup_active() -> Status {
    Status::unavailable("cleanup owns this sandbox; retry the runtime upgrade")
}
