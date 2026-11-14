//! Node-local runtime upgrades (`UpgradeRuntime`).
//!
//! An upgrade moves a stopped or suspended sandbox to another execution
//! environment by cold-booting its disk under that environment. It follows the
//! rules `docs/execution-environments.md` sets for an incompatible upgrade:
//!
//! 1. **Fence.** A `sandbox_runtime_upgrades` row, inserted only while the
//!    sandbox is stopped or suspended and owned by nothing else, refuses every
//!    other activation until the boot is verified or the old pair is back.
//! 2. **Preserve.** A read-only copy of the instance subvolume (rootfs, and
//!    for encrypted sandboxes `cow.luks` and the suspend volume) is kept under
//!    an internal snapshot name. The checkpoint directory is never touched.
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
//! substrate derives it from the pod UID, so kubelet's retries of a failed
//! create are one attempt. A rollback, or a refusal before anything was
//! fenced that no retry can change, is final for the attempt it was recorded
//! for: a retry of that attempt gets the recorded failure back and never
//! fences or boots again, even after a later attempt's outcome replaces it as
//! the operation's reported one, and only a new attempt starts over. A
//! transient refusal (the node busy or failing) is left unrecorded, so a retry
//! of the same attempt tries again. A caller settles on such an outcome only
//! when it names the attempt it's waiting on, then gives that attempt up, so
//! its own retry is a new one. An upgraded or already-on-target outcome is
//! answered to any attempt, but a started target's activation that fails
//! after it is recorded as refused for that attempt, final for it the same
//! way, unless the sandbox is still moving (see `rpc`).

use std::time::Duration;

use pplx_space_common_core::types::{LocalArtifactId, SandboxActivationAttemptId};
use pplx_space_execution_spec::{EnvironmentReference, ExecutionMetadata};
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
mod stale_copy;
#[cfg(test)]
mod tests;

pub(super) use node_effects::NodeRuntimeUpgradeEffects;

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
    /// guard drops.
    async fn exclude_disk_mutations(&self, sandbox_id: &str) -> Box<dyn Send>;
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
#[derive(Debug)]
pub(super) enum Refusal {
    /// Every retry of the same request is answered the same way: it
    /// contradicts what the node recorded or holds (the checkpoint, the edge,
    /// the authorization, a disk that's missing or unreadable as written), or
    /// names another operation. Recorded for the attempt, so its retries get it
    /// back.
    Final(Status),
    /// The node was busy or failing, or the sandbox may yet change, so a
    /// retry may be answered differently. Left unrecorded.
    Retry(Status),
}

impl Refusal {
    pub(super) fn into_status(self) -> Status {
        match self {
            Refusal::Final(status) | Refusal::Retry(status) => status,
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
/// final; a catalog that can't be loaded may load on a retry.
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
}

impl<E: RuntimeUpgradeEffects> RuntimeUpgrader<E> {
    pub(super) fn new(db: crate::db::SharedConnection, effects: E) -> Self {
        Self {
            db,
            effects,
            settle_timeout: SETTLE_TIMEOUT,
            settle_poll: SETTLE_POLL,
        }
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

    fn existing_sandbox(&self, sandbox_id: &str) -> Result<SandboxRow, Status> {
        self.sandbox(sandbox_id)?
            .ok_or_else(|| sandbox_not_found(sandbox_id))
    }

    /// `refusal`, final only while `sandbox` is still as `fence` read it. A
    /// cold `Start` takes neither the upgrade lock nor the artifact lock
    /// `fence` holds, and nothing is fenced until `begin_runtime_upgrade`, so
    /// a stopped sandbox may have started or rebound its execution while the
    /// disk was inspected; a refusal decided on the earlier read is then one a
    /// retry decides again. Compares what `begin_runtime_upgrade` compares to
    /// answer `SandboxChanged`. A row that can't be read is a retry too.
    fn final_unless_changed(&self, sandbox: &SandboxRow, refusal: Refusal) -> Refusal {
        let Refusal::Final(status) = refusal else {
            return refusal;
        };
        match self.sandbox(&sandbox.id) {
            Ok(Some(now))
                if now.state == sandbox.state
                    && now.current_local_artifact_id == sandbox.current_local_artifact_id
                    && now.execution_metadata == sandbox.execution_metadata =>
            {
                Refusal::Final(status)
            }
            _ => Refusal::Retry(status),
        }
    }

    fn advance(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        from: RuntimeUpgradePhase,
        to: RuntimeUpgradePhase,
    ) -> Result<(), Status> {
        let advanced = crate::db::advance_runtime_upgrade(&*self.db()?, operation_id, from, to)
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
    /// than its operation's live row or recorded outcome does: a retry may
    /// not turn an operation into another one.
    pub(super) fn refuse_another_request(
        &self,
        request: &RuntimeUpgradeRequest,
    ) -> Result<(), Status> {
        let recorded = crate::db::read::recorded_runtime_upgrade_request(
            &*self.db()?,
            request.operation_id,
        )
        .map_err(|error| Status::internal(format!("read runtime upgrade outcome: {error}")))?;
        let live = self
            .record(request.operation_id)?
            .map(|record| record.identity());
        if recorded
            .into_iter()
            .chain(live)
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
                return Err(request.names_another());
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
            Err(refusal) => {
                self.record_refusal(&request, &refusal);
                return Err(refusal.into_status());
            }
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
    /// included, starts over.
    pub(super) fn refuse_settled_for_attempt(
        &self,
        request: &RuntimeUpgradeRequest,
    ) -> Result<(), Status> {
        let attempt_id = &request.attempt_id;
        let Some(settled) = crate::db::read::runtime_upgrade_attempt_outcome(
            &*self.db()?,
            request.operation_id,
            attempt_id,
        )
        .map_err(|error| Status::internal(format!("read runtime upgrade outcome: {error}")))?
        else {
            return Ok(());
        };
        let operation_id = request.operation_id;
        let failure = settled.failure;
        let failure = failure.as_deref().unwrap_or("no reason recorded");
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
    /// failure) stays unrecorded, so a retry of the same attempt tries again.
    /// Best effort: an unrecorded refusal leaves the attempt's report as it
    /// was, which its caller doesn't settle on.
    pub(super) fn record_refusal(&self, request: &RuntimeUpgradeRequest, refusal: &Refusal) {
        let Refusal::Final(refusal) = refusal else {
            return;
        };
        let recorded = self.db().and_then(|db| {
            crate::db::record_runtime_upgrade_refusal(
                &db,
                &request.identity(),
                &request.attempt_id,
                refusal.message(),
            )
            .map_err(|error| Status::internal(error.to_string()))
        });
        match recorded {
            Ok(true) => {}
            Ok(false) => tracing::debug!(
                sandbox_id = %request.sandbox_id,
                operation_id = %request.operation_id,
                "runtime upgrade refusal not recorded: the operation's record keeps its outcome"
            ),
            Err(error) => tracing::warn!(
                sandbox_id = %request.sandbox_id,
                operation_id = %request.operation_id,
                %error,
                "could not record the runtime upgrade's refusal"
            ),
        }
    }

    /// Everything before the boot: decide whether the sandbox is already on
    /// the target, check the edge, and fence it. Every error here leaves
    /// nothing fenced for this operation.
    async fn fence(&self, request: &RuntimeUpgradeRequest) -> Result<Fence, Refusal> {
        // Snapshot rollback checks the fence under the same exclusion before
        // it rewrites the instance, so it either finishes before the fence or
        // sees it.
        let disk_mutations = self
            .effects
            .exclude_disk_mutations(&request.sandbox_id)
            .await;
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
        require_decided_checkpoint(&sandbox, request.checkpoint_local_artifact_id)
            .map_err(Refusal::Final)?;
        if !sandbox.state.is_unowned(SandboxState::Suspended)
            && saved_environment(&sandbox) == Some(request.target)
        {
            return self
                .already_on_target(request)
                .map(|()| Fence::AlreadyOnTarget);
        }
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
            Err(refusal) => return Err(self.final_unless_changed(&sandbox, refusal)),
        };
        if sandbox.state.is_unowned(SandboxState::Suspended)
            && disk
                .checkpoint_execution
                .as_ref()
                .and_then(ExecutionMetadata::environment)
                == Some(request.target)
        {
            return self
                .already_on_target(request)
                .map(|()| Fence::AlreadyOnTarget);
        }
        // An operation that already found its sandbox on the target never
        // boots: a replay after the sandbox moved to another environment
        // would otherwise undo that move.
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
            return Err(Refusal::Final(Status::failed_precondition(format!(
                "runtime upgrade {} found sandbox {} on its target before, and the sandbox \
                 moved since; a new attempt takes a new operation id",
                request.operation_id, request.sandbox_id
            ))));
        }
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
                self.final_unless_changed(&sandbox, refusal)
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
                attempt_id: &request.attempt_id,
            },
        )
        .map_err(|error| Status::internal(format!("fence runtime upgrade: {error}")))?;
        drop(disk_mutations);
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
            BeginRuntimeUpgrade::CleanupActive => {
                return Err(Refusal::Retry(Status::unavailable(
                    "cleanup owns this sandbox; retry the runtime upgrade",
                )));
            }
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
                return Err(Refusal::Final(Status::failed_precondition(
                    "upgrading a suspended sandbox discards its memory; the authorization must accept memory loss",
                )));
            }
            BeginRuntimeUpgrade::SandboxChanged => {
                return Err(Refusal::Retry(Status::aborted(
                    "the sandbox changed while the runtime upgrade inspected it; retry",
                )));
            }
        };
        Ok(Fence::Fenced(Box::new((record, disk))))
    }

    /// Record that the upgrade fenced nothing, so a caller that lost this
    /// answer reads it from `GetRuntimeUpgrade`.
    fn already_on_target(&self, request: &RuntimeUpgradeRequest) -> Result<(), Refusal> {
        // Recorded only if no other request reused the operation id first;
        // the per-sandbox upgrade lock doesn't serialize two sandboxes.
        let recorded = crate::db::record_runtime_upgrade_already_on_target(
            &*self.db()?,
            &request.identity(),
            &request.attempt_id,
        )
        .map_err(|error| Status::internal(format!("record runtime upgrade outcome: {error}")))?;
        if !recorded {
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
            operation_id,
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
        let bound = crate::db::bind_runtime_upgrade_consumer(&*self.db()?, operation_id, &consumer)
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
        if phase == RuntimeUpgradePhase::Booting {
            self.wait_until_settled(record).await?;
            self.effects
                .restore_disk(&record.sandbox_id, &preserved)
                .await?;
        }
        let rolled_back =
            crate::db::roll_back_runtime_upgrade(&*self.db()?, record.operation_id, failure)
                .map_err(|error| {
                    Status::internal(format!("record runtime upgrade rollback: {error}"))
                })?;
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
        crate::db::finish_runtime_upgrade_rollback(&*self.db()?, record.operation_id).map_err(
            |error| Status::internal(format!("finish runtime upgrade rollback: {error}")),
        )?;
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
                let cleaning = sandbox.cleanup_completion.is_some()
                    || crate::db::read::sandbox_view(&db, &record.sandbox_id)
                        .cleanup_episode()
                        .present()
                        .map_err(|error| {
                            Status::internal(format!("read cleanup episode: {error}"))
                        })?;
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
                record.operation_id,
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
            record.operation_id,
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
        .parse::<pplx_space_common_core::types::SandboxId>()
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
fn parse_attempt_id(attempt_id: Option<String>) -> Result<String, Status> {
    match attempt_id {
        None => Ok(crate::db::new_runtime_upgrade_attempt_id()),
        Some(id) if id.trim().is_empty() => Err(Status::invalid_argument(
            "a runtime upgrade's attempt_id is empty; omit it for a fresh attempt",
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
        let outcome = self
            .caller_upgrader(metadata, extensions)
            .upgrade(parsed)
            .await;
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
            );
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
