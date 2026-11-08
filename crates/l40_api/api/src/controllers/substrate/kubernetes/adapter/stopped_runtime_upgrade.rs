//! Upgrading a stopped sandbox's runtime without starting it
//! ([`SubstrateAdapter::upgrade_stopped_runtime`]).
//!
//! The start-time decision ([`KubernetesSubstrate::decide_runtime_upgrade`])
//! chooses the move, as for a start; the operation is a different one from a
//! start's ([`super::super::runtime_upgrade::DecidedUpgrade::pod_upgrade`]).
//! The sandbox's row stays `stopped` throughout; only an execution exists for
//! the boot. A row a drain's re-placement holds `restoring` answers a
//! retryable 503; any other lifecycle is a `Conflict` (409). Before any of
//! it, the operation refuses with `FailedPrecondition` (412) a sandbox whose
//! disk is on no node, one whose disk node isn't the registered node its
//! placement pins the disk to (`active_node_id`, mapped through `node_k8s`,
//! with no other re-placement target chosen), and one whose disk node isn't a
//! registered Kubernetes node or is outside the sandbox node selector. A disk node
//! that can't take work now (not Ready, cordoned, placement-blocked or out of
//! network slots), or a Node or pod reflector that hasn't synced, answers a
//! retryable 503 instead ([`KubernetesSubstrate::local_source_node_refusal`]).
//! A retry that will reuse the pod an earlier attempt left there needs no new
//! network slot, so that pod's own is discounted.
//!
//! 1. **Claim.** An activation attempt targeting `stopped`. Only a drain's
//!    re-placement claims that target too, and it claims it on a `restoring`
//!    row. Activation-completion receipts release only a `created`,
//!    `started` or `paused` target, so the claim never moves the row, and a
//!    user start waits for the attempt like any other.
//! 2. **Record** the upgrade (operation, target, authorization) on the
//!    claimed execution in the claim's own transaction, so no claim commits
//!    without it, before any pod is created, while the row is still
//!    `stopped`. The record is what makes a claim the upgrade's: any other
//!    activation that claims the execution clears it, and a drain doesn't
//!    join an attempt that carries one. Under the sandbox row's lock the
//!    claim also checks the disk node again: a drain that released or
//!    re-placed the sandbox since the check above leaves the row `stopped`,
//!    and answers a retryable 503 with nothing claimed.
//! 3. **Place** the pod a start would create, pinned by `spec.nodeName` to
//!    the disk node checked above, never resolved again or re-driven to
//!    another node, in its runtime-upgrade mode with target state `stopped`
//!    ([`super::super::pod_runtime_upgrade`]). Its shim boots the disk under
//!    the target and stops it again, or the node rolls back to the old
//!    environment. A pod already at the execution's name that doesn't carry
//!    this operation, a crashed start's say, is replaced under a new
//!    generation (`pod_ops/ensure_pod.rs`).
//! 4. **Wait** for the node's `GetRuntimeUpgrade` to settle, then, for an
//!    upgrade that reached the target, until the node has published the
//!    sandbox stopped under it. The publication is what says the sandbox
//!    ended at rest: the node's
//!    report is the upgrade's outcome, not the pod's, and a node that finds
//!    the sandbox on the target but running refuses the pod's create.
//! 5. **Back up** the upgraded disk: enqueue the current-instance disk
//!    backup the stop path enqueues, so the backup that replaces the durable
//!    copy is of the environment the node published. The enqueue goes to the
//!    node the execution names, the disk node the pod is pinned to, by name,
//!    not through the pod: the node settles the upgrade inside the shim's
//!    pod-sandbox create, before kubelet posts the pod's address. The node
//!    settles once the sandbox's cleanup returns, while node-manager's
//!    network-slot reset after it may still hold the sandbox's host-mutation
//!    claim, and node-manager refuses the backup's disk snapshot with
//!    `Unavailable` until it's released. Sandbox-manager passes that code
//!    through, and the enqueue asks again until the phase deadline
//!    ([`BACKUP_ENQUEUE_RETRY`]). A failed enqueue keeps the execution.
//! 6. **Retire** the execution: delete the pod under its UID and forget its
//!    tracking. A retire that fails, or finds a pod with another UID at the
//!    name, keeps the execution for a retry and answers a retryable 503,
//!    except for an execution whose pod name doesn't parse, which no retry
//!    can retire: it's kept for an operator, and the operation answers
//!    `Failed`, whether the upgrade reached the target or not.
//!
//! A rollback, or a call the node refused before it began, retires the
//! execution without a backup: the disk is the one the sandbox stopped with.
//! The node records either for the pod that asked, and the wait settles on
//! it only when it names that pod ([`super::super::pod_runtime_upgrade`]):
//! a rollback the api settles on is final, since no retry from that pod can
//! still upgrade the disk, and the api's own retry gets a new pod. A
//! rollback answers a retryable 503: a retry places the operation in a new
//! pod, which the node takes as a new attempt, and may succeed. A refusal
//! the node recorded answers `FailedPrecondition` (412), since a retry
//! decides the same operation from the same inputs and the node refuses it
//! until what it checked changes. A rollback whose retire
//! failed also answers 503 until a retry retires it: nothing but a retry
//! takes the execution and its pod over, and that retry reads the operation
//! from the node again before it retires. Such a failure expires the claim
//! but keeps its target and the recorded upgrade, as does a backup that
//! wasn't enqueued.
//!
//! An outcome the wait can't settle (one that hasn't settled yet, a node
//! that can't be read, a report that names no pod or another pod, a
//! publication that hasn't landed) may leave the node working, and deleting
//! the pod would race the node's own rollback or lose its publication. So
//! the attempt keeps its claim's lease running, with the pod and the
//! recorded upgrade: until the lease ends, a start, a drain or another retry
//! is refused rather than taking the execution mid-upgrade. A retry after
//! the lease ends re-drives it. A retry that finds such an
//! execution takes it over first, then reads what it recorded, so nothing
//! can be recorded or placed between the two. The takeover's claim ends as
//! soon as that read returns, however it went: expired with the record, or
//! given up when nothing is recorded. It gives that claim up again
//! to boot under a fresh one; an activation that takes the execution in
//! between owns it, sandbox-manager's fence serializes it behind any boot
//! still running, and this attempt answers 503. It renders that same upgrade
//! again, whatever the policy says now, so the node finishes, replays or
//! rolls back the operation it may still be running, and only then is the
//! execution backed up and retired.
//!
//! A claim targeting `stopped` with nothing recorded isn't the upgrade's: a
//! drain's re-placement left it, and its pod holds the sandbox's re-placed
//! copy. While its lease runs, the upgrade answers 503 and leaves the
//! execution to the drain. Once the lease has ended, the upgrade claims the
//! execution as any activation may claim one whose lease ended
//! (`assign_activation_attempt_in`, `store/activation_fence.rs`), and its
//! pod, which doesn't carry the upgrade, is replaced like any other.
//!
//! An activation other than this upgrade's retry that takes over an
//! execution whose claim still records the upgrade enqueues the disk backup
//! the upgrade may still owe before it claims the execution, whether or not
//! the node has published the upgrade's result, and refuses while that fails
//! ([`KubernetesSubstrate::enqueue_backup_owed_by_stopped_upgrade`]): with
//! `FailedPrecondition` (412) for a cause no retry changes, retryably
//! otherwise, including a sandbox the node hasn't stopped yet. Claiming
//! clears the record, and nothing else would back the upgraded disk up. The claim decides again under the sandbox row's lock,
//! since the lease may end after that check skipped a live claim: a claim
//! that would clear a record, with no backup enqueued for its operation, is
//! refused retryably (`assign_activation_attempt_in`, `store/activation_fence.rs`).
//!
//! A drain's stopped evacuation and this operation exclude each other under
//! the sandbox row's lock: the evacuation opens only while no upgrade holds a
//! live lease, and once its marker is set the claim and the takeover refuse
//! retryably. An upgrade whose lease had ended is taken over by the
//! evacuation, which deletes its pod before it mints its backup and retires
//! its execution at teardown (`super::super::stopped_evacuation`).
//!
//! Placement isn't changed: the claim, the record, the retire and the
//! re-drive live here in the Kubernetes adapter, not in shared code above it,
//! which the substrate rules (`controllers/substrate/AGENTS.md`) count as
//! debt. Moving them is known debt this operation doesn't pay.
//!
//! The pod such an attempt leaves is a placeholder that runs no microVM, so
//! only a retry of the same operation reuses it. A user start that takes the
//! expired claim first replaces the pod and the execution, as for any stale
//! pod (`pod_ops/ensure_pod.rs`), and the recorded upgrade goes with them;
//! a start that reuses an execution with no pod clears the record.

use super::super::pod_ops::LocalSourceNodeRefusal;
use super::super::pod_runtime_upgrade::{
    MissingPod, NODE_READ_FLOOR, PodUpgrade, PodUpgradeFailure,
};
use super::super::store::{RecordedRuntimeUpgrade, StoppedRuntimeUpgradeClaim};
use super::*;
use crate::controllers::substrate::execution_fence::{
    ExecutionIdentity, ObservedPod, RecordedExecution,
};
use crate::controllers::substrate::runtime_upgrade::{StoppedRuntimeUpgrade, UpgradeActivation};
use crate::controllers::substrate::{
    EnqueueArtifactBackupFailure, enqueue_artifact_backup_status_with_client,
};
use pplx_space_common_core::kubernetes::PodRuntimeUpgradeTargetState;
use pplx_space_common_core::types::{EnvironmentReference, SandboxActivationAttemptId};

/// The upgrade an earlier attempt recorded, placed again. Sandbox-manager
/// compares the target, the target state and the authorization's id; a
/// stopped sandbox's upgrade never accepts memory loss and names no
/// checkpoint.
fn upgrade_again(recorded: RecordedRuntimeUpgrade) -> PodUpgrade {
    PodUpgrade {
        authorization_reason: format!(
            "finish runtime upgrade {} of a stopped sandbox to {}",
            recorded.operation_id, recorded.target
        ),
        operation_id: recorded.operation_id,
        target: recorded.target,
        authorization_id: recorded.authorization_id,
        memory_loss: false,
        target_state: PodRuntimeUpgradeTargetState::Stopped,
        checkpoint_local_artifact_id: None,
    }
}

/// The retryable refusal for a stopped sandbox whose execution another
/// activation owns: a user start, or this upgrade on another replica.
fn activation_in_flight(sandbox_id: SandboxId) -> SubstrateError {
    SubstrateError::Unavailable {
        message: format!(
            "another activation owns stopped sandbox {sandbox_id}; retry its runtime upgrade"
        ),
    }
}

/// How often the operation rereads the node's publication once the upgrade
/// settled.
const PUBLICATION_POLL: Duration = Duration::from_millis(200);

/// How long the disk backup's enqueue waits before it asks a node that
/// refused it as unavailable again, the cadence the generation-bound
/// suspend's observation retries at (`adapter/suspend.rs`).
const BACKUP_ENQUEUE_RETRY: Duration = Duration::from_millis(250);

/// How long an activation keeps asking for the backup a stopped upgrade left
/// owed while the node refuses it as unavailable. It has no phase deadline of
/// its own to spend; this covers a host mutation such as a network-slot
/// reset.
const OWED_BACKUP_ENQUEUE_WINDOW: Duration = Duration::from_secs(2);

/// The pod a boot placed, as its execution named it, and the operation it
/// carries.
pub(in crate::controllers::substrate::kubernetes) struct BootedPod {
    pub(in crate::controllers::substrate::kubernetes) name: String,
    pub(in crate::controllers::substrate::kubernetes) uid: Option<PodUid>,
    pub(in crate::controllers::substrate::kubernetes) operation_id: uuid::Uuid,
}

impl KubernetesSubstrate {
    pub(super) async fn upgrade_stopped_runtime_on_kubernetes(
        &self,
        sandbox_id: SandboxId,
        timeout: Duration,
    ) -> Result<StoppedRuntimeUpgrade, SubstrateError> {
        let sandbox = self.store.require_sandbox(sandbox_id).await?;
        let lifecycle = normalize_lifecycle(&sandbox.lifecycle);
        if lifecycle != "stopped" {
            // A drain's re-placement holds a stopped sandbox `restoring`
            // while it moves the disk, and leaves it `stopped` again: as for
            // a disk still moving off a draining node, a retry may succeed.
            if lifecycle == "restoring" {
                let (_, marker, _) = self
                    .store
                    .fetch_restore_ownership_snapshot(sandbox_id)
                    .await?;
                if marker.as_deref() == Some(SandboxLifecycle::Stopped.as_str()) {
                    return Err(SubstrateError::Unavailable {
                        message: format!(
                            "a drain is re-placing stopped sandbox {sandbox_id}; retry its \
                             runtime upgrade once it's placed"
                        ),
                    });
                }
            }
            return Err(SubstrateError::Conflict {
                message: format!(
                    "a runtime upgrade without a start needs a stopped sandbox, not {}",
                    sandbox.lifecycle
                ),
            });
        }
        // The upgrade owes a current-instance disk backup, and an encrypted
        // sandbox's stopped disk has no artifact the platform can use: the
        // stopped capture synthesizes plaintext metadata, and sandbox-manager
        // stages no plaintext rootfs from an encrypted one, which is why the
        // drain never evacuates an encrypted stopped sandbox either. The
        // setting is fixed at create, so no retry changes the refusal.
        if sandbox.volume_encryption {
            return Err(SubstrateError::FailedPrecondition {
                message: format!(
                    "stopped sandbox {sandbox_id} runs on encrypted volumes, whose stopped disk \
                     can't be backed up, so its runtime can't be upgraded until it is started"
                ),
            });
        }
        let left = self.store.stopped_runtime_upgrade_claim(sandbox_id).await?;
        if left.as_ref().is_some_and(|claim| claim.live) {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "another runtime upgrade of stopped sandbox {sandbox_id}, or a drain's \
                     re-placement, is in flight; retry"
                ),
            });
        }
        // The pod an earlier attempt left at its execution, which a retry
        // reuses when it carries the recorded operation.
        let mut retained_pod_name = None;
        let upgrade = match left {
            // An earlier attempt of this upgrade left its execution. Take it
            // over first, so a start or another retry that claims it
            // meanwhile either wins, which fails the takeover, or waits for
            // this one; then read what it recorded, which nothing else can
            // record once this attempt owns it.
            Some(StoppedRuntimeUpgradeClaim {
                generation,
                activation_attempt_id,
                pod_name,
                upgrade: Some(_),
                ..
            }) => {
                retained_pod_name = Some(pod_name);
                let takeover = self
                    .store
                    .take_over_stopped_runtime_upgrade(
                        sandbox_id,
                        generation,
                        activation_attempt_id,
                        SandboxActivationAttemptId::new(),
                    )
                    .await
                    .map_err(|error| match error {
                        SubstrateError::ActivationInFlight { .. } => {
                            activation_in_flight(sandbox_id)
                        }
                        error => error,
                    })?;
                let recorded = self
                    .upgrade_recorded_under(sandbox_id, generation, takeover)
                    .await;
                // The takeover's claim ends here on every outcome of the
                // read, before anything can return: left live, it would
                // refuse starts and retries until its lease ran out. It's
                // expired with the record kept, or given up if nothing is
                // recorded (`expire_activation_attempt_best_effort`).
                self.store
                    .expire_activation_attempt_best_effort(sandbox_id, generation, takeover)
                    .await;
                // It placed this upgrade and didn't finish it. Place it again,
                // whatever the policy says now: the node finishes or replays
                // it, or rolls it back and says so.
                Some(upgrade_again(recorded?))
            }
            // A claim targeting `stopped` with nothing recorded isn't this
            // upgrade's: a drain's re-placement left it, and its pod is the
            // sandbox's re-placed copy. An upgrade's claim never commits
            // without its record. Its lease has ended (a live one was refused
            // above), so this upgrade claims the execution as any activation
            // may (`assign_activation_attempt_in`), and its own claim checks
            // the lease again under the row lock.
            Some(_) | None => self.plan_upgrade(sandbox_id).await?,
        };
        let Some(upgrade) = upgrade else {
            return Ok(StoppedRuntimeUpgrade::AsRecorded);
        };
        // A stopped sandbox whose disk is only in object storage has no node
        // to boot it on; the upgrade doesn't stage one.
        let source = self.store.restore_source_plan(sandbox_id).await?;
        if source.stopped_evacuation_unplaced {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id} is moving off a draining node; retry its runtime \
                     upgrade once it's placed"
                ),
            });
        }
        let Some(source_node) = source.node_name else {
            return Err(SubstrateError::FailedPrecondition {
                message: format!(
                    "stopped sandbox {sandbox_id}'s disk isn't on a node, so its runtime can't be \
                     upgraded until it is started"
                ),
            });
        };
        // The pod goes only where the placement pins the disk to a registered
        // node (`restore_source::disk_pin_node_name`), the one node an
        // attempt that ends before persisting its pod can still be backed up
        // on (`enqueue_backup_owed_by_stopped_upgrade`). A node named only by
        // execution history, or by a re-placement's chosen target, is
        // refused here rather than left owing a backup no node can take.
        if self.store.disk_pin_node_name(sandbox_id).await?.as_deref() != Some(source_node.as_str())
        {
            return Err(SubstrateError::FailedPrecondition {
                message: format!(
                    "stopped sandbox {sandbox_id}'s disk node {source_node} isn't the registered \
                     node its placement pins the disk to, so its runtime can't be upgraded until \
                     it is started"
                ),
            });
        }
        // The node named for the disk may be one that's gone: the name falls
        // back to the newest execution that named any. A pod pinned to it
        // would never run, and nowhere else holds the disk. A node that's
        // gone, or outside the sandbox node selector, stays that way for
        // this disk, so the refusal is for good (412). Anything else (cold
        // caches, a node that's not Ready, cordoned or placement-blocked, no
        // free network slot) may clear, so it's retryable (503).
        //
        // A retry that will reuse the pod an earlier attempt left there, one
        // carrying the recorded operation, needs no new network slot: that
        // pod already holds its own, so it's discounted.
        let held = retained_pod_name
            .as_deref()
            .and_then(|name| self.lookup_sandbox_pod(name))
            .filter(|pod| {
                pod.carries_runtime_upgrade(upgrade.operation_id)
                    && pod.node_name() == Some(source_node.as_str())
                    && !pod.is_deleting()
                    && !pod.is_terminal()
            })
            .map_or(0, |pod| pod.network_slot_request());
        if let Some(refusal) = self.local_source_node_refusal(&source_node, held) {
            let message = format!(
                "stopped sandbox {sandbox_id}'s disk is on node {source_node}, which {}, so its \
                 runtime can't be upgraded there",
                refusal.describe()
            );
            return Err(if refusal.is_lasting() {
                SubstrateError::FailedPrecondition { message }
            } else {
                SubstrateError::Unavailable {
                    message: format!("{message}; retry"),
                }
            });
        }

        let (wait_budget, phase_deadline) = ready_wait_bounds(timeout);
        // No client entry loaded the sandbox: the upgrade's pod is labeled
        // from the sandbox's own record, as a reconciler's rebuild is.
        let assignment = ActivationAttemptAssignment::new(
            SandboxActivationAttemptId::new(),
            SandboxTransitionTargetState::Stopped,
            crate::controllers::substrate::KeyResolution::Unresolved,
        );
        let mut booted_pod = None;
        let booted = match tokio::time::timeout(
            wait_budget,
            Box::pin(self.boot_for_upgrade(
                &sandbox,
                &source_node,
                &upgrade,
                &assignment,
                phase_deadline,
                &mut booted_pod,
            )),
        )
        .await
        {
            Ok(booted) => booted,
            Err(_elapsed) => Err(PodUpgradeFailure::MayStillRun(SubstrateError::Timeout {
                message: format!(
                    "the runtime upgrade of stopped sandbox {sandbox_id} didn't finish within \
                     {}s; retry to converge",
                    wait_budget.as_secs()
                ),
                phase: None,
            })),
        };
        let Some((attempt_id, generation)) = assignment.assigned() else {
            // Nothing was claimed, so nothing was placed.
            return Err(match booted {
                Ok(()) => SubstrateError::Failed {
                    message: "the runtime upgrade booted without an activation attempt".to_string(),
                },
                Err(failure) => failure.into_error(),
            });
        };
        // The node may still be working on it: the claim keeps its lease, so
        // nothing takes the execution mid-upgrade, and a retry once the lease
        // ends re-drives it.
        let settled = match booted {
            Err(PodUpgradeFailure::MayStillRun(error)) => return Err(error),
            Err(PodUpgradeFailure::Settled(failure) | PodUpgradeFailure::LeftOnTarget(failure)) => {
                Err(failure)
            }
            Ok(()) => Ok(()),
        };
        // Only an upgraded disk needs the backup: a rollback put back the disk
        // the sandbox stopped with.
        let retired = self
            .retire_upgrade_boot(
                sandbox_id,
                generation,
                attempt_id,
                booted_pod,
                settled.is_ok().then_some(phase_deadline),
            )
            .await;
        let error = match (settled, retired) {
            (Ok(()), Ok(())) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    operation_id = %upgrade.operation_id,
                    to = %upgrade.target,
                    "upgraded the stopped sandbox's runtime without starting it"
                );
                return Ok(StoppedRuntimeUpgrade::Upgraded {
                    target: upgrade.target,
                    operation_id: upgrade.operation_id,
                });
            }
            (Ok(()), Err(error)) => error,
            (Err(failure), Ok(())) => failure,
            (Err(failure), Err(retire_error)) => unretired_settled_failure(failure, retire_error),
        };
        self.store
            .expire_activation_attempt_best_effort(sandbox_id, generation, attempt_id)
            .await;
        Err(error)
    }

    /// Enqueue the disk backup a stopped sandbox's runtime upgrade may still
    /// owe, before an activation that isn't its retry claims the execution:
    /// the claim clears the record, and with it the only trace that the
    /// upgraded disk isn't backed up yet. Counted owed whenever the claim it
    /// left still records an upgrade and its lease has ended (a live one
    /// refuses the activation anyway), whether or not the node has published
    /// the upgrade's result: an upgrade still booting, or whose publication
    /// is late or lost, may have changed the disk all the same, and an
    /// upgrade that never booted costs one backup of the unchanged disk. The
    /// enqueue goes to the node holding the disk, by name, since the
    /// upgrade's pod may be gone: the node the execution names, or, when it
    /// names none, the node the sandbox's placement record says holds its
    /// disk (`active_node_id`, with no other target chosen), provided that
    /// node registered a Kubernetes node name. Execution history isn't
    /// consulted for it.
    ///
    /// The activation is refused until the backup is enqueued: the durable
    /// copy may still be the pre-upgrade disk. A refusal that no retry
    /// changes is `FailedPrecondition` (412), naming the reason, and what
    /// ends it depends on the reason. Neither the execution nor the sandbox's
    /// placement names a registered node holding the disk, or that node is
    /// gone, or sandbox-manager there holds no such sandbox (`NotFound`): no copy
    /// of the upgraded disk is reachable, and only deleting the sandbox ends
    /// the refusal. Sandbox-manager refuses the enqueue as
    /// `FailedPrecondition` or `InvalidArgument`: its sandbox is failed,
    /// quarantined or being deleted, its local files are missing, or the
    /// node lacks the configuration a backup needs. Before the capture, the
    /// node refuses to abandon the upgrade's operation as `InvalidArgument`:
    /// it records the operation for another sandbox, or an id doesn't parse.
    /// Neither is remembered here: each retry asks the node again, so the
    /// first retry that reaches the node after an operator repairs what it
    /// refused for (the sandbox's state or the node, for the enqueue; the
    /// node's record of the operation, for the abandon) goes through, and
    /// deleting the sandbox ends the refusal. A sandbox that isn't
    /// stopped yet, which sandbox-manager answers `Aborted` (a kubelet-retried
    /// boot of the upgrade's pod still running, say), and every other
    /// failure are retryable (503).
    ///
    /// Returns the operation whose backup it enqueued, which the activation's
    /// claim needs to clear the record. The claim decides again under the
    /// sandbox's lock, since the lease may end after this check: an upgrade
    /// still owed its backup there, with none enqueued for it, refuses the
    /// claim, and the retry enqueues it here
    /// (`assign_activation_attempt_in`, `store/activation_fence.rs`).
    pub(in crate::controllers::substrate::kubernetes) async fn enqueue_backup_owed_by_stopped_upgrade(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<uuid::Uuid>, SubstrateError> {
        let Some(StoppedRuntimeUpgradeClaim {
            generation,
            activation_attempt_id,
            node_name,
            live: false,
            upgrade: Some(recorded),
            ..
        }) = self.store.stopped_runtime_upgrade_claim(sandbox_id).await?
        else {
            return Ok(None);
        };
        let reason = |what: &str| owed_backup_reason(sandbox_id, &recorded, what);
        let unreachable = |cause: &str| owed_backup_unreachable(sandbox_id, &recorded, cause);
        // An attempt that ended between creating its pod and persisting it
        // leaves no node on the execution. The backup then goes only where
        // the placement pins the disk (`restore_source::disk_pin_node_name`):
        // backing up a node without the upgraded disk would clear the record
        // while the durable copy is still the pre-upgrade disk.
        let node_name = match node_name {
            Some(node_name) => node_name,
            None => self
                .store
                .disk_pin_node_name(sandbox_id)
                .await?
                .ok_or_else(|| {
                    unreachable(
                        "its execution names no node, and the sandbox's placement names no \
                     registered node holding its disk",
                    )
                })?,
        };
        if self.local_source_node_refusal(&node_name, 0)
            == Some(LocalSourceNodeRefusal::Unregistered)
        {
            return Err(unreachable(&format!(
                "its disk was on node {node_name}, which is gone"
            )));
        }
        // Nothing here may touch the upgrade's pod or operation while a retry
        // of it could own them, and the operation id is the retry's too, so
        // the expired claim is taken over first, in one fenced update: a
        // retry, a drain or another activation that comes later sees a live
        // claim and refuses. Every early answer below ends that claim's lease
        // again, so the next attempt retries at once.
        let reserved = SandboxActivationAttemptId::new();
        if !self
            .store
            .reserve_expired_stopped_upgrade(
                sandbox_id,
                generation,
                activation_attempt_id,
                reserved,
            )
            .await?
        {
            return Err(SubstrateError::Unavailable {
                message: reason(
                    "waits: the upgrade's execution changed hands since it was read; retry",
                ),
            });
        }
        let owed = self
            .finish_reserved_owed_backup(sandbox_id, generation, reserved, &recorded, &node_name)
            .await;
        if owed.is_err() {
            self.store
                .expire_activation_attempt_best_effort(sandbox_id, generation, reserved)
                .await;
        }
        owed
    }

    /// The rest of [`Self::enqueue_backup_owed_by_stopped_upgrade`], under
    /// the claim `reserved` holds on the upgrade's execution of `generation`.
    async fn finish_reserved_owed_backup(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        reserved: SandboxActivationAttemptId,
        recorded: &RecordedRuntimeUpgrade,
        node_name: &str,
    ) -> Result<Option<uuid::Uuid>, SubstrateError> {
        let reason = |what: &str| owed_backup_reason(sandbox_id, recorded, what);
        let unreachable = |cause: &str| owed_backup_unreachable(sandbox_id, recorded, cause);
        // The upgrade's pod may still be pending in its create, which kubelet
        // retries, and a retry could boot the upgrade after this captures the
        // disk: the claim would then clear the record of a backup that no
        // longer matches the disk. So the pod goes first, under its own UID,
        // and the capture waits until the API server no longer has it, as a
        // drain's evacuation does before it mints. Only while the execution
        // is still the expired claim this read: a retry of the upgrade that
        // has taken it over since owns the pod and the operation, and nothing
        // here may delete or abandon them.
        let leftover = self
            .store
            .leftover_stopped_upgrade(sandbox_id)
            .await?
            .filter(|leftover| {
                leftover.operation_id == recorded.operation_id
                    && leftover.generation == generation
                    && leftover.activation_attempt_id == reserved
            })
            .ok_or_else(|| SubstrateError::Unavailable {
                message: reason(
                    "waits: the upgrade's execution changed hands since it was read; retry",
                ),
            })?;
        if !self
            .remove_leftover_upgrade_pod(sandbox_id, &leftover)
            .await?
        {
            return Err(SubstrateError::Unavailable {
                message: reason(
                    "waits until the upgrade's pod, whose create kubelet may still retry, is \
                     gone; retry",
                ),
            });
        }
        self.enqueue_disk_backup_on_node(
            sandbox_id,
            node_name,
            Instant::now() + OWED_BACKUP_ENQUEUE_WINDOW,
        )
        .await
        .map_err(|failure| match failure {
            BackupEnqueueFailure::Unknown(cause) => {
                unreachable(&format!("node {node_name} holds no such sandbox ({cause})"))
            }
            BackupEnqueueFailure::Lasting(cause) => SubstrateError::FailedPrecondition {
                message: reason(&format!(
                    "was refused by node {node_name} ({cause}); it stays refused until an \
                     operator repairs the sandbox's state or the node, or deletes the sandbox"
                )),
            },
            BackupEnqueueFailure::Transient(cause) => SubstrateError::Unavailable {
                message: reason(&format!("couldn't be enqueued: {cause}; retry")),
            },
        })?;
        tracing::info!(
            sandbox_id = %sandbox_id,
            operation_id = %recorded.operation_id,
            node_name,
            "enqueued the disk backup a stopped sandbox's runtime upgrade left owed"
        );
        // The upgrade's execution, its pod gone and its backup enqueued, is
        // retired with the sandbox's placement kept, so the activation mints
        // an execution of its own: reusing this one, whose pod is gone, would
        // read as a create still in flight. Fenced on the generation and
        // attempt the claim read. An execution that changed hands since, say
        // to a retry of the upgrade that boots again after this capture, is
        // left to whoever holds it, and the enqueue proves nothing for it:
        // the operation id is the retry's too, so answering it would let the
        // claim clear the retry's record with this older capture.
        let Some(execution) = self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .filter(|execution| {
                execution.generation == generation
                    && execution.activation_attempt_id == Some(reserved)
            })
        else {
            return Err(SubstrateError::Unavailable {
                message: reason(
                    "is enqueued, but the upgrade's execution changed hands before it was \
                     retired; retry",
                ),
            });
        };
        let identity = ExecutionIdentity {
            pod_name: parse_pod_name(&execution.pod_name)?,
            generation: Some(execution.generation),
            activation_attempt_id: execution.activation_attempt_id,
            recorded: RecordedExecution::Read {
                pod_uid: execution.pod_uid.clone(),
                route_activation_id: execution.route_activation_id,
            },
            pod: ObservedPod::Absent,
        };
        if !self
            .store
            .retire_stopped_upgrade_execution(sandbox_id, &identity)
            .await?
        {
            return Err(SubstrateError::Unavailable {
                message: reason(
                    "is enqueued, but the upgrade's execution changed before it was retired; \
                     retry",
                ),
            });
        }
        Ok(Some(recorded.operation_id))
    }

    /// The upgrade the execution of `generation` records while `takeover`
    /// holds its claim. Anything else is an error: the execution changed
    /// hands, or recorded no upgrade.
    async fn upgrade_recorded_under(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        takeover: SandboxActivationAttemptId,
    ) -> Result<RecordedRuntimeUpgrade, SubstrateError> {
        self.store
            .stopped_runtime_upgrade_claim(sandbox_id)
            .await?
            .filter(|current| {
                current.generation == generation && current.activation_attempt_id == takeover
            })
            .and_then(|current| current.upgrade)
            .ok_or_else(|| SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id}'s execution changed hands during its runtime \
                     upgrade's takeover; retry"
                ),
            })
    }

    /// The upgrade the start-time decision chooses, or `None` to stay on the
    /// recorded environment.
    async fn plan_upgrade(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PodUpgrade>, SubstrateError> {
        Ok(self
            .decide_runtime_upgrade(sandbox_id, UpgradeActivation::Start, None)
            .await?
            .map(|decided| decided.pod_upgrade(sandbox_id, PodRuntimeUpgradeTargetState::Stopped)))
    }

    /// Claim the sandbox's execution under `assignment`, record `upgrade` on
    /// it, place its pod in the runtime-upgrade mode on `source_node`, the
    /// node holding its disk, and wait for the node to
    /// settle it and publish the result. Records the pod's name and UID in
    /// `pod` once this call owns the execution.
    async fn boot_for_upgrade(
        &self,
        sandbox: &Sandbox,
        source_node: &str,
        upgrade: &PodUpgrade,
        assignment: &ActivationAttemptAssignment,
        phase_deadline: Instant,
        pod: &mut Option<BootedPod>,
    ) -> Result<(), PodUpgradeFailure> {
        let sandbox_id = sandbox.id;
        let changed_hands = || SubstrateError::Unavailable {
            message: format!(
                "stopped sandbox {sandbox_id}'s execution changed hands before its runtime \
                 upgrade; retry"
            ),
        };
        // Once claimed, an error here may come after the pod was created, so
        // the execution stays for a retry, which reads what was recorded:
        // `ensure_pod_exists` records a `stopped` target's upgrade on every
        // execution it claims, in the claim's transaction and before any pod,
        // so an execution without one has none carrying the upgrade.
        // Pinned to the node checked above rather than resolved again: a
        // disk node resolved away in between would leave only the disk's
        // durable copy, and a pod booting that elsewhere, or re-driven
        // cross-node, can't boot the disk the upgrade checked.
        match self
            .ensure_pod_exists(
                sandbox,
                None,
                Some(source_node),
                None,
                PodMaterializationOwnership::new(None, None, Some(assignment.clone()))
                    .upgrading_runtime(Some(upgrade.clone())),
            )
            .await
        {
            Ok(()) => {}
            Err(SubstrateError::ActivationInFlight { .. }) => {
                return Err(PodUpgradeFailure::MayStillRun(activation_in_flight(
                    sandbox_id,
                )));
            }
            Err(error) => return Err(PodUpgradeFailure::MayStillRun(error)),
        }
        let attempt_id = assignment.assigned_or_requested();
        let fence = self
            .store
            .reconcile_activation_fence(
                sandbox_id,
                attempt_id,
                None,
                SandboxTransitionTargetState::Stopped,
            )
            .await
            .map_err(PodUpgradeFailure::MayStillRun)?;
        // Only an attempt this call owns may read the pod as the upgrade's.
        if fence.owned_attempt_id() != Some(attempt_id) {
            return Err(PodUpgradeFailure::MayStillRun(changed_hands()));
        }
        *pod = Some(BootedPod {
            name: fence.execution.pod_name.clone(),
            uid: fence.execution.pod_uid.clone(),
            operation_id: upgrade.operation_id,
        });
        // This call owns the attempt, so it created the pod or found it.
        self.settle_pod_runtime_upgrade(
            sandbox_id,
            &fence.execution.pod_name,
            MissingPod::Gone,
            upgrade,
            phase_deadline,
        )
        .await?;
        // The pod stays until the node has published the upgrade's result,
        // which is what says the sandbox ended at rest under the target.
        self.await_stopped_publication(sandbox_id, upgrade.target, phase_deadline)
            .await
    }

    /// Wait until the node has published `sandbox_id` stopped under
    /// `target`, or until `deadline`. Each read gets the time left to the
    /// deadline, at least the floor an observation of the upgrade gets, so a
    /// database that never answers holds the wait at most that past it.
    pub(in crate::controllers::substrate::kubernetes) async fn await_stopped_publication(
        &self,
        sandbox_id: SandboxId,
        target: EnvironmentReference,
        deadline: Instant,
    ) -> Result<(), PodUpgradeFailure> {
        let unpublished = || {
            PodUpgradeFailure::MayStillRun(SubstrateError::Timeout {
                message: format!(
                    "stopped sandbox {sandbox_id} is upgraded, but its node hasn't published it \
                     stopped under {target} yet; retry to converge"
                ),
                phase: None,
            })
        };
        loop {
            let read_deadline = deadline.max(Instant::now() + NODE_READ_FLOOR);
            let published = tokio::time::timeout_at(
                read_deadline,
                self.store.published_stopped_under(sandbox_id, target),
            )
            .await
            .map_err(|_elapsed| unpublished())?
            .map_err(PodUpgradeFailure::MayStillRun)?;
            if published {
                return Ok(());
            }
            if Instant::now() + PUBLICATION_POLL >= deadline {
                return Err(unpublished());
            }
            tokio::time::sleep(PUBLICATION_POLL).await;
        }
    }

    /// Delete the execution's pod and finish the execution. With `backup`,
    /// the upgraded disk's current-instance backup is enqueued first, so the
    /// backup that replaces the durable copy is of the environment the node
    /// published; a node that refuses it as unavailable is asked again until
    /// `backup`'s deadline, and a failed enqueue keeps the execution for a
    /// retry. `booted`
    /// is the pod as the boot saw it, for when the execution is no longer
    /// active and no other attempt adopted it (a rollback's receipt retired
    /// it, say): the pod at its name is still deleted, under the UID the boot
    /// saw or, when the execution hadn't recorded one, the UID of the pod
    /// there that carries the boot's operation.
    pub(in crate::controllers::substrate::kubernetes) async fn retire_upgrade_boot(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        attempt_id: SandboxActivationAttemptId,
        booted: Option<BootedPod>,
        backup: Option<Instant>,
    ) -> Result<(), SubstrateError> {
        let execution = match self
            .store
            .active_execution_status(sandbox_id)
            .await?
            .filter(|execution| execution.generation == generation)
        {
            Some(execution) if execution.activation_attempt_id == Some(attempt_id) => execution,
            // Another attempt adopted the execution, and its pod with it.
            // Otherwise it never materialized or something else finished it,
            // but not necessarily its pod. A pod at that name with another
            // UID isn't this boot's.
            adopted => {
                if adopted.is_none()
                    && let Some(BootedPod {
                        name,
                        uid,
                        operation_id,
                    }) = booted
                {
                    let name = parse_pod_name(&name)?;
                    let uid = match uid {
                        Some(uid) => Some(uid),
                        None => self.upgrade_pod_uid(&name, operation_id).await?,
                    };
                    // A pod there with another UID isn't this boot's, and
                    // no execution tracks it for this retire to forget.
                    if let Some(uid) = uid {
                        self.delete_upgrade_pod(&name, &uid).await?;
                    }
                }
                // The upgrade isn't done while its backup is unconfirmed.
                if backup.is_some() {
                    return Err(SubstrateError::Unavailable {
                        message: format!(
                            "stopped sandbox {sandbox_id}'s upgraded execution left this attempt \
                             before its disk backup was enqueued; retry"
                        ),
                    });
                }
                return Ok(());
            }
        };
        let pod_name = parse_pod_name(&execution.pod_name)?;
        // An attempt can end between creating the pod and recording its UID.
        // The pod's name is this generation's, so the live pod identifies it.
        let pod_uid = match execution.pod_uid.clone() {
            Some(uid) => Some(uid),
            None => self
                .lookup_or_fetch_sandbox_pod(pod_name.as_str())
                .await
                .map_err(|error| {
                    crate::controllers::substrate::kubernetes::pod_ops::kube_error_for_substrate(
                        "failed to read the upgrade's pod",
                        error,
                    )
                })?
                .and_then(|pod| pod.pod_uid()),
        };
        if let Some(deadline) = backup {
            self.enqueue_upgraded_disk_backup(
                sandbox_id,
                execution.k8s_node_name.as_deref(),
                deadline,
            )
            .await?;
        }
        // A pod at the name with another UID is left alone, and forgetting
        // the execution's tracking would leave it untracked, so the retire
        // refuses retryably, as the fenced teardown does
        // (`adapter/teardown.rs`); a retry takes the execution over again.
        if let Some(pod_uid) = &pod_uid
            && !self.delete_upgrade_pod(&pod_name, pod_uid).await?
        {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id}'s runtime upgrade pod {pod_name} is no longer \
                     the one with UID {pod_uid}, so its execution isn't retired; retry"
                ),
            });
        }
        let identity = ExecutionIdentity {
            pod_name,
            generation: Some(execution.generation),
            activation_attempt_id: execution.activation_attempt_id,
            recorded: RecordedExecution::Read {
                pod_uid: execution.pod_uid.clone(),
                route_activation_id: execution.route_activation_id,
            },
            pod: ObservedPod::Absent,
        };
        // Generation- and attempt-guarded: a successor keeps its execution.
        // An execution that changed since it was read stays active, with what
        // it recorded, so this retire isn't done. The sandbox stays placed on
        // the node holding its disk, so a drain still evacuates it.
        if !self
            .store
            .retire_stopped_upgrade_execution(sandbox_id, &identity)
            .await?
        {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id}'s runtime upgrade execution changed before it \
                     was retired; retry"
                ),
            });
        }
        Ok(())
    }

    /// Enqueue the backup of the stopped sandbox's current instance on
    /// `node_name`, the node the upgrade's execution names, as the stop path
    /// does before it releases a pod. It goes to the node by name rather than
    /// through the pod: the node settles a `stopped` upgrade inside the
    /// shim's pod-sandbox create, so the pod may have no address yet. Any
    /// failure is retryable: the durable copy may still be the pre-upgrade
    /// disk.
    async fn enqueue_upgraded_disk_backup(
        &self,
        sandbox_id: SandboxId,
        node_name: Option<&str>,
        deadline: Instant,
    ) -> Result<(), SubstrateError> {
        let unavailable = |reason: String| SubstrateError::Unavailable {
            message: format!(
                "stopped sandbox {sandbox_id} is upgraded, but its disk backup isn't enqueued: \
                 {reason}; retry"
            ),
        };
        let node_name =
            node_name.ok_or_else(|| unavailable("its execution names no node".to_string()))?;
        self.enqueue_disk_backup_on_node(sandbox_id, node_name, deadline)
            .await
            .map_err(|failure| unavailable(failure.into_cause()))
    }

    /// Enqueue the current-instance disk backup of `sandbox_id` on the
    /// node-api of `node_name`, the node holding its disk, or say why it
    /// wasn't enqueued. A node that answers `Unavailable` is asked again
    /// every [`BACKUP_ENQUEUE_RETRY`] until `deadline`: node-manager refuses
    /// the backup's disk snapshot while another host mutation holds the
    /// sandbox, and the network-slot reset that follows a stopped upgrade's
    /// cleanup holds it for a moment after the node settles the upgrade. Any
    /// other failure answers at once.
    async fn enqueue_disk_backup_on_node(
        &self,
        sandbox_id: SandboxId,
        node_name: &str,
        deadline: Instant,
    ) -> Result<(), BackupEnqueueFailure> {
        let (_addr, mut client) = self
            .node_worker_client(node_name)
            .await
            .map_err(BackupEnqueueFailure::Transient)?;
        loop {
            match enqueue_artifact_backup_status_with_client(
                &mut client,
                sandbox_id,
                ArtifactKind::Disk,
                BackupSource::CurrentInstance,
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(EnqueueArtifactBackupFailure::Refused(status))
                    if status.code() == tonic::Code::Unavailable
                        && Instant::now() + BACKUP_ENQUEUE_RETRY < deadline =>
                {
                    tracing::debug!(
                        sandbox_id = %sandbox_id,
                        node_name,
                        %status,
                        "retrying a stopped sandbox's disk backup enqueue the node refused as \
                         unavailable"
                    );
                    tokio::time::sleep(BACKUP_ENQUEUE_RETRY).await;
                }
                Err(failure) => return Err(BackupEnqueueFailure::of(failure)),
            }
        }
    }

    /// The UID of the pod at `pod_name` when it carries `operation_id`, from
    /// the reflector or, when it hasn't seen one there, an uncached read. A
    /// pod there without the operation isn't the boot's.
    async fn upgrade_pod_uid(
        &self,
        pod_name: &PodName,
        operation_id: uuid::Uuid,
    ) -> Result<Option<PodUid>, SubstrateError> {
        let pod = self
            .lookup_or_fetch_sandbox_pod(pod_name.as_str())
            .await
            .map_err(|error| SubstrateError::Unavailable {
                message: format!(
                    "failed to read the runtime upgrade's pod {pod_name}: {error}; retry"
                ),
            })?;
        Ok(pod
            .filter(|pod| pod.carries_runtime_upgrade(operation_id))
            .and_then(|pod| pod.pod_uid()))
    }

    /// Delete the pod at `pod_name`, but only while it's the one with `uid`.
    /// Answers `false` when a pod with another UID holds the name, which the
    /// delete leaves alone, and `true` once no pod with `uid` is there. A failed
    /// delete is retryable: the execution is kept, and a retry takes it over
    /// and retires it again.
    pub(in crate::controllers::substrate::kubernetes) async fn delete_upgrade_pod(
        &self,
        pod_name: &PodName,
        uid: &PodUid,
    ) -> Result<bool, SubstrateError> {
        match delete_sandbox_pod_fenced(&self.k8s, pod_name, uid, None).await {
            Ok(FencedPodDeleteOutcome::Deleted | FencedPodDeleteOutcome::NotFound) => Ok(true),
            Ok(FencedPodDeleteOutcome::PreconditionFailed) => Ok(false),
            Err(error) => Err(SubstrateError::Unavailable {
                message: format!(
                    "failed to delete the runtime upgrade's pod {pod_name}: {error}; retry"
                ),
            }),
        }
    }
}

/// Why a stopped sandbox's disk backup wasn't enqueued, split by whether a
/// retry can change it.
enum BackupEnqueueFailure {
    /// Sandbox-manager holds no such sandbox (`NotFound`).
    Unknown(String),
    /// Sandbox-manager refused it for a reason no retry changes
    /// (`FailedPrecondition`, `InvalidArgument`): its sandbox is failed,
    /// quarantined or being deleted, its local files are missing, or the
    /// node isn't configured for backups.
    Lasting(String),
    /// The node, or the way to it, failed or was busy, or the sandbox isn't
    /// stopped yet (`Aborted`): it may still get there, as when kubelet
    /// retried the upgrade pod's boot after the node settled the upgrade.
    Transient(String),
}

impl BackupEnqueueFailure {
    fn of(failure: EnqueueArtifactBackupFailure) -> Self {
        match failure {
            EnqueueArtifactBackupFailure::Refused(status) => {
                let cause = || format!("{:?}: {}", status.code(), status.message());
                match status.code() {
                    tonic::Code::NotFound => Self::Unknown(cause()),
                    tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument => {
                        Self::Lasting(cause())
                    }
                    _ => Self::Transient(status.to_string()),
                }
            }
            EnqueueArtifactBackupFailure::Response(error) => Self::Transient(error.to_string()),
        }
    }

    fn into_cause(self) -> String {
        match self {
            Self::Unknown(cause) | Self::Lasting(cause) | Self::Transient(cause) => cause,
        }
    }
}

/// The answer for a boot that settled without reaching the target (a
/// rollback, or a call refused before it began) whose execution the retire
/// then couldn't retire. The execution and its pod outlive the failure until
/// a retry takes them over, which nothing else does, so the caller has to
/// retry: retryable (503), whatever the failure said. Except a retire that
/// failed for good (`Failed`, an execution whose pod name doesn't parse),
/// which no retry can get past: that stays `Failed`, as it does for an
/// upgrade that reached the target.
pub(in crate::controllers::substrate::kubernetes) fn unretired_settled_failure(
    failure: SubstrateError,
    retire_error: SubstrateError,
) -> SubstrateError {
    match retire_error {
        SubstrateError::Failed { message } => SubstrateError::Failed {
            message: format!("{failure}; {message}"),
        },
        retire_error => SubstrateError::Unavailable {
            message: format!("{failure}; its execution isn't retired yet ({retire_error}); retry"),
        },
    }
}

/// The execution's pod name as a [`PodName`]. One that doesn't parse can't
/// name a pod to delete, so no retry retires the execution; the retire
/// refuses before it changes anything, and the execution stays for an
/// operator.
fn parse_pod_name(name: &str) -> Result<PodName, SubstrateError> {
    PodName::parse(name).map_err(|error| SubstrateError::Failed {
        message: format!(
            "the runtime upgrade's execution names an invalid pod {name:?} ({error}); it's kept \
             unretired"
        ),
    })
}

/// Why the disk backup a stopped sandbox's runtime upgrade owes `what`.
fn owed_backup_reason(
    sandbox_id: SandboxId,
    recorded: &RecordedRuntimeUpgrade,
    what: &str,
) -> String {
    format!(
        "stopped sandbox {sandbox_id}'s runtime upgrade {} to {} may have changed its disk, and \
         the disk backup it owes {what}",
        recorded.operation_id, recorded.target
    )
}

/// The lasting refusal of an owed backup that `cause` keeps from ever being
/// enqueued.
fn owed_backup_unreachable(
    sandbox_id: SandboxId,
    recorded: &RecordedRuntimeUpgrade,
    cause: &str,
) -> SubstrateError {
    SubstrateError::FailedPrecondition {
        message: owed_backup_reason(
            sandbox_id,
            recorded,
            &format!("can never be enqueued: {cause}; the sandbox can only be deleted"),
        ),
    }
}
