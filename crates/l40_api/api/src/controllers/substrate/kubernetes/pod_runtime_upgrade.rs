//! A runtime upgrade that boots in its pod's shim.
//!
//! On Kubernetes a sandbox's microVM launches in its pod's containerd shim at
//! container create, so its runtime upgrade has to run there too: the pod's
//! `runtime-upgrade` checkpoint makes the shim call sandbox-manager's
//! `UpgradeRuntime` with the pod's network namespace
//! ([`pplx_space_common_core::kubernetes::PodRuntimeUpgrade`]). The shim's
//! answer stays in the pod, so the control plane reads the outcome from the
//! node's `GetRuntimeUpgrade` instead ([`KubernetesSubstrate::await_pod_runtime_upgrade`]).
//! That report is the upgrade's outcome on the node, not the pod's, so it's
//! read together with the pod: a `started` target is reached only once the
//! pod's create has returned and its container runs.
//!
//! **A rollback the api settles on is final: no retry from that pod can
//! still upgrade the disk, and the api's own retry gets a new pod.** The shim
//! runs the upgrade at pod-sandbox create, so a failed upgrade leaves the pod
//! pending, with no container ever ending, while kubelet retries the create
//! under the same pod UID. The node records each rollback, and each refusal
//! before anything was fenced that no retry can change, for the pod UID the
//! request named, keeps it for that pod even after a later pod's outcome
//! replaces it in the report, and answers that pod's retries with the
//! recorded failure without fencing or booting again. A transient refusal
//! stays unrecorded, so kubelet's retry from the same pod tries again, and
//! the report it leaves (an earlier pod's, or none) settles nothing here. So
//! the wait settles a rolled-back or refused report only when it names the
//! pod it reads: that's proof the pod's upgrade is over, whatever
//! the pod's containers show. It then deletes that pod under its UID, so the
//! caller's retry places a new pod, whose create the node takes as a new
//! attempt. A report that names another pod is an earlier attempt's, which
//! the node keeps reporting until the new pod's shim reaches it, and one that
//! names no pod comes from a node that predates the field: neither is proof,
//! so the wait stays unsettled and runs to its deadline, keeping the pod and
//! its attempt as for any outcome it can't confirm. With no pod at the name,
//! a final report settles only when it names the pod this wait last saw, or
//! the one the execution recorded: a pod force-deleted while its create was
//! in flight must not settle on an earlier pod's outcome. Otherwise the wait
//! stays unsettled.
//!
//! **A `started` target's refused create is final for its pod too.** The node
//! can upgrade the sandbox, or find it on the target, and still fail the
//! pod's create (the sandbox runs another pod's execution, say), which leaves
//! the pod pending just as a rollback does. The node records that refusal for
//! the pod and answers the pod's retries with it, and its report names the
//! latest pod refused that way, so the wait settles on it, retryably, and
//! deletes the pod, under the same rule: only a report naming the pod at the
//! name settles it.
//!
//! **A start whose decided upgrade didn't take effect never reports it as
//! done: the sandbox runs on its old environment and the upgrade stays
//! visibly not done.** A start's success says the sandbox runs, not that it
//! moved. Nothing the control plane records for a start names the target as
//! reached: not its operation or activation result, and not the plain pod
//! and execution of a start that fell back. The upgrade's record is the
//! environment the node publishes the sandbox under, which
//! `GET /runtime-environments/population` counts and the next decision reads,
//! together with the node's outcome for the pod (`GetRuntimeUpgrade`). The
//! node never publishes a fenced sandbox under the target, so a rollback or
//! a refusal leaves it published on its old environment: the start is
//! refused retryably, and its retry decides the same upgrade again. A start
//! falls back to a plain start only when its decision changes, say an
//! operator allowed the old environment meanwhile, and the sandbox then runs
//! and is published there.
//!
//! The read goes to the node's node-api by node name, not through the pod: a
//! shim whose create failed removes the pod's data-plane redirect, so a
//! rolled-back upgrade's pod has no address to answer on.

use std::time::Duration;

use pplx_space_common_core::kubernetes::{PodRuntimeUpgrade, PodRuntimeUpgradeTargetState};
use pplx_space_common_core::types::{EnvironmentReference, LocalArtifactId, SandboxId};
use pplx_space_sbx_mgr_client::proto::{GetRuntimeUpgradeRequest, RuntimeUpgradeStatus};
use tokio::time::Instant;

use super::KubernetesSubstrate;
use super::sandbox_pod_view::SandboxPodView;
use super::store::RecordedRuntimeUpgrade;
use crate::controllers::substrate::SubstrateError;

#[cfg(test)]
mod tests;

/// How often [`KubernetesSubstrate::await_pod_runtime_upgrade`] rereads the
/// pod and the node.
const OUTCOME_POLL: Duration = Duration::from_millis(200);

/// The least time one observation of the upgrade (the pod read, from the
/// cache or, on a miss, uncached; with no pod, the read of its execution's
/// node from Postgres; and the `GetRuntimeUpgrade` read with any dial it
/// needs) gets, even at the wait's
/// deadline: a read a healthy API server, database or node answers well
/// inside it. The observation is otherwise bounded by the time left to the
/// deadline, not the API server's, the database pool's or the node-RPC
/// channel's timeouts, so a hung read, dial or call overshoots the deadline
/// by at most this. A stopped sandbox's upgrade bounds each read of the
/// node's publication the same way, after the outcome's wait, so a read of
/// each that hangs near the deadline can hold it up to twice this past it.
pub(super) const NODE_READ_FLOOR: Duration = Duration::from_secs(1);

/// A runtime upgrade as the control plane renders it on a sandbox pod; see
/// [`PodRuntimeUpgrade`], which borrows from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PodUpgrade {
    pub(super) operation_id: uuid::Uuid,
    pub(super) target: EnvironmentReference,
    pub(super) authorization_id: String,
    pub(super) authorization_reason: String,
    pub(super) memory_loss: bool,
    pub(super) target_state: PodRuntimeUpgradeTargetState,
    /// For a suspended source, the checkpoint the upgrade was decided from.
    pub(super) checkpoint_local_artifact_id: Option<LocalArtifactId>,
}

impl PodUpgrade {
    pub(super) fn rendered(&self) -> PodRuntimeUpgrade<'_> {
        PodRuntimeUpgrade {
            operation_id: self.operation_id,
            target_environment: self.target,
            authorization_id: &self.authorization_id,
            authorization_reason: &self.authorization_reason,
            memory_loss: self.memory_loss,
            target_state: self.target_state,
            checkpoint_local_artifact_id: self.checkpoint_local_artifact_id,
        }
    }

    /// What a stopped sandbox's upgrade records on its execution before its
    /// pod exists, so a retry renders this same upgrade again.
    pub(super) fn recorded(&self) -> RecordedRuntimeUpgrade {
        RecordedRuntimeUpgrade {
            operation_id: self.operation_id,
            target: self.target,
            authorization_id: self.authorization_id.clone(),
        }
    }
}

/// How a pod's runtime upgrade ended, once nothing of it can still run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PodUpgradeOutcome {
    /// The sandbox is on the target: the node upgraded it, or found it there
    /// already. For a `started` target, the pod's create also returned and
    /// its container runs the sandbox.
    Reached { already_on_target: bool },
    /// The node put the old disk and checkpoint back for this pod, or the
    /// pod is gone; `failure` says why.
    RolledBack { failure: Option<String> },
    /// The node refused this pod's call before the upgrade fenced anything,
    /// or the pod is gone; `failure` says why.
    Refused { failure: Option<String> },
    /// The node never recorded the operation and the pod's container has
    /// ended, or the pod is gone: the shim's call was refused before the
    /// upgrade fenced anything, by a node that records no refusal for it
    /// (one without the sandbox, or that predates refusal records).
    NotStarted,
    /// A `started` target only: the node holds the sandbox on the target, but
    /// the pod's create was refused or the pod is gone, so the pod doesn't
    /// run it. `GetRuntimeUpgrade` reports the upgrade's outcome, not the
    /// pod's: the node refuses a pod whose sandbox runs another pod's
    /// execution or moved off the execution the start was decided on, and a
    /// retry under the same operation is refused again unless the node's
    /// report says the start may be retried (`activation_claimed` unset on an
    /// already-on-target operation). `failure` is the refusal the node
    /// recorded for the pod, when it recorded one.
    PodRefused {
        already_on_target: bool,
        activation_claimed: bool,
        failure: Option<String>,
    },
}

impl PodUpgradeOutcome {
    /// The refusal for an outcome whose pod doesn't run the sandbox on the
    /// target, or `None` for one that reached it. Always retryable (503),
    /// whatever the target state. A rollback or refusal is too: it's final
    /// for the pod it names, which the wait deletes, and a retry from a new
    /// pod under the same operation fences and boots again, and may succeed.
    pub(super) fn refusal(
        &self,
        sandbox_id: SandboxId,
        upgrade: &PodUpgrade,
    ) -> Option<SubstrateError> {
        let reason = match self {
            Self::Reached { .. } => return None,
            Self::RolledBack { failure } => format!(
                "rolled back ({}); it stays on its environment, and a retry boots the upgrade \
                 again in a new pod",
                failure.as_deref().unwrap_or("the node recorded no reason")
            ),
            Self::Refused { failure } => format!(
                "was refused before it began ({}); it stays on its environment, and a retry \
                 tries again in a new pod",
                failure.as_deref().unwrap_or("the node recorded no reason")
            ),
            Self::NotStarted => {
                "was refused before it began; it stays on its environment".to_string()
            }
            Self::PodRefused {
                already_on_target,
                activation_claimed,
                failure,
            } => format!(
                "left it on the target{}, but its pod's create was refused, so the pod doesn't \
                 run it{}{}",
                if *already_on_target {
                    " where it already was"
                } else {
                    ""
                },
                failure
                    .as_deref()
                    .map(|failure| format!(" ({failure})"))
                    .unwrap_or_default(),
                if *activation_claimed {
                    "; the operation already claimed its start"
                } else {
                    ""
                }
            ),
        };
        Some(SubstrateError::Unavailable {
            message: format!(
                "sandbox {sandbox_id}'s runtime upgrade {} to {} {reason}",
                upgrade.operation_id, upgrade.target
            ),
        })
    }
}

/// What one look at the pod and the node shows.
enum Observation {
    Settled(PodUpgradeOutcome),
    /// Settled by a rollback or refusal the node recorded for the pod at the
    /// name, `pod_uid`: final for that pod, which is deleted so a retry
    /// places a new one.
    SettledForPod {
        outcome: PodUpgradeOutcome,
        pod_uid: String,
    },
    /// Nothing settled yet; the reason is what a deadline reports.
    Pending(String),
}

/// Whether the pod's own container has ended, or the pod is terminal, so its
/// shim no longer runs anything for the upgrade.
fn container_ended(pod: &SandboxPodView) -> bool {
    pod.is_terminal()
        || pod
            .container_statuses()
            .containers
            .iter()
            .any(|container| container.terminated.is_some())
}

/// Whether a container of the pod reports terminated, which happens only
/// once its shim's create has returned. Unlike [`container_ended`], a
/// terminal pod phase alone doesn't count.
fn container_terminated(pod: &SandboxPodView) -> bool {
    pod.container_statuses()
        .containers
        .iter()
        .any(|container| container.terminated.is_some())
}

/// Whether the pod's own container runs: its shim's create returned.
fn container_running(pod: &SandboxPodView) -> bool {
    !container_ended(pod)
        && pod
            .container_statuses()
            .containers
            .iter()
            .any(|container| container.running)
}

/// What the node reported for the operation.
struct NodeReport {
    status: RuntimeUpgradeStatus,
    failure: Option<String>,
    activation_claimed: bool,
    /// The pod the reported attempt ran for; `None` from a node that
    /// predates the field, or for a request that named no pod.
    pod_uid: Option<String>,
    /// The latest pod whose create an upgraded or already-on-target
    /// operation refused; `None` without one, and from a node that predates
    /// the field.
    refused_pod: Option<RefusedPod>,
}

/// A pod whose create the node refused after the operation reached its
/// target, and why, when it recorded a reason.
struct RefusedPod {
    pod_uid: String,
    failure: Option<String>,
}

/// The UID of `pod` when `named`, a UID the node's report names, is it:
/// proof that what the report says is this pod's. `None` otherwise, a report
/// naming no pod included.
fn named_uid(pod: &SandboxPodView, named: Option<&str>) -> Option<String> {
    pod.uid()
        .filter(|uid| named == Some(*uid))
        .map(str::to_owned)
}

/// Classify one `GetRuntimeUpgrade` answer against what the pod shows. The
/// answer is the upgrade's outcome on the node, not the pod's: the node can
/// record the upgrade done while it refuses the pod's create. `known_pod_uid`
/// is the UID of the attempt's own pod as the wait last knew it, which
/// stands in for the pod once it's gone.
fn observe(
    report: NodeReport,
    target_state: PodRuntimeUpgradeTargetState,
    pod: Option<&SandboxPodView>,
    known_pod_uid: Option<&str>,
) -> Observation {
    let NodeReport {
        status,
        failure,
        activation_claimed,
        pod_uid,
        refused_pod,
    } = report;
    match status {
        RuntimeUpgradeStatus::Upgraded | RuntimeUpgradeStatus::AlreadyOnTarget => {
            let already_on_target = status == RuntimeUpgradeStatus::AlreadyOnTarget;
            match target_state {
                // The pod is a placeholder, and the sandbox must end at rest
                // on the target: the caller waits for the node to publish
                // it stopped under the target, which a refused create (one
                // that found the sandbox running) never produces.
                PodRuntimeUpgradeTargetState::Stopped => {
                    Observation::Settled(PodUpgradeOutcome::Reached { already_on_target })
                }
                // The pod has to run the sandbox, which it does once its
                // create returns. A create the node refused for this pod is
                // final for it, as a rollback is: the pod stays pending in
                // it, with no container ever ending, while kubelet's retries
                // get the same refusal. Only a refusal naming this pod proves
                // that; one naming another pod is an earlier attempt's. The
                // node records none for a pod whose execution the sandbox
                // runs, so a refusal naming this pod wins over what its
                // containers show.
                PodRuntimeUpgradeTargetState::Started => {
                    let refused_uid = pod
                        .zip(refused_pod.as_ref())
                        .and_then(|(pod, refused)| named_uid(pod, Some(refused.pod_uid.as_str())));
                    match (pod, refused_uid) {
                        (_, Some(pod_uid)) => Observation::SettledForPod {
                            outcome: PodUpgradeOutcome::PodRefused {
                                already_on_target,
                                activation_claimed,
                                failure: refused_pod.and_then(|refused| refused.failure),
                            },
                            pod_uid,
                        },
                        (Some(pod), None) if container_running(pod) => {
                            Observation::Settled(PodUpgradeOutcome::Reached { already_on_target })
                        }
                        (Some(pod), None) if !container_ended(pod) => Observation::Pending(
                            "the node has the sandbox on the target, but the pod's create \
                             hasn't returned yet"
                                .to_string(),
                        ),
                        _ => Observation::Settled(PodUpgradeOutcome::PodRefused {
                            already_on_target,
                            activation_claimed,
                            failure: None,
                        }),
                    }
                }
            }
        }
        // A rollback or refusal is final for the pod the node recorded it
        // for: kubelet's retries of that pod's create get it back, and never
        // fence or boot again. Until a new pod's shim reaches the node, the
        // node still reports an earlier pod's, and a node that predates the
        // pod field names none, so only a report naming this pod settles it,
        // whatever its containers show. With the pod gone, no shim runs for
        // it, but the report may still be an earlier pod's: it settles only
        // when it names the pod this attempt knew it had.
        RuntimeUpgradeStatus::RolledBack | RuntimeUpgradeStatus::Refused => {
            let outcome = if status == RuntimeUpgradeStatus::RolledBack {
                PodUpgradeOutcome::RolledBack { failure }
            } else {
                PodUpgradeOutcome::Refused { failure }
            };
            let named_this = match pod {
                Some(pod) => named_uid(pod, pod_uid.as_deref()),
                None => pod_uid
                    .clone()
                    .filter(|uid| Some(uid.as_str()) == known_pod_uid),
            };
            match (pod, named_this) {
                (None, Some(_)) => Observation::Settled(outcome),
                (Some(_), Some(pod_uid)) => Observation::SettledForPod { outcome, pod_uid },
                (_, None) => Observation::Pending(format!(
                    "the node reports {} {}, which may be an earlier attempt's",
                    if status == RuntimeUpgradeStatus::RolledBack {
                        "a rollback"
                    } else {
                        "a refusal"
                    },
                    match pod_uid.as_deref() {
                        Some(other) => format!("for pod {other}, not this one"),
                        None => "naming no pod".to_string(),
                    }
                )),
            }
        }
        // The shim calls the node only from its create, and its container
        // ends only once that call returns, so an operation the node never
        // recorded by then never began. A node that holds the sandbox records
        // its refusal for the pod, which settles above; this is a node
        // without the sandbox, or one that predates refusal records. A pod
        // that's gone proves no such thing: it can be deleted while its
        // create's call is still on its way to the node, which may then
        // begin the upgrade, so that stays unsettled.
        // A terminated container proves the call returned; a pod phase
        // doesn't: a pod can be marked failed (evicted, past its deadline)
        // while its create's call is still on its way to the node.
        RuntimeUpgradeStatus::Absent if pod.is_some_and(container_terminated) => {
            Observation::Settled(PodUpgradeOutcome::NotStarted)
        }
        RuntimeUpgradeStatus::Absent if pod.is_none() => Observation::Pending(
            "the pod is gone and the node hasn't seen the upgrade, which its create may still \
             begin"
                .to_string(),
        ),
        RuntimeUpgradeStatus::Absent => {
            Observation::Pending("the node hasn't seen the upgrade yet".to_string())
        }
        RuntimeUpgradeStatus::InProgress => {
            Observation::Pending("the node is still running the upgrade".to_string())
        }
        RuntimeUpgradeStatus::Unspecified => {
            Observation::Pending("the node reported no status for the upgrade".to_string())
        }
    }
}

/// What a look shows when no node is known for the upgrade's pod: neither
/// the pod, nor its execution, nor an earlier look named one. A pod still
/// there isn't scheduled yet. A pod that's gone after it existed (`existed`:
/// this wait saw it, or its execution recorded its UID) may have been
/// scheduled and begun its create before anything recorded where, so its
/// node may still run the upgrade: that stays unsettled. Only a pod that
/// never existed proves the upgrade never began.
fn unlocated_observation(present: bool, existed: bool) -> Observation {
    if present {
        Observation::Pending("the pod isn't on a node yet".to_string())
    } else if existed {
        Observation::Pending(
            "the pod is gone, and no node it may have run its create on is recorded".to_string(),
        )
    } else {
        Observation::Settled(PodUpgradeOutcome::NotStarted)
    }
}

/// Why a pod's runtime upgrade didn't leave the sandbox on its target, which
/// decides whether the caller may let the pod and its attempt go.
#[derive(Debug)]
pub(super) enum PodUpgradeFailure {
    /// Nothing of the upgrade runs in the pod: the node rolled it back, never
    /// began it, or refused the pod's create. A rollback or refusal the node
    /// recorded for the pod also deleted it, so the caller's retry places a
    /// new one.
    Settled(SubstrateError),
    /// A `started` target only: nothing of it runs in the pod, but the node
    /// holds the sandbox on the target ([`PodUpgradeOutcome::PodRefused`]).
    /// The upgrade booted it and retired the checkpoint it superseded, so the
    /// sandbox isn't where it was before the activation: a caller mustn't
    /// put it back as if nothing had happened.
    LeftOnTarget(SubstrateError),
    /// The node may still be working on the upgrade, or the caller couldn't
    /// confirm its result yet.
    MayStillRun(SubstrateError),
}

impl PodUpgradeFailure {
    pub(super) fn into_error(self) -> SubstrateError {
        match self {
            Self::Settled(error) | Self::LeftOnTarget(error) | Self::MayStillRun(error) => error,
        }
    }
}

/// What a wait makes of no pod at the name it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MissingPod {
    /// The caller's own attempt created the pod, or found it: none there
    /// means it was deleted, so no shim runs for it.
    Gone,
    /// The caller joined another attempt's claim, whose create it can't see
    /// and that may not have been sent yet. No pod there settles nothing: the
    /// owner's pod may still boot the upgrade, so the wait runs to its
    /// deadline unless a pod shows up and settles.
    NotCreatedYet,
}

/// What a wait keeps across its polls.
#[derive(Default)]
struct WaitPolls {
    /// The node-api client it reads the node through, dropped after a
    /// transport failure.
    reader: Option<NodeReader>,
    /// The UID of the upgrade's pod as the wait last saw it.
    seen_pod_uid: Option<String>,
    /// The node the upgrade's pod was on when the wait last saw one, which a
    /// pod that's gone since still ran its create on.
    seen_node_name: Option<String>,
}

/// The node-api client a wait reads the node through, kept across its polls
/// and dropped after a transport failure.
struct NodeReader {
    node_name: String,
    addr: String,
    client: crate::controllers::substrate::RuntimeInternalClient,
}

impl KubernetesSubstrate {
    /// [`Self::await_pod_runtime_upgrade`], then `Ok` only for an upgrade that
    /// left the sandbox on its target: a refusal is [`PodUpgradeFailure::Settled`],
    /// and a wait that ended unsettled is [`PodUpgradeFailure::MayStillRun`].
    pub(super) async fn settle_pod_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        missing: MissingPod,
        upgrade: &PodUpgrade,
        deadline: Instant,
    ) -> Result<(), PodUpgradeFailure> {
        let outcome = self
            .await_pod_runtime_upgrade(sandbox_id, pod_name, missing, upgrade, deadline)
            .await
            .map_err(PodUpgradeFailure::MayStillRun)?;
        match outcome.refusal(sandbox_id, upgrade) {
            Some(refusal) if matches!(outcome, PodUpgradeOutcome::PodRefused { .. }) => {
                Err(PodUpgradeFailure::LeftOnTarget(refusal))
            }
            Some(refusal) => Err(PodUpgradeFailure::Settled(refusal)),
            None => Ok(()),
        }
    }

    /// [`Self::settle_pod_runtime_upgrade`] for an activation that owns the
    /// attempt `owned_attempt` (attempt, generation), or joined another's
    /// (`None`). An upgrade that settled without reaching the target leaves
    /// nothing of it running, and a rollback or refusal the node recorded for
    /// the pod also deleted it, so the owned attempt is given up: left set,
    /// its lease would refuse the retry, which places a new pod, until it ran
    /// out. An unsettled one keeps it.
    pub(super) async fn settle_pod_runtime_upgrade_for_attempt(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        missing: MissingPod,
        upgrade: &PodUpgrade,
        deadline: Instant,
        owned_attempt: Option<(
            pplx_space_common_core::types::SandboxActivationAttemptId,
            i64,
        )>,
    ) -> Result<(), PodUpgradeFailure> {
        let settled = self
            .settle_pod_runtime_upgrade(sandbox_id, pod_name, missing, upgrade, deadline)
            .await;
        if let Err(PodUpgradeFailure::Settled(_) | PodUpgradeFailure::LeftOnTarget(_)) = &settled
            && let Some((attempt_id, generation)) = owned_attempt
        {
            self.store
                .give_up_activation_attempt_best_effort(sandbox_id, generation, attempt_id)
                .await;
        }
        settled
    }

    /// Wait until `upgrade`, rendered on the pod `pod_name`, has settled on
    /// the pod's node, or until `deadline`. A deadline, or a pod at that name
    /// that carries another operation, is an error: the upgrade may still
    /// run, so the caller keeps the pod and its execution for a retry, which
    /// reads the same operation again.
    async fn await_pod_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        missing: MissingPod,
        upgrade: &PodUpgrade,
        deadline: Instant,
    ) -> Result<PodUpgradeOutcome, SubstrateError> {
        let mut polls = WaitPolls::default();
        loop {
            let pending = match self
                .observe_pod_runtime_upgrade(
                    sandbox_id, pod_name, missing, upgrade, &mut polls, deadline,
                )
                .await?
            {
                Observation::Settled(outcome) => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        operation_id = %upgrade.operation_id,
                        target_state = upgrade.target_state.as_str(),
                        ?outcome,
                        "the pod's runtime upgrade settled"
                    );
                    return Ok(outcome);
                }
                Observation::SettledForPod { outcome, pod_uid } => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        operation_id = %upgrade.operation_id,
                        target_state = upgrade.target_state.as_str(),
                        pod_name,
                        pod_uid,
                        ?outcome,
                        "the pod's runtime upgrade settled for good; deleting the pod"
                    );
                    if self
                        .delete_settled_upgrade_pod(sandbox_id, pod_name, &pod_uid)
                        .await
                    {
                        return Ok(outcome);
                    }
                    // Another pod holds the name: the read that settled was
                    // of a pod the API has replaced, such as a reflector
                    // still holding a pod a retry recreated. That pod may be
                    // booting the upgrade, so nothing settles until a read
                    // shows the pod that's there now.
                    format!(
                        "pod {pod_uid}'s outcome settled, but another pod now holds the name \
                         {pod_name}"
                    )
                }
                Observation::Pending(reason) => reason,
            };
            if Instant::now() + OUTCOME_POLL >= deadline {
                return Err(SubstrateError::Timeout {
                    message: format!(
                        "sandbox {sandbox_id}'s runtime upgrade {} hasn't settled: {pending}; \
                         retry to converge",
                        upgrade.operation_id
                    ),
                    phase: None,
                });
            }
            tokio::time::sleep(OUTCOME_POLL).await;
        }
    }

    async fn observe_pod_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        missing: MissingPod,
        upgrade: &PodUpgrade,
        polls: &mut WaitPolls,
        deadline: Instant,
    ) -> Result<Observation, SubstrateError> {
        let operation_id = upgrade.operation_id.to_string();
        let operation_id = operation_id.as_str();
        // One bound for every read below, so together they overshoot the
        // wait's deadline by at most the floor.
        let deadline = deadline.max(Instant::now() + NODE_READ_FLOOR);
        let pod =
            match tokio::time::timeout_at(deadline, self.lookup_or_fetch_sandbox_pod(pod_name))
                .await
            {
                Ok(read) => read.map_err(|error| {
                    super::pod_ops::kube_error_for_substrate(
                        "failed to read the upgrade's pod",
                        error,
                    )
                })?,
                Err(_elapsed) => {
                    return Ok(Observation::Pending(format!(
                        "the API server didn't answer the read of pod {pod_name} in time"
                    )));
                }
            };
        if pod.is_none() && missing == MissingPod::NotCreatedYet {
            return Ok(Observation::Pending(format!(
                "pod {pod_name} doesn't exist yet, and the attempt that owns its create may \
                 still send it"
            )));
        }
        if let Some(pod) = pod.as_deref()
            && !pod.carries_runtime_upgrade(upgrade.operation_id)
        {
            // Not this upgrade's pod. Its own work may run, and nothing here
            // can tell what this operation did.
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "pod {pod_name} doesn't carry sandbox {sandbox_id}'s runtime upgrade \
                     {operation_id}; retry"
                ),
            });
        }
        // The execution that owns the pod's name, read on every look: the
        // pod it recorded is the attempt's own, whatever a cache shows.
        let execution =
            match tokio::time::timeout_at(deadline, self.store.active_execution_status(sandbox_id))
                .await
            {
                Ok(execution) => execution?.filter(|execution| execution.pod_name == pod_name),
                Err(_elapsed) => {
                    return Ok(Observation::Pending(
                        "the database didn't answer the read of the upgrade's execution in time"
                            .to_string(),
                    ));
                }
            };
        let recorded_pod_uid = execution
            .as_ref()
            .and_then(|execution| execution.pod_uid.clone())
            .map(String::from);
        if let Some(pod) = pod.as_deref() {
            // A pod on its way out may be one a later attempt is replacing
            // at the same name, under the same operation: nothing settles on
            // it, and once it's gone the rules for a missing pod apply.
            if pod.is_deleting() {
                return Ok(Observation::Pending(format!(
                    "pod {pod_name} is being deleted, and a pod at its name may replace it"
                )));
            }
            // A cache can still hold an earlier attempt's pod at the name.
            if let Some(recorded) = recorded_pod_uid.as_deref()
                && pod.uid() != Some(recorded)
            {
                return Ok(Observation::Pending(format!(
                    "the pod at {pod_name} isn't the one its execution recorded"
                )));
            }
            if let Some(uid) = pod.uid() {
                polls.seen_pod_uid = Some(uid.to_owned());
            }
        } else if execution.is_none() && polls.seen_pod_uid.is_none() {
            // Neither this wait nor an execution that still owns the name
            // knows which pod was the attempt's, so nothing proves it never
            // existed or what its outcome was.
            return Ok(Observation::Pending(format!(
                "pod {pod_name} is gone, and no execution that owns its name records it"
            )));
        }
        // A missing pod the caller's own attempt created was deleted, and one
        // it never created never will be: either way no shim runs for it, and
        // the node still answers for the operation. The pod it knew is the
        // one its execution recorded, or else the one this wait saw.
        let known_pod_uid = recorded_pod_uid.or_else(|| polls.seen_pod_uid.clone());
        let node_name = match pod.as_deref() {
            Some(pod) => pod.node_name().map(str::to_owned),
            None => execution.and_then(|execution| execution.k8s_node_name),
        };
        let node_name = node_name.or_else(|| polls.seen_node_name.clone());
        let Some(node_name) = node_name else {
            return Ok(unlocated_observation(
                pod.is_some(),
                known_pod_uid.is_some(),
            ));
        };
        polls.seen_node_name = Some(node_name.clone());
        let report = match self
            .node_runtime_upgrade_report(
                &mut polls.reader,
                &node_name,
                sandbox_id,
                operation_id,
                deadline,
            )
            .await
        {
            Ok(report) => report,
            Err(reason) => return Ok(Observation::Pending(reason)),
        };
        let status = RuntimeUpgradeStatus::try_from(report.status)
            .unwrap_or(RuntimeUpgradeStatus::Unspecified);
        Ok(observe(
            NodeReport {
                status,
                failure: report.failure,
                activation_claimed: report.activation_claimed,
                pod_uid: report.pod_uid,
                refused_pod: report.refused_pod_uid.map(|pod_uid| RefusedPod {
                    pod_uid,
                    failure: report.refused_pod_failure,
                }),
            },
            upgrade.target_state,
            pod.as_deref(),
            known_pod_uid.as_deref(),
        ))
    }

    /// Delete the pod a rollback or refusal is final for, but only while it's
    /// the one with `pod_uid`: kubelet would retry its create forever, and
    /// the caller's retry must place a new pod for the node to take as a new
    /// attempt. Answers whether the outcome may settle: `false` when the API
    /// no longer has that pod or another pod holds the name, so the read
    /// that settled was of a pod that's gone or been replaced. A pod that
    /// can't be named or deleted is left for the retry, whose wait settles on
    /// the same report and deletes it then, and doesn't hold this one back.
    async fn delete_settled_upgrade_pod(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        pod_uid: &str,
    ) -> bool {
        let (Ok(name), Ok(uid)) = (
            super::PodName::parse(pod_name),
            super::PodUid::parse(pod_uid),
        ) else {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                pod_name,
                pod_uid,
                "can't name the settled runtime upgrade pod to delete it"
            );
            return true;
        };
        match super::delete_sandbox_pod_fenced(&self.k8s, &name, &uid, None).await {
            Ok(outcome) => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    pod_name,
                    pod_uid,
                    ?outcome,
                    "deleted the settled runtime upgrade pod, unless it was gone or replaced"
                );
                // Gone already means the read that settled was of a pod
                // the API no longer has, such as one a cache still held;
                // replaced means another pod holds the name. Either way the
                // next read decides, from what's there now.
                matches!(outcome, super::FencedPodDeleteOutcome::Deleted)
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    pod_name,
                    pod_uid,
                    %error,
                    "couldn't delete the settled runtime upgrade pod; a retry deletes it"
                );
                true
            }
        }
    }

    /// `GetRuntimeUpgrade` for `operation_id` from the node-api on
    /// `node_name`, or why it couldn't be read. The dial, when one is needed,
    /// and the call end by `deadline`. Reuses `reader` while it's for that
    /// node, and drops it after a transport failure or a read that ran out of
    /// time, so the next poll dials afresh.
    async fn node_runtime_upgrade_report(
        &self,
        reader: &mut Option<NodeReader>,
        node_name: &str,
        sandbox_id: SandboxId,
        operation_id: &str,
        deadline: Instant,
    ) -> Result<pplx_space_sbx_mgr_client::proto::GetRuntimeUpgradeResponse, String> {
        // Answers why the read failed and whether the client should be
        // dialed again.
        let read = async {
            let current = match reader.take() {
                Some(current) if current.node_name == node_name => current,
                _ => {
                    let (addr, client) = self
                        .node_worker_client(node_name)
                        .await
                        .map_err(|reason| (reason, false))?;
                    NodeReader {
                        node_name: node_name.to_string(),
                        addr,
                        client: client.internal_client(),
                    }
                }
            };
            let NodeReader { addr, client, .. } = reader.insert(current);
            client
                .get_runtime_upgrade(GetRuntimeUpgradeRequest {
                    sandbox_id: sandbox_id.to_string(),
                    operation_id: operation_id.to_string(),
                })
                .await
                .map(tonic::Response::into_inner)
                .map_err(|status| {
                    (
                        format!("node-api at {addr} answered {status}"),
                        is_transport_failure(&status),
                    )
                })
        };
        match tokio::time::timeout_at(deadline, read).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err((reason, redial))) => {
                if redial {
                    *reader = None;
                }
                Err(reason)
            }
            Err(_elapsed) => {
                *reader = None;
                Err(format!("node {node_name}'s node-api didn't answer in time"))
            }
        }
    }
}

/// Whether a failed call says the connection, not the node's answer, failed,
/// so the client is dialed again.
fn is_transport_failure(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::Unknown
            | tonic::Code::Cancelled
            | tonic::Code::DeadlineExceeded
    )
}
