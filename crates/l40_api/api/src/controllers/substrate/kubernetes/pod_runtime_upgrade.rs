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
//! refused, retryably for a rollback, and its retry decides the same upgrade
//! again. A refusal the node recorded answers `FailedPrecondition`, since
//! the node refuses that upgrade until what it checked changes. A start
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
use super::pod_ops::pod_matches_execution_generation;
use super::sandbox_pod_view::SandboxPodView;
use super::store::ActiveExecutionStatusRow;
use super::store::RecordedRuntimeUpgrade;
use super::support::execution_pod_create_in_flight;
use crate::controllers::substrate::SubstrateError;

#[cfg(test)]
mod tests;

/// How often [`KubernetesSubstrate::await_pod_runtime_upgrade`] rereads the
/// pod and the node.
const OUTCOME_POLL: Duration = Duration::from_millis(200);

/// The least time one observation of the upgrade (the pod read, from the
/// cache or, on a miss, uncached, and always uncached for a joiner; the read
/// of its execution from Postgres; and the `GetRuntimeUpgrade` read with any dial it
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
    /// The upgrade `pod` renders, read back from the annotations its create
    /// wrote, or `None` for a pod that renders none or whose annotations
    /// don't read as one.
    pub(super) fn carried_by(pod: &SandboxPodView) -> Option<Self> {
        use super::sandbox_pod_view::SandboxPodKey;
        let target_state = match pod.annotation(SandboxPodKey::RuntimeUpgradeTargetState)? {
            "started" => PodRuntimeUpgradeTargetState::Started,
            "stopped" => PodRuntimeUpgradeTargetState::Stopped,
            _ => return None,
        };
        Some(Self {
            operation_id: pod
                .annotation(SandboxPodKey::RuntimeUpgradeOperationId)?
                .parse()
                .ok()?,
            target: pod
                .annotation(SandboxPodKey::RuntimeUpgradeTargetEnvironment)?
                .parse()
                .ok()?,
            authorization_id: pod
                .annotation(SandboxPodKey::RuntimeUpgradeAuthorizationId)?
                .to_string(),
            authorization_reason: pod
                .annotation(SandboxPodKey::RuntimeUpgradeAuthorizationReason)
                .unwrap_or_default()
                .to_string(),
            memory_loss: pod.annotation(SandboxPodKey::RuntimeUpgradeMemoryLoss) == Some("true"),
            target_state,
            checkpoint_local_artifact_id: match pod
                .annotation(SandboxPodKey::RuntimeUpgradeCheckpointLocalArtifactId)
            {
                Some(id) => Some(id.parse().ok()?),
                None => None,
            },
        })
    }

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
    /// target, or `None` for one that reached it. Retryable (503) whatever
    /// the target state, except a refusal the node recorded. A rollback is
    /// final for the pod it names, which the wait deletes, and a retry from a
    /// new pod under the same operation fences and boots again, and may
    /// succeed. A recorded refusal is one the node classed as no retry
    /// changing (`FAILED_PRECONDITION` or `INVALID_ARGUMENT`: the edge isn't
    /// certified, the checkpoint isn't the decided one, consent is missing,
    /// the operation names another sandbox or target), and a retry decides the
    /// same operation from the same inputs, so it answers
    /// `FailedPrecondition` (412), as a stranded sandbox does, rather than
    /// invite a retry loop that places and deletes a pod each round. A retry
    /// once what the node checked has changed starts over in a new pod.
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
            Self::Refused { failure } => {
                return Some(SubstrateError::FailedPrecondition {
                    message: format!(
                        "sandbox {sandbox_id}'s runtime upgrade {} to {} was refused before it \
                         began ({}); it stays on its environment, and the node refuses the \
                         upgrade until what it checked changes",
                        upgrade.operation_id,
                        upgrade.target,
                        failure.as_deref().unwrap_or("the node recorded no reason")
                    ),
                });
            }
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
    pod.is_terminal() || container_terminated(pod)
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
    /// The control plane abandoned the operation on the node, so no call for
    /// it fences or boots there any more.
    abandoned: bool,
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
        abandoned,
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
                        // The pod is gone or ended, a terminal phase
                        // included. Unlike an absent operation, where only a
                        // terminated container proves the call returned, the
                        // node has already run this one: the outcome leaves
                        // the sandbox on the target (`LeftOnTarget`) and
                        // reverts nothing, so a create still on its way
                        // can't make it wrong.
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
        // when it names the pod this attempt knew it had. The one exception
        // is an operation the pod's node has abandoned since, below.
        RuntimeUpgradeStatus::RolledBack | RuntimeUpgradeStatus::Refused => {
            let (outcome, reported) = if status == RuntimeUpgradeStatus::RolledBack {
                (PodUpgradeOutcome::RolledBack { failure }, "a rollback")
            } else {
                (PodUpgradeOutcome::Refused { failure }, "a refusal")
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
                // Abandoned since: the node refuses every later call for the
                // operation before it records anything, so this pod's call
                // never began and nothing will name this pod.
                (_, None) if abandoned => Observation::Settled(PodUpgradeOutcome::NotStarted),
                (_, None) => Observation::Pending(format!(
                    "the node reports {reported} {}, which may be an earlier attempt's",
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
        // An operation the node never recorded that it has since abandoned
        // never began there and never will, whatever the pod shows: every
        // call for it is refused before it fences.
        // A started target's refused create the node kept for this pod after
        // the sandbox's row went: the report went with the row, so it
        // answers absent, but the refusal stays final for the pod, as it is
        // with the report standing, and the pod stays pending while kubelet
        // retries its create. Without the report the node can't say whether
        // the operation upgraded or found its target.
        RuntimeUpgradeStatus::Absent
            if target_state == PodRuntimeUpgradeTargetState::Started
                && refused_pod.as_ref().is_some_and(|refused| {
                    let named = Some(refused.pod_uid.as_str());
                    match pod {
                        Some(pod) => pod.uid() == named,
                        None => known_pod_uid == named,
                    }
                }) =>
        {
            let outcome = PodUpgradeOutcome::PodRefused {
                already_on_target: false,
                activation_claimed,
                failure: refused_pod.and_then(|refused| refused.failure),
            };
            match pod.and_then(SandboxPodView::uid) {
                Some(pod_uid) => Observation::SettledForPod {
                    outcome,
                    pod_uid: pod_uid.to_owned(),
                },
                None => Observation::Settled(outcome),
            }
        }
        RuntimeUpgradeStatus::Absent if abandoned => {
            Observation::Settled(PodUpgradeOutcome::NotStarted)
        }
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
    /// Nothing of the upgrade runs in the pod: the node rolled it back,
    /// refused the call before it began, or never began it. A rollback or
    /// refusal the node recorded for the pod also deleted it, so the caller's
    /// retry places a new one.
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
    /// and that may not have been sent yet. No pod there settles nothing
    /// while the execution records no pod: the owner's pod may still boot
    /// the upgrade, so the wait runs to its deadline unless a pod shows up
    /// and settles. Once the wait has seen the pod the execution records, a
    /// read finding no pod reads as [`MissingPod::Gone`].
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
    /// The node the wait last read the upgrade through: the pod's, or with
    /// no pod or an unbound one, its execution's or an earlier look's.
    seen_node_name: Option<String>,
    /// The upgrade's pod, by UID, and the node a look saw it bound to.
    seen_bound_pod: Option<(String, String)>,
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
    /// left the sandbox on its target: a rollback, a refused call or one never
    /// begun is [`PodUpgradeFailure::Settled`], a pod the node refused after
    /// reaching the target is [`PodUpgradeFailure::LeftOnTarget`], and a wait
    /// that ended unsettled is [`PodUpgradeFailure::MayStillRun`].
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
    /// (`None`). An upgrade that settled, without reaching the target or
    /// reaching it in a pod the node refused, leaves nothing of it running in
    /// the pod, and a rollback or refusal the node recorded for the pod also
    /// deleted it, so the owned attempt is given up: left set,
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

    /// The upgrade the pod at `pod_name` carries when it isn't `rendered`:
    /// an earlier activation's, on a pod a `restoring` sandbox keeps
    /// (`runtime_upgrade_stale_reason`). Read from the pod reflector's cache
    /// and, when the cache doesn't hold the pod the execution records, from
    /// the API, and taken only while it's the pod `sandbox_id`'s execution
    /// records, so a cache still holding a pod this materialization replaced
    /// settles nothing. An execution still `creating` with no pod recorded belongs
    /// to an activation that created its pod and failed to persist it, which
    /// keeps the pod (`ensure_pod.rs`), since its shim may be booting the
    /// upgrade. That pod is taken too, read as materialization hydrates it:
    /// from the API on a cache miss, and only while its annotations name this
    /// sandbox and the execution's generation, which its name fixes, and it
    /// isn't being deleted. Missing it would leave the upgrade running on
    /// the node, retried by kubelet, while every resume answers its own
    /// decision's refusal. `None` when there's no such pod, it carries no
    /// upgrade or the rendered one, or the execution or the pod can't be
    /// read.
    pub(super) async fn carried_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        pod_name: &str,
        rendered: Option<&PodUpgrade>,
    ) -> Option<PodUpgrade> {
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await
            .ok()??;
        if execution.pod_name != pod_name {
            return None;
        }
        self.upgrade_carried_by_execution_pod(sandbox_id, &execution, rendered)
            .await
    }

    /// [`Self::carried_runtime_upgrade`] for the pod at `execution`'s name,
    /// given the execution row already read.
    async fn upgrade_carried_by_execution_pod(
        &self,
        sandbox_id: SandboxId,
        execution: &ActiveExecutionStatusRow,
        rendered: Option<&PodUpgrade>,
    ) -> Option<PodUpgrade> {
        let pod_name = execution.pod_name.as_str();
        let pod = match &execution.pod_uid {
            // A reflector that hasn't seen the recorded pod, or still holds
            // an earlier one at the name, would hide a kept upgrade, and a
            // takeover that misses one lets the ordinary stale-pod rules
            // replace its pod unread. So anything but the recorded pod in the
            // cache is confirmed from the API.
            Some(recorded) => {
                let is_recorded = |pod: &SandboxPodView| pod.uid() == Some(recorded.as_str());
                match self
                    .lookup_sandbox_pod(pod_name)
                    .filter(|pod| is_recorded(pod))
                {
                    Some(pod) => pod,
                    None => self
                        .read_sandbox_pod_uncached(pod_name)
                        .await
                        .ok()?
                        .filter(|pod| is_recorded(pod))?,
                }
            }
            None if execution_pod_create_in_flight(execution.state) => self
                .lookup_or_fetch_sandbox_pod(pod_name)
                .await
                .ok()?
                .filter(|pod| {
                    !pod.is_deleting()
                        && pod_matches_execution_generation(pod, sandbox_id, execution.generation)
                })?,
            None => return None,
        };
        PodUpgrade::carried_by(&pod).filter(|carried| {
            rendered.is_none_or(|rendered| rendered.operation_id != carried.operation_id)
        })
    }

    /// The upgrade a pod `sandbox_id`'s active execution keeps carries, read
    /// as [`Self::carried_runtime_upgrade`] reads it, before this activation
    /// has claimed anything: for a resume taking over a restore an earlier
    /// one left `restoring`, which settles it before anything its own
    /// decision renders, whether that decision refuses, renders no upgrade or
    /// renders another.
    pub(super) async fn kept_runtime_upgrade(&self, sandbox_id: SandboxId) -> Option<PodUpgrade> {
        let execution = self
            .store
            .active_execution_status(sandbox_id)
            .await
            .ok()??;
        self.upgrade_carried_by_execution_pod(sandbox_id, &execution, None)
            .await
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
        // A joiner reads the pod from the API every time: a cache can still
        // hold an earlier attempt's pod the API no longer has, under the UID
        // the execution still records, and a joiner that took it for its
        // owner's pod would settle, or delete, on that attempt's outcome.
        let read = async {
            match missing {
                MissingPod::NotCreatedYet => self.read_sandbox_pod_uncached(pod_name).await,
                MissingPod::Gone => self.lookup_or_fetch_sandbox_pod(pod_name).await,
            }
        };
        let pod = match tokio::time::timeout_at(deadline, read).await {
            Ok(read) => read.map_err(|error| {
                super::pod_ops::kube_error_for_substrate("failed to read the upgrade's pod", error)
            })?,
            Err(_elapsed) => {
                return Ok(Observation::Pending(format!(
                    "the API server didn't answer the read of pod {pod_name} in time"
                )));
            }
        };
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
        // A joiner can't see its owner's create. No pod there is the owner's
        // pod gone only once this wait saw the pod the execution records and
        // the API, read again, has no pod at the name. A record the wait never
        // saw a pod for may be an earlier attempt's: an attempt that settled
        // deletes its pod and gives itself up, leaving the pod's UID on the
        // execution, and the attempt this joiner joined claims it with that
        // record standing until its own materialization clears it. The node
        // keeps reporting the earlier pod's outcome until the new pod's shim
        // reaches it, so settling on that record would end the joiner on an
        // earlier attempt's outcome while its owner boots the upgrade. A pod
        // this wait saw was the owner's own: the owner creates a pod only
        // after a materialization clears the recorded pod, so once it's gone
        // no create is on its way and nothing runs for the upgrade there.
        if pod.is_none() && missing == MissingPod::NotCreatedYet {
            if recorded_pod_uid.is_none() {
                return Ok(Observation::Pending(format!(
                    "pod {pod_name} doesn't exist yet, and the attempt that owns its create \
                     may still send it"
                )));
            }
            if polls.seen_pod_uid != recorded_pod_uid {
                return Ok(Observation::Pending(format!(
                    "pod {pod_name} isn't there, and this wait never saw the pod its execution \
                     records, which may be an earlier attempt's"
                )));
            }
            match tokio::time::timeout_at(deadline, self.read_sandbox_pod_uncached(pod_name)).await
            {
                Ok(Ok(None)) => {}
                Ok(Ok(Some(_))) => {
                    return Ok(Observation::Pending(format!(
                        "a pod appeared at {pod_name} after its execution's record was read"
                    )));
                }
                Ok(Err(error)) => {
                    return Err(super::pod_ops::kube_error_for_substrate(
                        "failed to confirm the upgrade's pod is gone",
                        error,
                    ));
                }
                Err(_elapsed) => {
                    return Ok(Observation::Pending(format!(
                        "the API server didn't answer the confirming read of pod {pod_name} in \
                         time"
                    )));
                }
            }
        }
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
                if let Some(node) = pod.node_name() {
                    polls.seen_bound_pod = Some((uid.to_owned(), node.to_owned()));
                }
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
        let pod_bound = pod.is_some() && node_name.is_some();
        let node_name = node_name.or_else(|| polls.seen_node_name.clone());
        let Some(node_name) = node_name else {
            return Ok(unlocated_observation(
                pod.is_some(),
                known_pod_uid.is_some(),
            ));
        };
        // A node abandons an operation for itself only, so its abandonment
        // speaks for the pod only when the pod runs its create there: a pod
        // bound to the node read, or a pod that's gone that a look saw bound
        // to it. An unbound pod, or a gone one read through any other node
        // (its execution's, an earlier look's), may run its create elsewhere.
        let abandonment_applies = pod_bound
            || (pod.is_none()
                && known_pod_uid.as_deref().is_some_and(|uid| {
                    polls.seen_bound_pod.as_ref() == Some(&(uid.to_owned(), node_name.clone()))
                }));
        polls.seen_node_name = Some(node_name.clone());
        // The node answers for this pod's attempt alone, which the shim
        // names by the pod UID: its own rollback or refusal, or absent in
        // place of another attempt's, so a later pod's call never changes
        // what this wait reads. A node that predates the field
        // answers the operation's latest outcome, which `observe` still
        // checks names the pod.
        let asked_pod_uid = pod
            .as_deref()
            .and_then(SandboxPodView::uid)
            .map(str::to_owned)
            .or_else(|| known_pod_uid.clone());
        let report = match self
            .node_runtime_upgrade_report(
                &mut polls.reader,
                &node_name,
                sandbox_id,
                operation_id,
                asked_pod_uid,
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
                // A pod's attempt is its UID: the shim names its
                // `UpgradeRuntime` attempt by the pod UID, so the report's
                // attempts are pods.
                pod_uid: report.attempt_id,
                refused_pod: report.refused_attempt_id.map(|pod_uid| RefusedPod {
                    pod_uid,
                    failure: report.refused_attempt_failure,
                }),
                abandoned: report.abandoned && abandonment_applies,
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
        pod_uid: Option<String>,
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
                    attempt_id: pod_uid,
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
