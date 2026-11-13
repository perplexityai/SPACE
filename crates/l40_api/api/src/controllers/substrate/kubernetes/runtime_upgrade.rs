//! The resume- and start-time runtime upgrade decision, on Kubernetes.
//!
//! [`KubernetesSubstrate::activation_runtime_upgrade`] reads the policy, the
//! sandbox's recorded execution and the environment catalog, and turns
//! [`decide`]'s plan into the [`PodUpgrade`] the activation's pod carries
//! (`pod_runtime_upgrade.rs`): its shim runs the upgrade at container create,
//! and the activation's Wake carries none. Placement doesn't change: a
//! restore pod placed on the node that holds the sandbox's checkpoint
//! upgrades it there, and one placed elsewhere carries the checkpoint's
//! storage URI, from which the node stages the checkpoint before it upgrades
//! it. A stopped sandbox's pod is bound to the node holding its disk, as for
//! any start.

use super::KubernetesSubstrate;
use super::pod_runtime_upgrade::PodUpgrade;
use crate::controllers::substrate::SubstrateError;
use crate::controllers::substrate::runtime_upgrade::{
    EnvironmentMove, RuntimePlan, UpgradeActivation, attribute, decide, stranded_error,
};
use pplx_space_common_core::kubernetes::PodRuntimeUpgradeTargetState;
use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;

impl KubernetesSubstrate {
    /// The runtime upgrade an activation of `sandbox_id` renders on its pod,
    /// leaving the sandbox started, or `None` to start or restore as
    /// recorded. A stranded sandbox is refused. `restore_storage_uri` is the
    /// restore's selected checkpoint, when it has one.
    pub(super) async fn activation_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        restore_storage_uri: Option<&str>,
    ) -> Result<Option<PodUpgrade>, SubstrateError> {
        if !self.k8s.runtime_upgrade_at_activation.is_enabled() {
            return Ok(None);
        }
        Ok(self
            .decide_runtime_upgrade(sandbox_id, activation, restore_storage_uri)
            .await?
            .map(|decided| decided.pod_upgrade(sandbox_id, PodRuntimeUpgradeTargetState::Started)))
    }

    /// [`Self::activation_runtime_upgrade`]'s decision, whatever the
    /// activation setting: the move and the upgrade that makes it, or `None`
    /// to stay on the recorded environment. A stranded sandbox is refused.
    pub(super) async fn decide_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        restore_storage_uri: Option<&str>,
    ) -> Result<Option<DecidedUpgrade>, SubstrateError> {
        let policy = self
            .runtime_policy
            .get(|| self.store.runtime_environment_policy())
            .await?;
        // Without a default the policy narrows nothing, so no evidence is
        // needed.
        if policy.default_environment.is_none() {
            record(sandbox_id, activation, RuntimePlan::AsRecorded);
            return Ok(None);
        }
        let recorded = match activation {
            UpgradeActivation::Restore => {
                self.store
                    .restore_checkpoint_execution(sandbox_id, restore_storage_uri)
                    .await?
            }
            UpgradeActivation::Start => self.store.last_published_execution(sandbox_id).await?,
        };
        // A resume's upgrade boots the node's copy of the checkpoint only if
        // the node can tell it's this one, by its local artifact; a checkpoint
        // without that identity is left to the node's own restore.
        let Some(recorded) = recorded.filter(|recorded| {
            activation == UpgradeActivation::Start || recorded.local_artifact_id.is_some()
        }) else {
            return Ok(unattributed(sandbox_id, activation));
        };
        // A bound record on an allowed environment stays whatever the catalog
        // says, so the common case reads no catalog.
        if let Some(bound) = recorded
            .execution
            .environment
            .as_deref()
            .and_then(|environment| environment.parse().ok())
            && policy.environments.get(&bound) == Some(&RuntimeEnvironmentState::Allowed)
        {
            record(sandbox_id, activation, RuntimePlan::AsRecorded);
            return Ok(None);
        }
        let catalog = self.store.unretired_runtime_environments().await?;
        let Some(from) = attribute(&recorded.execution, &catalog, policy.legacy_unbound_allowed)
        else {
            return Ok(unattributed(sandbox_id, activation));
        };
        let from_catalog = catalog
            .iter()
            .find(|environment| environment.reference == from);
        let plan = match decide(&policy, activation, from, from_catalog) {
            // The cached policy may be up to its TTL old. Before asking the
            // node to discard a checkpoint's memory, decide again on the
            // policy as it is now: an environment allowed again in the
            // meantime restores as recorded.
            RuntimePlan::ColdBootUpgrade(to_default)
                if to_default.authorization(activation).memory_loss =>
            {
                let current = self.store.runtime_environment_policy().await?;
                decide(&current, activation, from, from_catalog)
            }
            plan => plan,
        };
        record(sandbox_id, activation, plan);
        match plan {
            RuntimePlan::AsRecorded | RuntimePlan::RestoreOnDefault(_) => Ok(None),
            RuntimePlan::ColdBootUpgrade(to_default) => Ok(Some(DecidedUpgrade {
                movement: to_default,
                activation,
                subject: recorded.subject,
                checkpoint: recorded.local_artifact_id,
            })),
            RuntimePlan::Stranded(to_default) => Err(stranded_error(sandbox_id, to_default)),
        }
    }
}

/// A cold-boot upgrade [`KubernetesSubstrate::decide_runtime_upgrade`] chose.
#[derive(Clone, Debug)]
pub(super) struct DecidedUpgrade {
    pub(super) movement: EnvironmentMove,
    pub(super) activation: UpgradeActivation,
    /// What is booted: the checkpoint for a restore, the recorded execution
    /// for a start. It names the operation.
    pub(super) subject: String,
    /// The local artifact of the checkpoint a restore was decided from,
    /// which the node must find pinned to boot it.
    pub(super) checkpoint: Option<LocalArtifactId>,
}

impl DecidedUpgrade {
    /// The upgrade as `sandbox_id`'s pod renders it, leaving the sandbox in
    /// `target_state`. A `started` target derives its operation from what is
    /// booted, so every retry names the same operation under the same
    /// authorization. A `stopped` target names a fresh one on every
    /// decision: its claim records the operation on the execution, in the
    /// same transaction and before any pod carries it, and every retry
    /// reads it back from there. A fresh id also keeps it apart from a
    /// start's operation, which a node refuses to reuse for another target
    /// state, and from one the control plane abandoned on the node
    /// (`AbandonRuntimeUpgrade`), which the node refuses for good.
    pub(super) fn pod_upgrade(
        &self,
        sandbox_id: SandboxId,
        target_state: PodRuntimeUpgradeTargetState,
    ) -> PodUpgrade {
        let operation_id = match target_state {
            PodRuntimeUpgradeTargetState::Started => {
                self.movement
                    .operation_id(sandbox_id, self.activation, &self.subject)
            }
            PodRuntimeUpgradeTargetState::Stopped => uuid::Uuid::now_v7(),
        };
        let authorization = self.movement.authorization(self.activation);
        tracing::info!(
            sandbox_id = %sandbox_id,
            %operation_id,
            target_state = target_state.as_str(),
            "booting the sandbox's pod through its runtime upgrade"
        );
        PodUpgrade {
            operation_id,
            target: self.movement.to,
            authorization_id: authorization.authorization_id,
            authorization_reason: authorization.reason,
            memory_loss: authorization.memory_loss,
            target_state,
            checkpoint_local_artifact_id: self.checkpoint,
        }
    }
}

/// No recorded execution attributes the sandbox to a catalog environment. The
/// node's execution policy still refuses a runtime that may not execute; this
/// only means the control plane can't move it first.
fn unattributed<T>(sandbox_id: SandboxId, activation: UpgradeActivation) -> Option<T> {
    tracing::info!(
        sandbox_id = %sandbox_id,
        activation = activation.as_str(),
        "no recorded execution attributes this sandbox to a catalog environment; \
         activating as recorded"
    );
    crate::metrics::runtime_upgrade::activation_decision(
        activation.as_str(),
        "unattributed",
        "none",
    );
    None
}

/// Count and log one decision.
fn record(sandbox_id: SandboxId, activation: UpgradeActivation, plan: RuntimePlan) {
    let to_default = match plan {
        RuntimePlan::AsRecorded => None,
        RuntimePlan::RestoreOnDefault(to_default)
        | RuntimePlan::ColdBootUpgrade(to_default)
        | RuntimePlan::Stranded(to_default) => Some(to_default),
    };
    let cause = to_default.map_or("none", |to_default| to_default.cause.as_str());
    crate::metrics::runtime_upgrade::activation_decision(activation.as_str(), plan.as_str(), cause);
    if let Some(to_default) = to_default {
        tracing::info!(
            sandbox_id = %sandbox_id,
            activation = activation.as_str(),
            plan = plan.as_str(),
            from = %to_default.from,
            to = %to_default.to,
            cause,
            "decided the sandbox's runtime at activation"
        );
    }
}
