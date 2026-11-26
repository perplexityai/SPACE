//! The resume- and start-time runtime upgrade decision, on Kubernetes.
//!
//! [`KubernetesSubstrate::decide_activation_runtime`] reads the policy, the
//! sandbox's recorded execution and the environment catalog, and turns
//! [`decide`]'s plan into either an inline restore-time memory drop
//! ([`ActivationRuntimePlan::DropMemoryAndColdBoot`]) or the
//! `WakeRequest.runtime_upgrade` the activation's Wake carries
//! ([`ActivationRuntimePlan::WakeUpgrade`]). Placement doesn't change: a
//! restore pod placed on the node that holds the sandbox's checkpoint upgrades
//! it there, and one placed elsewhere receives the checkpoint's storage URI in
//! the Wake's start context, from which the node stages the disk before it
//! upgrades it. A stopped sandbox's pod is bound to the node holding its disk,
//! as for any start.

use super::KubernetesSubstrate;
use crate::controllers::substrate::SubstrateError;
use crate::controllers::substrate::runtime_upgrade::{
    EnvironmentMove, RuntimePlan, UpgradeActivation, attribute, decide,
    has_executing_memory_retired_environment, stranded_error, unattributed_memory_retired_error,
};
use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;
use pplx_space_sandbox_model::SandboxId;

/// What a Kubernetes activation does with the runtime-upgrade decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ActivationRuntimePlan {
    /// Start or restore as recorded, or restore on the default environment
    /// without a cold-boot upgrade.
    NoWakeUpgrade,
    /// Drop the checkpoint's memory in S3 and Postgres before any pod exists,
    /// then cold-boot the disk on the default environment.
    DropMemoryAndColdBoot(EnvironmentMove),
    /// Send a `WakeRuntimeUpgrade` on the Wake RPC.
    WakeUpgrade(pplx_space_sbx_mgr_client::proto::WakeRuntimeUpgrade),
}

impl KubernetesSubstrate {
    /// The runtime upgrade an activation of `sandbox_id` asks the node for,
    /// or `None` to start or restore as recorded. A stranded sandbox is
    /// refused. `restore_storage_uri` is the restore's selected checkpoint,
    /// when it has one.
    pub(super) async fn activation_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        restore_storage_uri: Option<&str>,
    ) -> Result<Option<pplx_space_sbx_mgr_client::proto::WakeRuntimeUpgrade>, SubstrateError> {
        match self
            .decide_activation_runtime(sandbox_id, activation, restore_storage_uri)
            .await?
        {
            ActivationRuntimePlan::NoWakeUpgrade => Ok(None),
            ActivationRuntimePlan::WakeUpgrade(upgrade) => Ok(Some(upgrade)),
            ActivationRuntimePlan::DropMemoryAndColdBoot(to_default) => {
                Err(SubstrateError::Timeout {
                    message: format!(
                        "sandbox {sandbox_id} requires a restore-time memory drop from {} to {} \
                         before pod placement",
                        to_default.from, to_default.to
                    ),
                    phase: None,
                })
            }
        }
    }

    /// Decide the activation runtime plan for `sandbox_id` before any pod
    /// exists.
    ///
    /// Restore-time memory drop runs whenever the checkpoint's environment has
    /// `memory_retired = true`, even when `runtime_upgrade_at_activation` is
    /// disabled (`SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED` stays `false` on
    /// Kubernetes when `SPACE_ENVIRONMENT_PLACEMENT_GATE=on`). Wake-carried
    /// `ColdBootUpgrade` plans on `Start` and non-memory-retired `Restore`
    /// remain gated on `runtime_upgrade_at_activation`.
    pub(super) async fn decide_activation_runtime(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        restore_storage_uri: Option<&str>,
    ) -> Result<ActivationRuntimePlan, SubstrateError> {
        let upgrade_at_activation = self.k8s.runtime_upgrade_at_activation.is_enabled();
        if !upgrade_at_activation && activation == UpgradeActivation::Start {
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
        }
        // Restores read a fresh policy serialized behind any in-flight policy
        // write (`FOR KEY SHARE` on `runtime_environment_policy`), rather than
        // reusing a 5-second cache entry or an unlocked snapshot: worker nodes
        // keep `deprecated` environments in `executing()`, so `space-api` must
        // either fail a concurrent `retire`'s coverage check via this restore's
        // committed `restoring` lease or wait for that `retire` to commit and
        // see `memory_retired = true`. Starts carry no checkpoint memory and
        // nodes enforce `blocked`, so starts keep the cached read.
        let policy = match activation {
            UpgradeActivation::Restore => {
                self.runtime_policy
                    .refresh(|| self.store.runtime_environment_policy_serialized())
                    .await?
            }
            UpgradeActivation::Start => {
                self.runtime_policy
                    .get(|| self.store.runtime_environment_policy())
                    .await?
            }
        };
        // Without a default the policy narrows nothing, so no evidence is
        // needed.
        if policy.default_environment.is_none() {
            if upgrade_at_activation {
                record(sandbox_id, activation, RuntimePlan::AsRecorded);
            }
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
        }
        if !upgrade_at_activation && policy.memory_retired_environments.is_empty() {
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
        }
        let guard_unattributed_restore = activation == UpgradeActivation::Restore
            && has_executing_memory_retired_environment(&policy);
        let recorded = match activation {
            UpgradeActivation::Restore => {
                self.store
                    .restore_checkpoint_execution(sandbox_id, restore_storage_uri)
                    .await?
            }
            UpgradeActivation::Start => self.store.last_published_execution(sandbox_id).await?,
        };
        let Some(recorded) = recorded else {
            if guard_unattributed_restore {
                unattributed(sandbox_id, activation);
                return Err(unattributed_memory_retired_error(sandbox_id, &policy));
            }
            if upgrade_at_activation {
                unattributed(sandbox_id, activation);
            }
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
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
            if upgrade_at_activation {
                record(sandbox_id, activation, RuntimePlan::AsRecorded);
            }
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
        }
        let catalog = self.store.unretired_runtime_environments().await?;
        let Some(from) = attribute(&recorded.execution, &catalog, policy.legacy_unbound_allowed)
        else {
            if guard_unattributed_restore {
                unattributed(sandbox_id, activation);
                return Err(unattributed_memory_retired_error(sandbox_id, &policy));
            }
            if upgrade_at_activation {
                unattributed(sandbox_id, activation);
            }
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
        };
        let from_catalog = catalog
            .iter()
            .find(|environment| environment.reference == from);
        let plan = decide(&policy, activation, from, from_catalog);
        let memory_retired =
            activation == UpgradeActivation::Restore && policy.is_memory_retired(from);
        if !upgrade_at_activation && !memory_retired {
            return Ok(ActivationRuntimePlan::NoWakeUpgrade);
        }
        record(sandbox_id, activation, plan);
        match plan {
            RuntimePlan::AsRecorded | RuntimePlan::RestoreOnDefault(_) => {
                Ok(ActivationRuntimePlan::NoWakeUpgrade)
            }
            RuntimePlan::DropMemoryAndColdBoot(to_default) => {
                Ok(ActivationRuntimePlan::DropMemoryAndColdBoot(to_default))
            }
            RuntimePlan::ColdBootUpgrade(to_default)
                if memory_retired && !upgrade_at_activation =>
            {
                // `from` was retired while `deprecated` and later `blocked` on a
                // cluster with `runtime_upgrade_at_activation` disabled: still
                // drop its checkpoint memory in S3 and Postgres rather than
                // booting retired memory or sending an unconfigured Wake upgrade.
                Ok(ActivationRuntimePlan::DropMemoryAndColdBoot(to_default))
            }
            RuntimePlan::ColdBootUpgrade(to_default) => {
                // A resume's Wake upgrade boots the node's copy of the
                // checkpoint only if the node can tell it's this one, by its
                // local artifact; a memory-retired checkpoint without that
                // identity goes through `DropMemoryAndColdBoot` so the missing
                // pin is refused cleanly rather than booting retired memory.
                if activation == UpgradeActivation::Restore && recorded.local_artifact_id.is_none()
                {
                    if memory_retired {
                        return Ok(ActivationRuntimePlan::DropMemoryAndColdBoot(to_default));
                    }
                    unattributed(sandbox_id, activation);
                    return Ok(ActivationRuntimePlan::NoWakeUpgrade);
                }
                let upgrade = to_default.wake_upgrade(
                    sandbox_id,
                    activation,
                    &recorded.subject,
                    recorded.local_artifact_id,
                );
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    operation_id = %upgrade.operation_id,
                    "waking the sandbox by its runtime upgrade"
                );
                Ok(ActivationRuntimePlan::WakeUpgrade(upgrade))
            }
            RuntimePlan::Stranded(to_default) => Err(stranded_error(sandbox_id, to_default)),
        }
    }

    /// Drop a restore's checkpoint memory in object storage and Postgres under
    /// the caller's `restore_lease_token`, before any pod is created.
    ///
    /// On a completed or already-completed drop, `commit_for_restore` parks the
    /// row back in `suspended` pinned to its new `disk` artifact and clears its
    /// restore lease; until disk cold-boot from `suspended` lands, the call
    /// returns a retryable 503 without creating a pod. Transient precondition or
    /// object-store failures park the unplaced restore back in `suspended` on
    /// its full pin and return a retryable 503, while permanent precondition
    /// refusals return 412 `FailedPrecondition` rather than booting the retired
    /// environment's memory.
    pub(super) async fn drop_memory_for_restore(
        &self,
        sandbox_id: SandboxId,
        restore_lease_token: Option<uuid::Uuid>,
        to_default: EnvironmentMove,
    ) -> Result<std::convert::Infallible, SubstrateError> {
        use crate::controllers::sandbox::memory_drop::MemoryDropOutcome;

        let Some(restore_lease_token) = restore_lease_token else {
            return Err(SubstrateError::Timeout {
                message: format!(
                    "sandbox {sandbox_id} requires a restore-time memory drop from {} to {} \
                     with a user restore lease; retry",
                    to_default.from, to_default.to
                ),
                phase: None,
            });
        };
        let Some(memory_drop) = &self.memory_drop else {
            return Err(SubstrateError::Timeout {
                message: format!(
                    "sandbox {sandbox_id} requires a restore-time memory drop from {} to {}, \
                     which is disabled on this API replica; retry",
                    to_default.from, to_default.to
                ),
                phase: None,
            });
        };
        match memory_drop
            .drop_for_restore(self.store.pool(), sandbox_id, restore_lease_token)
            .await
        {
            Ok(MemoryDropOutcome::Dropped(_) | MemoryDropOutcome::AlreadyDropped) => {
                Err(super::store::memory_dropped_restore_pending())
            }
            Ok(MemoryDropOutcome::NotEligible(ineligible)) => {
                let reason = ineligible.reason();
                let retryable = ineligible.is_retryable();
                tracing::warn!(
                    %sandbox_id,
                    from = %to_default.from,
                    to = %to_default.to,
                    %reason,
                    retryable,
                    "restore-time memory drop refused; parking sandbox back in suspended"
                );
                if retryable {
                    Err(SubstrateError::Timeout {
                        message: format!(
                            "sandbox {sandbox_id} requires a restore-time memory drop from {} to {} \
                             ({reason}); retry",
                            to_default.from, to_default.to
                        ),
                        phase: None,
                    })
                } else {
                    Err(SubstrateError::FailedPrecondition {
                        message: format!(
                            "sandbox {sandbox_id} cannot drop retired checkpoint memory from {} \
                             to {} ({reason}); unretire with \
                             PUT /admin/api/v1/runtime-environments/{}/memory (action: \"unretire\") \
                             or drop/delete the sandbox",
                            to_default.from, to_default.to, to_default.from
                        ),
                    })
                }
            }
            Err(error) => Err(SubstrateError::Timeout {
                message: error.to_string(),
                phase: None,
            }),
        }
    }
}

/// No recorded execution attributes the sandbox to a catalog environment. The
/// node's execution policy still refuses a runtime that may not execute; this
/// only means the control plane can't move it first.
fn unattributed(sandbox_id: SandboxId, activation: UpgradeActivation) {
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
}

/// Count and log one decision.
fn record(sandbox_id: SandboxId, activation: UpgradeActivation, plan: RuntimePlan) {
    let to_default = match plan {
        RuntimePlan::AsRecorded => None,
        RuntimePlan::RestoreOnDefault(to_default)
        | RuntimePlan::DropMemoryAndColdBoot(to_default)
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
