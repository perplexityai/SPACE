//! Policy replacement receipts and live, execution-fenced enforcement reads.
use super::*;
use pplx_space_common_svc::types::ReconcileReason;
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, NetworkPolicyV1, network_policy_hash,
};
use pplx_space_sandbox_store::network_policy as store;
use serde::Serialize;
use sha2::{Digest, Sha256};

fn store_error(error: pplx_space_sandbox_store::SandboxStoreError) -> ControllerError {
    db_contention_error("network policy storage failed", &error.into())
}

pub(crate) fn desired_policy_target(sandbox: &Sandbox) -> Result<Option<String>, ControllerError> {
    let (policy, scope, owner, generation) = if let Some(policy) = &sandbox.network_policy {
        (
            &policy.0,
            EffectiveNetworkPolicyRefScope::Sandbox,
            sandbox.id.to_string(),
            sandbox.network_policy_generation,
        )
    } else if let Some(policy) = &sandbox.template_network_policy {
        (
            &policy.0,
            EffectiveNetworkPolicyRefScope::Template,
            sandbox
                .template_id
                .clone()
                .ok_or_else(|| ControllerError::internal("missing template identity"))?,
            sandbox
                .template_network_policy_generation
                .unwrap_or(sandbox.network_policy_generation),
        )
    } else {
        return Ok(None);
    };
    let generation = u64::try_from(generation)
        .map_err(|_| ControllerError::internal("invalid policy generation"))?;
    EffectiveNetworkPolicyRef::cluster(scope, &owner, generation, network_policy_hash(policy))
        .map(|target| Some(target.to_string()))
        .map_err(|_| ControllerError::internal("invalid policy reference"))
}

pub(crate) fn policy_revision(sandbox: &Sandbox) -> Result<String, ControllerError> {
    let mut hash = Sha256::new();
    hash.update(sandbox.id.to_string());
    hash.update(sandbox.network_policy_generation.to_be_bytes());
    hash.update(desired_policy_target(sandbox)?.unwrap_or_default());
    Ok(hex::encode(hash.finalize()))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyObservation {
    pub revision: String,
    pub desired_target: Option<String>,
    pub applied_target: Option<String>,
    pub state: PolicyEnforcementState,
    pub execution_generation: Option<i64>,
    pub gateway_instance: Option<String>,
    pub error: Option<&'static str>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEnforcementState {
    Pending,
    Applied,
    Failed,
}

impl SandboxController {
    pub async fn set_network_policy(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        patch: SandboxNetworkPolicyPatch,
        expected_generation: Option<u64>,
    ) -> Result<u64, ControllerError> {
        let policy_override = self.compose_network_policy(pool, sandbox, patch).await?;
        let inherited = policy_override.is_none();
        let mut tx = pool.begin().await.map_err(|err| {
            db_contention_error("failed to begin sandbox network policy update", &err)
        })?;
        let generation = self
            .write_network_policy(&mut tx, sandbox, policy_override, expected_generation)
            .await?;
        tx.commit()
            .await
            .map_err(|err| db_contention_error("failed to commit network policy", &err))?;
        Self::network_policy_committed(sandbox.id, generation, inherited);
        Ok(generation)
    }

    pub(super) fn network_policy_committed(id: SandboxId, generation: u64, inherited: bool) {
        SandboxEvent::new(
            id,
            SandboxEventType::NetworkPolicyUpdateCompleted,
            SandboxEventSource::Api,
        )
        .emit_with(
            SandboxEventDetails::default()
                .operation(if inherited {
                    "inherit_template_default"
                } else {
                    "set_override"
                })
                .network_policy_source(if inherited { "template" } else { "override" })
                .network_policy_generation(generation.to_string())
                .result("desired_updated"),
        );
    }

    pub(super) async fn compose_network_policy(
        &self,
        pool: impl sqlx::PgExecutor<'_>,
        sandbox: &Sandbox,
        patch: SandboxNetworkPolicyPatch,
    ) -> Result<Option<pplx_space_net_common::NetworkPolicyV1>, ControllerError> {
        let policy_override = match patch {
            SandboxNetworkPolicyPatch::Set { policy } => Some(policy),
            SandboxNetworkPolicyPatch::InheritTemplateDefault => None,
        };
        let profiles = crate::egress_profiles::load(pool, sandbox.id).await?;
        let policy_override = self.egress_profiles.resolve(&profiles)?.compose_override(
            policy_override,
            sandbox
                .template_network_policy
                .as_ref()
                .map(|policy| &policy.0),
        )?;
        let policy_override = crate::tls_interception_exemptions::compose(
            self.tls_interception_exemptions.as_ref(),
            policy_override,
            sandbox
                .template_network_policy
                .as_ref()
                .map(|policy| &policy.0),
        );
        Ok(policy_override)
    }

    pub(super) async fn write_network_policy(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox: &Sandbox,
        policy_override: Option<pplx_space_net_common::NetworkPolicyV1>,
        expected_generation: Option<u64>,
    ) -> Result<u64, ControllerError> {
        let operation = if policy_override.is_some() {
            "set_override"
        } else {
            "inherit_template_default"
        };
        let base_details = || {
            let mut details = SandboxEventDetails::default()
                .operation(operation)
                .network_policy_source(if policy_override.is_some() {
                    "override"
                } else {
                    "template"
                });
            if let Some(expected_generation) = expected_generation {
                details =
                    details.expected_network_policy_generation(expected_generation.to_string());
            }
            details
        };
        SandboxEvent::new(
            sandbox.id,
            SandboxEventType::NetworkPolicyUpdateRequested,
            SandboxEventSource::Api,
        )
        .emit_with(base_details());
        let policy_override_json = policy_override
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| {
                let error = format!("failed to serialize network policy: {error}");
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::NetworkPolicyUpdateFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(base_details().error(error.clone()));
                ControllerError::internal(error)
            })?;

        let generation = pplx_space_sandbox_store::network_policy::set(
            &mut **tx,
            sandbox.id,
            policy_override_json,
            expected_generation.map(|generation| generation as i64),
        )
        .await
        .map_err(sqlx::Error::from)
        .map_err(|err| {
            let error = "failed to update sandbox network policy";
            let mapped = db_contention_error(error, &err);
            // Same rule as the enqueue arm below: the transaction has written
            // nothing on a lost race and the caller retries, so a
            // `NetworkPolicyUpdateFailed` event would record a transient
            // collision as a failed update on the sandbox's timeline.
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::NetworkPolicyUpdateFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(base_details().error(error));
            }
            mapped
        })?;
        let Some(generation) = generation else {
            SandboxEvent::new(
                sandbox.id,
                SandboxEventType::NetworkPolicyUpdateFailed,
                SandboxEventSource::Api,
            )
            .emit_with(
                base_details()
                    .error("network policy generation conflict")
                    .routine(),
            );
            return Err(ControllerError::precondition_failed(
                "sandbox network_policy_generation does not match If-Match",
            ));
        };

        let refs = [ReconcileRef::network_policy(NetworkPolicyTarget::sandbox(
            sandbox.id,
        ))];
        enqueue_sandbox_reconcile_and_trigger_tx(
            tx,
            sandbox.id,
            &refs,
            ReconcileReason::SandboxNetworkPolicyUpdated,
        )
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %sandbox.id,
                %error,
                "failed to enqueue sandbox network policy reconcile"
            );
            // Classified like the `begin`, the `UPDATE`, and the `commit`
            // around it. This is an ordinary INSERT into a hot queue table, so
            // it takes a row lock and can lose the race like any other write —
            // and leaving it on a bare 500 made the *rest* of the
            // transaction's classification unreachable, which is the exact
            // failure `db_contention_error`'s own docs warn about.
            let mapped =
                db_contention_error("failed to enqueue sandbox network policy reconcile", &error);
            // No failure event on a lost race: nothing was written and the
            // caller retries, so `NetworkPolicyUpdateFailed` would misreport a
            // transient collision as a failed update. Matches the sibling
            // enqueue in `ports.rs`.
            if !mapped.is_lock_contention() {
                SandboxEvent::new(
                    sandbox.id,
                    SandboxEventType::NetworkPolicyUpdateFailed,
                    SandboxEventSource::Api,
                )
                .emit_with(base_details().error(error.to_string()).logged_by_caller());
            }
            mapped
        })?;
        Ok(u64::try_from(generation).unwrap_or(0))
    }

    pub async fn replace_network_policy(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        policy: NetworkPolicyV1,
        expected_revision: &str,
        idempotency_digest: &str,
        request_hash: &str,
    ) -> Result<String, ControllerError> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| db_contention_error("begin policy replacement", &err))?;
        if !store::lock(&mut *tx, sandbox_id)
            .await
            .map_err(store_error)?
        {
            return Err(ControllerError::not_found("sandbox"));
        }
        // The row lock serializes receipts and policy mutations. A replay is
        // resolved before checking today's revision, even after a later edit.
        if let Some(receipt) = store::receipt(&mut *tx, sandbox_id, idempotency_digest)
            .await
            .map_err(store_error)?
        {
            if receipt.request_hash != request_hash {
                return Err(ControllerError::conflict(
                    "idempotency key was used for a different policy request",
                ));
            }
            return Ok(receipt.target_ref);
        }
        let sandbox = fetch_sandbox(&mut *tx, sandbox_id)
            .await
            .map_err(|err| db_contention_error("read policy replacement target", &err))?
            .ok_or_else(|| ControllerError::not_found("sandbox"))?;
        if policy_revision(&sandbox)? != expected_revision {
            return Err(ControllerError::conflict("network policy revision changed"));
        }
        let policy = self
            .compose_network_policy(
                &mut *tx,
                &sandbox,
                SandboxNetworkPolicyPatch::Set { policy },
            )
            .await?;
        let generation = self
            .write_network_policy(
                &mut tx,
                &sandbox,
                policy.clone(),
                Some(
                    u64::try_from(sandbox.network_policy_generation)
                        .map_err(|_| ControllerError::internal("invalid policy generation"))?,
                ),
            )
            .await?;
        let policy = policy
            .ok_or_else(|| ControllerError::internal("replacement requires an explicit policy"))?;
        let target = EffectiveNetworkPolicyRef::cluster(
            EffectiveNetworkPolicyRefScope::Sandbox,
            sandbox_id.to_string(),
            generation,
            network_policy_hash(&policy),
        )
        .map_err(|_| ControllerError::internal("invalid replacement policy reference"))?
        .to_string();
        store::record_receipt(
            &mut *tx,
            sandbox_id,
            idempotency_digest,
            &store::Receipt {
                request_hash: request_hash.to_owned(),
                target_ref: target.clone(),
            },
        )
        .await
        .map_err(store_error)?;
        tx.commit()
            .await
            .map_err(|err| db_contention_error("commit policy replacement", &err))?;
        Self::network_policy_committed(sandbox_id, generation, false);
        Ok(target)
    }

    pub async fn observe_network_policy(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
    ) -> Result<NetworkPolicyObservation, ControllerError> {
        let mut result = NetworkPolicyObservation {
            revision: policy_revision(sandbox)?,
            desired_target: desired_policy_target(sandbox)?,
            applied_target: None,
            state: PolicyEnforcementState::Pending,
            execution_generation: sandbox.active_execution_generation,
            gateway_instance: None,
            error: None,
        };
        if sandbox
            .lifecycle
            .parse::<pplx_space_common_core::types::SandboxLifecycle>()
            .ok()
            != Some(pplx_space_common_core::types::SandboxLifecycle::Started)
        {
            return Ok(result);
        }
        let Some(execution) = sandbox.active_execution_generation else {
            return Ok(result);
        };
        // Bound discovery plus RPC; this read never wakes or repairs a sandbox.
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            let mut client = self
                .reachable_runtime_client_or_unavailable(sandbox.id)
                .await
                .ok()?;
            client
                .sandbox_network_policy(pplx_space_sbx_mgr_client::proto::GetNetworkPolicyRequest {
                    sandbox_ids: vec![sandbox.id.to_string()],
                    observe_gateway: true,
                })
                .await
                .ok()?
                .into_inner()
                .policies
                .into_iter()
                .find(|detail| detail.sandbox_id == sandbox.id.to_string())?
                .observation
        })
        .await
        .ok()
        .flatten();
        let Some(observed) = observed else {
            return Ok(result);
        };
        // Placement can change while the old node is answering. Re-read the
        // authoritative execution and desired target after the network hop.
        let current = fetch_sandbox(pool, sandbox.id)
            .await
            .map_err(|err| db_contention_error("read policy observation fence", &err))?
            .ok_or_else(|| ControllerError::not_found("sandbox"))?;
        if current.active_execution_generation != Some(execution)
            || current.active_node_id != sandbox.active_node_id
            || current.active_activation_attempt_id != sandbox.active_activation_attempt_id
            || current.lifecycle != sandbox.lifecycle
            || policy_revision(&current)? != result.revision
            || observed.execution_generation != execution
            || observed.gateway_instance.is_empty()
        {
            return Ok(result);
        }
        result.gateway_instance = Some(observed.gateway_instance);
        if observed.ready && observed.pending_tcp_revocations == 0 {
            result.applied_target = Some(observed.installed_policy_ref);
            if result.applied_target == result.desired_target {
                result.state = PolicyEnforcementState::Applied;
            }
        } else if !observed.error.is_empty() {
            result.state = PolicyEnforcementState::Failed;
            result.error = Some("gateway policy is not ready");
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
