use super::*;
use crate::controllers::db_contention_error;
use crate::controllers::internal_error;
use crate::controllers::substrate::RuntimeRpcError;
use crate::controllers::substrate::{
    CreateExecutionOutcome, CreateRollbackExecutionFence, SubstrateAdapter,
};
use crate::public_operation_driver::{
    OperationExecutionConfig, OperationExecutor, drive_claimed_operation, execution_span,
};
use crate::public_operations::{
    ApiProduct, ClaimedPublicOperation, CreatePublicOperation, CreatePublicOperationResult,
    PublicOperationStore, claim_created_operation_tx, create_operation_tx,
};
use crate::public_sandbox_operations::{
    SandboxLifecycleOperationExecutor, SandboxLifecycleOperationPayload,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, OrgId, SandboxId};
use pplx_space_common_svc::types::ReconcileReason;
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};
use pplx_space_public_api::{
    OperationKind, OperationTargetType, PublicSandboxId,
    SandboxLifecycleAction as PublicSandboxLifecycleAction,
};
use pplx_space_sandbox_store::{self as sandbox_store, FieldPatch};
use pplx_space_sbx_mgr_client::proto::access_idle_update::Operation as AccessIdleOperation;
use tracing::Instrument as _;

/// `UpdateSandboxRequest` has no spelling for "clear this override and
/// inherit the template default", so an absent field means *unchanged*.
/// `FieldPatch::Clear` becomes reachable when the request type grows an
/// `InheritTemplateDefault` arm for resources, as the network-policy and
/// auto-lifecycle patches already have.
fn to_field_patch(value: Option<u32>) -> FieldPatch<i32> {
    value.map_or(FieldPatch::Unchanged, |value| FieldPatch::Set(value as i32))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AutoLifecycleTouchOutcome {
    Applied,
    AlreadyCurrent,
    Disabled,
    NotStarted,
    RuntimeUnreachable,
    /// The runtime endpoint existed, but the touch RPC hit a temporary
    /// transport failure. Callers must fail fast rather than turning an
    /// upstream outage into a new wake/restore attempt.
    TransportUnavailable,
}

pub(super) fn touch_auto_lifecycle_response_outcome(
    response: Result<tonic::Response<TouchAutoLifecycleResponse>, tonic::Status>,
) -> Result<AutoLifecycleTouchOutcome, ControllerError> {
    match response {
        Ok(response) => classify_auto_lifecycle_touch_response(response.into_inner()),
        Err(status) if pplx_space_common_grpc::is_temporary_tcp_failure(&status) => {
            Ok(AutoLifecycleTouchOutcome::TransportUnavailable)
        }
        Err(status) => Err(status.into()),
    }
}

fn classify_auto_lifecycle_touch_response(
    response: TouchAutoLifecycleResponse,
) -> Result<AutoLifecycleTouchOutcome, ControllerError> {
    match AutoLifecycleTouchResult::try_from(response.result) {
        Ok(AutoLifecycleTouchResult::Applied) => Ok(AutoLifecycleTouchOutcome::Applied),
        Ok(AutoLifecycleTouchResult::AlreadyCurrent) => {
            Ok(AutoLifecycleTouchOutcome::AlreadyCurrent)
        }
        Ok(AutoLifecycleTouchResult::Disabled) => Ok(AutoLifecycleTouchOutcome::Disabled),
        Ok(AutoLifecycleTouchResult::NotStarted) => Ok(AutoLifecycleTouchOutcome::NotStarted),
        Ok(AutoLifecycleTouchResult::Unspecified) | Err(_) => {
            Err(ControllerError::internal(format!(
                "sandbox manager returned an unknown auto-lifecycle touch result {}",
                response.result
            )))
        }
    }
}

pub(super) fn observed_access_idle_generation(
    sandbox_id: SandboxId,
    response: pplx_space_sbx_mgr_client::proto::StateResponse,
) -> Result<ExecutionGeneration, ControllerError> {
    let unavailable = || {
        ControllerError::service_unavailable("sandbox execution is not ready")
            .with_retry_after_header(Duration::from_secs(1))
    };
    let [observed] = response.sandboxes.as_slice() else {
        return Err(unavailable());
    };
    if observed.sandbox_id != sandbox_id.to_string()
        || !response.missing_ids.is_empty()
        || observed.state_degraded
        || observed.observed_lifecycle().ok()
            != Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started)
    {
        return Err(unavailable());
    }
    ExecutionGeneration::new(observed.execution_generation.ok_or_else(unavailable)?)
        .map_err(|_| unavailable())
}

#[derive(Debug)]
pub(crate) enum AccessIdleTimeoutOutcome {
    StoredOnly,
    Applied,
}

enum DeadlineExecutionReadiness {
    Ready,
    Pending,
}

fn classify_deadline_execution(
    sandbox_id: SandboxId,
    generation: i64,
    response: pplx_space_sbx_mgr_client::proto::StateResponse,
) -> Result<DeadlineExecutionReadiness, ControllerError> {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    let unavailable = || ControllerError::service_unavailable("sandbox execution is not ready");
    let [observed] = response.sandboxes.as_slice() else {
        return if response.sandboxes.is_empty() && response.missing_ids == [sandbox_id.to_string()]
        {
            Ok(DeadlineExecutionReadiness::Pending)
        } else {
            Err(unavailable())
        };
    };
    // Published ownership is already our fence. Older State producers may
    // omit the generation; only unpublished identity requires discovering it.
    if observed.sandbox_id != sandbox_id.to_string()
        || observed.state_degraded
        || !response.missing_ids.is_empty()
        || observed
            .execution_generation
            .is_some_and(|observed| observed != generation)
    {
        return Err(tonic::Status::failed_precondition("access deadline execution changed").into());
    }
    match observed.observed_lifecycle().ok() {
        Some(SandboxLifecycleState::Started) => Ok(DeadlineExecutionReadiness::Ready),
        Some(
            SandboxLifecycleState::Creating
            | SandboxLifecycleState::Created
            | SandboxLifecycleState::Starting
            | SandboxLifecycleState::Restoring
            | SandboxLifecycleState::Resuming,
        ) => Ok(DeadlineExecutionReadiness::Pending),
        _ => Err(unavailable()),
    }
}

/// Where a failed deadline RPC leaves the worker's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AccessIdleSendFailure {
    /// The dial failed locally, so no request bytes reached the worker.
    Untransmitted,
    /// The transport broke after the worker may have applied the update.
    Ambiguous,
    /// The worker answered; its status is the outcome.
    Rejected,
}

/// Uses the plain touch path's temporary-transport predicate so both access
/// paths agree on which failures are outages rather than worker verdicts.
pub(super) fn classify_access_idle_send_failure(status: &tonic::Status) -> AccessIdleSendFailure {
    if pplx_space_common_grpc::is_client_connect_failure(status) {
        AccessIdleSendFailure::Untransmitted
    } else if pplx_space_common_grpc::is_temporary_tcp_failure(status) {
        AccessIdleSendFailure::Ambiguous
    } else {
        AccessIdleSendFailure::Rejected
    }
}

pub(super) fn unconfirmed_access_idle_update() -> ControllerError {
    ControllerError::service_unavailable("sandbox deadline update was not confirmed")
        .with_retry_after_header(Duration::from_secs(1))
}

/// Whether a State read proves an ambiguous update's effect on the fenced
/// execution. An extension is a monotonic floor, so it is proven once no
/// auto-lifecycle event is due before that floor, whoever moved it there.
/// A reset is never proven: a restarted idle clock looks the same as an
/// earlier reset or later activity.
pub(super) fn access_idle_update_observed(
    sandbox_id: SandboxId,
    generation: i64,
    operation: &AccessIdleOperation,
    response: pplx_space_sbx_mgr_client::proto::StateResponse,
) -> Result<bool, ControllerError> {
    // Unlike activation readiness, proof needs the generation itself:
    // a replacement's deadline says nothing about this execution's update.
    let next_auto_event_at = response
        .sandboxes
        .first()
        .filter(|observed| observed.execution_generation == Some(generation))
        .and_then(|observed| observed.auto_lifecycle_status.as_ref())
        .and_then(|status| status.next_auto_event_at);
    match classify_deadline_execution(sandbox_id, generation, response)? {
        DeadlineExecutionReadiness::Pending => Ok(false),
        DeadlineExecutionReadiness::Ready => Ok(match operation {
            // The worker keeps deadlines at whole-second precision.
            AccessIdleOperation::ExtendUntil(until) => {
                next_auto_event_at.is_some_and(|next| next.seconds >= until.seconds)
            }
            AccessIdleOperation::ResetPolicy(_) => false,
        }),
    }
}

fn access_idle_deadline_request(
    sandbox_id: SandboxId,
    generation: i64,
    observed_at: prost_types::Timestamp,
    deadline: tokio::time::Instant,
    operation: AccessIdleOperation,
) -> Result<tonic::Request<TouchAutoLifecycleRequest>, ControllerError> {
    // Observation consumes the same operation budget as the mutation.
    let budget = deadline.saturating_duration_since(tokio::time::Instant::now());
    if budget.is_zero() {
        return Err(ControllerError::service_unavailable(
            "sandbox deadline update expired",
        ));
    }
    let expires = jiff::Timestamp::now() + jiff::SignedDuration::from_secs(budget.as_secs() as i64);
    let expires_at = prost_types::Timestamp {
        seconds: expires.as_second(),
        nanos: expires.subsec_nanosecond(),
    };
    let mut request = tonic::Request::new(TouchAutoLifecycleRequest {
        sandbox_id: sandbox_id.to_string(),
        source: pplx_space_sbx_mgr_client::proto::AutoLifecycleActivitySource::Access as i32,
        observed_at: Some(observed_at),
        access_idle_update: Some(pplx_space_sbx_mgr_client::proto::AccessIdleUpdate {
            execution_generation: generation,
            expires_at: Some(expires_at),
            operation: Some(operation),
        }),
    });
    request.set_timeout(budget);
    Ok(request)
}

impl SandboxController {
    pub async fn update(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        id: SandboxId,
        req: UpdateSandboxRequest,
        expected_network_policy_generation: Option<u64>,
    ) -> Result<Option<u64>, ControllerError> {
        let mut network_policy_generation = None;
        if let Some(metadata) = req.metadata {
            sandbox_store::update_desired_state(
                pool,
                id,
                sandbox_store::SandboxDesiredStatePatch {
                    metadata: Some(&metadata),
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| {
                // `SandboxStoreError` wraps one transparent `sqlx::Error`, so
                // the classification survives the store boundary.
                db_contention_error("failed to update sandbox metadata", &sqlx::Error::from(err))
            })?;
        }

        if req.vcpu.is_some() || req.memory_mib.is_some() || req.pids_max.is_some() {
            let mut client = self
                .control_runtime_client(pool, sandbox, super::lifecycle::SandboxAction::Update)
                .await?;
            client
                .update_sandbox(UpdateRequest {
                    sandbox_id: id.to_string(),
                    vcpu: req.vcpu,
                    memory_mib: req.memory_mib,
                    pids_max: req.pids_max,
                    expected_current_owner: None,
                    owner: None,
                    process_env: Default::default(),
                    replace_process_env: false,
                })
                .await?;

            // vcpu on the row is the activation allocation. A live quota
            // reduction must not replace it and cap every later activation.
            // CPU quota and pids_max updates apply only to this execution.
            if req.memory_mib.is_some() {
                sandbox_store::update_desired_state(
                    pool,
                    id,
                    sandbox_store::SandboxDesiredStatePatch {
                        memory_mib: to_field_patch(req.memory_mib),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|err| {
                    db_contention_error(
                        "failed to update sandbox resources",
                        &sqlx::Error::from(err),
                    )
                })?;
            }
        }

        if let Some(patch) = req.auto_lifecycle {
            let policy = match patch {
                SandboxAutoLifecyclePatch::Set { policy } => policy,
                SandboxAutoLifecyclePatch::InheritTemplateDefault => {
                    let template = sandbox.template_id.as_deref().ok_or_else(|| {
                        ControllerError::bad_request(
                            "cannot inherit template auto lifecycle: sandbox has no template",
                        )
                    })?;
                    self.template_default_auto_lifecycle(
                        pool,
                        template,
                        sandbox.content_hash.as_deref(),
                        RuntimeKind::Firecracker,
                        sandbox.org_id,
                    )
                    .await?
                }
            };
            self.set_auto_lifecycle_policy(pool, sandbox, policy)
                .await?;
        }

        if let Some(patch) = req.network_policy {
            network_policy_generation = Some(
                self.set_network_policy(pool, sandbox, patch, expected_network_policy_generation)
                    .await?,
            );
        }

        Ok(network_policy_generation)
    }

    pub async fn set_network_policy(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        patch: SandboxNetworkPolicyPatch,
        expected_generation: Option<u64>,
    ) -> Result<u64, ControllerError> {
        let policy_override = match patch {
            SandboxNetworkPolicyPatch::Set { policy } => Some(policy),
            SandboxNetworkPolicyPatch::InheritTemplateDefault => None,
        };
        let operation = if policy_override.is_some() {
            "set_override"
        } else {
            "inherit_template_default"
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

        let mut tx = pool.begin().await.map_err(|err| {
            db_contention_error("failed to begin sandbox network policy update", &err)
        })?;
        let generation = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE sandboxes
             SET network_policy_override_json = $1,
                 network_policy_generation =
                     CASE
                         WHEN network_policy_override_json IS DISTINCT FROM $1
                         THEN network_policy_generation + 1
                         ELSE network_policy_generation
                     END
             WHERE id = $2
               AND ($3::BIGINT IS NULL OR network_policy_generation = $3)
             RETURNING network_policy_generation"
        ))
        .bind(policy_override_json)
        .bind(sandbox.id)
        .bind(expected_generation.map(|generation| generation as i64))
        .fetch_optional(&mut *tx)
        .await
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
            &mut tx,
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
        tx.commit().await.map_err(|error| {
            SandboxEvent::new(
                sandbox.id,
                SandboxEventType::NetworkPolicyUpdateFailed,
                SandboxEventSource::Api,
            )
            .emit_with(base_details().error(error.to_string()));
            db_contention_error("failed to commit sandbox network policy", &error)
        })?;
        SandboxEvent::new(
            sandbox.id,
            SandboxEventType::NetworkPolicyUpdateCompleted,
            SandboxEventSource::Api,
        )
        .emit_with(
            base_details()
                .network_policy_generation(generation.to_string())
                .result("desired_updated"),
        );

        Ok(u64::try_from(generation).unwrap_or(0))
    }

    pub async fn set_snapshots_enabled_desired(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        enabled: bool,
    ) -> Result<serde_json::Value, ControllerError> {
        let mut tx = pool.begin().await.map_err(|err| {
            db_contention_error("failed to begin sandbox snapshot intent update", &err)
        })?;
        sqlx::query(sql!(
            "UPDATE sandboxes
             SET snapshots_enabled_desired = $1
             WHERE id = $2"
        ))
        .bind(enabled)
        .bind(sandbox.id)
        .execute(&mut *tx)
        .await
        .map_err(|err| db_contention_error("failed to update sandbox snapshot intent", &err))?;

        let refs = [ReconcileRef::sandbox_desired_state(sandbox.id)];
        let reconcile_queued = enqueue_sandbox_reconcile_and_trigger_tx(
            &mut tx,
            sandbox.id,
            &refs,
            ReconcileReason::SandboxDesiredStateUpdated,
        )
        .await
        .map_err(|error| {
            db_contention_error("failed to enqueue sandbox snapshot reconciliation", &error)
        })?
        .is_some();
        tx.commit().await.map_err(|error| {
            db_contention_error("failed to commit sandbox snapshot intent", &error)
        })?;
        Ok(serde_json::json!({
            "ok": true,
            "action": if enabled { "enable_snapshots" } else { "disable_snapshots" },
            "sandbox_id": sandbox.id.to_string(),
            "snapshots_enabled_desired": enabled,
            "reconcile_queued": reconcile_queued,
            "reconcile_error": null,
        }))
    }

    pub async fn set_auto_lifecycle_policy(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        mut policy: AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        policy.normalize_in_place();
        validate_v1_policy(&policy).map_err(ControllerError::bad_request)?;

        let mut tx = pool.begin().await.map_err(|err| {
            db_contention_error("failed to begin sandbox auto lifecycle update", &err)
        })?;
        Self::persist_auto_lifecycle_policy(&mut tx, sandbox.id, &policy).await?;
        tx.commit().await.map_err(|error| {
            db_contention_error("failed to commit sandbox auto lifecycle", &error)
        })?;

        Ok(())
    }

    async fn persist_auto_lifecycle_policy(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sandbox_id: SandboxId,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        sandbox_store::set_auto_lifecycle_policy(&mut **tx, sandbox_id, policy)
            .await
            .map_err(|error| {
                db_contention_error("failed to update sandbox auto lifecycle", &error.into())
            })?;

        let refs = [ReconcileRef::sandbox_desired_state(sandbox_id)];
        enqueue_sandbox_reconcile_and_trigger_tx(
            tx,
            sandbox_id,
            &refs,
            ReconcileReason::SandboxDesiredStateUpdated,
        )
        .await
        .map_err(|error| {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %error,
                "failed to enqueue sandbox auto lifecycle reconcile"
            );
            db_contention_error("failed to enqueue sandbox auto lifecycle reconcile", &error)
        })?;
        Ok(())
    }

    /// Persist timeout intent while serializing its absence decision with
    /// execution acquisition. The request budget includes the transaction and
    /// any observation of an already-owned activation; this never starts one.
    pub(crate) async fn set_access_idle_timeout(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        timeout_secs: u32,
        observed_at: prost_types::Timestamp,
        deadline: tokio::time::Instant,
        expected_policy: Option<&AutoLifecyclePolicy>,
    ) -> Result<AccessIdleTimeoutOutcome, ControllerError> {
        tokio::time::timeout_at(deadline, async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|error| db_contention_error("begin timeout intent", &error))?;
            // Lock separately from the joined read: after waiting for an
            // activation's row lock, READ COMMITTED must see its new claim.
            let locked = sqlx::query_scalar::<_, SandboxId>(sql!(
                "SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"
            ))
            .bind(sandbox.id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| db_contention_error("lock timeout intent", &error))?;
            if locked.is_none() {
                return Err(ControllerError::not_found("sandbox no longer exists"));
            }
            let execution = sandbox_store::auto_lifecycle_execution(&mut *tx, sandbox.id)
                .await
                .map_err(|error| db_contention_error("read timeout ownership", &error.into()))?
                .ok_or_else(|| ControllerError::not_found("sandbox no longer exists"))?;
            let lifecycle = execution
                .lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|error| {
                    ControllerError::internal(format!("invalid stored sandbox lifecycle: {error}"))
                })?;
            let needs_runtime = match lifecycle {
                SandboxLifecycle::Created
                | SandboxLifecycle::Paused
                | SandboxLifecycle::Suspended => false,
                SandboxLifecycle::Started => !execution.proves_no_execution(),
                SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Suspending
                | SandboxLifecycle::Checkpointing => true,
                SandboxLifecycle::Stopping
                | SandboxLifecycle::Stopped
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed => {
                    return Err(ControllerError::not_found("sandbox is terminal"));
                }
            };
            let current_policy = execution.policy.0.normalized();
            if expected_policy
                .is_some_and(|expected| expected.clone().normalized() != current_policy)
            {
                return Err(
                    tonic::Status::failed_precondition("sandbox lifecycle policy changed").into(),
                );
            }
            let policy =
                crate::e2b::policy_for_timeout_update(&current_policy, timeout_secs).normalized();
            if policy != current_policy {
                Self::persist_auto_lifecycle_policy(&mut tx, sandbox.id, &policy).await?;
            }
            tx.commit()
                .await
                .map_err(|error| db_contention_error("commit timeout intent", &error))?;
            if !needs_runtime {
                return Ok(AccessIdleTimeoutOutcome::StoredOnly);
            }
            // Only an identified execution is eligible for bounded readiness
            // observation. Missing identity alone cannot authorize a new wake.
            let (client, generation) = match execution.execution_generation {
                Some(generation) => (
                    self.observe_deadline_execution(pool, sandbox.id, generation, deadline)
                        .await?,
                    generation,
                ),
                None => {
                    self.resolve_access_idle_execution(sandbox.id, None, deadline)
                        .await?
                }
            };
            self.send_access_idle_deadline(
                client,
                sandbox.id,
                generation,
                observed_at,
                deadline,
                AccessIdleOperation::ResetPolicy(
                    crate::controllers::auto_lifecycle::policy_to_proto(&policy),
                ),
            )
            .await?;
            Ok(AccessIdleTimeoutOutcome::Applied)
        })
        .await
        .map_err(|_| ControllerError::service_unavailable("sandbox deadline update expired"))?
    }

    pub(crate) async fn update_access_idle_deadline(
        &self,
        sandbox: &Sandbox,
        observed_at: prost_types::Timestamp,
        deadline: tokio::time::Instant,
        operation: AccessIdleOperation,
    ) -> Result<(), ControllerError> {
        tokio::time::timeout_at(deadline, async {
            let (client, generation) = self
                .resolve_access_idle_execution(
                    sandbox.id,
                    sandbox.active_execution_generation,
                    deadline,
                )
                .await?;
            self.send_access_idle_deadline(
                client,
                sandbox.id,
                generation,
                observed_at,
                deadline,
                operation,
            )
            .await
        })
        .await
        .map_err(|_| ControllerError::service_unavailable("sandbox deadline update expired"))?
    }

    async fn resolve_access_idle_execution(
        &self,
        sandbox_id: SandboxId,
        generation: Option<i64>,
        deadline: tokio::time::Instant,
    ) -> Result<(RuntimeClient, i64), ControllerError> {
        let mut client = self
            .reachable_runtime_client_or_unavailable(sandbox_id)
            .await?;
        let generation = match generation {
            Some(generation) => generation,
            None => {
                // Missing published identity does not prove the runtime is absent.
                // Read its current generation; the mutation's generation fence
                // rejects a replacement between this read and the write.
                let request = tonic::Request::new(pplx_space_sbx_mgr_client::proto::StateRequest {
                    sandbox_ids: vec![sandbox_id.to_string()],
                });
                let response = client
                    .call(
                        &self.substrate,
                        sandbox_id,
                        deadline,
                        request,
                        |mut client, request| async move { client.sandbox_state(request).await },
                    )
                    .await?;
                observed_access_idle_generation(sandbox_id, response.into_inner())?.get()
            }
        };
        Ok((client, generation))
    }

    async fn send_access_idle_deadline(
        &self,
        mut client: RuntimeClient,
        sandbox_id: SandboxId,
        generation: i64,
        observed_at: prost_types::Timestamp,
        deadline: tokio::time::Instant,
        operation: AccessIdleOperation,
    ) -> Result<(), ControllerError> {
        let request = access_idle_deadline_request(
            sandbox_id,
            generation,
            observed_at,
            deadline,
            operation.clone(),
        )?;
        let response =
            client
                .call(
                    &self.substrate,
                    sandbox_id,
                    deadline,
                    request,
                    |mut client, request| async move {
                        client.touch_sandbox_auto_lifecycle(request).await
                    },
                )
                .await;
        let status = match response {
            Ok(response) => return require_access_idle_update_acknowledgement(response),
            Err(RuntimeRpcError::Rpc(status)) => status,
            Err(RuntimeRpcError::Client(error)) => return Err(error),
        };
        match classify_access_idle_send_failure(&status) {
            AccessIdleSendFailure::Rejected => Err(status.into()),
            AccessIdleSendFailure::Untransmitted => Err(unconfirmed_access_idle_update()),
            AccessIdleSendFailure::Ambiguous => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %status,
                    "access deadline update transport failed mid-request; observing its outcome"
                );
                if self
                    .observe_access_idle_update(sandbox_id, generation, &operation, deadline)
                    .await?
                {
                    Ok(())
                } else {
                    Err(unconfirmed_access_idle_update())
                }
            }
        }
    }

    /// The update may already have applied, so this only reads: resending a
    /// ResetPolicy would restart its idle clock. One observation, not a wait:
    /// a worker that is still restarting leaves the outcome unconfirmed.
    async fn observe_access_idle_update(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        operation: &AccessIdleOperation,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ControllerError> {
        let mut client = match self.substrate.probe_runtime_client(sandbox_id).await {
            Ok(Some(client)) => client,
            Ok(None) => return Ok(false),
            Err(error) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    %error,
                    "could not reach runtime to observe access deadline update"
                );
                return Ok(false);
            }
        };
        let request = tonic::Request::new(pplx_space_sbx_mgr_client::proto::StateRequest {
            sandbox_ids: vec![sandbox_id.to_string()],
        });
        match client
            .call(
                &self.substrate,
                sandbox_id,
                deadline,
                request,
                |mut client, request| async move { client.sandbox_state(request).await },
            )
            .await
        {
            Ok(response) => access_idle_update_observed(
                sandbox_id,
                generation,
                operation,
                response.into_inner(),
            ),
            Err(status) => {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    error = %status,
                    "could not observe access deadline update outcome"
                );
                Ok(false)
            }
        }
    }

    /// Observe only: retrying a possibly applied ResetPolicy would start its
    /// clock again. Reuse the resolved channel, and never retarget a replacement.
    async fn observe_deadline_execution(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        generation: i64,
        deadline: tokio::time::Instant,
    ) -> Result<RuntimeClient, ControllerError> {
        use pplx_space_sbx_mgr_client::proto::StateRequest;
        let mut client: Option<RuntimeClient> = None;
        loop {
            let current = sandbox_store::auto_lifecycle_execution(pool, sandbox_id)
                .await
                .map_err(|error| db_contention_error("observe timeout ownership", &error.into()))?
                .ok_or_else(|| ControllerError::not_found("sandbox no longer exists"))?;
            if current.execution_generation != Some(generation) {
                return Err(tonic::Status::failed_precondition(
                    "access deadline execution changed",
                )
                .into());
            }
            if client.is_none() {
                client = self.substrate.probe_runtime_client(sandbox_id).await?;
            }
            if let Some(runtime) = client.as_mut() {
                let mut request = tonic::Request::new(StateRequest {
                    sandbox_ids: vec![sandbox_id.to_string()],
                });
                request
                    .set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
                match runtime.sandbox_state(request).await {
                    Ok(response) => match classify_deadline_execution(
                        sandbox_id,
                        generation,
                        response.into_inner(),
                    )? {
                        DeadlineExecutionReadiness::Ready => {
                            return Ok(client.expect("resolved runtime"));
                        }
                        DeadlineExecutionReadiness::Pending => {}
                    },
                    Err(status)
                        if status.code() == tonic::Code::NotFound
                            || pplx_space_common_grpc::is_temporary_tcp_failure(&status) => {}
                    Err(status) => return Err(status.into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub(crate) async fn touch_auto_lifecycle_access_if_started(
        &self,
        sandbox: &Sandbox,
        observed_at: prost_types::Timestamp,
    ) -> Result<AutoLifecycleTouchOutcome, ControllerError> {
        let sandbox_id = sandbox.id;
        let Some(mut client) = self.substrate.reachable_runtime_client(sandbox_id).await? else {
            return Ok(AutoLifecycleTouchOutcome::RuntimeUnreachable);
        };

        let response = client
            .touch_sandbox_auto_lifecycle(TouchAutoLifecycleRequest {
                sandbox_id: sandbox.id.to_string(),
                source: pplx_space_sbx_mgr_client::proto::AutoLifecycleActivitySource::Access
                    as i32,
                observed_at: Some(observed_at),
                access_idle_update: None,
            })
            .await;
        touch_auto_lifecycle_response_outcome(response)
    }

    pub async fn list_snapshots(
        &self,
        _pool: &PgPool,
        sandbox: &Sandbox,
        id: SandboxId,
    ) -> Result<Vec<serde_json::Value>, ControllerError> {
        // Listing snapshots is a read — it must never recreate a pod, so use the
        // non-recovering client instead of `runtime_client`.
        let mut client = self.reachable_runtime_client_or_unavailable(id).await?;
        let _ = sandbox;
        let sandbox_id = id.to_string();
        let response = client
            .list_snapshots(ListSnapshotsRequest {
                sandbox_ids: vec![sandbox_id],
            })
            .await?
            .into_inner();
        Ok(snapshots_response_json(response))
    }

    pub async fn rollback(
        &self,
        pool: &PgPool,
        sandbox: &Sandbox,
        id: SandboxId,
        snapshot_name: String,
        auto_restart: bool,
    ) -> Result<serde_json::Value, ControllerError> {
        // Rollback is destructive and runs through `runtime_client`, which on
        // k8s recovers a missing pod for any recoverable-live lifecycle and
        // re-pins the new pod to `target_node_id`. Gate before the substrate
        // call so a rollback can't sneak past the drain reconciler.
        super::lifecycle::ensure_access_path_not_blocked_by_drain(
            pool,
            sandbox,
            super::lifecycle::SandboxAction::Rollback,
        )
        .await?;
        // Rollback is a recovery-capable data-plane op: if the pod is gone it is
        // recreated, so charge the cluster limiter for that recreation.
        let mut client = self
            .substrate
            .runtime_client(
                id,
                crate::controllers::substrate::RecoveryCharge::enforce(
                    self.cluster_admission.as_ref(),
                ),
            )
            .await?;
        let _ = sandbox;
        let route_identity = if auto_restart {
            self.substrate.reserve_rollback_route_identity(id).await?
        } else {
            None
        };
        let has_route_identity = route_identity.is_some();
        let result = client
            .rollback_sandbox(SbxRollbackRequest {
                sandbox_id: id.to_string(),
                snapshot_name,
                auto_restart,
                netns_path: None,
                route_identity,
            })
            .await?
            .into_inner();

        if has_route_identity {
            self.substrate
                .persist_rollback_route_identity(id, result.route_identity.clone())
                .await
                .map_err(ControllerError::from)?;
        }

        Ok(serde_json::json!({
            "sandbox_id": result.sandbox_id,
            "rolled_back_to": result.rolled_back_to,
        }))
    }

    pub(super) async fn rollback_created_sandbox(
        &self,
        pool: &PgPool,
        outcome: &CreateExecutionOutcome,
    ) {
        // Persist delete intent before touching the substrate. Direct VMs have
        // no orphan-runtime reconciler, and the Kubernetes orphan cleanup
        // preserves a `creating` execution while its activation lease is live or
        // it was updated within the create-age threshold. The compensation
        // operation is therefore the durable retry owner after an RPC error,
        // request cancellation, or process death.
        //
        // The operation is queued already claimed by this request, which runs
        // the first attempt itself through the driver and executor the
        // dispatcher uses. So the teardown doesn't wait for a dispatcher, and
        // it still runs where none is consuming operations
        // (`SPACE_PUBLIC_OPERATION_DISPATCHER_ENABLED=false`). A failed attempt
        // is released for the dispatcher to retry, and an attempt whose process
        // dies leaves its claim to go stale for the dispatcher to reclaim. With
        // no dispatcher, only the inline attempt runs.
        let sandbox_id = outcome.sandbox_id;
        let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id.as_uuid());
        let target_id = public_sandbox_id.to_string();
        let queued = async {
            let mut tx = pool.begin().await?;
            let Some((org_id, persisted_fence)) =
                claim_created_sandbox_for_compensation(&mut tx, outcome).await?
            else {
                return Ok(None);
            };
            let created = create_operation_tx(
                &mut tx,
                CreatePublicOperation {
                    org_id: OrgId::from_uuid(org_id),
                    actor_user_id: None,
                    // `native`: what keeps this away from a controller that
                    // predates it is the kind, because claims select by kind. A
                    // new product would not: a pre-upgrade claim query has no
                    // product filter and aborts its batch on a value it cannot
                    // decode.
                    api_product: ApiProduct::Native,
                    kind: OperationKind::CompensateCreateSandbox,
                    target_type: Some(OperationTargetType::Sandbox),
                    target_id: Some(&target_id),
                    payload_json: SandboxLifecycleOperationPayload {
                        sandbox_id: public_sandbox_id,
                        action: PublicSandboxLifecycleAction::Delete,
                        rollback: None,
                        create_rollback_fence: Some(persisted_fence),
                    }
                    .into_json(),
                    idempotency: None,
                    traceparent: crate::public_operations::request_traceparent(),
                },
            )
            .await?;
            let claimed = match created {
                CreatePublicOperationResult::Created(operation_id) => {
                    Some(claim_created_operation_tx(&mut tx, operation_id).await?)
                }
                other => {
                    tracing::error!(
                        sandbox_id = %sandbox_id,
                        outcome = ?other,
                        "durable create compensation returned an impossible operation result"
                    );
                    None
                }
            };
            tx.commit().await?;
            Ok::<_, crate::public_operations::PublicOperationStoreError>(Some(claimed))
        }
        .await;
        match queued {
            Ok(Some(Some((claim_token, operation)))) => {
                pplx_space_metrics::operations::created(
                    ApiProduct::Native.as_str(),
                    OperationKind::CompensateCreateSandbox.as_str(),
                    OperationTargetType::Sandbox.as_str(),
                );
                pplx_space_metrics::operations::claimed(
                    ApiProduct::Native.as_str(),
                    OperationKind::CompensateCreateSandbox.as_str(),
                );
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    operation_id = %operation.id,
                    "queued durable create compensation; running its first attempt inline"
                );
                self.run_claimed_compensation(pool, claim_token, operation)
                    .await;
            }
            Ok(Some(None) | None) => {}
            Err(err) => {
                tracing::error!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to queue durable create compensation; falling back \
                     to an immediate fenced teardown"
                );
                // **Never return ownership-free.** By here `create_execution` has
                // succeeded, so a guest is running. In the common failure the
                // queueing transaction never committed, so the `deleting` write
                // rolled back with it and no durable operation exists, and nothing
                // else would pick the guest up: the transient-lifecycle sweeper
                // answers `AliveOnNode` on a `creating` row with `Defer`, and
                // `orphaned_sandboxes` scans `managed_by = 'k8s'` only, so a direct
                // VM has no reconciler at all.
                //
                // **A failed `tx.commit()` does not prove the server did not
                // commit.** A connection reset after the server committed lands
                // here with the `deleting` write and the queued operation both
                // durable. The claim below then declines (the row is already
                // `deleting`), leaving the teardown to the dispatcher.
                //
                // So fall back to an immediate, best-effort teardown. Weaker than
                // the queued operation — a process death mid-call still loses it —
                // but better than no owner.
                //
                // **The fallback claims the row before it touches the substrate**,
                // with the same claim the queued path makes, in a transaction of
                // its own: the row lock, the lifecycle check, and the check that
                // the execution the create recorded is still the active one.
                // Create compensation owns only a
                // `creating|created|starting|started|restoring|failed` sandbox; a
                // `stopped` or `suspended` one belongs to the user, and a
                // successor that took the row over between the failed transaction
                // and this one owns it too. A user lifecycle makes the claim
                // decline, and so does a successor whose receipt or placement
                // differs from the create's, so the teardown only runs on a row
                // this claim moved to `deleting`.
                //
                // **What the claim still does not rule out.** On the direct
                // substrate it refuses a successor whose route receipt is present
                // and different, or whose `active_node_id` moved. A successor
                // activation on the same node whose receipt has not been persisted
                // yet presents as an absent receipt with an unchanged
                // `active_node_id`, which the claim accepts. It narrows the target;
                // it does not eliminate it.
                //
                // If the claim cannot be made, do nothing. A leaked instance is
                // recoverable by an operator; a deleted sandbox the user still owns
                // is not.
                let claimed = async {
                    let mut tx = pool.begin().await?;
                    let claimed = claim_created_sandbox_for_compensation(&mut tx, outcome).await?;
                    tx.commit().await?;
                    Ok::<_, sqlx::Error>(claimed)
                }
                .await;
                let fence = match claimed {
                    Ok(Some((_, fence))) => fence,
                    Ok(None) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            "fallback teardown declined: the sandbox is gone, no longer \
                             in a lifecycle create compensation owns, or backed by another \
                             execution"
                        );
                        return;
                    }
                    Err(claim_err) => {
                        tracing::error!(
                            sandbox_id = %sandbox_id,
                            error = %claim_err,
                            "could not claim the sandbox for a fallback teardown; \
                             declining to touch the substrate"
                        );
                        return;
                    }
                };
                if let Err(delete_err) = self
                    .delete_created_sandbox_for_rollback(pool, sandbox_id, &fence)
                    .await
                {
                    tracing::error!(
                        sandbox_id = %sandbox_id,
                        error = %delete_err,
                        "immediate fenced teardown also failed after the durable \
                         operation could not be queued; a substrate instance may be \
                         orphaned"
                    );
                }
            }
        }
    }

    /// Run the first attempt of a compensation this request queued claimed,
    /// in a task of its own so a cancelled request doesn't abandon it midway.
    /// The request waits for it, as it waited for the teardown before
    /// compensation was durable.
    async fn run_claimed_compensation(
        &self,
        pool: &PgPool,
        claim_token: String,
        operation: ClaimedPublicOperation,
    ) {
        let operation_id = operation.id;
        let span = execution_span(&operation);
        let executor = SandboxLifecycleOperationExecutor::new(pool.clone(), self.clone());
        let store = PublicOperationStore::new(pool.clone());
        let task = tokio::spawn(
            async move {
                let execution = {
                    let operation = operation.clone();
                    async move { executor.execute(&operation).await.into() }
                };
                drive_claimed_operation(
                    store,
                    claim_token,
                    OperationExecutionConfig::default(),
                    operation,
                    OperationExecutor::Inline,
                    execution,
                )
                .await
            }
            .instrument(span),
        );
        match task.await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(
                operation_id = %operation_id,
                %error,
                "inline create compensation could not record its outcome; the dispatcher \
                 reclaims it once its claim is stale"
            ),
            Err(error) => tracing::error!(
                operation_id = %operation_id,
                %error,
                "inline create compensation task failed; the dispatcher reclaims it once \
                 its claim is stale"
            ),
        }
    }

    /// Delete a just-created execution without applying the user-facing
    /// best-effort DELETE rule. Create compensation cannot retire the only
    /// durable route to a VM or pod until substrate absence is confirmed.
    pub(crate) async fn delete_created_sandbox_for_rollback(
        &self,
        pool: &PgPool,
        sandbox_id: SandboxId,
        fence: &CreateRollbackExecutionFence,
    ) -> Result<CreateCompensation, ControllerError> {
        // **Ownership before the teardown, not after it.** Compensation owns a
        // row it moved to `deleting`, and a row that's gone: the fence names
        // the execution on its own, and a best-effort user DELETE can retire the
        // row while the guest still runs. A row in any other lifecycle was taken
        // over after the compensation was queued, for instance by an action
        // already in flight whose result overwrote `deleting`. A stop is the dangerous
        // one: it clears the direct route receipt and leaves `active_node_id`
        // unchanged, which is exactly the shape the direct fence's
        // absent-receipt arm proceeds on, so a teardown here would delete the
        // state the stop retained.
        //
        // This is an unlocked read, so it narrows the window rather than
        // closing it: a result that commits between this read and the node
        // call, or during the call, still overwrites `deleting`; that
        // post-result write replaces cross-intent lifecycles in general (see
        // `lifecycle/characterization_tests.rs`), and the check after the call
        // then leaves the row and its reconcile state to it.
        let lifecycle: Option<String> =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id.as_uuid())
                .fetch_optional(pool)
                .await
                .map_err(|err| internal_error("failed to read rolled-back sandbox row", err))?;
        if let Some(lifecycle) = lifecycle
            .as_deref()
            .filter(|lifecycle| *lifecycle != "deleting")
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %lifecycle,
                "create rollback declined before its teardown: the row left `deleting`, \
                 so the sandbox belongs to whatever took it over"
            );
            return Ok(CreateCompensation::Declined);
        }

        match self
            .substrate
            .delete_created_execution(sandbox_id, fence)
            .await
        {
            Ok(()) | Err(SubstrateError::NotFound { .. }) => {}
            Err(err) => return Err(ControllerError::from(err)),
        }

        let cleanup_refs = sandbox_reconcile_cleanup_refs(sandbox_id);
        let mut tx = pool.begin().await.map_err(|err| {
            internal_error("failed to begin durable create rollback cleanup", err)
        })?;
        // Only the row this compensation claimed: one that left `deleting` was
        // taken over (an action already in flight can overwrite the lifecycle),
        // and its reconcile state is still that action's. Decide under the row
        // lock, before touching either. A row already gone leaves orphaned
        // reconcile state, which is the compensation's to clear.
        let lifecycle: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| internal_error("failed to lock rolled-back sandbox row", err))?;
        if let Some(lifecycle) = lifecycle
            .as_deref()
            .filter(|lifecycle| *lifecycle != "deleting")
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %lifecycle,
                "create rollback finished its teardown but the row left `deleting`; \
                 leaving the row and its reconcile state to whatever took it over"
            );
            tx.rollback().await.map_err(|err| {
                internal_error("failed to release rolled-back sandbox row lock", err)
            })?;
            return Ok(CreateCompensation::TornDown);
        }
        delete_reconcile_state_for_elements(&mut tx, &cleanup_refs)
            .await
            .map_err(|err| internal_error("failed to delete sandbox reconcile state", err))?;
        sqlx::query(sql!(
            "DELETE FROM sandboxes WHERE id = $1 AND lifecycle = 'deleting'"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|err| internal_error("failed to delete rolled-back sandbox row", err))?;
        tx.commit().await.map_err(|err| {
            internal_error("failed to commit durable create rollback cleanup", err)
        })?;
        Ok(CreateCompensation::TornDown)
    }
}

fn snapshots_response_json(
    response: pplx_space_sbx_mgr_client::proto::ListSnapshotsResponse,
) -> Vec<serde_json::Value> {
    response
        .snapshots
        .into_iter()
        .map(|snapshot| {
            let type_str = match SnapshotType::try_from(snapshot.r#type) {
                Ok(SnapshotType::Disk) => "disk",
                Ok(SnapshotType::Full) => "full",
                _ => "unknown",
            };
            serde_json::json!({
                "id": snapshot.id,
                "type": type_str,
                "state": snapshot.state,
                "timestamp": snapshot.timestamp,
                "label": snapshot.label,
                "retention_tier": snapshot.retention_tier,
                "size_bytes": snapshot.size_bytes,
            })
        })
        .collect()
}

/// What [`SandboxController::delete_created_sandbox_for_rollback`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateCompensation {
    /// The execution is confirmed gone.
    TornDown,
    /// The row left `deleting` before the teardown started, so the substrate
    /// was not touched.
    Declined,
}

/// Claim a just-created sandbox for create compensation: lock the row, check
/// that it's still in a lifecycle compensation owns and that the execution the
/// create recorded is still the active one, then move the row to `deleting`.
/// Returns the row's org and the fence to persist, or `None` when the row is
/// gone or something else owns it now. Run inside the caller's transaction, so
/// a successor that took the row over before the lock was taken, and whose
/// receipt or placement shows it, makes this decline instead of leaving the
/// successor in `deleting`. A direct successor on the same node that hasn't
/// persisted its receipt yet looks like the create's own execution and is
/// claimed.
async fn claim_created_sandbox_for_compensation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outcome: &CreateExecutionOutcome,
) -> Result<Option<(Uuid, CreateRollbackExecutionFence)>, sqlx::Error> {
    let sandbox_id = outcome.sandbox_id;
    let sandbox: Option<(Uuid, String, String, Option<Uuid>)> = sqlx::query_as(sql!(
        "SELECT org_id, lifecycle, managed_by, active_node_id
           FROM sandboxes
          WHERE id = $1
          FOR UPDATE"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await?;
    let Some((org_id, lifecycle, managed_by, active_node_id)) = sandbox else {
        return Ok(None);
    };
    if !matches!(
        lifecycle.as_str(),
        "creating" | "created" | "starting" | "started" | "restoring" | "failed"
    ) {
        tracing::warn!(
            sandbox_id = %sandbox_id,
            %lifecycle,
            "create rollback no longer owns a compensatable lifecycle"
        );
        return Ok(None);
    }
    let persisted_fence = match &outcome.rollback_fence {
        CreateRollbackExecutionFence::Direct {
            node_id,
            route_identity,
        } if managed_by == "space" => match route_identity {
            Some(identity) => {
                let matches = identity.sandbox_id() == sandbox_id
                    && identity.current_node_id() == *node_id
                    && sqlx::query_scalar::<_, bool>(sql!(
                        "SELECT EXISTS (
                             SELECT 1
                               FROM sandbox_direct_route_identities
                              WHERE sandbox_id = $1
                                AND route_generation = $2
                                AND current_node_id = $3
                                AND substrate_kind = $4
                                AND execution_generation = $5
                                AND activation_attempt_id = $6
                                AND pod_uid IS NOT DISTINCT FROM $7
                         )"
                    ))
                    .bind(sandbox_id.as_uuid())
                    .bind(identity.route_generation())
                    .bind(node_id.as_uuid())
                    .bind(identity.substrate_kind().as_str())
                    .bind(identity.execution_generation())
                    .bind(identity.activation_attempt_id().as_uuid())
                    .bind(identity.pod_uid().map(ToString::to_string))
                    .fetch_one(&mut **tx)
                    .await?;
                matches.then(|| outcome.rollback_fence.clone())
            }
            None => {
                let matches = active_node_id.is_none_or(|active| active == node_id.as_uuid())
                    && !sqlx::query_scalar::<_, bool>(sql!(
                        "SELECT EXISTS (
                             SELECT 1
                               FROM sandbox_direct_route_identities
                              WHERE sandbox_id = $1
                         )"
                    ))
                    .bind(sandbox_id.as_uuid())
                    .fetch_one(&mut **tx)
                    .await?;
                matches.then(|| outcome.rollback_fence.clone())
            }
        },
        CreateRollbackExecutionFence::Kubernetes {
            pod_name,
            generation,
            activation_attempt_id,
            route_activation_id,
            pod_uid,
        } if managed_by == "k8s" => {
            let recorded: Option<(Option<Uuid>, Option<String>)> = sqlx::query_as(sql!(
                "SELECT activation_attempt_id,
                        NULLIF(BTRIM(pod_uid), '')
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1
                    AND active
                    AND generation = $2
                    AND pod_name = $3
                    AND route_activation_id = $4
                  FOR UPDATE"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(*generation)
            .bind(pod_name.as_str())
            .bind(*route_activation_id)
            .fetch_optional(&mut **tx)
            .await?;
            let Some((current_attempt, recorded_uid)) = recorded else {
                return Ok(None);
            };
            let attempt_matches = activation_attempt_id.is_none_or(|expected| {
                current_attempt == Some(expected.as_uuid()) || current_attempt.is_none()
            });
            if !attempt_matches {
                return Ok(None);
            }
            let uid_matches = match recorded_uid {
                Some(recorded_uid) => recorded_uid == pod_uid.as_str(),
                None => {
                    sqlx::query(sql!(
                        "UPDATE sandbox_k8s_executions
                            SET pod_uid = $5,
                                updated_at = NOW()
                          WHERE sandbox_id = $1
                            AND active
                            AND generation = $2
                            AND pod_name = $3
                            AND route_activation_id = $4
                            AND NULLIF(BTRIM(pod_uid), '') IS NULL"
                    ))
                    .bind(sandbox_id.as_uuid())
                    .bind(*generation)
                    .bind(pod_name.as_str())
                    .bind(*route_activation_id)
                    .bind(pod_uid.as_str())
                    .execute(&mut **tx)
                    .await?
                    .rows_affected()
                        == 1
                }
            };
            uid_matches.then(|| CreateRollbackExecutionFence::Kubernetes {
                pod_name: pod_name.clone(),
                generation: *generation,
                activation_attempt_id: current_attempt
                    .map(pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid),
                route_activation_id: *route_activation_id,
                pod_uid: pod_uid.clone(),
            })
        }
        _ => None,
    };
    let Some(persisted_fence) = persisted_fence else {
        tracing::warn!(
            sandbox_id = %sandbox_id,
            fence = ?outcome.rollback_fence,
            "create rollback fence no longer matches the active execution"
        );
        return Ok(None);
    };
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&mut **tx)
    .await?;
    Ok(Some((org_id, persisted_fence)))
}

#[cfg(test)]
mod compensation_tests;
#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod transport_tests;

pub(super) fn require_access_idle_update_acknowledgement(
    response: tonic::Response<TouchAutoLifecycleResponse>,
) -> Result<(), ControllerError> {
    if response
        .metadata()
        .get(pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA)
        .is_some_and(|value| value == "1")
        && matches!(
            AutoLifecycleTouchResult::try_from(response.get_ref().result),
            Ok(AutoLifecycleTouchResult::Applied | AutoLifecycleTouchResult::AlreadyCurrent)
        )
    {
        Ok(())
    } else {
        Err(ControllerError::service_unavailable(
            "sandbox deadline was not acknowledged",
        ))
    }
}
