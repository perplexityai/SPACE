use super::*;
use crate::controllers::db_contention_error;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, SandboxId};
use pplx_space_common_svc::types::ReconcileReason;
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};
use pplx_space_sandbox_store::{self as sandbox_store, FieldPatch};

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
            .emit_with(base_details().error("network policy generation conflict"));
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
                .emit_with(base_details().error(error.to_string()));
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
            Self::send_access_idle_deadline(
                client,
                sandbox.id,
                generation,
                observed_at,
                deadline,
                pplx_space_sbx_mgr_client::proto::access_idle_update::Operation::ResetPolicy(
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
        operation: pplx_space_sbx_mgr_client::proto::access_idle_update::Operation,
    ) -> Result<(), ControllerError> {
        tokio::time::timeout_at(deadline, async {
            let (client, generation) = self
                .resolve_access_idle_execution(
                    sandbox.id,
                    sandbox.active_execution_generation,
                    deadline,
                )
                .await?;
            Self::send_access_idle_deadline(
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
                let mut request =
                    tonic::Request::new(pplx_space_sbx_mgr_client::proto::StateRequest {
                        sandbox_ids: vec![sandbox_id.to_string()],
                    });
                request
                    .set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
                let response = client.sandbox_state(request).await?;
                observed_access_idle_generation(sandbox_id, response.into_inner())?.get()
            }
        };
        Ok((client, generation))
    }

    async fn send_access_idle_deadline(
        mut client: RuntimeClient,
        sandbox_id: SandboxId,
        generation: i64,
        observed_at: prost_types::Timestamp,
        deadline: tokio::time::Instant,
        operation: pplx_space_sbx_mgr_client::proto::access_idle_update::Operation,
    ) -> Result<(), ControllerError> {
        // Observation consumes the same operation budget as the mutation.
        let budget = deadline.saturating_duration_since(tokio::time::Instant::now());
        if budget.is_zero() {
            return Err(ControllerError::service_unavailable(
                "sandbox deadline update expired",
            ));
        }
        let expires =
            jiff::Timestamp::now() + jiff::SignedDuration::from_secs(budget.as_secs() as i64);
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
        let response = client.touch_sandbox_auto_lifecycle(request).await?;
        require_access_idle_update_acknowledgement(response)
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

    pub(super) async fn rollback_created_sandbox(&self, pool: &PgPool, sandbox_id: SandboxId) {
        // Let the delete complete — it is NOT response-margin-critical: the
        // binding upstream cut is the node-RPC channel timeout (which bounds the
        // individual worker stop/delete RPCs), and the ALB idle timeout is far
        // higher (see `ALB_IDLE_TIMEOUT_SECS`). Cancelling a slow delete would
        // leak the backing instance (on direct, with no orphan GC, a running VM
        // invisibly), so the unbounded delete is the safe choice.
        if let Err(err) = self.substrate.delete_execution(sandbox_id).await {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %err,
                "failed to delete substrate backing instance during create rollback"
            );
        }
        let cleanup_refs = sandbox_reconcile_cleanup_refs(sandbox_id);
        match pool.begin().await {
            Ok(mut tx) => {
                let result = async {
                    delete_reconcile_state_for_elements(&mut tx, &cleanup_refs).await?;
                    sqlx::query("DELETE FROM sandboxes WHERE id = $1")
                        .bind(sandbox_id.as_uuid())
                        .execute(&mut *tx)
                        .await?;
                    tx.commit().await
                }
                .await;
                if let Err(err) = result {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "failed to clean sandbox reconcile state during create rollback"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "failed to begin sandbox create rollback cleanup transaction"
                );
                if let Err(delete_err) = sqlx::query("DELETE FROM sandboxes WHERE id = $1")
                    .bind(sandbox_id.as_uuid())
                    .execute(pool)
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %delete_err,
                        "best-effort sandbox row cleanup failed after rollback transaction begin failure"
                    );
                }
            }
        }
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

#[cfg(test)]
mod postgres_tests;

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
