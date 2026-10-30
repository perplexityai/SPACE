use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_common_core::sql;
use std::time::Duration;

use axum::http::StatusCode;
use pplx_space_common_core::types::{
    NodeId, NodePoolName, SandboxId, SandboxLifecycle, SandboxLifecycleParseError,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_public_api::{
    OperationKind, OperationState, PublicErrorCode, PublicOperationResult, PublicSandboxId,
    PublicSandboxOperationResult, RollbackSandboxRequest, SandboxLifecycleAction,
    SandboxLifecycleState, SandboxNetworkPolicyPatch as PublicSandboxNetworkPolicyPatch,
    UpdateSandboxRequest as PublicUpdateSandboxRequest,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::Instrument;

use crate::controllers::sandbox::FederatedSandboxCreateContext;
use crate::controllers::sandbox::lifecycle::SandboxAction;
use crate::controllers::sandbox::{SandboxController, fetch_sandbox};
use crate::controllers::substrate::ExecutionActivityKind;
use crate::controllers::{ControllerError, SnapshotUploadCheck, SnapshotUploadInFlight};
use crate::models::{
    CreateSandboxRequest as ControllerCreateSandboxRequest, SandboxMetadata,
    SandboxMetadataValue as ControllerSandboxMetadataValue,
    SandboxNetworkPolicyPatch as ControllerSandboxNetworkPolicyPatch,
    UpdateSandboxRequest as ControllerUpdateSandboxRequest, User,
};
use crate::public_operations::{ApiProduct, ClaimedPublicOperation, PublicOperationRequeueReason};
use crate::public_sandbox_projection::public_lifecycle_state as project_public_lifecycle_state;

const CREATE_RETRY_AFTER: Duration = Duration::from_secs(5);
/// The floor of the spacing between checks by a native delete held off by a
/// snapshot upload. The spacing grows with how long the delete has waited.
const SNAPSHOT_UPLOAD_RETRY_AFTER: Duration = Duration::from_secs(3);
/// How long a native delete is patient with snapshot uploads before failing
/// with the upload conflict. It is an API bound, not a measure of the upload
/// worker. It is sized against the worker's backoff ladder between attempts,
/// 5 + 30 + 120 + 300 s, about eight minutes. The worker reuses one row across
/// its attempts and heartbeats it at the start of each.
///
/// The delete fails once the stalest blocking upload has gone this long
/// without activity: a row whose worker is gone, or a single attempt that runs
/// longer than this (an attempt may run for up to two hours). It also fails
/// once the delete itself has been in flight this long, which bounds a run of
/// backups that each leave a new row. Waiting on would hold the sandbox's
/// queue until the M1 deadline.
pub(crate) const SNAPSHOT_UPLOAD_WAIT_LIMIT: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSandboxOperationPayload {
    pub sandbox_id: PublicSandboxId,
    pub request: pplx_space_public_api::CreateSandboxRequest,
    /// Operator-only isolation node pool pin, carried from the admin create
    /// surface. Absent (`None`) on every public/E2B create.
    ///
    /// Mixed-version safety is by **consumer-first rollout**, not a code gate.
    /// `skip_serializing_if = "Option::is_none"` keeps every unpinned payload
    /// byte-identical to what pre-placement producers emit, and `default`
    /// deserializes those old-shape payloads. A *pinned* payload carries a key
    /// an old executor's `deny_unknown_fields` would reject — so every
    /// `space-cluster-controller` replica must be fully on this release before
    /// any pin is issued. Pins are operator-only and experiment-only, so this
    /// is an operational precondition, not a runtime capability handshake.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_pool: Option<NodePoolName>,
    /// Consumers must be upgraded before federated native admission is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federated_create: Option<crate::fork_operations::ForkFederatedCreateContext>,
    /// The warm-pool slot claimed for this create at admission, whose reserved
    /// ID is `sandbox_id`. The executor adopts that exact slot, or builds cold
    /// under the same ID if the slot can no longer be adopted.
    ///
    /// Same mixed-version rule as `node_pool`: absent payloads serialize
    /// byte-identically and old payloads parse unchanged, while a present key
    /// is rejected by an older executor's `deny_unknown_fields` — and an
    /// executor that ignored it would cold-boot under the slot's ID next to
    /// the slot's pod. Every `space-cluster-controller` replica must run this
    /// release before any producer sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_claim: Option<CreateSandboxPoolClaim>,
}

/// Identifies the warm-pool slot claim recorded on an admitted native create.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSandboxPoolClaim {
    pub claim_id: uuid::Uuid,
    pub pool_slot_id: uuid::Uuid,
}

impl From<CreateSandboxPoolClaim> for crate::controllers::substrate::AdmittedPoolClaim {
    fn from(claim: CreateSandboxPoolClaim) -> Self {
        Self {
            claim_id: claim.claim_id,
            pool_slot_id: claim.pool_slot_id,
        }
    }
}

impl From<crate::controllers::substrate::AdmittedPoolClaim> for CreateSandboxPoolClaim {
    fn from(claim: crate::controllers::substrate::AdmittedPoolClaim) -> Self {
        Self {
            claim_id: claim.claim_id,
            pool_slot_id: claim.pool_slot_id,
        }
    }
}

impl std::fmt::Debug for CreateSandboxOperationPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateSandboxOperationPayload")
            .field("sandbox_id", &self.sandbox_id)
            .field("node_pool", &self.node_pool)
            .field("federated", &self.federated_create.is_some())
            .field("pool_claim", &self.pool_claim)
            .finish_non_exhaustive()
    }
}

impl CreateSandboxOperationPayload {
    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("create sandbox operation payload serializes")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxLifecycleOperationPayload {
    pub sandbox_id: PublicSandboxId,
    pub action: SandboxLifecycleAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackSandboxRequest>,
    /// The activity the system's wake for a session was queued for, which
    /// the executor judges the sandbox's wake policy by when the wake runs.
    /// Only that wake carries it (see [`is_implicit_wake`]), and only while
    /// [`IMPLICIT_WAKE_ACTIVITY_ENV`] is on.
    ///
    /// Same mixed-version rule as `CreateSandboxOperationPayload::node_pool`:
    /// an absent key serializes byte-identically to the payload older
    /// producers emit, and a wake without it, such as one an older API
    /// queued, is judged as a command, as an older executor judges every
    /// system wake. A present key is rejected by an older executor's
    /// `deny_unknown_fields`, so every `space-cluster-controller` replica
    /// must run this release before any producer sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake_activity: Option<ImplicitWakeActivity>,
}

/// A session's activity, as the wake policy names it, recorded on the
/// system's wake for that session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImplicitWakeActivity {
    Command,
    Access,
}

impl From<ExecutionActivityKind> for ImplicitWakeActivity {
    fn from(activity: ExecutionActivityKind) -> Self {
        match activity {
            ExecutionActivityKind::Command => Self::Command,
            ExecutionActivityKind::Access => Self::Access,
        }
    }
}

impl From<ImplicitWakeActivity> for ExecutionActivityKind {
    fn from(activity: ImplicitWakeActivity) -> Self {
        match activity {
            ImplicitWakeActivity::Command => Self::Command,
            ImplicitWakeActivity::Access => Self::Access,
        }
    }
}

/// The switch for recording [`SandboxLifecycleOperationPayload::wake_activity`]
/// on the system's wakes the API queues: `1`, `true`, `yes`, `on` or
/// `enabled`, in any case, turns it on; unset or anything else keeps it off.
/// Off, every system wake goes in without it and runs as a command wake, as
/// before the field. Turn it on only once every `space-cluster-controller`
/// replica runs a release that reads the field, and off again, letting the
/// wakes queued with it drain, before rolling one back.
pub(crate) const IMPLICIT_WAKE_ACTIVITY_ENV: &str = "SPACE_IMPLICIT_WAKE_ACTIVITY_ENABLED";

/// Whether the API records the activity on the system's wakes it queues; see
/// [`IMPLICIT_WAKE_ACTIVITY_ENV`].
pub(crate) fn implicit_wake_activity_enabled() -> bool {
    pplx_space_common_core::env::env_flag_var(IMPLICIT_WAKE_ACTIVITY_ENV, false)
}

impl SandboxLifecycleOperationPayload {
    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("sandbox lifecycle operation payload serializes")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSandboxOperationPayload {
    pub sandbox_id: PublicSandboxId,
    pub request: PublicUpdateSandboxRequest,
}

impl UpdateSandboxOperationPayload {
    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("update sandbox operation payload serializes")
    }
}

#[derive(Debug, Clone)]
pub struct PublicOperationExecutionResult {
    pub state: OperationState,
    pub progress_message: String,
    pub result_json: Option<serde_json::Value>,
    /// The error code of a failed result. A requeue may carry one too: the
    /// error it stands for, recorded if the driver fails the operation instead
    /// of requeueing it (its deadline, deferral window, or retry budget). The
    /// carried code replaces the driver's own.
    pub terminal_error_code: Option<PublicErrorCode>,
    /// The error envelope of a failed result. A requeue's carried envelope is
    /// merged under the driver's own evidence: the driver keeps its fields and
    /// adds the carried ones it lacks.
    pub terminal_error_json: Option<serde_json::Value>,
    pub retry_after: Option<Duration>,
    pub requeue_reason: PublicOperationRequeueReason,
}

impl PublicOperationExecutionResult {
    fn succeeded(progress_message: impl Into<String>, result: PublicOperationResult) -> Self {
        Self {
            state: OperationState::Succeeded,
            progress_message: progress_message.into(),
            result_json: Some(
                serde_json::to_value(result).expect("public operation result serializes"),
            ),
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }

    fn failed(
        progress_message: impl Into<String>,
        code: PublicErrorCode,
        error_json: serde_json::Value,
    ) -> Self {
        Self {
            state: OperationState::Failed,
            progress_message: progress_message.into(),
            result_json: None,
            terminal_error_code: Some(code),
            terminal_error_json: Some(error_json),
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }

    fn retry(progress_message: impl Into<String>, retry_after: Duration) -> Self {
        Self {
            state: OperationState::Queued,
            progress_message: progress_message.into(),
            result_json: None,
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: Some(retry_after),
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        }
    }

    pub(crate) fn workflow_progress(
        progress_message: impl Into<String>,
        retry_after: Duration,
    ) -> Self {
        Self {
            requeue_reason: PublicOperationRequeueReason::WorkflowProgress,
            ..Self::retry(progress_message, retry_after)
        }
    }

    /// Mark a `retry` as backpressure so the dispatcher re-queues it without
    /// consuming the transient-error retry budget.
    fn with_backpressure(mut self) -> Self {
        self.requeue_reason = PublicOperationRequeueReason::ActivationBackpressure;
        self
    }
}

#[derive(Clone)]
pub struct UpdateSandboxOperationExecutor {
    pool: PgPool,
    controller: SandboxController,
}

impl UpdateSandboxOperationExecutor {
    pub fn new(pool: PgPool, controller: SandboxController) -> Self {
        Self { pool, controller }
    }

    pub async fn execute(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        match self.execute_inner(operation).await {
            Ok(result) => result,
            Err(error) => error.into_result(operation),
        }
    }

    async fn execute_inner(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> Result<PublicOperationExecutionResult, UpdateSandboxExecutionError> {
        let payload: UpdateSandboxOperationPayload =
            serde_json::from_value(operation.payload.clone())
                .map_err(UpdateSandboxExecutionError::InvalidPayload)?;
        if payload
            .request
            .resources
            .as_ref()
            .and_then(|resources| resources.disk_mib)
            .is_some()
        {
            return Err(UpdateSandboxExecutionError::RequiresRestartResource);
        }
        let target_id = operation
            .target_id
            .as_deref()
            .ok_or(UpdateSandboxExecutionError::MissingTargetId)?;
        if target_id != payload.sandbox_id.to_string() {
            return Err(UpdateSandboxExecutionError::TargetMismatch {
                target_id: target_id.to_string(),
                payload_sandbox_id: payload.sandbox_id,
            });
        }

        let sandbox_uuid = payload.sandbox_id;
        let Some(sandbox) = fetch_sandbox(&self.pool, sandbox_uuid.into()).await? else {
            return Err(UpdateSandboxExecutionError::SandboxNotFound);
        };
        if sandbox.org_id != operation.org_id {
            return Err(UpdateSandboxExecutionError::TargetOrgMismatch);
        }

        self.controller
            .update(
                &self.pool,
                &sandbox,
                sandbox_uuid.into(),
                controller_update_request(payload.request),
                None,
            )
            .await?;

        let updated = fetch_sandbox(&self.pool, sandbox_uuid.into()).await?;
        let state = updated
            .as_ref()
            .map(|sandbox| public_lifecycle_state(&sandbox.lifecycle))
            .transpose()?
            .unwrap_or(SandboxLifecycleState::Deleted);

        Ok(PublicOperationExecutionResult::succeeded(
            "sandbox update applied",
            PublicOperationResult::Sandbox(PublicSandboxOperationResult {
                sandbox_id: payload.sandbox_id,
                state,
                action: None,
                controller_response: Some(serde_json::json!({
                    "ok": true,
                    "sandbox_id": payload.sandbox_id.to_string(),
                    "updated": true,
                })),
            }),
        ))
    }
}

fn operation_sandbox_id(operation: &ClaimedPublicOperation) -> Option<String> {
    operation.sandbox_id()
}

#[derive(Clone)]
pub struct CreateSandboxOperationExecutor {
    pool: PgPool,
    controller: SandboxController,
}

impl CreateSandboxOperationExecutor {
    pub fn new(pool: PgPool, controller: SandboxController) -> Self {
        Self { pool, controller }
    }

    pub async fn execute(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        let span = tracing::info_span!(
            "sandbox_lifecycle_operation",
            operation_id = %operation.id,
            claim_generation = operation.claim_generation,
            operation_kind = operation.kind.as_str(),
            target_id = operation.target_id.as_deref().unwrap_or("none"),
            sandbox_id = operation_sandbox_id(operation),
        );
        match self.execute_inner(operation).instrument(span).await {
            Ok(result) => result,
            Err(error) => error.into_result(operation),
        }
    }

    async fn execute_inner(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> Result<PublicOperationExecutionResult, CreateSandboxExecutionError> {
        let payload: CreateSandboxOperationPayload =
            serde_json::from_value(operation.payload.clone())
                .map_err(CreateSandboxExecutionError::InvalidPayload)?;
        let target_id = operation
            .target_id
            .as_deref()
            .ok_or(CreateSandboxExecutionError::MissingTargetId)?;
        if target_id != payload.sandbox_id.to_string() {
            return Err(CreateSandboxExecutionError::TargetMismatch {
                target_id: target_id.to_string(),
                payload_sandbox_id: payload.sandbox_id,
            });
        }

        if let Some(existing) = fetch_sandbox(&self.pool, payload.sandbox_id.into()).await? {
            if existing.org_id != operation.org_id
                || (payload.federated_create.is_some()
                    && Some(existing.user_id) != operation.actor_user_id)
                || payload.federated_create.as_ref().is_some_and(|context| {
                    !crate::controllers::sandbox::sandbox_matches_federated_owner(
                        &existing,
                        &context.owner(),
                    )
                })
            {
                return Err(CreateSandboxExecutionError::TargetOrgMismatch);
            }
            // A retry can renew the grant after an earlier attempt persisted
            // the row; completion scrubs the payload, so persist it now.
            if let Some(context) = &payload.federated_create {
                let (owner, grant) = context
                    .restore()
                    .map_err(CreateSandboxExecutionError::InvalidFederatedContext)?;
                self.controller
                    .upsert_federated_service_grant(&self.pool, existing.id, &owner, &grant)
                    .await?;
            }
            // A failed adoption can leave its row behind for the pool's
            // cleanup to tear down; that row is not a created sandbox.
            if payload.pool_claim.is_some()
                && self
                    .controller
                    .admitted_pool_cleanup_pending(existing.id)
                    .await?
            {
                return Ok(PublicOperationExecutionResult::workflow_progress(
                    "waiting for a failed warm-pool adoption to be cleaned up",
                    CREATE_RETRY_AFTER,
                ));
            }
            let lifecycle = existing.lifecycle.parse::<SandboxLifecycle>()?;
            ensure_existing_row_confirms_create(existing.active_node_id, lifecycle)?;
            return Ok(create_success_result(
                payload.sandbox_id,
                lifecycle,
                "sandbox already exists for create operation",
            ));
        }

        let actor_user_id = operation
            .actor_user_id
            .ok_or(CreateSandboxExecutionError::MissingActor)?;
        let actor = fetch_actor(&self.pool, actor_user_id, operation.org_id).await?;
        let federated = payload
            .federated_create
            .as_ref()
            .map(crate::fork_operations::ForkFederatedCreateContext::restore)
            .transpose()
            .map_err(CreateSandboxExecutionError::InvalidFederatedContext)?;
        if let Some((owner, _)) = &federated {
            let effective = crate::federated_identity::materialize_owner(&self.pool, owner)
                .await
                .map_err(ControllerError::from)?;
            if effective.org_id != operation.org_id || effective.user_id != actor.id {
                return Err(CreateSandboxExecutionError::TargetOrgMismatch);
            }
        }
        let planned_sandbox_id = SandboxId::from(payload.sandbox_id);
        let controller_request = controller_create_request(payload.request, payload.node_pool);
        // Legacy queued creates predate profiles; never apply execution-time defaults.
        let empty_profiles = crate::egress_profiles::EgressProfileSelection::default();
        let federated_context = FederatedSandboxCreateContext::new(
            federated.as_ref().map(|(owner, _)| owner),
            federated.as_ref().map(|(_, grant)| grant),
        )
        .with_egress_profiles(Some(
            payload
                .federated_create
                .as_ref()
                .and_then(|context| context.egress_profiles.as_ref())
                .unwrap_or(&empty_profiles),
        ));
        let response = match payload.pool_claim {
            None => {
                self.controller
                    .create_with_sandbox_id(
                        &self.pool,
                        &actor,
                        &controller_request,
                        SandboxApiMetricIdentity::native("create"),
                        planned_sandbox_id,
                        federated_context,
                    )
                    .await?
            }
            Some(pool_claim) => {
                // Adoption always waits for readiness; `wait_until_ready` on
                // the request still governs only the cold path.
                self.controller
                    .create_with_admitted_pool_claim(
                        &self.pool,
                        &actor,
                        &controller_request,
                        SandboxApiMetricIdentity::native("create"),
                        planned_sandbox_id,
                        pool_claim.into(),
                        federated_context,
                    )
                    .await?
            }
        };
        let lifecycle = response
            .lifecycle
            .as_deref()
            .map(str::parse)
            .transpose()?
            .unwrap_or(SandboxLifecycle::Creating);

        Ok(create_success_result(
            payload.sandbox_id,
            lifecycle,
            "sandbox create dispatched",
        ))
    }
}

#[derive(Clone)]
pub struct SandboxLifecycleOperationExecutor {
    pool: PgPool,
    controller: SandboxController,
}

impl SandboxLifecycleOperationExecutor {
    pub fn new(pool: PgPool, controller: SandboxController) -> Self {
        Self { pool, controller }
    }

    pub async fn execute(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        match self.execute_inner(operation).await {
            Ok(result) => result,
            Err(error) => error.into_result(operation),
        }
    }

    /// A delete refused for an in-flight snapshot upload.
    ///
    /// A native request waits for the upload. Native deletes carry an
    /// Idempotency-Key, and a failed operation is immutable, so a terminal
    /// conflict would leave a caller's cleanup delete failed for the key's
    /// lifetime. ECW deletes carry no key, so an ECW caller can retry after
    /// the 409. Admin force-deletes keep the 409 for the operator to act on.
    /// An ECW delete is told apart by its missing actor
    /// ([`crate::public_operations::PublicOperationStore::create_or_join_active_sandbox_delete`]).
    /// `actor_user_id` is cleared if the user row is deleted, so a native
    /// delete whose user is deleted while it waits fails at its next check.
    ///
    /// The wait ends with the conflict once the stalest blocking upload has
    /// gone [`SNAPSHOT_UPLOAD_WAIT_LIMIT`] without activity, or once that long
    /// has passed since the delete's first claim. That clock also counts
    /// retries before the delete first met an upload.
    async fn snapshot_upload_result(
        &self,
        operation: &ClaimedPublicOperation,
        sandbox_id: SandboxId,
        upload: SnapshotUploadInFlight,
        error: ControllerError,
    ) -> PublicOperationExecutionResult {
        let conflict = controller_error_result(operation, error);
        if operation.api_product == ApiProduct::Native && operation.actor_user_id.is_some() {
            let waited = crate::public_operations::PublicOperationStore::new(self.pool.clone())
                .time_since_first_claim(operation.id)
                .await;
            match waited {
                Ok(waited) => {
                    let waited = waited.unwrap_or_default();
                    if upload.idle_for() < SNAPSHOT_UPLOAD_WAIT_LIMIT
                        && waited < SNAPSHOT_UPLOAD_WAIT_LIMIT
                    {
                        return waiting_for_snapshot_upload(operation, conflict, waited);
                    }
                    tracing::warn!(
                        operation_id = %operation.id,
                        sandbox_id = %sandbox_id,
                        upload_idle_for_secs = upload.idle_for().as_secs(),
                        waited_secs = waited.as_secs(),
                        "snapshot upload outlasted the delete's wait; failing the delete"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        operation_id = %operation.id,
                        sandbox_id = %sandbox_id,
                        error = %error,
                        "retrying sandbox delete after database error reading its wait"
                    );
                    return PublicOperationExecutionResult::retry(
                        "database unavailable while checking the snapshot upload",
                        CREATE_RETRY_AFTER,
                    );
                }
            }
        }
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::DeleteFailed,
            SandboxEventSource::Api,
        )
        .emit_with(
            SandboxEventDetails::default()
                .action("delete")
                .reason(match upload.check {
                    SnapshotUploadCheck::Preflight => "in_flight_suspend_upload",
                    SnapshotUploadCheck::Locked => "in_flight_suspend_upload_locked",
                })
                .routine()
                .trigger(SandboxEventTrigger::User),
        );
        conflict
    }

    async fn execute_inner(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> Result<PublicOperationExecutionResult, SandboxLifecycleExecutionError> {
        let payload: SandboxLifecycleOperationPayload =
            serde_json::from_value(operation.payload.clone())
                .map_err(SandboxLifecycleExecutionError::InvalidPayload)?;
        let target_id = operation
            .target_id
            .as_deref()
            .ok_or(SandboxLifecycleExecutionError::MissingTargetId)?;
        if target_id != payload.sandbox_id.to_string() {
            return Err(SandboxLifecycleExecutionError::TargetMismatch {
                target_id: target_id.to_string(),
                payload_sandbox_id: payload.sandbox_id,
            });
        }
        if payload.action.operation_kind() != operation.kind {
            return Err(SandboxLifecycleExecutionError::KindMismatch {
                operation_kind: operation.kind,
                action: payload.action,
            });
        }

        let sandbox_uuid = payload.sandbox_id;
        let mut sandbox = fetch_sandbox(&self.pool, sandbox_uuid.into()).await?;
        if payload.action == SandboxLifecycleAction::Delete
            && sandbox.is_none()
            && operation.api_product != ApiProduct::Admin
        {
            // A create can answer before its sandbox has a row. Without the
            // dispatcher's keyed claim, which holds a delete behind its
            // sandbox's create, this delete can run while that create builds
            // the sandbox. Finishing now would report a sandbox the create
            // then records, so wait for it to end.
            if !crate::public_operations::PublicOperationStore::new(self.pool.clone())
                .sandbox_creates_in_flight(operation.org_id, payload.sandbox_id)
                .await?
                .is_empty()
            {
                return Ok(PublicOperationExecutionResult::workflow_progress(
                    "waiting for the sandbox's create to finish",
                    CREATE_RETRY_AFTER,
                ));
            }
            // A create that succeeds commits its row before it leaves queued
            // or running, so one that finished since the first read has a
            // row by now.
            sandbox = fetch_sandbox(&self.pool, sandbox_uuid.into()).await?;
        }
        if let Some(sandbox) = sandbox.as_ref()
            && sandbox.org_id != operation.org_id
        {
            return Err(SandboxLifecycleExecutionError::TargetOrgMismatch);
        }

        if payload.action == SandboxLifecycleAction::Delete {
            if sandbox.is_none() && operation.api_product != ApiProduct::Admin {
                return Ok(lifecycle_success_result(
                    payload.sandbox_id,
                    payload.action,
                    SandboxLifecycleState::Deleted,
                    serde_json::json!({
                        "ok": true,
                        "action": payload.action.as_str(),
                        "sandbox_id": payload.sandbox_id.to_string(),
                    }),
                    "sandbox already deleted",
                ));
            }
            if let Err(error) = self
                .controller
                .delete_control_plane(&self.pool, sandbox_uuid.into(), sandbox.as_ref())
                .await
            {
                let Some(upload) = error.snapshot_upload_in_flight_details() else {
                    return Err(error.into());
                };
                return Ok(self
                    .snapshot_upload_result(operation, sandbox_uuid.into(), upload, error)
                    .await);
            }
            return Ok(lifecycle_success_result(
                payload.sandbox_id,
                payload.action,
                SandboxLifecycleState::Deleted,
                serde_json::json!({
                    "ok": true,
                    "action": payload.action.as_str(),
                    "sandbox_id": payload.sandbox_id.to_string(),
                }),
                "sandbox delete dispatched",
            ));
        }

        let Some(sandbox) = sandbox.as_ref() else {
            return Err(SandboxLifecycleExecutionError::SandboxNotFound);
        };
        let current_state = public_lifecycle_state(&sandbox.lifecycle)?;
        if let Some(state) = completed_lifecycle_target_state(payload.action, current_state) {
            return Ok(lifecycle_success_result(
                payload.sandbox_id,
                payload.action,
                state,
                serde_json::json!({
                    "ok": true,
                    "action": payload.action.as_str(),
                    "sandbox_id": payload.sandbox_id.to_string(),
                    "already_completed": true,
                }),
                "sandbox lifecycle action already completed",
            ));
        }
        if payload.action == SandboxLifecycleAction::Rollback {
            let rollback = payload
                .rollback
                .ok_or(SandboxLifecycleExecutionError::MissingRollbackRequest)?;
            if rollback.snapshot_name.trim().is_empty() {
                return Err(SandboxLifecycleExecutionError::MissingRollbackSnapshotName);
            }
            let body = self
                .controller
                .rollback(
                    &self.pool,
                    sandbox,
                    sandbox_uuid.into(),
                    rollback.snapshot_name,
                    rollback.auto_restart,
                )
                .await?;
            let updated = fetch_sandbox(&self.pool, sandbox_uuid.into()).await?;
            let state = updated
                .as_ref()
                .map(|sandbox| public_lifecycle_state(&sandbox.lifecycle))
                .transpose()?
                .unwrap_or(SandboxLifecycleState::Deleted);
            return Ok(lifecycle_success_result(
                payload.sandbox_id,
                payload.action,
                state,
                body,
                "sandbox rollback completed",
            ));
        }
        let controller_action = if is_implicit_wake(operation, payload.action) {
            let activity = implicit_wake_activity(payload.wake_activity);
            if !activity.allows_implicit_wake(&sandbox.auto_lifecycle.0) {
                return Err(SandboxLifecycleExecutionError::ImplicitWakeNotAllowed);
            }
            SandboxAction::ImplicitWake(activity)
        } else {
            sandbox_action(payload.action)?
        };
        let body = self
            .controller
            .action(
                &self.pool,
                sandbox_uuid.into(),
                Some(sandbox),
                SandboxApiMetricIdentity::native(payload.action.as_str()),
                controller_action,
                None,
            )
            .await?;
        let updated = fetch_sandbox(&self.pool, sandbox_uuid.into()).await?;
        let state = updated
            .as_ref()
            .map(|sandbox| public_lifecycle_state(&sandbox.lifecycle))
            .transpose()?
            .unwrap_or(SandboxLifecycleState::Deleted);

        Ok(lifecycle_success_result(
            payload.sandbox_id,
            payload.action,
            state,
            body,
            "sandbox lifecycle action dispatched",
        ))
    }
}

pub(crate) fn controller_create_request(
    request: pplx_space_public_api::CreateSandboxRequest,
    node_pool: Option<NodePoolName>,
) -> ControllerCreateSandboxRequest {
    let resources = request
        .resources
        .unwrap_or(pplx_space_public_api::SandboxResources {
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
        });
    ControllerCreateSandboxRequest {
        name: request.name,
        template: Some(request.template),
        runtime: request.runtime.map(|runtime| runtime.alias().to_string()),
        boot_mode: None,
        env: request.env,
        metadata: controller_metadata(request.metadata),
        vcpu: resources.vcpu,
        memory_mib: resources.memory_mib,
        disk_mib: resources.disk_mib,
        file_changes: None,
        auto_lifecycle: request.auto_lifecycle,
        wait_until_ready: false,
        network_policy: request.network_policy,
        // Placement pin carried from the operator (admin) create surface; the
        // public API never sets it.
        node_pool,
    }
}

fn controller_update_request(
    request: PublicUpdateSandboxRequest,
) -> ControllerUpdateSandboxRequest {
    let resources = request
        .resources
        .unwrap_or(pplx_space_public_api::SandboxResources {
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
        });
    ControllerUpdateSandboxRequest {
        metadata: request.metadata.map(controller_metadata),
        vcpu: resources.vcpu,
        memory_mib: resources.memory_mib,
        pids_max: None,
        auto_lifecycle: request.auto_lifecycle,
        network_policy: request.network_policy.map(|patch| match patch {
            PublicSandboxNetworkPolicyPatch::Set { policy } => {
                ControllerSandboxNetworkPolicyPatch::Set { policy }
            }
            PublicSandboxNetworkPolicyPatch::InheritTemplateDefault => {
                ControllerSandboxNetworkPolicyPatch::InheritTemplateDefault
            }
        }),
    }
}

fn controller_metadata(metadata: pplx_space_public_api::PublicSandboxMetadata) -> SandboxMetadata {
    metadata
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                pplx_space_public_api::PublicSandboxMetadataValue::String(value) => {
                    ControllerSandboxMetadataValue::String(value)
                }
                pplx_space_public_api::PublicSandboxMetadataValue::Number(value) => {
                    ControllerSandboxMetadataValue::Number(value)
                }
                pplx_space_public_api::PublicSandboxMetadataValue::Bool(value) => {
                    ControllerSandboxMetadataValue::Bool(value)
                }
            };
            (key, value)
        })
        .collect()
}

async fn fetch_actor(
    pool: &PgPool,
    user_id: UserId,
    org_id: OrgId,
) -> Result<User, CreateSandboxExecutionError> {
    let actor = sqlx::query_as::<_, User>(sql!(
        "SELECT *
         FROM users
         WHERE id = $1 AND org_id = $2"
    ))
    .bind(user_id)
    .bind(org_id)
    .fetch_optional(pool)
    .await?;
    actor.ok_or(CreateSandboxExecutionError::ActorNotFound)
}

pub(crate) fn create_success_result(
    sandbox_id: PublicSandboxId,
    lifecycle: SandboxLifecycle,
    progress_message: &'static str,
) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult::succeeded(
        progress_message,
        PublicOperationResult::Sandbox(PublicSandboxOperationResult {
            sandbox_id,
            state: project_public_lifecycle_state(lifecycle),
            action: None,
            controller_response: None,
        }),
    )
}

/// Accept only existing-row states that prove the sandbox was dispatched or
/// progressed beyond creation. Ambiguous failure/cleanup states count as
/// retries so the operation can never claim success for a sandbox that did not
/// finish creating. The explicit match makes new lifecycle variants fail to
/// compile until their create-operation disposition is chosen deliberately.
fn ensure_existing_row_confirms_create(
    active_node_id: Option<NodeId>,
    lifecycle: SandboxLifecycle,
) -> Result<(), CreateSandboxExecutionError> {
    match lifecycle {
        SandboxLifecycle::Creating if active_node_id.is_none() => Err(
            CreateSandboxExecutionError::ExistingRowNotCreateSuccess(lifecycle),
        ),
        SandboxLifecycle::Deleting | SandboxLifecycle::Quarantined | SandboxLifecycle::Failed => {
            Err(CreateSandboxExecutionError::ExistingRowNotCreateSuccess(
                lifecycle,
            ))
        }
        SandboxLifecycle::Created
        | SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Started
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Paused
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Suspending
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Stopped
        | SandboxLifecycle::Checkpointing => Ok(()),
    }
}

fn lifecycle_success_result(
    sandbox_id: PublicSandboxId,
    action: SandboxLifecycleAction,
    state: SandboxLifecycleState,
    controller_response: serde_json::Value,
    progress_message: &'static str,
) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult::succeeded(
        progress_message,
        PublicOperationResult::Sandbox(PublicSandboxOperationResult {
            sandbox_id,
            state,
            action: Some(action),
            controller_response: Some(controller_response),
        }),
    )
}

fn completed_lifecycle_target_state(
    action: SandboxLifecycleAction,
    current_state: SandboxLifecycleState,
) -> Option<SandboxLifecycleState> {
    match action {
        SandboxLifecycleAction::Start
        | SandboxLifecycleAction::Wake
        | SandboxLifecycleAction::Resume
        | SandboxLifecycleAction::Restore
            if current_state == SandboxLifecycleState::Running =>
        {
            Some(SandboxLifecycleState::Running)
        }
        SandboxLifecycleAction::Pause if current_state == SandboxLifecycleState::Paused => {
            Some(SandboxLifecycleState::Paused)
        }
        SandboxLifecycleAction::Suspend if current_state == SandboxLifecycleState::Suspended => {
            Some(SandboxLifecycleState::Suspended)
        }
        SandboxLifecycleAction::Stop | SandboxLifecycleAction::Kill
            if current_state == SandboxLifecycleState::Stopped =>
        {
            Some(SandboxLifecycleState::Stopped)
        }
        _ => None,
    }
}

/// A native wake operation with no actor is the system's wake for a session
/// (`create_or_join_active_sandbox_wake`): no caller asked for it, so it runs
/// only while the sandbox's wake policy, read now, still lets the session's
/// activity wake it (see [`implicit_wake_activity`]). Nothing else creates one,
/// with one exception: `public_operations.actor_user_id` is `ON DELETE SET
/// NULL`, so a caller's queued `:wake` whose user is deleted before it runs is
/// recognized here too, and refused if the policy forbids a command wake; no
/// caller remains to want it. A controller without this check runs it as the
/// plain `:wake` the policy allowed at enqueue. A caller's `:wake` joins the
/// system's wake only while it is queued, giving it that caller as its actor,
/// so the claim that takes it reads that actor and it runs as their `:wake`.
fn is_implicit_wake(operation: &ClaimedPublicOperation, action: SandboxLifecycleAction) -> bool {
    action == SandboxLifecycleAction::Wake
        && operation.kind == OperationKind::WakeSandbox
        && operation.api_product == ApiProduct::Native
        && operation.actor_user_id.is_none()
}

/// The activity the system's wake is judged for when it runs: the one it
/// records, or, for a wake without one, a command. Such a wake was queued by
/// an API from before the field or with [`IMPLICIT_WAKE_ACTIVITY_ENV`] off,
/// and an executor from before the field runs it by the command rule too, so
/// no wake an older executor would run is refused here.
fn implicit_wake_activity(recorded: Option<ImplicitWakeActivity>) -> ExecutionActivityKind {
    recorded.map_or(ExecutionActivityKind::Command, ExecutionActivityKind::from)
}

fn sandbox_action(
    action: SandboxLifecycleAction,
) -> Result<SandboxAction, SandboxLifecycleExecutionError> {
    SandboxAction::try_from(action)
        .map_err(|_| SandboxLifecycleExecutionError::UnsupportedAction(action))
}

fn public_lifecycle_state(
    lifecycle: &str,
) -> Result<SandboxLifecycleState, SandboxLifecycleParseError> {
    Ok(project_public_lifecycle_state(
        lifecycle.parse::<SandboxLifecycle>()?,
    ))
}

#[derive(Debug, thiserror::Error)]
enum UpdateSandboxExecutionError {
    #[error("invalid update_sandbox operation payload: {0}")]
    InvalidPayload(#[source] serde_json::Error),
    #[error("update_sandbox operation is missing target_id")]
    MissingTargetId,
    #[error(
        "update_sandbox operation target_id '{target_id}' does not match payload sandbox_id '{payload_sandbox_id}'"
    )]
    TargetMismatch {
        target_id: String,
        payload_sandbox_id: PublicSandboxId,
    },
    #[error("update_sandbox operation target belongs to a different org")]
    TargetOrgMismatch,
    #[error("disk_mib is not mutable in place")]
    RequiresRestartResource,
    #[error("sandbox not found")]
    SandboxNotFound,
    #[error("unknown sandbox lifecycle: {0}")]
    UnknownLifecycle(#[from] SandboxLifecycleParseError),
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("sandbox update failed: {0}")]
    Controller(#[from] ControllerError),
}

impl UpdateSandboxExecutionError {
    fn into_result(self, operation: &ClaimedPublicOperation) -> PublicOperationExecutionResult {
        match self {
            Self::InvalidPayload(_)
            | Self::MissingTargetId
            | Self::TargetMismatch { .. }
            | Self::TargetOrgMismatch
            | Self::RequiresRestartResource
            | Self::UnknownLifecycle(_) => PublicOperationExecutionResult::failed(
                "sandbox update operation is invalid",
                PublicErrorCode::InvalidRequest,
                terminal_error_json(operation, self.to_string()),
            ),
            Self::SandboxNotFound => PublicOperationExecutionResult::failed(
                "sandbox not found",
                PublicErrorCode::NotFound,
                terminal_error_json(operation, self.to_string()),
            ),
            Self::Sql(error) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    sandbox_id = operation_sandbox_id(operation),
                    error = %error,
                    "retrying update_sandbox operation after database error"
                );
                PublicOperationExecutionResult::retry(
                    "database unavailable while updating sandbox",
                    CREATE_RETRY_AFTER,
                )
            }
            Self::Controller(error) => controller_error_result(operation, error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum SandboxLifecycleExecutionError {
    #[error("invalid sandbox lifecycle operation payload: {0}")]
    InvalidPayload(#[source] serde_json::Error),
    #[error("sandbox lifecycle operation is missing target_id")]
    MissingTargetId,
    #[error(
        "sandbox lifecycle operation target_id '{target_id}' does not match payload sandbox_id '{payload_sandbox_id}'"
    )]
    TargetMismatch {
        target_id: String,
        payload_sandbox_id: PublicSandboxId,
    },
    #[error("sandbox lifecycle operation kind '{operation_kind}' does not match action '{action}'")]
    KindMismatch {
        operation_kind: OperationKind,
        action: SandboxLifecycleAction,
    },
    #[error("sandbox lifecycle operation target belongs to a different org")]
    TargetOrgMismatch,
    #[error("sandbox not found")]
    SandboxNotFound,
    #[error("rollback action is missing rollback request")]
    MissingRollbackRequest,
    #[error("rollback action is missing snapshot_name")]
    MissingRollbackSnapshotName,
    #[error("unsupported sandbox lifecycle action '{0}'")]
    UnsupportedAction(SandboxLifecycleAction),
    #[error("the sandbox's wake policy no longer lets the waiting session's activity wake it")]
    ImplicitWakeNotAllowed,
    #[error("unknown sandbox lifecycle: {0}")]
    UnknownLifecycle(#[from] SandboxLifecycleParseError),
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("sandbox lifecycle action failed: {0}")]
    Controller(#[from] ControllerError),
}

impl SandboxLifecycleExecutionError {
    fn into_result(self, operation: &ClaimedPublicOperation) -> PublicOperationExecutionResult {
        match self {
            Self::InvalidPayload(_)
            | Self::MissingTargetId
            | Self::TargetMismatch { .. }
            | Self::KindMismatch { .. }
            | Self::TargetOrgMismatch
            | Self::MissingRollbackRequest
            | Self::MissingRollbackSnapshotName
            | Self::UnsupportedAction(_)
            | Self::UnknownLifecycle(_) => PublicOperationExecutionResult::failed(
                "sandbox lifecycle operation is invalid",
                PublicErrorCode::InvalidRequest,
                terminal_error_json(operation, self.to_string()),
            ),
            Self::SandboxNotFound => PublicOperationExecutionResult::failed(
                "sandbox not found",
                PublicErrorCode::NotFound,
                terminal_error_json(operation, self.to_string()),
            ),
            Self::ImplicitWakeNotAllowed => PublicOperationExecutionResult::failed(
                "sandbox wake policy no longer allows this wake",
                PublicErrorCode::Conflict,
                terminal_error_json(operation, self.to_string()),
            ),
            Self::Sql(error) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    sandbox_id = operation_sandbox_id(operation),
                    error = %error,
                    "retrying sandbox lifecycle operation after database error"
                );
                PublicOperationExecutionResult::retry(
                    "database unavailable while applying sandbox lifecycle action",
                    CREATE_RETRY_AFTER,
                )
            }
            Self::Controller(error) => controller_error_result(operation, error),
        }
    }
}

/// Requeues a delete behind a snapshot upload, carrying `conflict`'s error as
/// the one to record if the operation cannot be requeued again.
///
/// The wait is workflow progress, so it spends no attempt. It is spaced like a
/// deferred retry, by how long the delete has `waited`, so a long wait checks
/// about once a minute.
fn waiting_for_snapshot_upload(
    operation: &ClaimedPublicOperation,
    conflict: PublicOperationExecutionResult,
    waited: Duration,
) -> PublicOperationExecutionResult {
    let retry_after = crate::public_operation_driver::deferred_retry_delay(
        operation.id,
        waited,
        SNAPSHOT_UPLOAD_RETRY_AFTER,
    );
    PublicOperationExecutionResult {
        terminal_error_code: conflict.terminal_error_code,
        terminal_error_json: conflict.terminal_error_json,
        ..PublicOperationExecutionResult::workflow_progress(
            "waiting for the snapshot upload to finalize before deleting",
            retry_after,
        )
    }
}

#[derive(Debug, thiserror::Error)]
enum CreateSandboxExecutionError {
    #[error("invalid create_sandbox operation payload: {0}")]
    InvalidPayload(#[source] serde_json::Error),
    #[error("invalid federated create context")]
    InvalidFederatedContext(#[source] crate::fork_operations::ForkFederatedCreateContextError),
    #[error("create_sandbox operation is missing target_id")]
    MissingTargetId,
    #[error(
        "create_sandbox operation target_id '{target_id}' does not match payload sandbox_id '{payload_sandbox_id}'"
    )]
    TargetMismatch {
        target_id: String,
        payload_sandbox_id: PublicSandboxId,
    },
    #[error("create_sandbox operation target belongs to a different org")]
    TargetOrgMismatch,
    #[error("create_sandbox operation has no actor_user_id")]
    MissingActor,
    #[error("create_sandbox operation actor was not found")]
    ActorNotFound,
    #[error(
        "sandbox create's existing row in lifecycle '{0}' does not confirm create success; retrying"
    )]
    ExistingRowNotCreateSuccess(SandboxLifecycle),
    #[error("unknown sandbox lifecycle: {0}")]
    UnknownLifecycle(#[from] SandboxLifecycleParseError),
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("sandbox create failed: {0}")]
    Controller(#[from] ControllerError),
}

impl CreateSandboxExecutionError {
    fn into_result(self, operation: &ClaimedPublicOperation) -> PublicOperationExecutionResult {
        match self {
            Self::InvalidPayload(_)
            | Self::InvalidFederatedContext(_)
            | Self::MissingTargetId
            | Self::TargetMismatch { .. }
            | Self::TargetOrgMismatch
            | Self::MissingActor
            | Self::ActorNotFound
            | Self::UnknownLifecycle(_) => PublicOperationExecutionResult::failed(
                "sandbox create operation is invalid",
                PublicErrorCode::InvalidRequest,
                terminal_error_json(operation, self.to_string()),
            ),
            Self::ExistingRowNotCreateSuccess(lifecycle) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    sandbox_id = operation_sandbox_id(operation),
                    lifecycle = %lifecycle,
                    "existing sandbox row does not confirm create success; retrying create \
                     instead of reporting it as success"
                );
                PublicOperationExecutionResult::retry(
                    Self::ExistingRowNotCreateSuccess(lifecycle).to_string(),
                    CREATE_RETRY_AFTER,
                )
            }
            Self::Sql(error) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    sandbox_id = operation_sandbox_id(operation),
                    error = %error,
                    "retrying create_sandbox operation after database error"
                );
                PublicOperationExecutionResult::retry(
                    "database unavailable while creating sandbox",
                    CREATE_RETRY_AFTER,
                )
            }
            Self::Controller(error) => controller_error_result(operation, error),
        }
    }
}

fn controller_error_result(
    operation: &ClaimedPublicOperation,
    error: ControllerError,
) -> PublicOperationExecutionResult {
    if let Some(retry_after) = error.retry_after_header()
        && error.retry_after_cause()
            == Some(crate::controllers::ControllerRetryAfterCause::PoolSlotRelease)
    {
        // Waiting for the pool to collect a slot holding this create's ID is
        // bounded by the pool's own sweep, so it is progress, not a failure.
        return PublicOperationExecutionResult::workflow_progress(error.to_string(), retry_after);
    }
    if let Some(retry_after) = error.retry_after_header() {
        let result = PublicOperationExecutionResult::retry(error.to_string(), retry_after);
        // A cluster activation admission 429 is backpressure, not a failure:
        // re-queue behind the limiter without burning the retry budget. Key off
        // the admission cause specifically — not the generic 429 status — so an
        // unrelated controller 429 (e.g. a downstream quota) can't get retried
        // forever without ever exhausting the transient-error budget.
        return if error.retry_after_cause()
            == Some(crate::controllers::ControllerRetryAfterCause::ClusterActivationAdmission)
        {
            result.with_backpressure()
        } else {
            result
        };
    }

    match error.status() {
        StatusCode::BAD_REQUEST => PublicOperationExecutionResult::failed(
            "sandbox create request is invalid",
            PublicErrorCode::InvalidRequest,
            terminal_error_json(operation, error.to_string()),
        ),
        StatusCode::NOT_FOUND => PublicOperationExecutionResult::failed(
            "sandbox create dependency was not found",
            PublicErrorCode::NotFound,
            terminal_error_json(operation, error.to_string()),
        ),
        StatusCode::CONFLICT => PublicOperationExecutionResult::failed(
            "sandbox create conflicted",
            PublicErrorCode::Conflict,
            terminal_error_json(operation, error.to_string()),
        ),
        StatusCode::PRECONDITION_FAILED => PublicOperationExecutionResult::failed(
            "sandbox create precondition failed",
            PublicErrorCode::FailedPrecondition,
            terminal_error_json(operation, error.to_string()),
        ),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
            PublicOperationExecutionResult::retry(error.to_string(), CREATE_RETRY_AFTER)
        }
        status if status.is_server_error() && is_non_retryable_create_controller_error(&error) => {
            PublicOperationExecutionResult::failed(
                "sandbox create cannot start from the selected template snapshot",
                PublicErrorCode::FailedPrecondition,
                terminal_error_json(operation, error.to_string()),
            )
        }
        status if status.is_server_error() => {
            tracing::warn!(
                operation_id = %operation.id,
                sandbox_id = operation_sandbox_id(operation),
                status = status.as_u16(),
                error = %error,
                "retrying create_sandbox operation after controller error"
            );
            PublicOperationExecutionResult::retry(error.to_string(), CREATE_RETRY_AFTER)
        }
        _ => PublicOperationExecutionResult::failed(
            "sandbox create failed",
            PublicErrorCode::OperationFailed,
            terminal_error_json(operation, error.to_string()),
        ),
    }
}

pub(crate) fn is_non_retryable_create_controller_error(error: &ControllerError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("load snapshot failed")
        && (message.contains("failed to set kvm vcpu xcrs")
            || message.contains("failed to restore vcpus")
            || message.contains("firecracker api put /snapshot/load: 400 bad request"))
}

fn terminal_error_json(operation: &ClaimedPublicOperation, message: String) -> serde_json::Value {
    serde_json::json!({
        "operation_id": operation.id.to_string(),
        "kind": operation.kind.as_str(),
        "message": message,
    })
}

#[cfg(test)]
mod tests;
