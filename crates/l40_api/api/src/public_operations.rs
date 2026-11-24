use pplx_space_accounts::{OrgId, UserId};

use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::Duration;

use crate::metrics::operations::ClaimAttempt;
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicErrorCode,
    PublicOperationEvent, PublicOperationEventType, PublicOperationRecord, PublicOperationResult,
    RequestFingerprint,
};
use pplx_space_public_operation_store as operation_store;
use pplx_space_public_operation_store::operations::{ClaimedPublicOperationRow, JoinableOperation};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_store as sandbox_store;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::public_operation_driver::with_carried_error;
use crate::public_store::{InvalidStoredEnum, parse_stored_enum};
use pplx_space_sandbox_store::fork_source_lifecycle::lock_fork_source_lifecycle;

const DEFAULT_OPERATION_GC_DAYS: i64 = 7;

const DEFAULT_IDEMPOTENCY_TTL_HOURS: i64 = 24;
/// Advisory transaction lock that serializes dispatcher claims across
/// controller replicas so the keyed-claim rule sees every committed claim.
pub const PUBLIC_OPERATION_CLAIM_LOCK_KEY: i64 = 0x7370_6163_655f_6f70;

mod key_bindings;
pub(crate) use key_bindings::{KEY_BINDING_TTL_MINUTES, bind_idempotency_key};
mod responses;
pub(crate) use responses::ResponseAdmissionError;
mod response_reservations;
pub(crate) use response_reservations::ResponseReservation;
mod sandbox_quota;
pub use sandbox_quota::{
    SandboxQuota, SandboxQuotaOutcome, SandboxQuotaRefusal, SubjectSandboxQuota,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiProduct {
    Native,
    Admin,
}

impl ApiProduct {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Admin => "admin",
        }
    }
}

impl std::fmt::Display for ApiProduct {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ApiProduct {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "native" => Self::Native,
            "admin" => Self::Admin,
            _ => return Err(()),
        })
    }
}

impl serde::Serialize for ApiProduct {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpOperationMethod {
    Post,
    Patch,
    Delete,
}

impl HttpOperationMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Post => "POST",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationPathTemplate(&'static str);

impl OperationPathTemplate {
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublicOperationStoreError {
    #[error("stored public operation has invalid {field}: {value}")]
    InvalidStoredEnum { field: &'static str, value: String },
    #[error("stored public operation result is invalid: {0}")]
    InvalidStoredResult(#[source] serde_json::Error),
    #[error("stored fork operation result has a non-fork result type")]
    InvalidStoredForkResult,
    #[error("stored fork operation payload is invalid: {0}")]
    InvalidStoredForkPayload(#[source] crate::fork_operations::ForkPayloadDecodeError),
    #[error("stored idempotency fingerprint is invalid: {0}")]
    InvalidStoredFingerprint(String),
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
}

impl From<InvalidStoredEnum> for PublicOperationStoreError {
    fn from(error: InvalidStoredEnum) -> Self {
        Self::InvalidStoredEnum {
            field: error.field,
            value: error.value,
        }
    }
}

#[derive(Clone)]
pub struct PublicOperationStore {
    pool: PgPool,
}

#[derive(Clone, Debug)]
pub struct SandboxOperationEventCursor {
    pub created_at: OffsetDateTime,
    pub operation_id: OperationId,
    pub sequence: i64,
}

impl PublicOperationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn remaining_deadline(
        &self,
        operation_id: OperationId,
        hard_deadline: Duration,
    ) -> Result<Option<Duration>, PublicOperationStoreError> {
        let seconds = operation_store::operations::remaining_deadline_secs(
            &self.pool,
            operation_id,
            i64::try_from(hard_deadline.as_secs()).unwrap_or(i64::MAX),
        )
        .await?;
        Ok(seconds.map(|seconds| Duration::from_secs(u64::try_from(seconds).unwrap_or(0))))
    }

    /// How long ago the operation was first claimed, if it has been.
    pub async fn time_since_first_claim(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Duration>, PublicOperationStoreError> {
        let seconds =
            operation_store::operations::secs_since_first_claim(&self.pool, operation_id).await?;
        Ok(seconds.map(|seconds| Duration::from_secs_f64(seconds.max(0.0))))
    }

    pub async fn fetch_operation(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
    ) -> Result<Option<PublicOperationRecord>, PublicOperationStoreError> {
        let row = operation_store::operations::fetch(&self.pool, org_id, operation_id).await?;
        row.map(public_operation_record).transpose()
    }

    /// Queued creates have no sandbox row yet. Their durable actor is the
    /// authorization boundary, including after the target has been deleted.
    pub(crate) async fn operation_in_owner_scope(
        &self,
        org_id: OrgId,
        subject_id: Option<UserId>,
        operation_id: OperationId,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(operation_store::operations::in_owner_scope(
            &self.pool,
            org_id,
            subject_id,
            operation_id,
        )
        .await?)
    }

    /// Whether a create of `target` still queued or running is one the
    /// caller's owner scope can see, as [`Self::operation_in_owner_scope`]
    /// scopes it. Such a create may not have recorded its sandbox row yet.
    pub(crate) async fn sandbox_create_in_flight_in_owner_scope(
        &self,
        org_id: OrgId,
        subject_id: Option<UserId>,
        target: pplx_space_public_api::PublicSandboxId,
    ) -> Result<bool, PublicOperationStoreError> {
        for create in sandbox_creates_in_flight(&self.pool, org_id, target).await? {
            if self
                .operation_in_owner_scope(org_id, subject_id, create)
                .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// [`sandbox_creates_in_flight`], outside any transaction.
    pub(crate) async fn sandbox_creates_in_flight(
        &self,
        org_id: OrgId,
        target: pplx_space_public_api::PublicSandboxId,
    ) -> Result<Vec<OperationId>, sqlx::Error> {
        sandbox_creates_in_flight(&self.pool, org_id, target).await
    }

    /// A create retried while waiting for capacity can carry a newer grant.
    /// Do not change an executing claim or resurrect a terminal payload. The
    /// producer ordering is the same as sandbox_federated_service_grants.
    pub(crate) async fn renew_queued_native_create_grant(
        &self,
        org_id: OrgId,
        actor_user_id: UserId,
        operation_id: OperationId,
        context: &crate::fork_operations::ForkFederatedCreateContext,
    ) -> Result<(), PublicOperationStoreError> {
        let context = serde_json::to_value(context).expect("federated create context serializes");
        operation_store::operations::renew_queued_native_create_grant(
            &self.pool,
            org_id,
            actor_user_id,
            operation_id,
            context,
        )
        .await?;
        Ok(())
    }

    pub async fn fetch_fork_outcome(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
    ) -> Result<Option<ForkOperationOutcome>, PublicOperationStoreError> {
        let row =
            operation_store::operations::fork_outcome(&self.pool, org_id, operation_id).await?;
        row.map(|row| {
            let operation_store::operations::ForkOutcomeRow {
                state,
                result_json: result,
                terminal_error_code,
                terminal_error_json: error,
            } = row;
            let state = parse_stored_enum("state", &state)?;
            match state {
                OperationState::Succeeded => {
                    let result = result.map_or(serde_json::Value::Null, |value| value.0);
                    let results = match result {
                        serde_json::Value::Array(results) => results,
                        tagged => {
                            let result = serde_json::from_value::<PublicOperationResult>(tagged)
                                .map_err(PublicOperationStoreError::InvalidStoredResult)?;
                            let PublicOperationResult::Fork(result) = result else {
                                return Err(PublicOperationStoreError::InvalidStoredForkResult);
                            };
                            result.results
                        }
                    };
                    Ok(ForkOperationOutcome::Succeeded(serde_json::Value::Array(
                        results,
                    )))
                }
                OperationState::Failed | OperationState::Canceled | OperationState::Expired => {
                    Ok(ForkOperationOutcome::Failed {
                        code: terminal_error_code
                            .as_deref()
                            .map(|value| parse_stored_enum("terminal_error_code", value))
                            .transpose()?,
                        error: error
                            .map(|value| value.0)
                            .unwrap_or_else(|| serde_json::json!({})),
                    })
                }
                OperationState::Queued | OperationState::Running => {
                    Ok(ForkOperationOutcome::Pending)
                }
                // Written by a newer server during a rollout: this build cannot
                // tell whether it is terminal, so report it instead of guessing.
                OperationState::Other(other) => Err(PublicOperationStoreError::InvalidStoredEnum {
                    field: "state",
                    value: other,
                }),
            }
        })
        .transpose()
    }

    pub async fn fetch_fork_hard_deadline(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
    ) -> Result<Option<Duration>, PublicOperationStoreError> {
        let payload =
            operation_store::operations::fork_payload(&self.pool, org_id, operation_id).await?;

        payload
            .map(|payload| {
                let payload = crate::fork_operations::decode_fork_payload(payload.0)
                    .map_err(PublicOperationStoreError::InvalidStoredForkPayload)?;
                Ok(payload.hard_deadline())
            })
            .transpose()
    }

    /// The admin reads list every operation column and, for a sandbox
    /// create, the payload's `requested_by_user_id`, which only the admin
    /// record exposes. No other kind's payload defines that key, so they
    /// never read it. They also join the actor's current email, so a page of
    /// operations names its actors without a read per row.
    pub async fn fetch_operation_admin(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<AdminPublicOperationRecord>, PublicOperationStoreError> {
        let row = operation_store::operations::fetch_admin(&self.pool, operation_id).await?;

        row.map(admin_public_operation_record).transpose()
    }

    pub async fn list_operations_admin(
        &self,
        input: ListAdminPublicOperations,
    ) -> Result<Vec<AdminPublicOperationRecord>, PublicOperationStoreError> {
        let state = input.state.as_ref();
        let kind = input.kind;
        let target_type = input.target_type.as_ref();
        let rows = operation_store::operations::list_admin(
            &self.pool,
            operation_store::operations::AdminOperationFilter {
                org_id: input.org_id,
                state,
                kind,
                target_type,
                target_ids: input.target_id.as_deref().map(admin_target_id_forms),
                limit: i64::from(input.limit),
            },
        )
        .await?;

        rows.into_iter()
            .map(admin_public_operation_record)
            .collect()
    }

    pub async fn fetch_operation_events(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
        after_sequence: Option<i64>,
        limit: i64,
    ) -> Result<Option<Vec<PublicOperationEvent>>, PublicOperationStoreError> {
        let exists =
            operation_store::operations::exists_in_org(&self.pool, org_id, operation_id).await?;
        if !exists {
            return Ok(None);
        }

        let rows = operation_store::events::list_after_sequence(
            &self.pool,
            operation_id,
            after_sequence,
            limit,
        )
        .await?;

        rows.into_iter()
            .map(public_operation_event)
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// Events of the operations that target `sandbox_id`, forks of it
    /// included, which name it by either form; see [`sandbox_target_id_forms`].
    pub async fn fetch_sandbox_operation_events(
        &self,
        org_id: OrgId,
        sandbox_id: SandboxId,
        after: Option<&SandboxOperationEventCursor>,
        limit: i64,
    ) -> Result<Vec<PublicOperationEvent>, PublicOperationStoreError> {
        let target_ids = sandbox_target_id_forms(sandbox_id);
        let rows = if let Some(after) = after {
            operation_store::events::list_for_targets_after(
                &self.pool,
                org_id,
                &target_ids[..],
                operation_store::events::EventCursor {
                    created_at: after.created_at,
                    operation_id: after.operation_id.as_uuid(),
                    sequence: after.sequence,
                },
                limit,
            )
            .await?
        } else {
            operation_store::events::list_recent_for_targets(
                &self.pool,
                org_id,
                &target_ids[..],
                limit,
            )
            .await?
        };

        rows.into_iter()
            .map(public_operation_event)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn create_operation(
        &self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let mut tx = self.pool.begin().await?;
        let (result, superseded) = create_operation_tx(&mut tx, input).await?;
        tx.commit().await?;
        superseded.record();
        if matches!(result, CreatePublicOperationResult::Created(_)) {
            crate::metrics::operations::created(
                api_product.as_str(),
                kind.as_str(),
                target_type
                    .as_ref()
                    .map(OperationTargetType::as_str)
                    .unwrap_or("none"),
            );
        }
        Ok(result)
    }

    /// Create or join the one active sandbox delete for this tenant and target.
    ///
    /// The transaction-scoped advisory lock makes the lookup plus insert atomic
    /// without a partial index, which repository migration policy forbids.
    pub async fn create_or_join_active_sandbox_delete(
        &self,
        org_id: OrgId,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        self.create_or_join_active_sandbox_operation(
            org_id,
            OperationKind::DeleteSandbox,
            target_id,
            payload_json,
            traceparent,
        )
        .await
    }

    /// Create or join the one active sandbox wake for this tenant and target,
    /// as [`Self::create_or_join_active_sandbox_delete`] does for deletes. A
    /// queued or running wake joins, whoever asked for it, so many commands on
    /// one sandbox at rest share a single operation, unless another operation
    /// for the sandbox, such as a `:suspend`, is queued after it: then a new
    /// wake is queued behind that one. A caller's `:wake` joins it by the same
    /// rule while it is still queued
    /// ([`PublicOperationAdmission::create_or_join_active_wake`]).
    pub async fn create_or_join_active_sandbox_wake(
        &self,
        org_id: OrgId,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        self.create_or_join_active_sandbox_operation(
            org_id,
            OperationKind::WakeSandbox,
            target_id,
            payload_json,
            traceparent,
        )
        .await
    }

    /// [`Self::create_or_join_active_sandbox_wake`], except that a wake it
    /// creates is also claimed in the same transaction, for the caller to
    /// execute, when nothing else for the sandbox is queued or running and
    /// `offer` allows it (see
    /// [`PublicOperationAdmission::create_claimed_if_sole_active`], and its
    /// precondition). A joined wake is never claimed: whoever created it, or
    /// the dispatcher, runs it.
    pub async fn create_or_join_active_sandbox_wake_claimed_if_sole_active<P>(
        &self,
        org_id: OrgId,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
        offer: InlineClaimOffer<impl FnOnce() -> Option<P>>,
    ) -> Result<CreateInlinePublicOperationResult<P>, PublicOperationStoreError> {
        let kind = OperationKind::WakeSandbox;
        let (mut tx, result, superseded) = match self
            .join_or_insert_active_sandbox_operation(
                org_id,
                kind,
                target_id,
                payload_json,
                traceparent,
            )
            .await?
        {
            JoinedOrInserted::Joined(operation_id) => {
                return Ok(CreateInlinePublicOperationResult::NotCreated(
                    CreatePublicOperationResult::Existing(operation_id),
                ));
            }
            JoinedOrInserted::Inserted {
                tx,
                result,
                superseded,
            } => (tx, result, superseded),
        };
        let creation = CreationRecord {
            superseded,
            api_product: ApiProduct::Native,
            kind,
            target_type: Some(OperationTargetType::Sandbox),
        };
        let CreatePublicOperationResult::Created(operation_id) = result else {
            tx.commit().await?;
            creation.superseded.record();
            return Ok(CreateInlinePublicOperationResult::NotCreated(result));
        };
        let decision =
            claim_created_if_sole_active_tx(&mut tx, org_id, target_id, operation_id, offer)
                .await?;
        decision.record(tx, operation_id, creation).await
    }

    /// The system's (actorless) operation of `kind` on a sandbox, joining one
    /// already queued or running. The lock key includes the kind, so a delete
    /// and a wake for one sandbox never serialize on each other here; the
    /// dispatcher orders them.
    async fn create_or_join_active_sandbox_operation(
        &self,
        org_id: OrgId,
        kind: OperationKind,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        match self
            .join_or_insert_active_sandbox_operation(
                org_id,
                kind,
                target_id,
                payload_json,
                traceparent,
            )
            .await?
        {
            JoinedOrInserted::Joined(operation_id) => {
                Ok(CreatePublicOperationResult::Existing(operation_id))
            }
            JoinedOrInserted::Inserted {
                tx,
                result,
                superseded,
            } => {
                tx.commit().await?;
                superseded.record();
                if matches!(result, CreatePublicOperationResult::Created(_)) {
                    crate::metrics::operations::created(
                        ApiProduct::Native.as_str(),
                        kind.as_str(),
                        OperationTargetType::Sandbox.as_str(),
                    );
                }
                Ok(result)
            }
        }
    }

    /// [`Self::create_or_join_active_sandbox_operation`] up to its commit: a
    /// join commits here, and an insert is left in its transaction, holding
    /// the sandbox's locks, for the caller to claim or commit.
    async fn join_or_insert_active_sandbox_operation(
        &self,
        org_id: OrgId,
        kind: OperationKind,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
    ) -> Result<JoinedOrInserted, PublicOperationStoreError> {
        let mut tx = self.pool.begin().await?;
        lock_active_sandbox_operation(&mut tx, org_id, kind, target_id).await?;
        let existing = if kind == OperationKind::DeleteSandbox {
            operation_store::operations::earliest_active_on_sandbox(
                &mut *tx, org_id, kind, target_id,
            )
            .await?
        } else {
            joinable_active_sandbox_operation(&mut tx, org_id, kind, target_id, None)
                .await?
                .map(|joinable| joinable.operation_id)
        };
        if let Some(operation_id) = existing {
            tx.commit().await?;
            return Ok(JoinedOrInserted::Joined(OperationId::from_uuid(
                operation_id,
            )));
        }
        let (result, superseded) = create_operation_tx(
            &mut tx,
            CreatePublicOperation {
                org_id,
                // The lifecycle executor tells an ECW delete apart by this
                // missing actor; it keeps the snapshot-upload 409 for them.
                actor_user_id: None,
                api_product: ApiProduct::Native,
                kind,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(target_id),
                payload_json,
                idempotency: None,
                traceparent,
            },
        )
        .await?;
        Ok(JoinedOrInserted::Inserted {
            tx,
            result,
            superseded,
        })
    }

    pub async fn begin_operation_admission(
        &self,
        org_id: OrgId,
        api_product: ApiProduct,
        idempotency: &CreatePublicOperationIdempotency<'_>,
    ) -> Result<PublicOperationAdmission, PublicOperationStoreError> {
        let mut tx = self.pool.begin().await?;
        lock_idempotency_key(&mut tx, org_id, api_product, idempotency).await?;
        let observation = classify_idempotency_record(
            find_idempotency_record(&mut tx, org_id, api_product, idempotency).await?,
            idempotency,
        );
        Ok(PublicOperationAdmission {
            tx,
            observation,
            quota_lock_timeout: sandbox_quota::QUOTA_LOCK_TIMEOUT,
        })
    }

    pub async fn observe_operation_idempotency(
        &self,
        org_id: OrgId,
        api_product: ApiProduct,
        idempotency: &CreatePublicOperationIdempotency<'_>,
    ) -> Result<ObservePublicOperationIdempotency, PublicOperationStoreError> {
        let row = operation_store::idempotency::find_live(
            &self.pool,
            api_product.as_str(),
            idempotency.store_key(org_id),
        )
        .await?
        .map(PublicIdempotencyRecord::try_from)
        .transpose()?;
        Ok(classify_idempotency_record(row, idempotency))
    }

    /// Create an operation and claim it in the same transaction for inline
    /// execution by the calling request handler.
    ///
    /// The atomic claim keeps the dispatcher from racing the caller on a
    /// fresh operation while preserving the durable-intent contract: the
    /// operation row exists before any external side effect, so if the
    /// caller's task dies mid-execution the claim goes stale and the
    /// cluster-controller dispatcher reclaims and re-drives the same
    /// idempotent operation to convergence.
    pub async fn create_claimed_operation(
        &self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreateClaimedPublicOperationResult, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();

        let mut tx = self.pool.begin().await?;
        let (created, superseded) = create_operation_tx(&mut tx, input).await?;
        let operation_id = match created {
            CreatePublicOperationResult::Created(operation_id) => operation_id,
            CreatePublicOperationResult::Existing(operation_id) => {
                tx.commit().await?;
                return Ok(CreateClaimedPublicOperationResult::Existing(operation_id));
            }
            CreatePublicOperationResult::IdempotencyConflict {
                operation_id,
                expected_fingerprint,
            } => {
                tx.commit().await?;
                return Ok(CreateClaimedPublicOperationResult::IdempotencyConflict {
                    operation_id,
                    expected_fingerprint,
                });
            }
            CreatePublicOperationResult::AdmissionDenied => {
                tx.commit().await?;
                return Ok(CreateClaimedPublicOperationResult::AdmissionDenied);
            }
        };
        let (claim_token, operation) = claim_created_operation_tx(&mut tx, operation_id).await?;
        tx.commit().await?;
        superseded.record();

        crate::metrics::operations::created(
            api_product.as_str(),
            kind.as_str(),
            target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
        crate::metrics::operations::claimed(api_product.as_str(), kind.as_str());
        Ok(CreateClaimedPublicOperationResult::Claimed {
            claim_token,
            operation,
        })
    }

    pub async fn claim_operations(
        &self,
        input: ClaimPublicOperations<'_>,
    ) -> Result<PublicOperationClaim, PublicOperationStoreError> {
        claim_operations(&self.pool, input, ClaimMode::Keyed).await
    }

    /// The claim before per-sandbox keying: plain request order with no
    /// key rule and no claim lock. Kept as the dispatcher's kill switch.
    pub async fn claim_operations_unkeyed(
        &self,
        input: ClaimPublicOperations<'_>,
    ) -> Result<PublicOperationClaim, PublicOperationStoreError> {
        claim_operations(&self.pool, input, ClaimMode::Unkeyed).await
    }

    pub async fn complete_claimed_operation(
        &self,
        input: CompleteClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        complete_claimed_operation(&self.pool, input).await
    }

    pub async fn renew_claimed_operation(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(operation_store::operations::renew_claim(
            &self.pool,
            operation.id,
            claim_token,
            operation.claim_generation,
        )
        .await?)
    }

    pub async fn fail_claimed_operation(
        &self,
        input: FailClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        fail_claimed_operation(&self.pool, input).await
    }

    /// Record an operation claimed for the first time as `expired` rather
    /// than run it: the terminal write [`Self::fail_claimed_operation`]
    /// makes, in the `expired` state, with an `operation.expired` event. The
    /// claim's attempt and start are undone, as the operation never ran. Its
    /// `operation.claimed` event stays, already committed with the claim and
    /// possibly read, so the history reads claimed, then expired.
    pub async fn expire_claimed_operation(
        &self,
        input: ExpirePublicOperation,
        claim_token: &str,
    ) -> Result<bool, PublicOperationStoreError> {
        expire_claimed_operation(&self.pool, input, claim_token).await
    }

    /// Expire up to `input.limit` of the oldest queued operations of
    /// `input.kinds` created more than `input.max_age` ago that have never
    /// started, whatever holds them back, recording for each the terminal
    /// error `expiry` builds. Returns the operations expired.
    pub async fn expire_overdue_queued_operations(
        &self,
        input: OverdueQueuedPublicOperations<'_>,
        expiry: impl Fn(&OverdueQueuedPublicOperation) -> ExpirePublicOperation,
    ) -> Result<Vec<OverdueQueuedPublicOperation>, PublicOperationStoreError> {
        expire_overdue_queued_operations(&self.pool, input, expiry).await
    }

    /// Requeue a claimed operation, or with `input.cancel_if_superseded`,
    /// cancel a create instead when a delete of its sandbox requested after
    /// it is still active, so the delete need not wait out the create's
    /// retries and the create never runs after it.
    pub async fn release_claimed_operation(
        &self,
        input: ReleaseClaimedPublicOperation,
    ) -> Result<ReleasedClaim, PublicOperationStoreError> {
        release_claimed_operation(&self.pool, input).await
    }

    pub async fn update_claimed_payload(
        &self,
        input: UpdateClaimedPublicOperationPayload,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(operation_store::operations::set_claimed_payload(
            &self.pool,
            input.operation_id,
            &input.claim_token,
            input.claim_generation,
            input.payload_json,
        )
        .await?)
    }

    pub async fn update_claimed_payload_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: UpdateClaimedPublicOperationPayload,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(operation_store::operations::set_claimed_payload(
            &mut **tx,
            input.operation_id,
            &input.claim_token,
            input.claim_generation,
            input.payload_json,
        )
        .await?)
    }

    pub async fn scrub_claimed_fork_federated_service_grant(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(operation_store::operations::scrub_claimed_payload_path(
            &self.pool,
            operation.id,
            claim_token,
            operation.claim_generation,
            crate::fork_operations::FORK_FEDERATED_SERVICE_GRANT_JSON_PATH,
        )
        .await?)
    }

    pub async fn gc_expired(
        &self,
        input: PublicOperationGcRequest,
    ) -> Result<PublicOperationGcReport, PublicOperationStoreError> {
        gc_expired(&self.pool, input).await
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ForkOperationOutcome {
    Pending,
    Succeeded(serde_json::Value),
    Failed {
        code: Option<PublicErrorCode>,
        error: serde_json::Value,
    },
}

#[derive(Debug, Clone)]
pub struct CreatePublicOperation<'a> {
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: ApiProduct,
    pub kind: OperationKind,
    pub target_type: Option<OperationTargetType>,
    pub target_id: Option<&'a str>,
    pub payload_json: serde_json::Value,
    pub idempotency: Option<CreatePublicOperationIdempotency<'a>>,
    /// The W3C traceparent of the request creating the operation, usually
    /// [`request_traceparent`], so each attempt to execute it links back to
    /// the request. A value that is not a valid traceparent is not stored.
    pub traceparent: Option<String>,
}

/// The current request's W3C traceparent, for a [`CreatePublicOperation`].
pub fn request_traceparent() -> Option<String> {
    pplx_space_observability::trace_propagation::current_traceparent()
        .filter(|traceparent| pplx_space_observability::is_storable_traceparent(traceparent))
}

#[derive(Debug, Clone)]
pub struct CreatePublicOperationIdempotency<'a> {
    pub method: HttpOperationMethod,
    pub path_template: OperationPathTemplate,
    pub key: &'a IdempotencyKey,
    pub request_fingerprint: &'a RequestFingerprint,
}

impl CreatePublicOperationIdempotency<'_> {
    /// This key as `org_id`'s record identity in the operation store.
    fn store_key(&self, org_id: OrgId) -> operation_store::idempotency::IdempotencyKeyRef<'_> {
        operation_store::idempotency::IdempotencyKeyRef {
            org_id,
            method: self.method.as_str(),
            path_template: self.path_template.as_str(),
            idempotency_key: self.key.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePublicOperationResult {
    Created(OperationId),
    Existing(OperationId),
    IdempotencyConflict {
        operation_id: Option<OperationId>,
        expected_fingerprint: RequestFingerprint,
    },
    /// A caller-specific admission check rejected the operation before it was
    /// inserted. Fork admission uses this when the source lifecycle lock cannot
    /// be acquired for a genuinely new operation.
    AdmissionDenied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservePublicOperationIdempotency {
    Missing,
    Existing(OperationId),
    Conflict {
        operation_id: Option<OperationId>,
        expected_fingerprint: RequestFingerprint,
    },
}

/// Claim an operation this transaction just created, as
/// [`PublicOperationStore::claim_operations`] would: `running`, one attempt,
/// a fresh claim token and generation, and a `claimed` event. Committed with
/// the create, so no other claimant ever sees the operation queued.
///
/// The claim is stamped when it is taken, not at the transaction's start:
/// the transaction may have waited on idempotency, quota, join and queue
/// locks first, and a claim stamped before that wait could already be stale
/// to the dispatcher when it commits.
async fn claim_created_operation_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: OperationId,
) -> Result<(String, ClaimedPublicOperation), PublicOperationStoreError> {
    let claim_token = new_admission_claim_token();
    let row =
        operation_store::operations::claim_created(&mut **tx, operation_id, &claim_token).await?;
    let operation = ClaimedPublicOperation::try_from(row)?;
    append_operation_event_tx(
        tx,
        operation.id,
        PublicOperationEventType::Claimed,
        serde_json::json!({
            "kind": operation.kind.as_str(),
            "attempt": operation.attempts,
        }),
    )
    .await?;
    Ok((claim_token, operation))
}

/// [`claim_created_operation_tx`] for `operation_id`, an operation of the
/// sandbox `target_id` this transaction just created, only if nothing else
/// for the sandbox is queued (due or not) or running (under a live claim or
/// a stale one), and the organization is under `org_limit`. Otherwise it
/// stays queued for the dispatcher, behind them.
///
/// Its insert holds the sandbox's queue lock until this transaction commits,
/// so what this statement reads is all there is for the sandbox: an insert
/// committed earlier is visible, and a later one waits for the lock. Rows
/// leave queued or running only for a terminal state, which never reverts.
/// So when nothing else is active here, the dispatcher has nothing on the
/// sandbox it could claim beside this one, and any of its claims that sees a
/// later operation for the sandbox also sees this claim live and holds that
/// operation back. It counts every kind in the sandbox's queue
/// ([`OUTSIDE_SANDBOX_QUEUE_KINDS`]), not only the ones a claim may take.
///
/// Sound only while every process that inserts operations takes the
/// sandbox's queue lock and the dispatcher claims keyed. An API process on a
/// release without the lock can insert for the sandbox unseen by this check
/// and commit before it, and the dispatcher then claims that operation
/// beside this one; the unkeyed claim (the dispatcher's kill switch) holds
/// nothing back behind a live claim. Callers must not claim inline
/// otherwise: a route that adopts this stays off until every API process,
/// on every surface, runs a release that takes the lock, and is turned off
/// before the keyed claim is.
///
/// The organization limit is applied as [`claim_created_within_org_limit_tx`]
/// applies it.
async fn claim_created_if_sole_active_tx<P>(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    target_id: &str,
    operation_id: OperationId,
    offer: InlineClaimOffer<impl FnOnce() -> Option<P>>,
) -> Result<InlineClaimDecision<P>, PublicOperationStoreError> {
    let others_active = operation_store::operations::other_active_on_sandbox(
        &mut **tx,
        org_id,
        target_id,
        OUTSIDE_SANDBOX_QUEUE_KINDS,
        operation_id,
    )
    .await?;
    if others_active {
        return Ok(InlineClaimDecision::Queued(InlineQueueReason::Busy));
    }
    claim_created_within_org_limit_tx(tx, org_id, operation_id, offer).await
}

/// [`claim_created_operation_tx`] for `operation_id`, an operation this
/// transaction just created, under the permit `offer` acquires, unless it
/// acquires none or `org_id` is at the offer's organization limit; then it
/// stays queued for the dispatcher. Every admission that claims for inline
/// execution claims through here, so the limit holds on all of them, and
/// none holds a permit before it gets here.
///
/// The limit is soft: it counts claims committed before this statement's
/// snapshot (see [`inline_org_at_limit_tx`]).
async fn claim_created_within_org_limit_tx<P>(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    operation_id: OperationId,
    offer: InlineClaimOffer<impl FnOnce() -> Option<P>>,
) -> Result<InlineClaimDecision<P>, PublicOperationStoreError> {
    let Some(permit) = (offer.acquire)() else {
        return Ok(InlineClaimDecision::Queued(InlineQueueReason::NoPermit));
    };
    if let Some(org_limit) = offer.org_limit
        && inline_org_at_limit_tx(tx, org_id, org_limit).await?
    {
        return Ok(InlineClaimDecision::Queued(InlineQueueReason::OrgLimit));
    }
    let (claim_token, operation) = claim_created_operation_tx(tx, operation_id).await?;
    Ok(InlineClaimDecision::Claimed(Box::new(InlineClaimed {
        claim_token,
        operation,
        permit,
    })))
}

/// What admission needs to claim an operation it creates for the caller to
/// execute inline.
pub struct InlineClaimOffer<F> {
    /// The organization limit the claim is held to.
    pub org_limit: Option<InlineOrgLimit>,
    /// Takes the caller's permit to execute the operation, called only once
    /// the operation is created and nothing else for its sandbox holds it
    /// back; `None` leaves it queued ([`InlineQueueReason::NoPermit`]). A
    /// request that joins an operation, or waits for the sandbox's locks,
    /// holds no permit meanwhile.
    pub acquire: F,
}

/// Whether [`claim_created_within_org_limit_tx`] claimed an operation.
enum InlineClaimDecision<P> {
    Claimed(Box<InlineClaimed<P>>),
    Queued(InlineQueueReason),
}

impl<P> InlineClaimDecision<P> {
    /// Hand a claimed `operation_id` back in its transaction, uncommitted;
    /// commit one left queued, and count its creation.
    async fn record(
        self,
        tx: Transaction<'static, Postgres>,
        operation_id: OperationId,
        creation: CreationRecord,
    ) -> Result<CreateInlinePublicOperationResult<P>, PublicOperationStoreError> {
        match self {
            Self::Claimed(claimed) => Ok(CreateInlinePublicOperationResult::Claimed(InlineClaim(
                Box::new(InlineClaimParts {
                    tx,
                    claimed: *claimed,
                    creation,
                }),
            ))),
            Self::Queued(reason) => {
                tx.commit().await?;
                creation.record();
                Ok(CreateInlinePublicOperationResult::Queued(
                    operation_id,
                    reason,
                ))
            }
        }
    }
}

/// What [`PublicOperationStore::join_or_insert_active_sandbox_operation`]
/// did.
enum JoinedOrInserted {
    /// Joined this operation, already queued or running; committed.
    Joined(OperationId),
    /// Recorded `result` in `tx`, not yet committed.
    Inserted {
        tx: Transaction<'static, Postgres>,
        result: CreatePublicOperationResult,
        superseded: SupersededCreates,
    },
}

/// What to count once the transaction that created an operation commits.
struct CreationRecord {
    superseded: SupersededCreates,
    api_product: ApiProduct,
    kind: OperationKind,
    target_type: Option<OperationTargetType>,
}

impl CreationRecord {
    fn record(self) {
        self.superseded.record();
        crate::metrics::operations::created(
            self.api_product.as_str(),
            self.kind.as_str(),
            self.target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
    }
}

/// An operation created and claimed for inline execution, with the caller's
/// permit to execute it, in a transaction not yet committed.
///
/// Commit it ([`Self::commit`]) in the task that executes it, not in the
/// request: a request dropped once the commit is sent, before the execution
/// starts, would leave a committed claim that nothing runs or renews, and
/// the sandbox's other work held back behind it until it goes stale.
/// Dropped uncommitted, the transaction rolls back and nothing was created.
pub struct InlineClaim<P>(Box<InlineClaimParts<P>>);

struct InlineClaimParts<P> {
    tx: Transaction<'static, Postgres>,
    claimed: InlineClaimed<P>,
    creation: CreationRecord,
}

impl<P> InlineClaim<P> {
    pub fn operation(&self) -> &ClaimedPublicOperation {
        &self.0.claimed.operation
    }

    /// Commit the operation and its claim. The caller then owns this attempt
    /// and must record its outcome under the claim token; if the caller
    /// dies, the claim goes stale and the dispatcher re-drives it.
    pub async fn commit(self) -> Result<InlineClaimed<P>, PublicOperationStoreError> {
        let InlineClaimParts {
            tx,
            claimed,
            creation,
        } = *self.0;
        tx.commit().await?;
        creation.record();
        Ok(claimed)
    }
}

impl<P> std::fmt::Debug for InlineClaim<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineClaim")
            .field("operation_id", &self.operation().id)
            .finish_non_exhaustive()
    }
}

/// An inline claim: its admission claim token, the operation it claimed and
/// the caller's permit to execute it.
#[derive(Debug)]
pub struct InlineClaimed<P> {
    pub claim_token: String,
    pub operation: ClaimedPublicOperation,
    pub permit: P,
}

/// The most operations one organization may have claimed for inline
/// execution at once, across every API process, beyond which admission
/// leaves new ones queued for the dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlineOrgLimit {
    pub limit: NonZeroU32,
    /// The claims counted are the live ones: a claim not renewed within
    /// this long belongs to a claimant that died, and the dispatcher takes
    /// its operation over.
    pub claim_stale_after: Duration,
}

/// Whether `org_id` already has `limit` operations other than teardowns
/// running under a live admission claim, the claims processes take to
/// execute what they admit (warm-pool creates included). The dispatcher's
/// claims are not counted: its own per-organization limit covers those.
///
/// Soft: it reads only claims committed before this statement's snapshot, so
/// admissions for the organization that commit at the same moment can each
/// see room for one more and overshoot the limit by up to their number less
/// one. Each such admission holds an inline permit in its process, so the
/// overshoot is bounded by the permits free across the API processes.
///
/// Counted with
/// [`INLINE_ORG_AT_LIMIT`](operation_store::operations::INLINE_ORG_AT_LIMIT);
/// see there for what it reads.
async fn inline_org_at_limit_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    org_limit: InlineOrgLimit,
) -> Result<bool, sqlx::Error> {
    operation_store::operations::inline_org_at_limit(
        &mut **tx,
        org_id,
        i64::from(org_limit.limit.get()),
        ADMISSION_CLAIM_TOKEN_PREFIX,
        duration_secs_i64(org_limit.claim_stale_after),
        TEARDOWN_KINDS,
    )
    .await
}

/// Why admission left an operation it created queued instead of claiming it
/// for inline execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineQueueReason {
    /// Other work for its sandbox is queued or running.
    Busy,
    /// Its organization is at its [`InlineOrgLimit`].
    OrgLimit,
    /// The offer acquired no permit to execute it.
    NoPermit,
}

/// What [`PublicOperationAdmission::create_claimed_if_sole_active`] or
/// [`PublicOperationAdmission::create_claimed_or_roll_back`] recorded.
#[derive(Debug)]
pub enum CreateInlinePublicOperationResult<P = ()> {
    /// Created and claimed, with an admission claim token, under the offer's
    /// permit; not yet committed (see [`InlineClaim`]).
    Claimed(InlineClaim<P>),
    /// Created and left queued for the dispatcher, for this reason.
    Queued(OperationId, InlineQueueReason),
    /// Nothing was created: a replay, a key conflict or an admission denial.
    NotCreated(CreatePublicOperationResult),
}

pub struct PublicOperationAdmission {
    tx: Transaction<'static, Postgres>,
    observation: ObservePublicOperationIdempotency,
    /// The longest a create waits for one quota lock:
    /// [`sandbox_quota::QUOTA_LOCK_TIMEOUT`]. Tests wait less.
    quota_lock_timeout: Duration,
}

impl PublicOperationAdmission {
    pub const fn observation(&self) -> &ObservePublicOperationIdempotency {
        &self.observation
    }

    pub(crate) async fn sandbox_belongs_to_org(
        &mut self,
        sandbox_id: SandboxId,
        org_id: OrgId,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(sandbox_store::get(&mut *self.tx, sandbox_id)
            .await
            .map_err(sqlx::Error::from)?
            .is_some_and(|sandbox| sandbox.org_id == org_id))
    }

    /// Whether a delete of `sandbox_id` names something of `org_id`'s: its
    /// sandbox row, or a create of it still queued or running, which records
    /// that row only when it executes.
    ///
    /// Each statement reads its own snapshot. A create that succeeds commits
    /// its row before it leaves queued or running, so one that finishes
    /// between the first two reads leaves a row the last one sees.
    pub(crate) async fn delete_target_belongs_to_org(
        &mut self,
        sandbox_id: pplx_space_public_api::PublicSandboxId,
        org_id: OrgId,
    ) -> Result<bool, PublicOperationStoreError> {
        let row = SandboxId::from(sandbox_id);
        if self.sandbox_belongs_to_org(row, org_id).await? {
            return Ok(true);
        }
        if !sandbox_creates_in_flight(&mut *self.tx, org_id, sandbox_id)
            .await?
            .is_empty()
        {
            return Ok(true);
        }
        self.sandbox_belongs_to_org(row, org_id).await
    }

    pub(crate) async fn lock_target_lifecycle(
        &mut self,
        target_id: SandboxId,
    ) -> Result<bool, PublicOperationStoreError> {
        lock_fork_source_lifecycle(&mut self.tx, target_id).await?;
        let exists = sandbox_store::get(&mut *self.tx, target_id)
            .await
            .map_err(sqlx::Error::from)?
            .is_some_and(|record| record.lifecycle != SandboxLifecycle::Deleting.as_str());
        Ok(exists)
    }

    /// Whether a memory drop holds the target sealed
    /// (`pplx_space_sandbox_store::memory_drop::sealed`). Read after
    /// [`Self::lock_target_lifecycle`], under the fork-source lock the drop's
    /// seal also takes, so either the seal committed first and this sees it, or
    /// this fork commits first and the seal sees it queued and refuses.
    pub(crate) async fn target_memory_drop_sealed(
        &mut self,
        target_id: SandboxId,
    ) -> Result<bool, PublicOperationStoreError> {
        pplx_space_sandbox_store::memory_drop::sealed(&mut *self.tx, target_id)
            .await
            .map_err(Into::into)
    }

    pub async fn create(
        mut self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let (result, superseded) = create_operation_tx(&mut self.tx, input).await?;
        self.tx.commit().await?;
        superseded.record();
        if matches!(result, CreatePublicOperationResult::Created(_)) {
            crate::metrics::operations::created(
                api_product.as_str(),
                kind.as_str(),
                target_type
                    .as_ref()
                    .map(OperationTargetType::as_str)
                    .unwrap_or("none"),
            );
        }
        Ok(result)
    }

    /// [`Self::create`], except that a newly created sandbox operation is
    /// also claimed in the same transaction, for the caller to execute
    /// inline, when nothing else for its sandbox is queued or running and
    /// its organization is under `org_limit`. Anything else is recorded as
    /// [`Self::create`] records it: queued for the dispatcher, or not at all
    /// for a replay or a conflict.
    ///
    /// The claim takes an admission claim token, which the dispatcher's
    /// per-organization slot limit does not count. It is sound only under
    /// the precondition on `claim_created_if_sole_active_tx`: every API
    /// process takes the queue lock, and the dispatcher claims keyed.
    pub async fn create_claimed_if_sole_active<P>(
        mut self,
        input: CreatePublicOperation<'_>,
        offer: InlineClaimOffer<impl FnOnce() -> Option<P>>,
    ) -> Result<CreateInlinePublicOperationResult<P>, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let org_id = input.org_id;
        let queue = sandbox_queue_target(&input);
        let (created, superseded) = create_operation_tx(&mut self.tx, input).await?;
        let operation_id = match created {
            CreatePublicOperationResult::Created(operation_id) => operation_id,
            other => {
                self.tx.commit().await?;
                return Ok(CreateInlinePublicOperationResult::NotCreated(other));
            }
        };
        let decision = match queue {
            Some(target_id) => {
                claim_created_if_sole_active_tx(
                    &mut self.tx,
                    org_id,
                    target_id,
                    operation_id,
                    offer,
                )
                .await?
            }
            // In no sandbox's queue, so nothing rules out other work on it.
            None => InlineClaimDecision::Queued(InlineQueueReason::Busy),
        };
        let creation = CreationRecord {
            superseded,
            api_product,
            kind,
            target_type,
        };
        decision.record(self.tx, operation_id, creation).await
    }

    /// [`Self::create`] for a caller's `:wake`, except that it joins a wake
    /// for the sandbox with nothing else for the sandbox queued behind it, as
    /// [`PublicOperationStore::create_or_join_active_sandbox_wake`] does: the
    /// caller's own wake, queued or running, or the system's while it is still
    /// queued. A `:wake` sent after a command then shares the command's wake,
    /// as one sent before it already did.
    ///
    /// A joined system wake becomes the caller's, so the claim that takes it
    /// reads them as its actor and runs it as their `:wake`, not as a command
    /// wake under the sandbox's wake policy, and their owner scope can read
    /// it. One already claimed is not joined: its attempt runs by the actor
    /// its claim read, and a policy narrowed since the command could refuse it.
    /// The hand-over locks the row and re-checks that the wake is still
    /// queued, so of a claim and a hand-over that race, whichever commits
    /// second sees the other; a hand-over that loses queues the caller's own
    /// wake instead.
    ///
    /// The caller's key records the joined wake, so a replay answers with it.
    /// A key this admission did not observe missing goes through
    /// [`Self::create`], which answers the replay or the conflict.
    pub(crate) async fn create_or_join_active_wake(
        mut self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        if let Some(operation_id) = self.join_active_wake(&input).await? {
            self.tx.commit().await?;
            return Ok(CreatePublicOperationResult::Existing(operation_id));
        }
        self.create(input).await
    }

    /// [`Self::create_or_join_active_wake`], except that a wake it creates
    /// rather than joins goes through [`Self::create_claimed_if_sole_active`].
    /// A joined wake answers `NotCreated(Existing(..))` and is never claimed,
    /// so a caller never takes over a system wake an inline claim already
    /// runs: it is not queued any more, so the caller's wake is created
    /// behind it, left queued since the sandbox is busy, and completes as
    /// already done once that wake has placed the sandbox.
    pub(crate) async fn create_or_join_active_wake_claimed_if_sole_active<P>(
        mut self,
        input: CreatePublicOperation<'_>,
        offer: InlineClaimOffer<impl FnOnce() -> Option<P>>,
    ) -> Result<CreateInlinePublicOperationResult<P>, PublicOperationStoreError> {
        if let Some(operation_id) = self.join_active_wake(&input).await? {
            self.tx.commit().await?;
            return Ok(CreateInlinePublicOperationResult::NotCreated(
                CreatePublicOperationResult::Existing(operation_id),
            ));
        }
        self.create_claimed_if_sole_active(input, offer).await
    }

    /// The wake [`Self::create_or_join_active_wake`] joins for `input`, with
    /// the caller's key recorded for it in this transaction, which the caller
    /// commits; `None` when there is none to join and the wake is to be
    /// created.
    async fn join_active_wake(
        &mut self,
        input: &CreatePublicOperation<'_>,
    ) -> Result<Option<OperationId>, PublicOperationStoreError> {
        let request = (
            input.kind,
            input.target_id,
            input.actor_user_id,
            input.idempotency.as_ref(),
        );
        debug_assert!(
            matches!(
                request,
                (OperationKind::WakeSandbox, Some(_), Some(_), Some(_))
            ),
            "only a caller's keyed sandbox wake can join one"
        );
        let (OperationKind::WakeSandbox, Some(target_id), Some(actor_user_id), Some(idempotency)) =
            request
        else {
            return Ok(None);
        };
        if !matches!(self.observation, ObservePublicOperationIdempotency::Missing) {
            return Ok(None);
        }
        lock_active_sandbox_operation(&mut self.tx, input.org_id, input.kind, target_id).await?;
        let Some(joinable) = joinable_active_sandbox_operation(
            &mut self.tx,
            input.org_id,
            input.kind,
            target_id,
            Some(actor_user_id),
        )
        .await?
        else {
            return Ok(None);
        };
        if joinable.system {
            let handed_over = operation_store::operations::hand_over_queued_system_operation(
                &mut *self.tx,
                joinable.operation_id,
                actor_user_id,
            )
            .await?;
            if !handed_over {
                return Ok(None);
            }
        }
        let operation_id = OperationId::from_uuid(joinable.operation_id);
        delete_expired_idempotency_record_tx(
            &mut self.tx,
            input.org_id,
            input.api_product,
            idempotency.method,
            idempotency.path_template,
            idempotency.key,
        )
        .await?;
        insert_idempotency_record_tx(
            &mut self.tx,
            input.org_id,
            input.api_product,
            idempotency,
            operation_id,
        )
        .await?;
        Ok(Some(operation_id))
    }

    pub async fn finish(self) -> Result<(), PublicOperationStoreError> {
        self.tx.commit().await?;
        Ok(())
    }

    /// The admission's transaction, for work that must commit only with a
    /// newly created operation (an admission-time warm-pool claim). Use with
    /// [`Self::create_or_roll_back`].
    pub(crate) fn transaction(&mut self) -> &mut Transaction<'static, Postgres> {
        &mut self.tx
    }

    /// [`Self::create`], except that only a `Created` result commits. Any
    /// other result rolls the transaction back, taking with it whatever the
    /// caller did through [`Self::transaction`].
    pub(crate) async fn create_or_roll_back(
        mut self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let (result, superseded) = create_operation_tx(&mut self.tx, input).await?;
        if !matches!(result, CreatePublicOperationResult::Created(_)) {
            self.tx.rollback().await?;
            return Ok(result);
        }
        self.tx.commit().await?;
        superseded.record();
        crate::metrics::operations::created(
            api_product.as_str(),
            kind.as_str(),
            target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
        Ok(result)
    }

    /// [`Self::create_or_roll_back`], except that a newly created operation
    /// is also claimed in the same transaction, for the caller to execute
    /// inline, when `offer` acquires a permit and its organization is under
    /// the offer's limit; otherwise it commits queued for the dispatcher.
    /// Nothing is created for a replay or a conflict, and both roll back, as
    /// there.
    pub(crate) async fn create_claimed_or_roll_back<P>(
        mut self,
        input: CreatePublicOperation<'_>,
        offer: InlineClaimOffer<impl FnOnce() -> Option<P>>,
    ) -> Result<CreateInlinePublicOperationResult<P>, PublicOperationStoreError> {
        let org_id = input.org_id;
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let (created, superseded) = create_operation_tx(&mut self.tx, input).await?;
        let CreatePublicOperationResult::Created(operation_id) = created else {
            self.tx.rollback().await?;
            return Ok(CreateInlinePublicOperationResult::NotCreated(created));
        };
        let decision =
            claim_created_within_org_limit_tx(&mut self.tx, org_id, operation_id, offer).await?;
        let creation = CreationRecord {
            superseded,
            api_product,
            kind,
            target_type,
        };
        decision.record(self.tx, operation_id, creation).await
    }
}

#[derive(Debug, Clone)]
pub enum CreateClaimedPublicOperationResult {
    /// Created and claimed; the caller owns this attempt and must persist
    /// its outcome (complete/fail/release) under `claim_token`.
    Claimed {
        claim_token: String,
        operation: ClaimedPublicOperation,
    },
    /// An idempotent replay matched an existing operation; nothing was
    /// claimed.
    Existing(OperationId),
    IdempotencyConflict {
        operation_id: Option<OperationId>,
        expected_fingerprint: RequestFingerprint,
    },
    /// The caller marked a genuinely new operation inadmissible; nothing was
    /// inserted or claimed.
    AdmissionDenied,
}

#[derive(Debug, Clone)]
pub struct ClaimPublicOperations<'a> {
    pub limits: ClaimLimits,
    pub claim_stale_after: Duration,
    pub claimable_kinds: &'a [OperationKind],
}

/// Sandbox deletes and kills: operations that tear down a sandbox's VM and
/// so free capacity, which the dispatcher keeps execution slots for.
pub const TEARDOWN_KINDS: &[OperationKind] =
    &[OperationKind::DeleteSandbox, OperationKind::KillSandbox];

/// How many operations one claim may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimLimits {
    /// Operations of any kind.
    pub total: u32,
    /// Of those, operations other than teardowns ([`TEARDOWN_KINDS`]), so
    /// execution slots kept for teardowns are not taken by other work.
    pub non_teardown: u32,
    /// Of those, teardowns while this claim also picks other work (at least
    /// one row under `non_teardown`), so execution slots kept for other work
    /// are not taken by teardowns. When the other pick is empty, including
    /// when `non_teardown` is 0, teardowns may take all `total`.
    pub teardown_while_other_due: u32,
    /// The most operations other than teardowns that one organization may
    /// have running under a live claim, counting those it already has,
    /// across every dispatcher. One claimed as it was created, which the
    /// process that created it runs, is not counted. `None` sets no such
    /// limit.
    pub per_org_non_teardown: Option<OrgSlotLimit>,
}

/// How many operations other than teardowns one organization may have
/// running at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrgSlotLimit {
    /// While another organization is waiting: has such work due and holds
    /// fewer than this many.
    pub contended: NonZeroU32,
    /// While no other organization is waiting, organizations may go past
    /// `contended`, but the claim leaves this many of its `non_teardown`
    /// limit, the free slots open to other work, untaken by operations past
    /// it, so work another organization submits later can start.
    pub headroom: u32,
}

impl ClaimLimits {
    /// Up to `total` operations, of any kinds.
    pub const fn any(total: u32) -> Self {
        Self {
            total,
            non_teardown: total,
            teardown_while_other_due: total,
            per_org_non_teardown: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListAdminPublicOperations {
    pub limit: u16,
    pub org_id: Option<OrgId>,
    pub state: Option<OperationState>,
    pub kind: Option<OperationKind>,
    pub target_type: Option<OperationTargetType>,
    pub target_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PublicOperationClaim {
    pub claim_token: String,
    pub operations: Vec<ClaimedPublicOperation>,
    /// The claim found due operations other than teardowns, and its
    /// organization limit held back every one of them, so until an execution
    /// ends or another organization's work arrives, claiming again takes
    /// none of them either. Work held back only by earlier work on its
    /// sandbox, or by a row another transaction has locked, does not count.
    pub org_limit_held_back: bool,
}

#[derive(Debug, Clone)]
pub struct ClaimedPublicOperation {
    pub id: OperationId,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: ApiProduct,
    pub kind: OperationKind,
    pub target_type: Option<OperationTargetType>,
    pub target_id: Option<String>,
    pub payload: serde_json::Value,
    pub attempts: i32,
    pub claim_generation: i64,
    /// The W3C traceparent of the request that created the operation, which
    /// each attempt links to. `None` for an operation created outside a
    /// traced request or before the column existed.
    pub traceparent: Option<String>,
    /// How many whole seconds ago the operation was created, by the database
    /// clock, when this claim, its first start, took it. `None` once the
    /// operation has started before, including a reclaimed stale claim.
    pub age_at_first_claim_secs: Option<u32>,
}

impl ClaimedPublicOperation {
    /// The canonical `sandbox_id` telemetry value, the bare UUID, of the
    /// sandbox the operation targets. Public operations name it by its
    /// `sbx_<uuid>` target id and a fork by its source's id in either form;
    /// a target that names no sandbox yields `None`, which leaves the field
    /// off the span or log line.
    pub fn sandbox_id(&self) -> Option<String> {
        let target_id = self.target_id.as_deref()?;
        let sandbox_id = match target_id.parse::<pplx_space_public_api::PublicSandboxId>() {
            Ok(public_id) => SandboxId::from(public_id),
            Err(_) => target_id.parse::<SandboxId>().ok()?,
        };
        Some(sandbox_id.to_string())
    }

    /// The work-queue key the dispatcher serializes on: the `sbx_<uuid>`
    /// target of an operation in a sandbox's queue, the one the queue lock
    /// is keyed by. A fork, and any operation that names no sandbox, has no
    /// key and never waits on another operation.
    pub fn work_key(&self) -> Option<&str> {
        sandbox_queue_key(
            self.target_type.as_ref(),
            self.kind,
            self.target_id.as_deref(),
        )
    }

    /// Whether this claim was the operation's first start.
    pub fn is_first_start(&self) -> bool {
        self.age_at_first_claim_secs.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct UpdateClaimedPublicOperationPayload {
    pub claim_token: String,
    pub operation_id: OperationId,
    pub claim_generation: i64,
    pub payload_json: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicOperationRequeueReason {
    TransientFailure,
    WorkflowProgress,
    ActivationBackpressure,
    /// The execution handed a retry wait back to the dispatcher instead of
    /// sleeping in its slot. Not a failure of its own: it spends no attempt
    /// and is bounded instead by the deferral window, wall-clock time from
    /// the operation's first claim.
    RetryDeferred,
}

impl PublicOperationRequeueReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransientFailure => "transient_failure",
            Self::WorkflowProgress => "workflow_progress",
            Self::ActivationBackpressure => "activation_backpressure",
            Self::RetryDeferred => "retry_deferred",
        }
    }

    pub const fn preserves_retry_budget(self) -> bool {
        matches!(
            self,
            Self::WorkflowProgress | Self::ActivationBackpressure | Self::RetryDeferred
        )
    }
}

impl std::fmt::Display for PublicOperationRequeueReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct CompleteClaimedPublicOperation {
    pub claim_token: String,
    pub operation_id: OperationId,
    pub progress_message: String,
    pub result_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct FailClaimedPublicOperation {
    pub claim_token: String,
    pub operation_id: OperationId,
    pub progress_message: String,
    pub terminal_error_code: PublicErrorCode,
    pub terminal_error_json: serde_json::Value,
}

/// The terminal record of an operation that passed its deadline before it
/// could run.
#[derive(Debug, Clone)]
pub struct ExpirePublicOperation {
    pub operation_id: OperationId,
    pub progress_message: String,
    pub terminal_error_code: PublicErrorCode,
    pub terminal_error_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct OverdueQueuedPublicOperations<'a> {
    pub api_product: ApiProduct,
    pub kinds: &'a [OperationKind],
    pub max_age: Duration,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct OverdueQueuedPublicOperation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub attempts: i32,
    pub progress_message: String,
}

#[derive(Debug, Clone)]
pub struct ReleaseClaimedPublicOperation {
    pub claim_token: String,
    pub operation_id: OperationId,
    pub progress_message: String,
    pub retry_after: Duration,
    /// Whether this release counts against the transient-error retry budget.
    /// `false` for backpressure re-queues (cluster admission 429): the claim's
    /// `attempts` increment is rolled back so a long limiter wait never
    /// exhausts the budget for a later ordinary error.
    pub count_attempt: bool,
    /// Whether the claim's start is undone too: `true` for a claim that was
    /// the operation's first start and handed it back without running it.
    /// The operation then still reads as never started, so it can expire
    /// unrun past its deadline.
    pub undo_first_start: bool,
    /// Whether a create ends `canceled` instead of requeued when a delete of
    /// its sandbox requested after it is still active. `false` for a create
    /// whose execution may still be running, since the delete would then
    /// no longer wait for it.
    pub cancel_if_superseded: bool,
    /// The attempt's error, recorded under the cancel's own fields if the
    /// release cancels a superseded create.
    pub carried_error: Option<serde_json::Value>,
}

/// What a release did with its claimed operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleasedClaim {
    /// Requeued, to be claimed again.
    Requeued,
    /// A create canceled instead, superseded by this delete of its sandbox.
    Superseded { delete_id: OperationId },
    /// The claim was lost; the operation is unchanged.
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicOperationGcRequest {
    pub max_records: u32,
    pub payload_scrub_rules: &'static [PublicOperationPayloadScrubRule],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicOperationPayloadScrubRule {
    pub kind: OperationKind,
    pub json_path: &'static [&'static str],
}

impl PublicOperationPayloadScrubRule {
    pub const fn new(kind: OperationKind, json_path: &'static [&'static str]) -> Self {
        Self { kind, json_path }
    }
}

/// Includes cancellations/expiry that never reach an executor's terminal write.
pub const FEDERATED_OPERATION_PAYLOAD_SCRUB_RULES: &[PublicOperationPayloadScrubRule] = &[
    PublicOperationPayloadScrubRule::new(
        OperationKind::ForkSandbox,
        crate::fork_operations::FORK_FEDERATED_SERVICE_GRANT_JSON_PATH,
    ),
    PublicOperationPayloadScrubRule::new(
        OperationKind::CreateSandbox,
        &["federated_create", "service_grant_base64"],
    ),
];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PublicOperationGcReport {
    pub operations_deleted: u64,
    pub idempotency_records_deleted: u64,
}

impl PublicOperationGcReport {
    pub fn total(self) -> u64 {
        self.operations_deleted
            .saturating_add(self.idempotency_records_deleted)
    }
}

/// Creates that a delete created in a transaction canceled, to be counted
/// once that transaction commits.
#[must_use]
#[derive(Debug, Default)]
pub struct SupersededCreates(Vec<ApiProduct>);

impl SupersededCreates {
    /// Count the canceled creates; call after the transaction commits.
    pub fn record(self) {
        for api_product in self.0 {
            crate::metrics::operations::completed(
                api_product.as_str(),
                OperationKind::CreateSandbox.as_str(),
                OperationState::Canceled.as_str(),
            );
        }
    }
}

pub async fn create_operation_tx(
    tx: &mut Transaction<'_, Postgres>,
    input: CreatePublicOperation<'_>,
) -> Result<(CreatePublicOperationResult, SupersededCreates), PublicOperationStoreError> {
    if let Some(idempotency) = input.idempotency.as_ref() {
        lock_idempotency_key(tx, input.org_id, input.api_product, idempotency).await?;
        if let Some(existing) =
            find_idempotency_record(tx, input.org_id, input.api_product, idempotency).await?
        {
            if existing.request_fingerprint == *idempotency.request_fingerprint
                && let Some(operation_id) = existing.operation_id
            {
                return Ok((
                    CreatePublicOperationResult::Existing(operation_id),
                    SupersededCreates::default(),
                ));
            }
            return Ok((
                CreatePublicOperationResult::IdempotencyConflict {
                    operation_id: existing.operation_id,
                    expected_fingerprint: existing.request_fingerprint,
                },
                SupersededCreates::default(),
            ));
        }
        delete_expired_idempotency_record_tx(
            tx,
            input.org_id,
            input.api_product,
            idempotency.method,
            idempotency.path_template,
            idempotency.key,
        )
        .await?;
    }

    if let Some(target_id) = sandbox_queue_target(&input) {
        lock_sandbox_queue(tx, input.org_id, target_id).await?;
    }
    let operation_id = OperationId::new();
    // Stamp `requested_at` at insert, not at the transaction's start: a lock
    // wait above or in the caller can outlast an operation for the same
    // target that commits first, and the dispatcher orders by this column.
    // Under the sandbox's queue lock no other insert for it commits between
    // this stamp and this transaction's commit, so its operations' request
    // order is their commit order.
    operation_store::operations::insert(
        &mut **tx,
        operation_store::operations::NewOperation {
            operation_id,
            org_id: input.org_id,
            actor_user_id: input.actor_user_id,
            api_product: input.api_product.as_str(),
            kind: input.kind,
            target_type: input.target_type.as_ref(),
            target_id: input.target_id,
            payload_json: input.payload_json,
            gc_after_days: DEFAULT_OPERATION_GC_DAYS,
            traceparent: input.traceparent.as_deref().filter(|traceparent| {
                pplx_space_observability::is_storable_traceparent(traceparent)
            }),
        },
    )
    .await?;

    append_operation_event_tx(
        tx,
        operation_id,
        PublicOperationEventType::Created,
        serde_json::json!({
                "kind": input.kind.as_str(),
                "state": OperationState::Queued.as_str(),
        }),
    )
    .await?;

    let mut superseded = SupersededCreates::default();
    if input.kind == OperationKind::DeleteSandbox
        && input.target_type == Some(OperationTargetType::Sandbox)
        && let Some(target_id) = input.target_id
    {
        superseded =
            cancel_creates_superseded_by_delete_tx(tx, input.org_id, target_id, operation_id)
                .await?;
    }

    if let Some(idempotency) = input.idempotency.as_ref() {
        insert_idempotency_record_tx(
            tx,
            input.org_id,
            input.api_product,
            idempotency,
            operation_id,
        )
        .await?;
    }

    Ok((
        CreatePublicOperationResult::Created(operation_id),
        superseded,
    ))
}

/// The creates of `target`, a sandbox of `org_id`, that are still queued or
/// running. A create can answer before its sandbox has a row, and one that
/// succeeds commits its row before it leaves queued or running.
async fn sandbox_creates_in_flight<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    org_id: OrgId,
    target: pplx_space_public_api::PublicSandboxId,
) -> Result<Vec<OperationId>, sqlx::Error> {
    let creates =
        operation_store::operations::sandbox_creates_in_flight(executor, org_id, target).await?;
    Ok(creates.into_iter().map(OperationId::from_uuid).collect())
}

/// The terminal record of a create canceled because a delete of its sandbox
/// was requested before it could finish.
fn superseded_create(
    create_id: OperationId,
    delete_id: OperationId,
    carried_error: Option<serde_json::Value>,
) -> (String, PublicErrorCode, serde_json::Value) {
    (
        "Superseded by a delete of the same sandbox requested before the create finished."
            .to_owned(),
        PublicErrorCode::OperationFailed,
        with_carried_error(
            serde_json::json!({
                "operation_id": create_id.to_string(),
                "kind": OperationKind::CreateSandbox.as_str(),
                "reason": "superseded_by_delete",
                "delete_operation_id": delete_id.to_string(),
            }),
            carried_error,
        ),
    )
}

/// Record `create_id`, whose row this transaction has locked, `canceled` as
/// superseded by `delete_id`.
async fn cancel_superseded_create_tx(
    tx: &mut Transaction<'_, Postgres>,
    create_id: OperationId,
    delete_id: OperationId,
    carried_error: Option<serde_json::Value>,
) -> Result<(), PublicOperationStoreError> {
    let (progress_message, code, error) = superseded_create(create_id, delete_id, carried_error);
    operation_store::operations::cancel_superseded_create(
        &mut **tx,
        create_id,
        &progress_message,
        code.as_str(),
        error.clone(),
    )
    .await?;
    append_operation_event_tx(
        tx,
        create_id,
        PublicOperationEventType::Canceled,
        serde_json::json!({
            "state": OperationState::Canceled.as_str(),
            "code": code.as_str(),
            "error": error,
        }),
    )
    .await?;
    Ok(())
}

/// Cancel the queued, unclaimed creates of `target_id` that `delete_id`, a
/// delete of the same sandbox just requested in this transaction,
/// supersedes, so it need not wait for them to run out their retries. A
/// create under a claim, live or stale, is left alone: its execution may be
/// building the sandbox, and it is canceled instead of requeued when its
/// attempt ends.
///
/// Every active create is locked first, waiting for a claim or a release
/// that holds one, so none slips past both cancels: a release that requeues
/// a create without seeing this delete has locked the row first, and this
/// then sees the requeued create once the release commits.
async fn cancel_creates_superseded_by_delete_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    target_id: &str,
    delete_id: OperationId,
) -> Result<SupersededCreates, PublicOperationStoreError> {
    let active =
        operation_store::operations::lock_active_creates_on_sandbox(tx, org_id, target_id).await?;
    let creates = active
        .into_iter()
        .filter(|create| create.unclaimed)
        .map(|create| (create.operation_id, create.api_product));
    let mut superseded = SupersededCreates::default();
    for (create, api_product) in creates {
        superseded
            .0
            .push(parse_stored_enum("api_product", &api_product)?);
        cancel_superseded_create_tx(tx, OperationId::from_uuid(create), delete_id, None).await?;
    }
    Ok(superseded)
}

/// The earliest delete of `create_id`'s sandbox requested after it that is
/// still active, if `create_id` is a sandbox create.
async fn superseding_delete_tx(
    tx: &mut Transaction<'_, Postgres>,
    create_id: OperationId,
) -> Result<Option<OperationId>, PublicOperationStoreError> {
    let delete = operation_store::operations::superseding_delete(&mut **tx, create_id).await?;
    Ok(delete.map(OperationId::from_uuid))
}

async fn insert_idempotency_record_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    api_product: ApiProduct,
    idempotency: &CreatePublicOperationIdempotency<'_>,
    operation_id: OperationId,
) -> Result<(), sqlx::Error> {
    operation_store::idempotency::insert_for_operation(
        &mut **tx,
        Uuid::now_v7(),
        api_product.as_str(),
        idempotency.store_key(org_id),
        idempotency.request_fingerprint.as_str(),
        operation_id.as_uuid(),
        DEFAULT_IDEMPOTENCY_TTL_HOURS,
    )
    .await
}

/// Serialize creating or joining the active `kind` operation for one sandbox.
/// Deletes keep the lock key they always had.
pub(crate) async fn lock_active_sandbox_operation(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
) -> Result<(), sqlx::Error> {
    let lock_key = if kind == OperationKind::DeleteSandbox {
        format!("{org_id}:{target_id}")
    } else {
        format!("{org_id}:{target_id}:{}", kind.as_str())
    };
    operation_store::operations::lock_sandbox_key(tx, &lock_key).await
}

/// Kinds that name a sandbox as their target but are not in its queue. A
/// fork names its source sandbox by the bare UUID, or, while
/// `SPACE_FORK_PUBLIC_TARGET_ID_ENABLED` is on, by the same `sbx_<uuid>` the
/// sandbox's queue uses, so only its kind keeps it out; it is ordered by the
/// source's lifecycle lock instead.
///
/// This is the one definition of a sandbox's queue: every operation that
/// targets a sandbox and is not of one of these kinds. [`sandbox_queue_key`]
/// reads it for the queue lock and the dispatcher's busy keys, and the keyed
/// claim (`blocked`) and [`claim_created_if_sole_active_tx`] bind it into
/// their SQL, so who serializes, who holds work back and who counts as other
/// work for the sandbox cannot drift apart.
const OUTSIDE_SANDBOX_QUEUE_KINDS: &[OperationKind] = &[OperationKind::ForkSandbox];

/// The sandbox whose queue an operation of `kind` on `target_id` joins: its
/// target when that names a sandbox, unless `kind` is one of
/// [`OUTSIDE_SANDBOX_QUEUE_KINDS`].
fn sandbox_queue_key<'a>(
    target_type: Option<&OperationTargetType>,
    kind: OperationKind,
    target_id: Option<&'a str>,
) -> Option<&'a str> {
    if target_type != Some(&OperationTargetType::Sandbox)
        || OUTSIDE_SANDBOX_QUEUE_KINDS.contains(&kind)
    {
        return None;
    }
    target_id
}

/// The sandbox whose queue `input` joins (see [`sandbox_queue_key`]).
fn sandbox_queue_target<'a>(input: &CreatePublicOperation<'a>) -> Option<&'a str> {
    sandbox_queue_key(input.target_type.as_ref(), input.kind, input.target_id)
}

/// Serialize inserting operations for one sandbox, until this transaction
/// commits.
///
/// Every insert of an operation into a sandbox's queue must go through
/// [`create_operation_tx`] and take this lock: the sandbox's request order is
/// then its commit order, and a transaction that claims the operation it
/// inserts ([`claim_created_if_sole_active_tx`]) relies on that to see
/// everything queued for the sandbox before it.
/// Dropping the lock, or inserting such an operation any other way, breaks
/// both. (The only other insert into `public_operations`,
/// `insert_completed_session_operation_tx`, records a session's operation
/// born `succeeded`, which joins no sandbox's queue.)
///
/// Every insert into a sandbox's queue takes this lock last, just before its
/// `INSERT`, so it waits for an earlier insert to commit and nothing checked
/// earlier in the transaction (idempotency, quota, a joinable operation) is
/// held up by it. Afterwards only row locks follow: a delete locks its
/// sandbox's active creates to cancel them. Claims, renewals, releases and
/// completions take row locks and never this lock, so no transaction waits
/// for it while holding a row an insert waits for.
async fn lock_sandbox_queue(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    target_id: &str,
) -> Result<(), sqlx::Error> {
    operation_store::operations::lock_sandbox_key(tx, &format!("{org_id}:{target_id}:queue")).await
}

/// The queued or running `kind` operation on a sandbox that a new request
/// joins, under [`lock_active_sandbox_operation`]. Only one that nothing else
/// in the sandbox's queue is queued behind, in the dispatcher's order (a fork
/// of the sandbox is outside its queue and does not count): a wake ahead
/// of a `:suspend` leaves the sandbox at rest, so the new wake is queued after
/// that instead. Only committed operations are seen: other kinds never take
/// this lock, so a `:suspend` still uncommitted here is missed, the new
/// request joins the earlier wake, and the suspend then sorts after it, so a
/// command finds the sandbox at rest. The system (`joiner` `None`) joins any
/// such operation. A caller joins only a native one: their own, or the
/// system's while it is still queued (see
/// [`PublicOperationAdmission::create_or_join_active_wake`]).
async fn joinable_active_sandbox_operation(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    kind: OperationKind,
    target_id: &str,
    joiner: Option<UserId>,
) -> Result<Option<JoinableOperation>, sqlx::Error> {
    operation_store::operations::joinable_on_sandbox(
        &mut **tx,
        org_id,
        kind,
        target_id,
        joiner,
        OUTSIDE_SANDBOX_QUEUE_KINDS,
    )
    .await
}

fn new_claim_token() -> String {
    format!("public-operation-{}", Uuid::now_v7())
}

/// How the claim token of an operation claimed as it is created begins. The
/// process that created it runs it, in no dispatcher slot, so the
/// per-organization slot limit does not count it.
const ADMISSION_CLAIM_TOKEN_PREFIX: &str = "public-operation-admission-";

fn new_admission_claim_token() -> String {
    format!("{ADMISSION_CLAIM_TOKEN_PREFIX}{}", Uuid::now_v7())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimMode {
    Keyed,
    Unkeyed,
}

async fn claim_operations(
    pool: &PgPool,
    input: ClaimPublicOperations<'_>,
    mode: ClaimMode,
) -> Result<PublicOperationClaim, PublicOperationStoreError> {
    if input.limits.total == 0 || input.claimable_kinds.is_empty() {
        return Ok(PublicOperationClaim {
            claim_token: new_claim_token(),
            operations: Vec::new(),
            org_limit_held_back: false,
        });
    }

    let claim_token = new_claim_token();
    let mut tx = pool.begin().await?;
    let keyed = mode == ClaimMode::Keyed;
    if keyed {
        // The key rule reads other rows' claim state from this statement's
        // snapshot, so overlapping claimers could both run one sandbox: one
        // takes a delete past a wake in backoff while the other, on an older
        // snapshot, takes that wake as it comes due. The dispatcher runs
        // behind leader election, so this only matters around a handover; a
        // busy lock makes this tick claim nothing rather than wait. The same
        // round trip turns off JIT, which the keyed claim's cost estimate can
        // trigger and which costs more than the query.
        let claim_lock_held =
            operation_store::claim_queue::try_lock_claims(&mut tx, PUBLIC_OPERATION_CLAIM_LOCK_KEY)
                .await?;
        if !claim_lock_held {
            tx.rollback().await?;
            return Ok(PublicOperationClaim {
                claim_token,
                operations: Vec::new(),
                org_limit_held_back: false,
            });
        }
    }
    // The claim's rules, and which rows it leaves locked, are
    // `claim_queue::claim_due`'s.
    let claimed = operation_store::claim_queue::claim_due(
        &mut tx,
        operation_store::claim_queue::DueClaim {
            total: i64::from(input.limits.total),
            claim_stale_after_secs: duration_secs_i64(input.claim_stale_after),
            claim_token: &claim_token,
            claimable_kinds: input.claimable_kinds,
            outside_queue_kinds: OUTSIDE_SANDBOX_QUEUE_KINDS,
            keyed,
            non_teardown: i64::from(input.limits.non_teardown.min(input.limits.total)),
            teardown_while_other_due: i64::from(
                input
                    .limits
                    .teardown_while_other_due
                    .min(input.limits.total),
            ),
            teardown_kinds: TEARDOWN_KINDS,
            per_org_non_teardown: input
                .limits
                .per_org_non_teardown
                .map_or(0, |limit| i64::from(limit.contended.get())),
            per_org_headroom: input
                .limits
                .per_org_non_teardown
                .map_or(0, |limit| i64::from(limit.headroom)),
            admission_claim_token_prefix: ADMISSION_CLAIM_TOKEN_PREFIX,
        },
    )
    .await?;

    let org_limit_held_back = claimed.org_limit_held_back;
    let mut operations = Vec::with_capacity(claimed.operations.len());
    let mut queue_waits = Vec::with_capacity(claimed.operations.len());
    for row in claimed.operations {
        let row = row?;
        let operation = ClaimedPublicOperation::try_from(row.operation)?;
        // A stale reclaim was already running, so its wait is not queue time.
        queue_waits.push((!row.reclaimed).then_some(row.queue_wait_ms));
        append_operation_event_tx(
            &mut tx,
            operation.id,
            PublicOperationEventType::Claimed,
            serde_json::json!({
                "kind": operation.kind.as_str(),
                "attempt": operation.attempts,
            }),
        )
        .await?;
        operations.push(operation);
    }
    tx.commit().await?;

    for (operation, wait_ms) in operations.iter().zip(queue_waits) {
        let Some(wait_ms) = wait_ms else { continue };
        let attempt = if operation.attempts > 1 {
            ClaimAttempt::Retry
        } else {
            ClaimAttempt::First
        };
        crate::metrics::operations::queue_wait(
            operation.api_product.as_str(),
            operation.kind.as_str(),
            attempt,
            wait_ms.max(0.0),
        );
    }

    Ok(PublicOperationClaim {
        claim_token,
        operations,
        org_limit_held_back,
    })
}

async fn complete_claimed_operation(
    pool: &PgPool,
    input: CompleteClaimedPublicOperation,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = operation_store::operations::complete_claimed(
        &mut *tx,
        input.operation_id,
        &input.claim_token,
        &input.progress_message,
        input.result_json.clone(),
    )
    .await?;

    if updated {
        append_operation_event_tx(
            &mut tx,
            input.operation_id,
            PublicOperationEventType::Completed,
            serde_json::json!({
                "state": OperationState::Succeeded.as_str(),
                "result": input.result_json,
            }),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(updated)
}

async fn fail_claimed_operation(
    pool: &PgPool,
    input: FailClaimedPublicOperation,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = operation_store::operations::fail_claimed(
        &mut *tx,
        input.operation_id,
        &input.claim_token,
        operation_store::operations::TerminalError {
            progress_message: &input.progress_message,
            code: input.terminal_error_code.as_str(),
            error: input.terminal_error_json.clone(),
        },
    )
    .await?;

    if updated {
        append_operation_event_tx(
            &mut tx,
            input.operation_id,
            PublicOperationEventType::Failed,
            serde_json::json!({
                "state": OperationState::Failed.as_str(),
                "code": input.terminal_error_code.as_str(),
                "error": input.terminal_error_json,
            }),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(updated)
}

async fn expire_claimed_operation(
    pool: &PgPool,
    input: ExpirePublicOperation,
    claim_token: &str,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = operation_store::operations::expire_claimed(
        &mut *tx,
        input.operation_id,
        claim_token,
        operation_store::operations::TerminalError {
            progress_message: &input.progress_message,
            code: input.terminal_error_code.as_str(),
            error: input.terminal_error_json.clone(),
        },
    )
    .await?;

    if updated {
        append_operation_event_tx(
            &mut tx,
            input.operation_id,
            PublicOperationEventType::Expired,
            expired_event(&input),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(updated)
}

fn expired_event(input: &ExpirePublicOperation) -> serde_json::Value {
    serde_json::json!({
        "state": OperationState::Expired.as_str(),
        "code": input.terminal_error_code.as_str(),
        "error": input.terminal_error_json,
    })
}

async fn expire_overdue_queued_operations(
    pool: &PgPool,
    input: OverdueQueuedPublicOperations<'_>,
    expiry: impl Fn(&OverdueQueuedPublicOperation) -> ExpirePublicOperation,
) -> Result<Vec<OverdueQueuedPublicOperation>, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    // Locked, so nothing claims them between this read and the write below.
    let rows = operation_store::operations::lock_overdue_queued(
        &mut tx,
        input.api_product.as_str(),
        input.kinds,
        duration_secs_i64(input.max_age),
        i64::from(input.limit),
    )
    .await?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(Vec::new());
    }
    let overdue = rows
        .into_iter()
        .map(|row| {
            Ok(OverdueQueuedPublicOperation {
                id: OperationId::from_uuid(row.operation_id),
                kind: parse_stored_enum("kind", &row.kind)?,
                attempts: row.attempts,
                progress_message: row.progress_message,
            })
        })
        .collect::<Result<Vec<_>, PublicOperationStoreError>>()?;
    let expiries = overdue.iter().map(&expiry).collect::<Vec<_>>();
    let ids = expiries
        .iter()
        .map(|expiry| expiry.operation_id.as_uuid())
        .collect::<Vec<_>>();
    let messages = expiries
        .iter()
        .map(|expiry| expiry.progress_message.clone())
        .collect::<Vec<_>>();
    let codes = expiries
        .iter()
        .map(|expiry| expiry.terminal_error_code.as_str().to_string())
        .collect::<Vec<_>>();
    let errors = expiries
        .iter()
        .map(|expiry| Json(expiry.terminal_error_json.clone()))
        .collect::<Vec<_>>();
    let events = expiries
        .iter()
        .map(|expiry| Json(expired_event(expiry)))
        .collect::<Vec<_>>();
    operation_store::operations::expire_each(
        &mut *tx,
        operation_store::operations::Expiries {
            operation_ids: &ids,
            progress_messages: &messages,
            terminal_error_codes: &codes,
            terminal_error_jsons: &errors,
        },
    )
    .await?;
    operation_store::events::append_to_each(
        &mut *tx,
        &ids,
        &events,
        PublicOperationEventType::Expired,
    )
    .await?;
    tx.commit().await?;
    Ok(overdue)
}

async fn release_claimed_operation(
    pool: &PgPool,
    input: ReleaseClaimedPublicOperation,
) -> Result<ReleasedClaim, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    // Locked before looking for a delete, so one committing meanwhile is
    // either seen below or, waiting on this row, sees the requeued create
    // and cancels it itself.
    let kind = operation_store::operations::claimed_kind_locked(
        &mut tx,
        input.operation_id,
        &input.claim_token,
    )
    .await?;
    let Some(kind) = kind else {
        tx.rollback().await?;
        return Ok(ReleasedClaim::Lost);
    };
    if input.cancel_if_superseded
        && kind == OperationKind::CreateSandbox.as_str()
        && let Some(delete_id) = superseding_delete_tx(&mut tx, input.operation_id).await?
    {
        cancel_superseded_create_tx(&mut tx, input.operation_id, delete_id, input.carried_error)
            .await?;
        tx.commit().await?;
        return Ok(ReleasedClaim::Superseded { delete_id });
    }
    let updated = operation_store::operations::requeue_claimed(
        &mut *tx,
        input.operation_id,
        &input.claim_token,
        operation_store::operations::Requeue {
            progress_message: &input.progress_message,
            retry_after_secs: input.retry_after.as_secs_f64(),
            count_attempt: input.count_attempt,
            undo_first_start: input.undo_first_start,
        },
    )
    .await?;

    if updated {
        append_operation_event_tx(
            &mut tx,
            input.operation_id,
            PublicOperationEventType::Requeued,
            serde_json::json!({
                "state": OperationState::Queued.as_str(),
                "retry_after_secs": duration_secs_i64(input.retry_after),
            }),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(if updated {
        ReleasedClaim::Requeued
    } else {
        ReleasedClaim::Lost
    })
}

async fn gc_expired(
    pool: &PgPool,
    input: PublicOperationGcRequest,
) -> Result<PublicOperationGcReport, PublicOperationStoreError> {
    if input.max_records == 0 {
        return Ok(PublicOperationGcReport::default());
    }

    let max_records = i64::from(input.max_records);
    let mut tx = pool.begin().await?;

    // Cancellation and expiry can happen without an active executor claim.
    // Operation owners register sensitive payload paths so terminal rows can
    // be scrubbed independently of row-retention expiry.
    for rule in input.payload_scrub_rules {
        operation_store::operations::scrub_finished_payload_path(
            &mut *tx,
            rule.kind,
            rule.json_path,
            max_records,
        )
        .await?;
    }

    let operation_ids = operation_store::operations::due_for_gc(&mut *tx, max_records).await?;

    let idempotency_rows =
        operation_store::idempotency::delete_expired_batch(&mut *tx, max_records).await?;

    let dependent_idempotency_rows =
        operation_store::idempotency::delete_for_operations(&mut *tx, &operation_ids).await?;

    let operation_rows =
        operation_store::operations::delete_by_ids(&mut *tx, &operation_ids).await?;

    tx.commit().await?;

    Ok(PublicOperationGcReport {
        operations_deleted: u64::try_from(operation_rows.len()).unwrap_or(u64::MAX),
        idempotency_records_deleted: u64::try_from(
            idempotency_rows
                .len()
                .saturating_add(dependent_idempotency_rows.len()),
        )
        .unwrap_or(u64::MAX),
    })
}

async fn find_idempotency_record(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    api_product: ApiProduct,
    idempotency: &CreatePublicOperationIdempotency<'_>,
) -> Result<Option<PublicIdempotencyRecord>, PublicOperationStoreError> {
    let row = operation_store::idempotency::lock_live(
        tx,
        api_product.as_str(),
        idempotency.store_key(org_id),
    )
    .await?;

    row.map(PublicIdempotencyRecord::try_from).transpose()
}

pub(crate) async fn delete_expired_idempotency_record_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    api_product: ApiProduct,
    method: HttpOperationMethod,
    path_template: OperationPathTemplate,
    key: &IdempotencyKey,
) -> Result<(), sqlx::Error> {
    // Lookup intentionally ignores expired reservations. Remove the matching
    // row in the transaction that inserts its successor, so retention cleanup
    // timing cannot turn a valid post-expiry request into a uniqueness
    // violation. A caller that performs the live-record lookup separately must
    // either hold the per-key advisory lock or treat a concurrently inserted
    // live record as the winner.
    operation_store::idempotency::delete_expired(
        &mut **tx,
        api_product.as_str(),
        operation_store::idempotency::IdempotencyKeyRef {
            org_id,
            method: method.as_str(),
            path_template: path_template.as_str(),
            idempotency_key: key.as_str(),
        },
    )
    .await?;
    Ok(())
}

fn classify_idempotency_record(
    existing: Option<PublicIdempotencyRecord>,
    idempotency: &CreatePublicOperationIdempotency<'_>,
) -> ObservePublicOperationIdempotency {
    match existing {
        None => ObservePublicOperationIdempotency::Missing,
        Some(existing) if existing.request_fingerprint == *idempotency.request_fingerprint => {
            match existing.operation_id {
                Some(operation_id) => ObservePublicOperationIdempotency::Existing(operation_id),
                None => ObservePublicOperationIdempotency::Conflict {
                    operation_id: None,
                    expected_fingerprint: existing.request_fingerprint,
                },
            }
        }
        Some(existing) => ObservePublicOperationIdempotency::Conflict {
            operation_id: existing.operation_id,
            expected_fingerprint: existing.request_fingerprint,
        },
    }
}

async fn lock_idempotency_key(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    api_product: ApiProduct,
    idempotency: &CreatePublicOperationIdempotency<'_>,
) -> Result<(), PublicOperationStoreError> {
    operation_store::idempotency::lock_key(
        tx,
        idempotency_lock_key(org_id, api_product, idempotency),
    )
    .await?;
    Ok(())
}

fn idempotency_lock_key(
    org_id: OrgId,
    api_product: ApiProduct,
    idempotency: &CreatePublicOperationIdempotency<'_>,
) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(org_id.as_uuid().as_bytes());
    hasher.update([0]);
    hasher.update(api_product.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(idempotency.method.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(idempotency.path_template.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(idempotency.key.as_str().as_bytes());
    let digest = hasher.finalize();
    let mut key = [0_u8; 8];
    key.copy_from_slice(
        digest
            .get(..8)
            .expect("sha256 digest should contain at least eight bytes"),
    );
    i64::from_be_bytes(key)
}

pub async fn append_operation_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: OperationId,
    event_type: PublicOperationEventType,
    event_json: serde_json::Value,
) -> Result<i64, sqlx::Error> {
    operation_store::events::append(&mut **tx, operation_id, event_type, event_json).await
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminPublicOperationRecord {
    pub id: OperationId,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    /// The email `actor_user_id`'s user has when the operation is read, so it
    /// follows a change of email. `None` when the operation has no actor,
    /// which includes once the user is deleted: that clears `actor_user_id`.
    /// Unlike an admin audit event's `actor_email`, a never-null copy taken
    /// when the request ran, it does not record who the actor was.
    pub actor_email: Option<String>,
    pub api_product: ApiProduct,
    pub kind: OperationKind,
    pub state: OperationState,
    pub target_type: Option<OperationTargetType>,
    pub target_id: Option<String>,
    /// For a sandbox create, the administrator who submitted it on behalf of
    /// `actor_user_id`, when the create recorded one. `None` for every other
    /// kind, for a create the owner requested, and for an admin create queued
    /// by an older API release or while `SPACE_ADMIN_CREATE_REQUESTED_BY_ENABLED`
    /// was off.
    pub requested_by_user_id: Option<UserId>,
    pub progress_message: String,
    pub terminal_error_code: Option<PublicErrorCode>,
    pub terminal_error: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub attempts: i32,
    #[serde(with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub next_attempt_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
struct PublicIdempotencyRecord {
    operation_id: Option<OperationId>,
    request_fingerprint: RequestFingerprint,
}

/// The `target_id` values an admin filter on `target_id` matches: the typed
/// string itself, and, when it parses as a sandbox id in either form, both
/// forms (see [`sandbox_target_id_forms`]).
/// The typed string stays even when it parses: a target that is not a
/// sandbox, such as a template whose name is 32 hex digits, is recorded as
/// typed, not in either canonical form.
fn admin_target_id_forms(target_id: &str) -> Vec<String> {
    let mut forms = vec![target_id.to_string()];
    let sandbox_id = target_id
        .parse::<pplx_space_public_api::PublicSandboxId>()
        .map(SandboxId::from)
        .or_else(|_| target_id.parse::<SandboxId>());
    if let Ok(sandbox_id) = sandbox_id {
        for form in sandbox_target_id_forms(sandbox_id) {
            if !forms.contains(&form) {
                forms.push(form);
            }
        }
    }
    forms
}

/// Both `target_id` forms an operation on `sandbox_id` may have recorded:
/// `sbx_<uuid>`, and the bare UUID a fork records while
/// `SPACE_FORK_PUBLIC_TARGET_ID_ENABLED` is off.
///
/// Matching the bare form can go one release after
/// `SPACE_FORK_PUBLIC_TARGET_ID_ENABLED` is on everywhere, plus the 7 days a
/// finished operation is kept: by then no row holds it. The fork readers in
/// `fork_operation_store`, `pplx_space_sandbox_store::fork_source_lifecycle`
/// and `fork_operation_executor` drop it at the same time.
fn sandbox_target_id_forms(sandbox_id: SandboxId) -> [String; 2] {
    [
        pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string(),
        sandbox_id.to_string(),
    ]
}

fn public_operation_record(
    row: operation_store::operations::PublicOperationRow,
) -> Result<PublicOperationRecord, PublicOperationStoreError> {
    Ok(PublicOperationRecord {
        id: OperationId::from_uuid(row.operation_id),
        kind: parse_stored_enum("kind", &row.kind)?,
        state: parse_stored_enum("state", &row.state)?,
        target_type: row
            .target_type
            .as_deref()
            .map(|value| parse_stored_enum("target_type", value))
            .transpose()?,
        target_id: row.target_id,
        progress_message: row.progress_message,
        terminal_error_code: row
            .terminal_error_code
            .as_deref()
            .map(|value| parse_stored_enum("terminal_error_code", value))
            .transpose()?,
        terminal_error: row.terminal_error_json.map(|value| value.0),
        result: row
            .result_json
            .map(|value| serde_json::from_value::<PublicOperationResult>(value.0))
            .transpose()
            .map_err(PublicOperationStoreError::InvalidStoredResult)?,
        created_at: row.created_at,
        started_at: row.started_at,
        completed_at: row.completed_at,
        updated_at: row.updated_at,
    })
}

fn admin_public_operation_record(
    row: operation_store::operations::AdminPublicOperationRow,
) -> Result<AdminPublicOperationRecord, PublicOperationStoreError> {
    let requested_by_user_id = row
        .requested_by_user_id
        .map(|value| {
            value
                .parse::<UserId>()
                .map_err(|_| PublicOperationStoreError::InvalidStoredEnum {
                    field: "requested_by_user_id",
                    value,
                })
        })
        .transpose()?;
    let actor_email = row.actor_email;
    let row = row.operation;
    Ok(AdminPublicOperationRecord {
        id: OperationId::from_uuid(row.operation_id),
        org_id: row.org_id,
        actor_user_id: row.actor_user_id,
        actor_email,
        api_product: parse_stored_enum("api_product", &row.api_product)?,
        kind: parse_stored_enum("kind", &row.kind)?,
        state: parse_stored_enum("state", &row.state)?,
        target_type: row
            .target_type
            .as_deref()
            .map(|value| parse_stored_enum("target_type", value))
            .transpose()?,
        target_id: row.target_id,
        requested_by_user_id,
        progress_message: row.progress_message,
        terminal_error_code: row
            .terminal_error_code
            .as_deref()
            .map(|value| parse_stored_enum("terminal_error_code", value))
            .transpose()?,
        terminal_error: row.terminal_error_json.map(|value| value.0),
        result: row.result_json.map(|value| value.0),
        attempts: row.attempts,
        requested_at: row.requested_at,
        next_attempt_at: row.next_attempt_at,
        created_at: row.created_at,
        started_at: row.started_at,
        completed_at: row.completed_at,
        updated_at: row.updated_at,
    })
}

fn public_operation_event(
    row: operation_store::events::PublicOperationEventRow,
) -> Result<PublicOperationEvent, PublicOperationStoreError> {
    Ok(PublicOperationEvent {
        operation_id: OperationId::from_uuid(row.operation_id),
        sequence: row.sequence,
        event_type: parse_stored_enum("event_type", &row.event_type)?,
        event: row.event_json.0,
        created_at: row.created_at,
    })
}

impl TryFrom<operation_store::idempotency::OperationKeyRow> for PublicIdempotencyRecord {
    type Error = PublicOperationStoreError;

    fn try_from(row: operation_store::idempotency::OperationKeyRow) -> Result<Self, Self::Error> {
        Ok(Self {
            operation_id: row.operation_id.map(OperationId::from_uuid),
            request_fingerprint: RequestFingerprint::parse(row.request_fingerprint.clone())
                .map_err(|_| {
                    PublicOperationStoreError::InvalidStoredFingerprint(row.request_fingerprint)
                })?,
        })
    }
}

impl TryFrom<ClaimedPublicOperationRow> for ClaimedPublicOperation {
    type Error = PublicOperationStoreError;

    fn try_from(row: ClaimedPublicOperationRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: OperationId::from_uuid(row.operation_id),
            org_id: row.org_id,
            actor_user_id: row.actor_user_id,
            api_product: parse_stored_enum("api_product", &row.api_product)?,
            kind: parse_stored_enum("kind", &row.kind)?,
            target_type: row
                .target_type
                .as_deref()
                .map(|value| parse_stored_enum("target_type", value))
                .transpose()?,
            target_id: row.target_id,
            payload: row.payload_json.0,
            attempts: row.attempts,
            claim_generation: row.claim_generation,
            traceparent: row.traceparent.filter(|traceparent| {
                pplx_space_observability::is_storable_traceparent(traceparent)
            }),
            age_at_first_claim_secs: row
                .age_at_first_claim_secs
                .map(|secs| u32::try_from(secs.max(0)).unwrap_or(u32::MAX)),
        })
    }
}

fn duration_secs_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod inline_org_limit_plan_tests;
#[cfg(test)]
pub(crate) mod plan_explain;
#[cfg(test)]
mod tests;
