use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_common_core::sql;
use std::str::FromStr;
use std::time::Duration;

use pplx_space_common_core::types::{IdempotencyKey, SandboxId, SandboxLifecycle};
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicErrorCode,
    PublicOperationEvent, PublicOperationEventType, PublicOperationRecord, PublicOperationResult,
    RequestFingerprint,
};
use pplx_space_sandbox_store as sandbox_store;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::public_store::{InvalidStoredEnum, parse_stored_enum};
use pplx_space_sandbox_store::fork_source_lifecycle::lock_fork_source_lifecycle;

const DEFAULT_OPERATION_GC_DAYS: i64 = 7;

fn update_claimed_payload_sql() -> &'static str {
    sql!(
        "UPDATE public_operations
         SET payload_json = $4, updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND claim_generation = $3
           AND state = 'running'"
    )
}
const DEFAULT_IDEMPOTENCY_TTL_HOURS: i64 = 24;

mod responses;
pub(crate) use responses::ResponseAdmissionError;

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
        let seconds = sqlx::query_scalar::<_, i64>(pplx_space_common_core::sql!(
            "SELECT GREATEST(
                 0,
                 FLOOR(EXTRACT(EPOCH FROM (
                     created_at + ($2::BIGINT * INTERVAL '1 second') - NOW()
                 )))::BIGINT
             )
             FROM public_operations
             WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .bind(i64::try_from(hard_deadline.as_secs()).unwrap_or(i64::MAX))
        .fetch_optional(&self.pool)
        .await?;
        Ok(seconds.map(|seconds| Duration::from_secs(u64::try_from(seconds).unwrap_or(0))))
    }

    pub async fn fetch_operation(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
    ) -> Result<Option<PublicOperationRecord>, PublicOperationStoreError> {
        let row = sqlx::query_as::<_, PublicOperationRow>(sql!(
            "SELECT *
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2"
        ))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        row.map(PublicOperationRecord::try_from).transpose()
    }

    /// Queued creates have no sandbox row yet. Their durable actor is the
    /// authorization boundary, including after the target has been deleted.
    pub(crate) async fn operation_in_owner_scope(
        &self,
        org_id: OrgId,
        subject_id: Option<UserId>,
        operation_id: OperationId,
    ) -> Result<bool, PublicOperationStoreError> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT EXISTS(
                 SELECT 1 FROM public_operations op
                 LEFT JOIN users actor ON actor.id = op.actor_user_id
                 WHERE op.org_id = $1 AND op.operation_id = $2
                   AND ($3::UUID IS NULL OR op.api_product = 'native')
                   AND (($3::UUID IS NOT NULL AND op.actor_user_id = $3)
                     OR ($3::UUID IS NULL AND
                         (op.actor_user_id IS NULL OR actor.owner_source = 'local')))
             )"
        ))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .bind(subject_id)
        .fetch_one(&self.pool)
        .await?)
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
        sqlx::query(sql!(
            "UPDATE public_operations
             SET payload_json = jsonb_set(payload_json, '{federated_create}', $4),
                 updated_at = NOW()
             WHERE org_id = $1 AND actor_user_id = $2 AND operation_id = $3
               AND api_product = 'native' AND kind = 'create_sandbox' AND state = 'queued'
               AND payload_json #>> '{federated_create,issuer}' = $4->>'issuer'
               AND payload_json #>> '{federated_create,org_id}' = $4->>'org_id'
               AND payload_json #>> '{federated_create,subject_id}' = $4->>'subject_id'
               AND ($4->>'service_grant_expires_at_unix')::BIGINT > EXTRACT(EPOCH FROM NOW())
               AND CASE
                   WHEN $4->>'service_grant_issued_at_unix' IS NOT NULL
                    AND payload_json #>> '{federated_create,service_grant_issued_at_unix}' IS NOT NULL
                   THEN ($4->>'service_grant_issued_at_unix')::BIGINT >
                     (payload_json #>> '{federated_create,service_grant_issued_at_unix}')::BIGINT
                   WHEN $4->>'service_grant_issued_at_unix' IS NOT NULL THEN TRUE
                   WHEN payload_json #>> '{federated_create,service_grant_issued_at_unix}' IS NOT NULL THEN FALSE
                   ELSE ($4->>'service_grant_expires_at_unix')::BIGINT >
                     (payload_json #>> '{federated_create,service_grant_expires_at_unix}')::BIGINT
               END"
        ))
        .bind(org_id)
        .bind(actor_user_id)
        .bind(operation_id.as_uuid())
        .bind(Json(context))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_fork_outcome(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
    ) -> Result<Option<ForkOperationOutcome>, PublicOperationStoreError> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                Option<Json<serde_json::Value>>,
                Option<String>,
                Option<Json<serde_json::Value>>,
            ),
        >(sql!(
            "SELECT state, result_json, terminal_error_code, terminal_error_json
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
        ))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(state, result, terminal_error_code, error)| {
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
        let payload = sqlx::query_scalar::<_, Json<serde_json::Value>>(sql!(
            "SELECT payload_json
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
        ))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        payload
            .map(|payload| {
                let payload = crate::fork_operations::decode_fork_payload(payload.0)
                    .map_err(PublicOperationStoreError::InvalidStoredForkPayload)?;
                Ok(payload.hard_deadline())
            })
            .transpose()
    }

    pub async fn fetch_operation_admin(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<AdminPublicOperationRecord>, PublicOperationStoreError> {
        let row = sqlx::query_as::<_, AdminPublicOperationRow>(sql!(
            "SELECT *
             FROM public_operations
             WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        row.map(AdminPublicOperationRecord::try_from).transpose()
    }

    pub async fn list_operations_admin(
        &self,
        input: ListAdminPublicOperations,
    ) -> Result<Vec<AdminPublicOperationRecord>, PublicOperationStoreError> {
        let state = input.state.as_ref().map(OperationState::as_str);
        let kind = input.kind.map(OperationKind::as_str);
        let target_type = input.target_type.as_ref().map(OperationTargetType::as_str);
        let rows = sqlx::query_as::<_, AdminPublicOperationRow>(sql!(
            "SELECT *
             FROM public_operations
             WHERE ($1::UUID IS NULL OR org_id = $1)
               AND ($2::TEXT IS NULL OR state = $2)
               AND ($3::TEXT IS NULL OR kind = $3)
               AND ($4::TEXT IS NULL OR target_type = $4)
               AND ($5::TEXT IS NULL OR target_id = $5)
             ORDER BY created_at DESC, operation_id DESC
             LIMIT $6"
        ))
        .bind(input.org_id)
        .bind(state)
        .bind(kind)
        .bind(target_type)
        .bind(input.target_id.as_deref())
        .bind(i64::from(input.limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(AdminPublicOperationRecord::try_from)
            .collect()
    }

    pub async fn fetch_operation_events(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
        after_sequence: Option<i64>,
        limit: i64,
    ) -> Result<Option<Vec<PublicOperationEvent>>, PublicOperationStoreError> {
        let exists = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS(
                 SELECT 1
                 FROM public_operations
                 WHERE org_id = $1 AND operation_id = $2
             )"
        ))
        .bind(org_id)
        .bind(operation_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        if !exists {
            return Ok(None);
        }

        let rows = sqlx::query_as::<_, PublicOperationEventRow>(sql!(
            "SELECT e.operation_id, e.sequence, e.event_type, e.event_json, e.created_at
             FROM public_operation_events e
             WHERE e.operation_id = $1
               AND e.sequence > COALESCE($2, 0)
             ORDER BY e.sequence ASC
             LIMIT $3"
        ))
        .bind(operation_id.as_uuid())
        .bind(after_sequence)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(PublicOperationEvent::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub async fn fetch_sandbox_operation_events(
        &self,
        org_id: OrgId,
        sandbox_id: &str,
        after: Option<&SandboxOperationEventCursor>,
        limit: i64,
    ) -> Result<Vec<PublicOperationEvent>, PublicOperationStoreError> {
        let rows = if let Some(after) = after {
            sqlx::query_as::<_, PublicOperationEventRow>(sql!(
                "SELECT e.operation_id, e.sequence, e.event_type, e.event_json, e.created_at
                 FROM public_operation_events e
                 JOIN public_operations o ON o.operation_id = e.operation_id
                 WHERE o.org_id = $1
                   AND o.target_type = 'sandbox'
                   AND o.target_id = $2
                   AND (e.created_at, e.operation_id, e.sequence) > ($3, $4, $5)
                 ORDER BY e.created_at ASC, e.operation_id ASC, e.sequence ASC
                 LIMIT $6"
            ))
            .bind(org_id)
            .bind(sandbox_id)
            .bind(after.created_at)
            .bind(after.operation_id.as_uuid())
            .bind(after.sequence)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, PublicOperationEventRow>(sql!(
                "SELECT operation_id, sequence, event_type, event_json, created_at
                 FROM (
                     SELECT e.operation_id, e.sequence, e.event_type, e.event_json, e.created_at
                     FROM public_operation_events e
                     JOIN public_operations o ON o.operation_id = e.operation_id
                     WHERE o.org_id = $1
                       AND o.target_type = 'sandbox'
                       AND o.target_id = $2
                     ORDER BY e.created_at DESC, e.operation_id DESC, e.sequence DESC
                     LIMIT $3
                 ) recent
                 ORDER BY created_at ASC, operation_id ASC, sequence ASC"
            ))
            .bind(org_id)
            .bind(sandbox_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(PublicOperationEvent::try_from)
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
        let result = create_operation_tx(&mut tx, input).await?;
        tx.commit().await?;
        if matches!(result, CreatePublicOperationResult::Created(_)) {
            pplx_space_metrics::operations::created(
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
        let lock_key = format!("{org_id}:{target_id}");
        let mut tx = self.pool.begin().await?;
        sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
        ))
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
        let existing = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT operation_id
               FROM public_operations
              WHERE org_id = $1
                AND kind = 'delete_sandbox'
                AND target_type = 'sandbox'
                AND target_id = $2
                AND state IN ('queued', 'running')
              ORDER BY requested_at, operation_id
              LIMIT 1"
        ))
        .bind(org_id)
        .bind(target_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(operation_id) = existing {
            tx.commit().await?;
            return Ok(CreatePublicOperationResult::Existing(
                OperationId::from_uuid(operation_id),
            ));
        }
        let result = create_operation_tx(
            &mut tx,
            CreatePublicOperation {
                org_id,
                actor_user_id: None,
                api_product: ApiProduct::Native,
                kind: OperationKind::DeleteSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(target_id),
                payload_json,
                idempotency: None,
                traceparent,
            },
        )
        .await?;
        tx.commit().await?;
        if matches!(result, CreatePublicOperationResult::Created(_)) {
            pplx_space_metrics::operations::created(
                ApiProduct::Native.as_str(),
                OperationKind::DeleteSandbox.as_str(),
                OperationTargetType::Sandbox.as_str(),
            );
        }
        Ok(result)
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
        Ok(PublicOperationAdmission { tx, observation })
    }

    pub async fn observe_operation_idempotency(
        &self,
        org_id: OrgId,
        api_product: ApiProduct,
        idempotency: &CreatePublicOperationIdempotency<'_>,
    ) -> Result<ObservePublicOperationIdempotency, PublicOperationStoreError> {
        let row = sqlx::query_as::<_, PublicIdempotencyRow>(sql!(
            "SELECT operation_id, request_fingerprint
             FROM public_idempotency_records
             WHERE org_id = $1
               AND api_product = $2
               AND method = $3
               AND path_template = $4
               AND idempotency_key = $5
               AND expires_at > NOW()"
        ))
        .bind(org_id)
        .bind(api_product.as_str())
        .bind(idempotency.method.as_str())
        .bind(idempotency.path_template.as_str())
        .bind(idempotency.key.as_str())
        .fetch_optional(&self.pool)
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
        let operation_id = match create_operation_tx(&mut tx, input).await? {
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
        let claim_token = new_claim_token();
        let row = sqlx::query_as::<_, ClaimedPublicOperationRow>(sql!(
            "UPDATE public_operations
             SET state = $3,
                 attempts = attempts + 1,
                 claim_token = $2,
                 claim_generation = claim_generation + 1,
                 claimed_at = NOW(),
                 started_at = COALESCE(started_at, NOW()),
                 progress_message = 'operation claimed',
                 updated_at = NOW()
             WHERE operation_id = $1
             RETURNING operation_id,
                       org_id,
                       actor_user_id,
                       api_product,
                       kind,
                       target_type,
                       target_id,
                       payload_json,
                       attempts,
                       claim_generation,
                       traceparent"
        ))
        .bind(operation_id.as_uuid())
        .bind(&claim_token)
        .bind(OperationState::Running.as_str())
        .fetch_one(&mut *tx)
        .await?;
        let operation = ClaimedPublicOperation::try_from(row)?;
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
        tx.commit().await?;

        pplx_space_metrics::operations::created(
            api_product.as_str(),
            kind.as_str(),
            target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
        pplx_space_metrics::operations::claimed(api_product.as_str(), kind.as_str());
        Ok(CreateClaimedPublicOperationResult::Claimed {
            claim_token,
            operation,
        })
    }

    pub async fn claim_operations(
        &self,
        input: ClaimPublicOperations<'_>,
    ) -> Result<PublicOperationClaim, PublicOperationStoreError> {
        claim_operations(&self.pool, input).await
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
        let updated = sqlx::query(sql!(
            "UPDATE public_operations
             SET claimed_at = NOW(), updated_at = NOW()
             WHERE operation_id = $1
               AND claim_token = $2
               AND claim_generation = $3
               AND state = $4"
        ))
        .bind(operation.id.as_uuid())
        .bind(claim_token)
        .bind(operation.claim_generation)
        .bind(OperationState::Running.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated == 1)
    }

    pub async fn fail_claimed_operation(
        &self,
        input: FailClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        fail_claimed_operation(&self.pool, input).await
    }

    pub async fn release_claimed_operation(
        &self,
        input: ReleaseClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        release_claimed_operation(&self.pool, input).await
    }

    pub async fn update_claimed_payload(
        &self,
        input: UpdateClaimedPublicOperationPayload,
    ) -> Result<bool, PublicOperationStoreError> {
        let changed = sqlx::query(update_claimed_payload_sql())
            .bind(input.operation_id.as_uuid())
            .bind(input.claim_token)
            .bind(input.claim_generation)
            .bind(Json(input.payload_json))
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(changed == 1)
    }

    pub async fn update_claimed_payload_in_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: UpdateClaimedPublicOperationPayload,
    ) -> Result<bool, PublicOperationStoreError> {
        let changed = sqlx::query(update_claimed_payload_sql())
            .bind(input.operation_id.as_uuid())
            .bind(input.claim_token)
            .bind(input.claim_generation)
            .bind(Json(input.payload_json))
            .execute(&mut **tx)
            .await?
            .rows_affected();
        Ok(changed == 1)
    }

    pub async fn scrub_claimed_fork_federated_service_grant(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> Result<bool, PublicOperationStoreError> {
        let updated = sqlx::query(sql!(
            "UPDATE public_operations
             SET payload_json = payload_json #- $4::text[],
                 updated_at = NOW()
             WHERE operation_id = $1
               AND claim_token = $2
               AND claim_generation = $3
               AND state = 'running'"
        ))
        .bind(operation.id.as_uuid())
        .bind(claim_token)
        .bind(operation.claim_generation)
        .bind(crate::fork_operations::FORK_FEDERATED_SERVICE_GRANT_JSON_PATH)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated == 1)
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

pub struct PublicOperationAdmission {
    tx: Transaction<'static, Postgres>,
    observation: ObservePublicOperationIdempotency,
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

    pub async fn create(
        mut self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let result = create_operation_tx(&mut self.tx, input).await?;
        self.tx.commit().await?;
        if matches!(result, CreatePublicOperationResult::Created(_)) {
            pplx_space_metrics::operations::created(
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
        let result = create_operation_tx(&mut self.tx, input).await?;
        if !matches!(result, CreatePublicOperationResult::Created(_)) {
            self.tx.rollback().await?;
            return Ok(result);
        }
        self.tx.commit().await?;
        pplx_space_metrics::operations::created(
            api_product.as_str(),
            kind.as_str(),
            target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
        Ok(result)
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
    pub limit: u32,
    pub claim_stale_after: Duration,
    pub claimable_kinds: &'a [OperationKind],
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
}

impl ClaimedPublicOperation {
    /// The canonical `sandbox_id` telemetry value, the bare UUID, of the
    /// sandbox the operation targets. Public operations name it by its
    /// `sbx_<uuid>` target id and a fork by its source's bare UUID; a target
    /// that names no sandbox yields `None`, which leaves the field off the
    /// span or log line.
    pub fn sandbox_id(&self) -> Option<String> {
        let target_id = self.target_id.as_deref()?;
        let sandbox_id = match target_id.parse::<pplx_space_public_api::PublicSandboxId>() {
            Ok(public_id) => SandboxId::from(public_id),
            Err(_) => target_id.parse::<SandboxId>().ok()?,
        };
        Some(sandbox_id.to_string())
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
}

impl PublicOperationRequeueReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransientFailure => "transient_failure",
            Self::WorkflowProgress => "workflow_progress",
            Self::ActivationBackpressure => "activation_backpressure",
        }
    }

    pub const fn preserves_retry_budget(self) -> bool {
        matches!(self, Self::WorkflowProgress | Self::ActivationBackpressure)
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

pub async fn create_operation_tx(
    tx: &mut Transaction<'_, Postgres>,
    input: CreatePublicOperation<'_>,
) -> Result<CreatePublicOperationResult, PublicOperationStoreError> {
    if let Some(idempotency) = input.idempotency.as_ref() {
        lock_idempotency_key(tx, input.org_id, input.api_product, idempotency).await?;
        if let Some(existing) =
            find_idempotency_record(tx, input.org_id, input.api_product, idempotency).await?
        {
            if existing.request_fingerprint == *idempotency.request_fingerprint
                && let Some(operation_id) = existing.operation_id
            {
                return Ok(CreatePublicOperationResult::Existing(operation_id));
            }
            return Ok(CreatePublicOperationResult::IdempotencyConflict {
                operation_id: existing.operation_id,
                expected_fingerprint: existing.request_fingerprint,
            });
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

    let operation_id = OperationId::new();
    sqlx::query(sql!(
        "INSERT INTO public_operations (
             operation_id,
             org_id,
             actor_user_id,
             api_product,
             kind,
             target_type,
             target_id,
             payload_json,
             gc_after,
             traceparent
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8,
             NOW() + ($9::TEXT || ' days')::INTERVAL,
             $10
         )"
    ))
    .bind(operation_id.as_uuid())
    .bind(input.org_id)
    .bind(input.actor_user_id)
    .bind(input.api_product.as_str())
    .bind(input.kind.as_str())
    .bind(input.target_type.as_ref().map(OperationTargetType::as_str))
    .bind(input.target_id)
    .bind(Json(input.payload_json))
    .bind(DEFAULT_OPERATION_GC_DAYS.to_string())
    .bind(
        input
            .traceparent
            .as_deref()
            .filter(|traceparent| pplx_space_observability::is_storable_traceparent(traceparent)),
    )
    .execute(&mut **tx)
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

    if let Some(idempotency) = input.idempotency.as_ref() {
        sqlx::query(sql!(
            "INSERT INTO public_idempotency_records (
                 id,
                 org_id,
                 api_product,
                 method,
                 path_template,
                 idempotency_key,
                 request_fingerprint,
                 operation_id,
                 expires_at
             )
             VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8,
                 NOW() + ($9::TEXT || ' hours')::INTERVAL
             )"
        ))
        .bind(Uuid::now_v7())
        .bind(input.org_id)
        .bind(input.api_product.as_str())
        .bind(idempotency.method.as_str())
        .bind(idempotency.path_template.as_str())
        .bind(idempotency.key.as_str())
        .bind(idempotency.request_fingerprint.as_str())
        .bind(operation_id.as_uuid())
        .bind(DEFAULT_IDEMPOTENCY_TTL_HOURS.to_string())
        .execute(&mut **tx)
        .await?;
    }

    Ok(CreatePublicOperationResult::Created(operation_id))
}

fn new_claim_token() -> String {
    format!("public-operation-{}", Uuid::now_v7())
}

async fn claim_operations(
    pool: &PgPool,
    input: ClaimPublicOperations<'_>,
) -> Result<PublicOperationClaim, PublicOperationStoreError> {
    if input.limit == 0 || input.claimable_kinds.is_empty() {
        return Ok(PublicOperationClaim {
            claim_token: new_claim_token(),
            operations: Vec::new(),
        });
    }

    let claim_token = new_claim_token();
    let kind_labels = input
        .claimable_kinds
        .iter()
        .map(|kind| kind.as_str().to_string())
        .collect::<Vec<_>>();

    let mut tx = pool.begin().await?;
    let rows = sqlx::query_as::<_, ClaimedPublicOperationRow>(sql!(
        "WITH picked AS (
             SELECT operation_id
             FROM public_operations
             WHERE state IN ($4, $5)
               AND kind = ANY($6)
               AND next_attempt_at <= NOW()
               AND (
                   claim_token = ''
                   OR claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
             ORDER BY requested_at ASC, operation_id ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE public_operations o
         SET state = $5,
             attempts = o.attempts + 1,
             claim_token = $3,
             claim_generation = o.claim_generation + 1,
             claimed_at = NOW(),
             started_at = COALESCE(o.started_at, NOW()),
             progress_message = CASE
                 WHEN o.progress_message = '' THEN 'operation claimed'
                 ELSE o.progress_message
             END,
             updated_at = NOW()
         FROM picked
         WHERE o.operation_id = picked.operation_id
         RETURNING o.operation_id,
                   o.org_id,
                   o.actor_user_id,
                   o.api_product,
                   o.kind,
                   o.target_type,
                   o.target_id,
                   o.payload_json,
                   o.attempts,
                   o.claim_generation,
                   o.traceparent"
    ))
    .bind(i64::from(input.limit))
    .bind(duration_secs_i64(input.claim_stale_after))
    .bind(&claim_token)
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .bind(&kind_labels)
    .fetch_all(&mut *tx)
    .await?;

    let mut operations = Vec::with_capacity(rows.len());
    for row in rows {
        let operation = ClaimedPublicOperation::try_from(row)?;
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

    Ok(PublicOperationClaim {
        claim_token,
        operations,
    })
}

async fn complete_claimed_operation(
    pool: &PgPool,
    input: CompleteClaimedPublicOperation,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $4,
             result_json = $5,
             payload_json = CASE WHEN kind = 'create_sandbox'
                 THEN payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE payload_json END,
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $6
         RETURNING operation_id"
    ))
    .bind(input.operation_id.as_uuid())
    .bind(&input.claim_token)
    .bind(OperationState::Succeeded.as_str())
    .bind(&input.progress_message)
    .bind(Json(input.result_json.clone()))
    .bind(OperationState::Running.as_str())
    .fetch_optional(&mut *tx)
    .await?;

    if updated.is_some() {
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
    Ok(updated.is_some())
}

async fn fail_claimed_operation(
    pool: &PgPool,
    input: FailClaimedPublicOperation,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $4,
             terminal_error_code = $5,
             terminal_error_json = $6,
             payload_json = CASE WHEN kind = 'create_sandbox'
                 THEN payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE payload_json END,
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $7
         RETURNING operation_id"
    ))
    .bind(input.operation_id.as_uuid())
    .bind(&input.claim_token)
    .bind(OperationState::Failed.as_str())
    .bind(&input.progress_message)
    .bind(input.terminal_error_code.as_str())
    .bind(Json(input.terminal_error_json.clone()))
    .bind(OperationState::Running.as_str())
    .fetch_optional(&mut *tx)
    .await?;

    if updated.is_some() {
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
    Ok(updated.is_some())
}

async fn release_claimed_operation(
    pool: &PgPool,
    input: ReleaseClaimedPublicOperation,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             progress_message = $4,
             next_attempt_at = NOW() + make_interval(secs => $5::BIGINT),
             attempts = GREATEST(attempts - CASE WHEN $7 THEN 0 ELSE 1 END, 0),
             updated_at = NOW()
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $6
         RETURNING operation_id"
    ))
    .bind(input.operation_id.as_uuid())
    .bind(&input.claim_token)
    .bind(OperationState::Queued.as_str())
    .bind(&input.progress_message)
    .bind(duration_secs_i64(input.retry_after))
    .bind(OperationState::Running.as_str())
    .bind(input.count_attempt)
    .fetch_optional(&mut *tx)
    .await?;

    if updated.is_some() {
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
    Ok(updated.is_some())
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
        sqlx::query(sql!(
            "WITH terminal_operations AS (
             SELECT operation_id
             FROM public_operations
             WHERE kind = $2
               AND state IN ($3, $4, $5, $6)
               AND payload_json #> $7::text[] IS NOT NULL
             ORDER BY updated_at ASC, operation_id ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE public_operations AS operation
         SET payload_json = operation.payload_json #- $7::text[],
             updated_at = NOW()
         FROM terminal_operations
         WHERE operation.operation_id = terminal_operations.operation_id"
        ))
        .bind(max_records)
        .bind(rule.kind.as_str())
        .bind(OperationState::Succeeded.as_str())
        .bind(OperationState::Failed.as_str())
        .bind(OperationState::Canceled.as_str())
        .bind(OperationState::Expired.as_str())
        .bind(rule.json_path)
        .execute(&mut *tx)
        .await?;
    }

    let operation_ids = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id
         FROM public_operations
         WHERE state IN ($2, $3, $4, $5)
           AND gc_after <= NOW()
         ORDER BY gc_after ASC, operation_id ASC
         LIMIT $1"
    ))
    .bind(max_records)
    .bind(OperationState::Succeeded.as_str())
    .bind(OperationState::Failed.as_str())
    .bind(OperationState::Canceled.as_str())
    .bind(OperationState::Expired.as_str())
    .fetch_all(&mut *tx)
    .await?;

    let idempotency_rows = sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_idempotency_records
         WHERE id IN (
             SELECT id
             FROM public_idempotency_records
             WHERE expires_at <= NOW()
             ORDER BY expires_at ASC, id ASC
             LIMIT $1
         )
         RETURNING id"
    ))
    .bind(max_records)
    .fetch_all(&mut *tx)
    .await?;

    let dependent_idempotency_rows = sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_idempotency_records
         WHERE operation_id = ANY($1::UUID[])
         RETURNING id"
    ))
    .bind(&operation_ids)
    .fetch_all(&mut *tx)
    .await?;

    let operation_rows = sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_operations
         WHERE operation_id = ANY($1::UUID[])
         RETURNING operation_id"
    ))
    .bind(&operation_ids)
    .fetch_all(&mut *tx)
    .await?;

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
    let row = sqlx::query_as::<_, PublicIdempotencyRow>(sql!(
        "SELECT operation_id, request_fingerprint
         FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = $2
           AND method = $3
           AND path_template = $4
           AND idempotency_key = $5
           AND expires_at > NOW()
         FOR UPDATE"
    ))
    .bind(org_id)
    .bind(api_product.as_str())
    .bind(idempotency.method.as_str())
    .bind(idempotency.path_template.as_str())
    .bind(idempotency.key.as_str())
    .fetch_optional(&mut **tx)
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
    sqlx::query(sql!(
        "DELETE FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = $2
           AND method = $3
           AND path_template = $4
           AND idempotency_key = $5
           AND expires_at <= NOW()"
    ))
    .bind(org_id)
    .bind(api_product.as_str())
    .bind(method.as_str())
    .bind(path_template.as_str())
    .bind(key.as_str())
    .execute(&mut **tx)
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
    sqlx::query(sql!("SELECT pg_advisory_xact_lock($1)"))
        .bind(idempotency_lock_key(org_id, api_product, idempotency))
        .execute(&mut **tx)
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
    sqlx::query_scalar::<_, i64>(sql!(
        "WITH next_sequence AS (
             SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence
             FROM public_operation_events
             WHERE operation_id = $1
         )
         INSERT INTO public_operation_events (
             operation_id,
             sequence,
             event_type,
             event_json
         )
         SELECT $1, sequence, $2, $3
         FROM next_sequence
         RETURNING sequence"
    ))
    .bind(operation_id.as_uuid())
    .bind(event_type.as_str())
    .bind(Json(event_json))
    .fetch_one(&mut **tx)
    .await
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminPublicOperationRecord {
    pub id: OperationId,
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub api_product: ApiProduct,
    pub kind: OperationKind,
    pub state: OperationState,
    pub target_type: Option<OperationTargetType>,
    pub target_id: Option<String>,
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

#[derive(Debug, FromRow)]
struct ClaimedPublicOperationRow {
    operation_id: Uuid,
    org_id: OrgId,
    actor_user_id: Option<UserId>,
    api_product: String,
    kind: String,
    target_type: Option<String>,
    target_id: Option<String>,
    payload_json: Json<serde_json::Value>,
    attempts: i32,
    claim_generation: i64,
    traceparent: Option<String>,
}

#[derive(Debug, FromRow)]
struct PublicOperationRow {
    operation_id: Uuid,
    kind: String,
    target_type: Option<String>,
    target_id: Option<String>,
    state: String,
    progress_message: String,
    terminal_error_code: Option<String>,
    terminal_error_json: Option<Json<serde_json::Value>>,
    result_json: Option<Json<serde_json::Value>>,
    created_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct AdminPublicOperationRow {
    operation_id: Uuid,
    org_id: OrgId,
    actor_user_id: Option<UserId>,
    api_product: String,
    kind: String,
    target_type: Option<String>,
    target_id: Option<String>,
    state: String,
    progress_message: String,
    terminal_error_code: Option<String>,
    terminal_error_json: Option<Json<serde_json::Value>>,
    result_json: Option<Json<serde_json::Value>>,
    attempts: i32,
    requested_at: OffsetDateTime,
    next_attempt_at: OffsetDateTime,
    created_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct PublicOperationEventRow {
    operation_id: Uuid,
    sequence: i64,
    event_type: String,
    event_json: Json<serde_json::Value>,
    created_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct PublicIdempotencyRow {
    operation_id: Option<Uuid>,
    request_fingerprint: String,
}

impl TryFrom<PublicOperationRow> for PublicOperationRecord {
    type Error = PublicOperationStoreError;

    fn try_from(row: PublicOperationRow) -> Result<Self, Self::Error> {
        Ok(Self {
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
}

impl TryFrom<AdminPublicOperationRow> for AdminPublicOperationRecord {
    type Error = PublicOperationStoreError;

    fn try_from(row: AdminPublicOperationRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: OperationId::from_uuid(row.operation_id),
            org_id: row.org_id,
            actor_user_id: row.actor_user_id,
            api_product: parse_stored_enum("api_product", &row.api_product)?,
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
}

impl TryFrom<PublicOperationEventRow> for PublicOperationEvent {
    type Error = PublicOperationStoreError;

    fn try_from(row: PublicOperationEventRow) -> Result<Self, Self::Error> {
        Ok(Self {
            operation_id: OperationId::from_uuid(row.operation_id),
            sequence: row.sequence,
            event_type: parse_stored_enum("event_type", &row.event_type)?,
            event: row.event_json.0,
            created_at: row.created_at,
        })
    }
}

impl TryFrom<PublicIdempotencyRow> for PublicIdempotencyRecord {
    type Error = PublicOperationStoreError;

    fn try_from(row: PublicIdempotencyRow) -> Result<Self, Self::Error> {
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
        })
    }
}

fn duration_secs_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests;
