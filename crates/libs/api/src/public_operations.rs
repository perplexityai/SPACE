use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_common_core::sql;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::types::{IdempotencyKey, SandboxId, SandboxLifecycle};
use pplx_space_metrics::operations::ClaimAttempt;
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicErrorCode,
    PublicOperationEvent, PublicOperationEventType, PublicOperationRecord, PublicOperationResult,
    RequestFingerprint,
};
use pplx_space_sandbox_store as sandbox_store;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Row, Transaction, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::public_operation_driver::with_carried_error;
use crate::public_store::{InvalidStoredEnum, parse_stored_enum};
use pplx_space_sandbox_store::fork_source_lifecycle::lock_fork_source_lifecycle;

const DEFAULT_OPERATION_GC_DAYS: i64 = 7;

/// The columns every whole-operation read selects, in [`PublicOperationRow`]'s
/// shape. Named rather than `*`: adding or dropping a `public_operations`
/// column changes what `SELECT *` returns, and every pooled connection that
/// already ran one then fails it with `0A000` ("cached plan must not change
/// result type") until the pool recycles that connection. A new column a read
/// needs is selected here and decoded in [`PublicOperationRow`], once for all
/// three reads; a record that exposes it also needs the field and its line in
/// that record's `TryFrom<PublicOperationRow>`.
const PUBLIC_OPERATION_COLUMNS: &str = "operation_id,
       org_id,
       actor_user_id,
       api_product,
       kind,
       target_type,
       target_id,
       state,
       progress_message,
       terminal_error_code,
       terminal_error_json,
       result_json,
       attempts,
       requested_at,
       next_attempt_at,
       created_at,
       started_at,
       completed_at,
       updated_at";

/// `sql` with `{public_operation_columns}` replaced by
/// [`PUBLIC_OPERATION_COLUMNS`], the one fragment these statements take (the
/// data-store skill's exception for one row type's column list). Each
/// statement is otherwise complete, built once at its query site, and binds
/// every value.
fn public_operation_query(sql: &str) -> String {
    sql.replace("{public_operation_columns}", PUBLIC_OPERATION_COLUMNS)
}

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
/// Advisory transaction lock that serializes dispatcher claims across
/// controller replicas so the keyed-claim rule sees every committed claim.
pub const PUBLIC_OPERATION_CLAIM_LOCK_KEY: i64 = 0x7370_6163_655f_6f70;

mod key_bindings;
pub(crate) use key_bindings::{KEY_BINDING_TTL_MINUTES, bind_idempotency_key};
mod responses;
pub(crate) use responses::ResponseAdmissionError;
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

    /// How long ago the operation was first claimed, if it has been.
    pub async fn time_since_first_claim(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Duration>, PublicOperationStoreError> {
        let seconds = sqlx::query_scalar::<_, Option<f64>>(pplx_space_common_core::sql!(
            "SELECT EXTRACT(EPOCH FROM NOW() - started_at)::DOUBLE PRECISION
             FROM public_operations
             WHERE operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        Ok(seconds
            .flatten()
            .map(|seconds| Duration::from_secs_f64(seconds.max(0.0))))
    }

    pub async fn fetch_operation(
        &self,
        org_id: OrgId,
        operation_id: OperationId,
    ) -> Result<Option<PublicOperationRecord>, PublicOperationStoreError> {
        static FETCH_OPERATION_SQL: LazyLock<String> = LazyLock::new(|| {
            sql!(public_operation_query(
                "SELECT {public_operation_columns}
                 FROM public_operations
                 WHERE org_id = $1 AND operation_id = $2"
            ))
        });
        let row = sqlx::query_as::<_, PublicOperationRow>(sqlx::AssertSqlSafe(
            FETCH_OPERATION_SQL.as_str(),
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
        static FETCH_OPERATION_ADMIN_SQL: LazyLock<String> = LazyLock::new(|| {
            sql!(public_operation_query(
                "SELECT {public_operation_columns}
                 FROM public_operations
                 WHERE operation_id = $1"
            ))
        });
        let row = sqlx::query_as::<_, PublicOperationRow>(sqlx::AssertSqlSafe(
            FETCH_OPERATION_ADMIN_SQL.as_str(),
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
        static LIST_OPERATIONS_ADMIN_SQL: LazyLock<String> = LazyLock::new(|| {
            sql!(public_operation_query(
                "SELECT {public_operation_columns}
                 FROM public_operations
                 WHERE ($1::UUID IS NULL OR org_id = $1)
                   AND ($2::TEXT IS NULL OR state = $2)
                   AND ($3::TEXT IS NULL OR kind = $3)
                   AND ($4::TEXT IS NULL OR target_type = $4)
                   AND ($5::TEXT IS NULL OR target_id = $5)
                 ORDER BY created_at DESC, operation_id DESC
                 LIMIT $6"
            ))
        });
        let rows = sqlx::query_as::<_, PublicOperationRow>(sqlx::AssertSqlSafe(
            LIST_OPERATIONS_ADMIN_SQL.as_str(),
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
        let (result, superseded) = create_operation_tx(&mut tx, input).await?;
        tx.commit().await?;
        superseded.record();
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
    /// execute, when nothing else for the sandbox is queued or running (see
    /// [`PublicOperationAdmission::create_claimed_if_sole_active`], and its
    /// precondition). A joined wake is never claimed: whoever created it, or
    /// the dispatcher, runs it.
    pub async fn create_or_join_active_sandbox_wake_claimed_if_sole_active(
        &self,
        org_id: OrgId,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
    ) -> Result<CreateInlinePublicOperationResult, PublicOperationStoreError> {
        let (result, claimed) = self
            .create_or_join_active_sandbox_operation_tx(
                org_id,
                OperationKind::WakeSandbox,
                target_id,
                payload_json,
                traceparent,
                true,
            )
            .await?;
        Ok(match (result, claimed) {
            (_, Some((claim_token, operation))) => CreateInlinePublicOperationResult::Claimed {
                claim_token,
                operation,
            },
            (CreatePublicOperationResult::Created(operation_id), None) => {
                CreateInlinePublicOperationResult::Queued(operation_id)
            }
            (result, None) => CreateInlinePublicOperationResult::NotCreated(result),
        })
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
        let (result, _) = self
            .create_or_join_active_sandbox_operation_tx(
                org_id,
                kind,
                target_id,
                payload_json,
                traceparent,
                false,
            )
            .await?;
        Ok(result)
    }

    /// [`Self::create_or_join_active_sandbox_operation`]; with
    /// `claim_if_sole_active`, an operation it creates is also claimed when
    /// nothing else for the sandbox is active.
    async fn create_or_join_active_sandbox_operation_tx(
        &self,
        org_id: OrgId,
        kind: OperationKind,
        target_id: &str,
        payload_json: serde_json::Value,
        traceparent: Option<String>,
        claim_if_sole_active: bool,
    ) -> Result<
        (
            CreatePublicOperationResult,
            Option<(String, ClaimedPublicOperation)>,
        ),
        PublicOperationStoreError,
    > {
        let mut tx = self.pool.begin().await?;
        lock_active_sandbox_operation(&mut tx, org_id, kind, target_id).await?;
        let existing = if kind == OperationKind::DeleteSandbox {
            sqlx::query_scalar::<_, Uuid>(sql!(
                "SELECT operation_id
                   FROM public_operations
                  WHERE org_id = $1
                    AND kind = $3
                    AND target_type = 'sandbox'
                    AND target_id = $2
                    AND state IN ('queued', 'running')
                  ORDER BY requested_at, operation_id
                  LIMIT 1"
            ))
            .bind(org_id)
            .bind(target_id)
            .bind(kind.as_str())
            .fetch_optional(&mut *tx)
            .await?
        } else {
            joinable_active_sandbox_operation(&mut tx, org_id, kind, target_id, None)
                .await?
                .map(|joinable| joinable.operation_id)
        };
        if let Some(operation_id) = existing {
            tx.commit().await?;
            return Ok((
                CreatePublicOperationResult::Existing(OperationId::from_uuid(operation_id)),
                None,
            ));
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
        let claimed = match result {
            CreatePublicOperationResult::Created(operation_id) if claim_if_sole_active => {
                claim_created_if_sole_active_tx(&mut tx, org_id, target_id, operation_id).await?
            }
            _ => None,
        };
        tx.commit().await?;
        superseded.record();
        if matches!(result, CreatePublicOperationResult::Created(_)) {
            pplx_space_metrics::operations::created(
                ApiProduct::Native.as_str(),
                kind.as_str(),
                OperationTargetType::Sandbox.as_str(),
            );
        }
        Ok((result, claimed))
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
    let row = sqlx::query_as::<_, ClaimedPublicOperationRow>(sql!(
        "UPDATE public_operations
         SET state = $3,
             attempts = attempts + 1,
             claim_token = $2,
             claim_generation = claim_generation + 1,
             claimed_at = clock_timestamp(),
             started_at = COALESCE(started_at, clock_timestamp()),
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
                   traceparent,
                   FLOOR(EXTRACT(EPOCH FROM NOW() - created_at))::BIGINT
                       AS age_at_first_claim_secs"
    ))
    .bind(operation_id.as_uuid())
    .bind(&claim_token)
    .bind(OperationState::Running.as_str())
    .fetch_one(&mut **tx)
    .await?;
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
/// a stale one). Otherwise it stays queued for the dispatcher, behind them.
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
async fn claim_created_if_sole_active_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    target_id: &str,
    operation_id: OperationId,
) -> Result<Option<(String, ClaimedPublicOperation)>, PublicOperationStoreError> {
    let others_active = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
               FROM public_operations
              WHERE org_id = $1
                AND target_type = $2
                AND target_id = $3
                AND kind <> ALL($4)
                AND state IN ($5, $6)
                AND operation_id <> $7
         )"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(outside_sandbox_queue_labels())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .bind(operation_id.as_uuid())
    .fetch_one(&mut **tx)
    .await?;
    if others_active {
        return Ok(None);
    }
    Ok(Some(claim_created_operation_tx(tx, operation_id).await?))
}

/// What [`PublicOperationAdmission::create_claimed_if_sole_active`] recorded.
#[derive(Debug, Clone)]
pub enum CreateInlinePublicOperationResult {
    /// Created and claimed, with an admission claim token: the caller owns
    /// this attempt and must record its outcome under `claim_token`. If the
    /// caller dies, the claim goes stale and the dispatcher re-drives it.
    Claimed {
        claim_token: String,
        operation: ClaimedPublicOperation,
    },
    /// Created and left queued for the dispatcher, because other work for
    /// its sandbox is queued or running.
    Queued(OperationId),
    /// Nothing was created: a replay, a key conflict or an admission denial.
    NotCreated(CreatePublicOperationResult),
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

    /// [`Self::create`], except that a newly created sandbox operation is
    /// also claimed in the same transaction, for the caller to execute
    /// inline, when nothing else for its sandbox is queued or running.
    /// Anything else is recorded as [`Self::create`] records it: queued for
    /// the dispatcher, or not at all for a replay or a conflict.
    ///
    /// The claim takes an admission claim token, which the dispatcher's
    /// per-organization slot limit does not count. It is sound only under
    /// the precondition on `claim_created_if_sole_active_tx`: every API
    /// process takes the queue lock, and the dispatcher claims keyed.
    pub async fn create_claimed_if_sole_active(
        mut self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreateInlinePublicOperationResult, PublicOperationStoreError> {
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
        let claimed = match queue {
            Some(target_id) => {
                claim_created_if_sole_active_tx(&mut self.tx, org_id, target_id, operation_id)
                    .await?
            }
            None => None,
        };
        self.tx.commit().await?;
        superseded.record();
        pplx_space_metrics::operations::created(
            api_product.as_str(),
            kind.as_str(),
            target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
        Ok(match claimed {
            Some((claim_token, operation)) => CreateInlinePublicOperationResult::Claimed {
                claim_token,
                operation,
            },
            None => CreateInlinePublicOperationResult::Queued(operation_id),
        })
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
            return self.create(input).await;
        };
        if !matches!(self.observation, ObservePublicOperationIdempotency::Missing) {
            return self.create(input).await;
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
            return self.create(input).await;
        };
        if joinable.system {
            let handed_over = sqlx::query_scalar::<_, Uuid>(sql!(
                "UPDATE public_operations
                    SET actor_user_id = $2, updated_at = NOW()
                  WHERE operation_id = $1
                    AND actor_user_id IS NULL
                    AND state = 'queued'
                  RETURNING operation_id"
            ))
            .bind(joinable.operation_id)
            .bind(actor_user_id)
            .fetch_optional(&mut *self.tx)
            .await?;
            if handed_over.is_none() {
                return self.create(input).await;
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
        self.tx.commit().await?;
        Ok(CreatePublicOperationResult::Existing(operation_id))
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

    /// [`Self::create_or_roll_back`], except that a newly created operation
    /// is also claimed in the same transaction, for the caller to execute
    /// inline. Nothing is claimed for a replay or a conflict, and both roll
    /// back, as there.
    pub(crate) async fn create_claimed_or_roll_back(
        mut self,
        input: CreatePublicOperation<'_>,
    ) -> Result<CreateClaimedPublicOperationResult, PublicOperationStoreError> {
        let api_product = input.api_product;
        let kind = input.kind;
        let target_type = input.target_type.clone();
        let (created, superseded) = create_operation_tx(&mut self.tx, input).await?;
        let operation_id = match created {
            CreatePublicOperationResult::Created(operation_id) => operation_id,
            CreatePublicOperationResult::Existing(operation_id) => {
                self.tx.rollback().await?;
                return Ok(CreateClaimedPublicOperationResult::Existing(operation_id));
            }
            CreatePublicOperationResult::IdempotencyConflict {
                operation_id,
                expected_fingerprint,
            } => {
                self.tx.rollback().await?;
                return Ok(CreateClaimedPublicOperationResult::IdempotencyConflict {
                    operation_id,
                    expected_fingerprint,
                });
            }
            CreatePublicOperationResult::AdmissionDenied => {
                self.tx.rollback().await?;
                return Ok(CreateClaimedPublicOperationResult::AdmissionDenied);
            }
        };
        let (claim_token, operation) =
            claim_created_operation_tx(&mut self.tx, operation_id).await?;
        self.tx.commit().await?;
        superseded.record();
        pplx_space_metrics::operations::created(
            api_product.as_str(),
            kind.as_str(),
            target_type
                .as_ref()
                .map(OperationTargetType::as_str)
                .unwrap_or("none"),
        );
        Ok(CreateClaimedPublicOperationResult::Claimed {
            claim_token,
            operation,
        })
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
            pplx_space_metrics::operations::completed(
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
             traceparent,
             requested_at
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8,
             NOW() + ($9::TEXT || ' days')::INTERVAL,
             $10,
             clock_timestamp()
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
    let creates = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id FROM public_operations
         WHERE org_id = $1
           AND target_type = $2
           AND target_id = $3
           AND kind = $4
           AND state IN ($5, $6)"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target.to_string())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_all(executor)
    .await?;
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
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = $2,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $3,
             terminal_error_code = $4,
             terminal_error_json = $5,
             payload_json = payload_json #- '{federated_create,service_grant_base64}'::text[],
             updated_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(create_id.as_uuid())
    .bind(OperationState::Canceled.as_str())
    .bind(&progress_message)
    .bind(code.as_str())
    .bind(Json(error.clone()))
    .execute(&mut **tx)
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
    let active = sqlx::query_as::<_, (Uuid, String, bool)>(sql!(
        "SELECT operation_id, api_product, state = $5 AND claim_token = ''
         FROM public_operations
         WHERE org_id = $1
           AND target_type = $2
           AND target_id = $3
           AND kind = $4
           AND state IN ($5, $6)
           AND requested_at <= NOW()
         ORDER BY requested_at, operation_id
         FOR UPDATE"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_all(&mut **tx)
    .await?;
    let creates = active
        .into_iter()
        .filter_map(|(create, api_product, unclaimed)| unclaimed.then_some((create, api_product)));
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
    let delete = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT delete_op.operation_id
         FROM public_operations create_op
         JOIN public_operations delete_op
           ON delete_op.org_id = create_op.org_id
          AND delete_op.target_type = create_op.target_type
          AND delete_op.target_id = create_op.target_id
         WHERE create_op.operation_id = $1
           AND create_op.kind = $2
           AND create_op.target_type = $3
           AND delete_op.kind = $4
           AND delete_op.state IN ($5, $6)
           AND (delete_op.requested_at, delete_op.operation_id)
               > (create_op.requested_at, create_op.operation_id)
         ORDER BY delete_op.requested_at, delete_op.operation_id
         LIMIT 1"
    ))
    .bind(create_id.as_uuid())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(OperationKind::DeleteSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    Ok(delete.map(OperationId::from_uuid))
}

async fn insert_idempotency_record_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    api_product: ApiProduct,
    idempotency: &CreatePublicOperationIdempotency<'_>,
    operation_id: OperationId,
) -> Result<(), sqlx::Error> {
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
    .bind(org_id)
    .bind(api_product.as_str())
    .bind(idempotency.method.as_str())
    .bind(idempotency.path_template.as_str())
    .bind(idempotency.key.as_str())
    .bind(idempotency.request_fingerprint.as_str())
    .bind(operation_id.as_uuid())
    .bind(DEFAULT_IDEMPOTENCY_TTL_HOURS.to_string())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Serialize creating or joining the active `kind` operation for one sandbox.
/// Deletes keep the lock key they always had.
async fn lock_active_sandbox_operation(
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
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(lock_key)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Kinds that name a sandbox as their target but are not in its queue. A
/// fork names its source by a different id form (the bare UUID) and is
/// ordered by the source's lifecycle lock instead.
///
/// This is the one definition of a sandbox's queue: every operation that
/// targets a sandbox and is not of one of these kinds. [`sandbox_queue_key`]
/// reads it for the queue lock and the dispatcher's busy keys, and the keyed
/// claim (`blocked`) and [`claim_created_if_sole_active_tx`] bind it into
/// their SQL, so who serializes, who holds work back and who counts as other
/// work for the sandbox cannot drift apart.
const OUTSIDE_SANDBOX_QUEUE_KINDS: &[OperationKind] = &[OperationKind::ForkSandbox];

/// [`OUTSIDE_SANDBOX_QUEUE_KINDS`] as the labels SQL binds.
fn outside_sandbox_queue_labels() -> Vec<&'static str> {
    OUTSIDE_SANDBOX_QUEUE_KINDS
        .iter()
        .map(|kind| kind.as_str())
        .collect()
}

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
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(format!("{org_id}:{target_id}:queue"))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// An active operation [`joinable_active_sandbox_operation`] found.
struct JoinableOperation {
    operation_id: Uuid,
    /// No actor: the system's, as a command or process session queues it.
    system: bool,
}

/// The queued or running `kind` operation on a sandbox that a new request
/// joins, under [`lock_active_sandbox_operation`]. Only one that nothing else
/// for the sandbox is queued behind, in the dispatcher's order: a wake ahead
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
    let joinable = sqlx::query_as::<_, (Uuid, bool)>(sql!(
        "SELECT active.operation_id, active.actor_user_id IS NULL
           FROM public_operations active
          WHERE active.org_id = $1
            AND active.kind = $3
            AND active.target_type = 'sandbox'
            AND active.target_id = $2
            AND active.state IN ('queued', 'running')
            AND ($4::UUID IS NULL
                 OR (active.api_product = 'native'
                     AND (active.actor_user_id = $4
                          OR (active.actor_user_id IS NULL AND active.state = 'queued'))))
            AND NOT EXISTS (
                SELECT 1
                  FROM public_operations later
                 WHERE later.org_id = active.org_id
                   AND later.target_type = 'sandbox'
                   AND later.target_id = active.target_id
                   AND later.kind <> active.kind
                   AND later.state IN ('queued', 'running')
                   AND (later.requested_at, later.operation_id)
                       > (active.requested_at, active.operation_id)
            )
          ORDER BY active.requested_at, active.operation_id
          LIMIT 1"
    ))
    .bind(org_id)
    .bind(target_id)
    .bind(kind.as_str())
    .bind(joiner)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(joinable.map(|(operation_id, system)| JoinableOperation {
        operation_id,
        system,
    }))
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
    let kind_labels = input
        .claimable_kinds
        .iter()
        .map(|kind| kind.as_str().to_string())
        .collect::<Vec<_>>();

    let teardown_labels = TEARDOWN_KINDS
        .iter()
        .map(|kind| kind.as_str().to_string())
        .collect::<Vec<_>>();
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
        let (claim_lock_held, _jit) = sqlx::query_as::<_, (bool, String)>(sql!(
            "SELECT pg_try_advisory_xact_lock($1), set_config('jit', 'off', true)"
        ))
        .bind(PUBLIC_OPERATION_CLAIM_LOCK_KEY)
        .fetch_one(&mut *tx)
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
    // Across keys, claim in order of when each operation became claimable,
    // so a requeued retry waits behind work queued before its backoff
    // expired. With `$11` (keyed) set, `blocked` lets at most one operation
    // per sandbox start: none while one runs under a live claim; otherwise a
    // stale claim first, so its token is rotated before anything else on the
    // sandbox can start beside its old execution; otherwise the
    // earliest-requested one if it is due; otherwise the earliest due delete
    // with no create requested before it, so a delete is not stuck behind
    // work in retry backoff but never overtakes a create. Only sandboxes with
    // more than one active operation can hold anything back, so the window
    // pass reads just those, in one sort. Kinds outside a sandbox's queue
    // (`$8`, `OUTSIDE_SANDBOX_QUEUE_KINDS`: forks) stay outside the key.
    // With `$11` unset (the kill switch) `blocked` is empty. Teardowns
    // (`$14`: deletes and kills) and other work are picked apart. At most
    // `$12` of the claim are other work, which keeps
    // the slots reserved for teardowns free of it. While other work is due, at
    // most `$13` are teardowns, which keeps the slots reserved for other work
    // free of them; with none due, teardowns may take any free slot. The claim
    // then takes the first `$1` of both, in the same order. Both picks lock
    // their rows before that last `LIMIT`, so up to `$12 + $13 - $1` rows stay
    // locked but unclaimed until this transaction commits. A concurrent
    // unkeyed claim skips them meanwhile, and a keyed one does not run while
    // this claim holds the claim lock. With `$15` above zero, it limits each
    // organization's operations other than teardowns: `org_slots` counts the
    // ones it runs under a live claim, except one claimed as it was created
    // (`$17`), which the process that created it runs, and, in claim order,
    // the ones this claim would add. An organization is waiting while it has
    // such work due and holds fewer than `$15`. While another one is
    // waiting, an organization gets no more than `$15`. While none is, it may
    // go past `$15`, but operations past their organization's limit are taken
    // only among the first `$12 - $16` admissible ones in claim order, so
    // that `$16` of the free slots open to other work stay free for work
    // another organization submits later. Organizations all at their limit
    // share that one budget.
    // No more than `2 * $12` of an organization's due operations can be
    // admitted, so the rank reads only those: `due_other` reads every due
    // one once, an organization with more gets a top-N pass of its own, and
    // the others' go to the window whole, which keeps one organization's
    // backlog out of the sort.
    // That rank needs a window, which cannot share a level with `FOR
    // UPDATE`, so `first_other` admits up to `2 * $12` and `picked_other`
    // locks the first `$12` of them it can by key, re-checking that each is
    // still claimable. Admitting more than it takes keeps a row another
    // transaction has locked from shrinking the claim, unless more than `$12`
    // of the admitted ones are. A locked row still counts toward its
    // organization's limit and the headroom budget in this claim, since
    // finding it locked takes the lock, so work admitted only past it waits
    // for the next claim. When it claims nothing, the statement returns
    // one row with no operation, whose `org_limit_held_back` says whether
    // other work was due and the organization limit held all of it back.
    let rows = sqlx::query(sql!(
        "WITH shared AS (
             SELECT org_id, target_id
             FROM public_operations
             WHERE $11::BOOLEAN
               AND state IN ($4, $5)
               AND target_type = $7
               AND target_id IS NOT NULL
               AND kind = ANY($6)
               AND kind <> ALL($8)
             GROUP BY org_id, target_id
             HAVING COUNT(*) > 1
         ),
         active AS (
             SELECT operation_id,
                    op.org_id,
                    op.target_id,
                    requested_at,
                    kind,
                    next_attempt_at <= NOW() AS due,
                    state = $5 AND claim_token <> ''
                        AND claimed_at > NOW() - make_interval(secs => $2::BIGINT) AS live,
                    state = $5 AND claim_token <> ''
                        AND claimed_at <= NOW() - make_interval(secs => $2::BIGINT) AS stale
             FROM shared
             JOIN public_operations op
               ON op.org_id = shared.org_id
              AND op.target_type = $7
              AND op.target_id = shared.target_id
             WHERE op.state IN ($4, $5)
               AND op.kind = ANY($6)
               AND op.kind <> ALL($8)
         ),
         ranked AS (
             SELECT operation_id,
                    ROW_NUMBER() OVER queue AS position,
                    FIRST_VALUE(due) OVER queue AS head_due,
                    BOOL_OR(live) OVER whole AS sandbox_live,
                    BOOL_OR(stale) OVER whole AS sandbox_stale,
                    stale AND NOT COALESCE(BOOL_OR(stale) OVER earlier, FALSE) AS first_stale,
                    kind = $9
                        AND due
                        AND NOT COALESCE(BOOL_OR(kind = $10) OVER earlier, FALSE)
                        AND NOT COALESCE(BOOL_OR(kind = $9 AND due) OVER earlier, FALSE)
                        AS first_free_delete
             FROM active
             WINDOW queue AS (
                        PARTITION BY org_id, target_id ORDER BY requested_at, operation_id
                    ),
                    whole AS (queue ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                    earlier AS (queue ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
         ),
         blocked AS (
             SELECT operation_id
             FROM ranked
             WHERE sandbox_live
                OR (sandbox_stale AND NOT first_stale)
                OR (NOT sandbox_stale AND head_due AND position > 1)
                OR (NOT sandbox_stale AND NOT head_due AND NOT first_free_delete)
         ),
         org_live AS (
             SELECT org_id, COUNT(*) AS live
             FROM public_operations
             WHERE $15::BIGINT > 0
               AND state = $5
               AND claim_token <> ''
               AND NOT starts_with(claim_token, $17)
               AND claimed_at > NOW() - make_interval(secs => $2::BIGINT)
               AND kind = ANY($6)
               AND kind <> ALL($14)
             GROUP BY org_id
         ),
         due_other AS MATERIALIZED (
             SELECT candidate.operation_id,
                    candidate.org_id,
                    candidate.next_attempt_at,
                    candidate.requested_at,
                    GREATEST(candidate.requested_at, candidate.next_attempt_at) AS claimable_at,
                    candidate.claim_token <> '' AS reclaimed,
                    candidate.started_at IS NULL AS first_start
             FROM public_operations candidate
             WHERE candidate.state IN ($4, $5)
               AND candidate.kind = ANY($6)
               AND candidate.kind <> ALL($14)
               AND candidate.next_attempt_at <= NOW()
               AND (
                   candidate.claim_token = ''
                   OR candidate.claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
               AND candidate.operation_id NOT IN (SELECT operation_id FROM blocked)
         ),
         due_orgs AS (
             SELECT org_id, COUNT(*) AS due
             FROM due_other
             GROUP BY org_id
         ),
         org_first AS (
             SELECT due_other.*
             FROM due_other
             JOIN due_orgs ON due_orgs.org_id = due_other.org_id
             WHERE due_orgs.due <= 2 * $12
             UNION ALL
             SELECT first.*
             FROM due_orgs
             CROSS JOIN LATERAL (
                 SELECT *
                 FROM due_other
                 WHERE due_other.org_id = due_orgs.org_id
                 ORDER BY next_attempt_at, requested_at, operation_id
                 LIMIT 2 * $12
             ) first
             WHERE due_orgs.due > 2 * $12
         ),
         other_candidates AS (
             SELECT org_first.*,
                    COALESCE(org_live.live, 0) AS org_held,
                    COALESCE(org_live.live, 0) + ROW_NUMBER() OVER (
                        PARTITION BY org_first.org_id
                        ORDER BY org_first.next_attempt_at, org_first.requested_at, org_first.operation_id
                    ) AS org_slots
             FROM org_first
             LEFT JOIN org_live ON org_live.org_id = org_first.org_id
         ),
         waiting AS (
             SELECT COUNT(*) AS orgs
             FROM due_orgs
             LEFT JOIN org_live ON org_live.org_id = due_orgs.org_id
             WHERE COALESCE(org_live.live, 0) < $15
         ),
         admissible AS (
             SELECT other.*,
                    ROW_NUMBER() OVER (
                        ORDER BY other.next_attempt_at, other.requested_at, other.operation_id
                    ) AS admissible_rank
             FROM other_candidates other
             WHERE $15::BIGINT = 0
                OR other.org_slots <= $15
                -- Past its limit only while no other organization waits.
                OR (SELECT orgs FROM waiting) = CASE WHEN other.org_held < $15 THEN 1 ELSE 0 END
         ),
         first_other AS (
             SELECT operation_id, next_attempt_at, requested_at, claimable_at, reclaimed, first_start
             FROM admissible
             WHERE $15::BIGINT = 0
                OR org_slots <= $15
                OR admissible_rank <= $12 - $16
             ORDER BY next_attempt_at ASC, requested_at ASC, operation_id ASC
             LIMIT 2 * $12
         ),
         picked_other AS (
             SELECT candidate.operation_id,
                    first_other.next_attempt_at,
                    first_other.requested_at,
                    first_other.claimable_at,
                    first_other.reclaimed,
                    first_other.first_start
             FROM first_other
             JOIN public_operations candidate
               ON candidate.operation_id = first_other.operation_id
             WHERE candidate.state IN ($4, $5)
               AND candidate.next_attempt_at <= NOW()
               AND (
                   candidate.claim_token = ''
                   OR candidate.claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
             ORDER BY first_other.next_attempt_at ASC,
                      first_other.requested_at ASC,
                      first_other.operation_id ASC
             LIMIT $12
             FOR UPDATE OF candidate SKIP LOCKED
         ),
         picked_teardown AS (
             SELECT candidate.operation_id,
                    candidate.next_attempt_at,
                    candidate.requested_at,
                    GREATEST(candidate.requested_at, candidate.next_attempt_at) AS claimable_at,
                    candidate.claim_token <> '' AS reclaimed,
                    candidate.started_at IS NULL AS first_start
             FROM public_operations candidate
             WHERE candidate.state IN ($4, $5)
               AND candidate.kind = ANY($6)
               AND candidate.kind = ANY($14)
               AND candidate.next_attempt_at <= NOW()
               AND (
                   candidate.claim_token = ''
                   OR candidate.claimed_at <= NOW() - make_interval(secs => $2::BIGINT)
               )
               AND candidate.operation_id NOT IN (SELECT operation_id FROM blocked)
             ORDER BY candidate.next_attempt_at ASC, candidate.requested_at ASC, candidate.operation_id ASC
             LIMIT CASE WHEN EXISTS (SELECT 1 FROM picked_other) THEN $13 ELSE $1 END
             FOR UPDATE OF candidate SKIP LOCKED
         ),
         picked AS (
             SELECT operation_id, claimable_at, reclaimed, first_start
             FROM (
                 SELECT * FROM picked_other
                 UNION ALL
                 SELECT * FROM picked_teardown
             ) claimable
             ORDER BY next_attempt_at ASC, requested_at ASC, operation_id ASC
             LIMIT $1
         ),
         claimed AS (
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
                       o.traceparent,
                       CASE WHEN picked.first_start
                           THEN FLOOR(EXTRACT(EPOCH FROM NOW() - o.created_at))::BIGINT
                       END AS age_at_first_claim_secs,
                       (EXTRACT(EPOCH FROM NOW() - picked.claimable_at) * 1000)::DOUBLE PRECISION
                           AS queue_wait_ms,
                       picked.reclaimed
         )
         SELECT claimed.*, summary.org_limit_held_back
         FROM (
             SELECT $12::BIGINT > 0
                    AND NOT EXISTS (SELECT 1 FROM first_other)
                    AND EXISTS (SELECT 1 FROM other_candidates) AS org_limit_held_back
         ) summary
         LEFT JOIN claimed ON TRUE"
    ))
    .bind(i64::from(input.limits.total))
    .bind(duration_secs_i64(input.claim_stale_after))
    .bind(&claim_token)
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .bind(&kind_labels)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(outside_sandbox_queue_labels())
    .bind(OperationKind::DeleteSandbox.as_str())
    .bind(OperationKind::CreateSandbox.as_str())
    .bind(keyed)
    .bind(i64::from(input.limits.non_teardown.min(input.limits.total)))
    .bind(i64::from(
        input
            .limits
            .teardown_while_other_due
            .min(input.limits.total),
    ))
    .bind(&teardown_labels)
    .bind(
        input
            .limits
            .per_org_non_teardown
            .map_or(0, |limit| i64::from(limit.contended.get())),
    )
    .bind(
        input
            .limits
            .per_org_non_teardown
            .map_or(0, |limit| i64::from(limit.headroom)),
    )
    .bind(ADMISSION_CLAIM_TOKEN_PREFIX)
    .fetch_all(&mut *tx)
    .await?;

    let mut operations = Vec::with_capacity(rows.len());
    let mut queue_waits = Vec::with_capacity(rows.len());
    let mut org_limit_held_back = false;
    for row in rows {
        if row.try_get::<Option<Uuid>, _>("operation_id")?.is_none() {
            org_limit_held_back = row.try_get("org_limit_held_back")?;
            continue;
        }
        let row = DispatcherClaimRow::from_row(&row)?;
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
        pplx_space_metrics::operations::queue_wait(
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

async fn expire_claimed_operation(
    pool: &PgPool,
    input: ExpirePublicOperation,
    claim_token: &str,
) -> Result<bool, PublicOperationStoreError> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             attempts = GREATEST(attempts - 1, 0),
             started_at = NULL,
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
    .bind(claim_token)
    .bind(OperationState::Expired.as_str())
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
            PublicOperationEventType::Expired,
            expired_event(&input),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(updated.is_some())
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
    let kind_labels = input
        .kinds
        .iter()
        .map(|kind| kind.as_str().to_string())
        .collect::<Vec<_>>();
    let mut tx = pool.begin().await?;
    // Locked, so nothing claims them between this read and the write below.
    let rows = sqlx::query_as::<_, (Uuid, String, i32, String)>(sql!(
        "SELECT operation_id, kind, attempts, progress_message
         FROM public_operations
         WHERE state = $1
           AND claim_token = ''
           AND started_at IS NULL
           AND api_product = $2
           AND kind = ANY($3)
           AND created_at <= NOW() - make_interval(secs => $4::BIGINT)
         ORDER BY created_at, operation_id
         LIMIT $5
         FOR UPDATE SKIP LOCKED"
    ))
    .bind(OperationState::Queued.as_str())
    .bind(input.api_product.as_str())
    .bind(&kind_labels)
    .bind(duration_secs_i64(input.max_age))
    .bind(i64::from(input.limit))
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(Vec::new());
    }
    let overdue = rows
        .into_iter()
        .map(|(operation_id, kind, attempts, progress_message)| {
            Ok(OverdueQueuedPublicOperation {
                id: OperationId::from_uuid(operation_id),
                kind: parse_stored_enum("kind", &kind)?,
                attempts,
                progress_message,
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
    sqlx::query(sql!(
        "UPDATE public_operations o
         SET state = $1,
             completed_at = NOW(),
             progress_message = expiry.progress_message,
             terminal_error_code = expiry.terminal_error_code,
             terminal_error_json = expiry.terminal_error_json,
             payload_json = CASE WHEN o.kind = 'create_sandbox'
                 THEN o.payload_json #- '{federated_create,service_grant_base64}'::text[]
                 ELSE o.payload_json END,
             updated_at = NOW()
         FROM UNNEST($2::UUID[], $3::TEXT[], $4::TEXT[], $5::JSONB[])
              AS expiry(operation_id, progress_message, terminal_error_code, terminal_error_json)
         WHERE o.operation_id = expiry.operation_id"
    ))
    .bind(OperationState::Expired.as_str())
    .bind(&ids)
    .bind(&messages)
    .bind(&codes)
    .bind(&errors)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO public_operation_events (operation_id, sequence, event_type, event_json)
         SELECT expired.operation_id,
                COALESCE(
                    (SELECT MAX(event.sequence)
                     FROM public_operation_events event
                     WHERE event.operation_id = expired.operation_id),
                    0
                ) + 1,
                $3,
                expired.event_json
         FROM UNNEST($1::UUID[], $2::JSONB[]) AS expired(operation_id, event_json)"
    ))
    .bind(&ids)
    .bind(&events)
    .bind(PublicOperationEventType::Expired.as_str())
    .execute(&mut *tx)
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
    let kind = sqlx::query_scalar::<_, String>(sql!(
        "SELECT kind
         FROM public_operations
         WHERE operation_id = $1
           AND claim_token = $2
           AND state = $3
         FOR UPDATE"
    ))
    .bind(input.operation_id.as_uuid())
    .bind(&input.claim_token)
    .bind(OperationState::Running.as_str())
    .fetch_optional(&mut *tx)
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
    let updated = sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE public_operations
         SET state = $3,
             claim_token = '',
             progress_message = $4,
             next_attempt_at = NOW() + make_interval(secs => $5::DOUBLE PRECISION),
             attempts = GREATEST(attempts - CASE WHEN $7 THEN 0 ELSE 1 END, 0),
             started_at = CASE WHEN $8 THEN NULL ELSE started_at END,
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
    .bind(input.retry_after.as_secs_f64())
    .bind(OperationState::Running.as_str())
    .bind(input.count_attempt)
    .bind(input.undo_first_start)
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
    Ok(if updated.is_some() {
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

    // The batch's ids are collected first, as an array, so the delete finds
    // them through the primary key rather than joining the whole table. The
    // outer `expires_at` check is what a selected row is held to once its
    // lock is free: a record renewed while this waits on it is rechecked as
    // renewed and kept.
    let idempotency_rows = sqlx::query_scalar::<_, Uuid>(sql!(
        "DELETE FROM public_idempotency_records
         WHERE id = ANY(ARRAY(
             SELECT id
             FROM public_idempotency_records
             WHERE expires_at <= NOW()
             ORDER BY expires_at ASC, id ASC
             LIMIT $1
         ))
           AND expires_at <= NOW()
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
    age_at_first_claim_secs: Option<i64>,
}

#[derive(Debug, FromRow)]
struct DispatcherClaimRow {
    #[sqlx(flatten)]
    operation: ClaimedPublicOperationRow,
    queue_wait_ms: f64,
    reclaimed: bool,
}

/// One whole operation, as [`PUBLIC_OPERATION_COLUMNS`] selects it. Both the
/// public and the admin record are built from it.
#[derive(Debug, FromRow)]
struct PublicOperationRow {
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

impl TryFrom<PublicOperationRow> for AdminPublicOperationRecord {
    type Error = PublicOperationStoreError;

    fn try_from(row: PublicOperationRow) -> Result<Self, Self::Error> {
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
mod tests;
