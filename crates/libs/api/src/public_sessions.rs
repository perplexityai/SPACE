use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_core::types::IdempotencyKey;
use pplx_space_public_api::{
    DataPlaneAttachInfo, DataPlaneScope, DataPlaneSessionId, DataPlaneTokenTransport,
    OperationKind, OperationState, OperationTargetType, PublicOperationEventType,
    PublicOperationResult, PublicSandboxId, PublicSession, PublicSessionOperationResult,
    RequestFingerprint, SessionKind, SessionState, SessionTargetProtocol,
    data_plane_session_events_path,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::auth::hash_token;
use crate::public_operations::{ApiProduct, HttpOperationMethod, OperationPathTemplate};
use crate::public_store::{InvalidStoredEnum, parse_stored_enum};

const API_PRODUCT_NATIVE: ApiProduct = ApiProduct::Native;
const SESSION_OPERATION_PROGRESS: &str = "session created";
const DEFAULT_ATTACH_TOKEN_TTL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_ATTACH_DEADLINE: Duration = Duration::from_secs(60);
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const DATA_PLANE_TOKEN_PREFIX: &str = "dpt_";
const DEFAULT_DATA_PLANE_BASE_URL: &str = "";
const DATA_PLANE_BASE_URL_ENV: &str = "SPACE_DATA_PLANE_PUBLIC_URL";

pub(crate) fn data_plane_base_url_from_env() -> String {
    std::env::var(DATA_PLANE_BASE_URL_ENV)
        .unwrap_or_else(|_| DEFAULT_DATA_PLANE_BASE_URL.to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum PublicSessionStoreError {
    #[error("stored session has invalid {field}: {value}")]
    InvalidStoredEnum { field: &'static str, value: String },
    #[error("stored session has invalid id: {0}")]
    InvalidStoredSessionId(String),
    #[error("stored session has invalid sandbox id: {0}")]
    InvalidStoredSandboxId(Uuid),
    #[error("stored session has invalid target port: {0}")]
    InvalidStoredTargetPort(i32),
    #[error("stored session has invalid reconnect count: {0}")]
    InvalidStoredReconnectCount(i64),
    #[error("stored idempotency fingerprint is invalid: {0}")]
    InvalidStoredFingerprint(String),
    #[error("failed to serialize session idempotency fingerprint: {0}")]
    SerializeFingerprint(#[source] serde_json::Error),
    #[error("session idempotency conflict")]
    IdempotencyConflict,
    #[error("session idempotency record has no operation")]
    MissingIdempotencyOperation,
    #[error("session idempotency operation has no target session")]
    MissingIdempotencyTarget,
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
}

impl From<InvalidStoredEnum> for PublicSessionStoreError {
    fn from(error: InvalidStoredEnum) -> Self {
        Self::InvalidStoredEnum {
            field: error.field,
            value: error.value,
        }
    }
}

#[derive(Clone)]
pub struct PublicSessionStore {
    pool: PgPool,
    data_plane_base_url: String,
}

impl PublicSessionStore {
    pub fn new(pool: PgPool) -> Self {
        Self::with_data_plane_base_url(pool, data_plane_base_url_from_env())
    }

    pub fn with_data_plane_base_url(pool: PgPool, data_plane_base_url: impl Into<String>) -> Self {
        Self {
            pool,
            data_plane_base_url: data_plane_base_url.into(),
        }
    }

    pub async fn create_session(
        &self,
        input: CreatePublicSession<'_>,
    ) -> Result<PublicSession, PublicSessionStoreError> {
        let fingerprint = request_fingerprint(input.fingerprint_payload)?;
        let mut tx = self.pool.begin().await?;

        lock_idempotency_key(&mut tx, input.org_id, input.idempotency.clone()).await?;
        if let Some(existing) =
            find_idempotency_record(&mut tx, input.org_id, input.idempotency.clone()).await?
        {
            if existing.request_fingerprint != fingerprint {
                return Err(PublicSessionStoreError::IdempotencyConflict);
            }
            let operation_id = existing
                .operation_id
                .ok_or(PublicSessionStoreError::MissingIdempotencyOperation)?;
            let session_id = fetch_operation_target_session(&mut tx, operation_id)
                .await?
                .ok_or(PublicSessionStoreError::MissingIdempotencyTarget)?;
            let token = refresh_attach_token_tx(&mut tx, session_id).await?;
            let row = fetch_session_tx(&mut tx, input.org_id, input.sandbox_id, session_id)
                .await?
                .ok_or(PublicSessionStoreError::MissingIdempotencyTarget)?;
            tx.commit().await?;
            return PublicSessionRow::try_into_public(
                row,
                Some(token),
                self.data_plane_base_url.as_str(),
            );
        }
        let session_id = DataPlaneSessionId::new_for_kind(input.kind);
        let scope = input.kind.connect_scope();
        let token = DataPlaneToken::new();
        let operation_id = Uuid::now_v7();
        let session_id_string = session_id.to_string();
        let now = OffsetDateTime::now_utc();
        let token_expires_at = now + DEFAULT_ATTACH_TOKEN_TTL;
        let attach_deadline_at = now + DEFAULT_ATTACH_DEADLINE;
        let expires_at = now + DEFAULT_SESSION_TTL;

        let row = sqlx::query_as::<_, PublicSessionRow>(sql!(
            "INSERT INTO data_plane_sessions (
                 session_id,
                 org_id,
                 actor_user_id,
                 sandbox_id,
                 kind,
                 state,
                 scopes,
                 token_hash,
                 token_expires_at,
                 attach_deadline_at,
                 target_port,
                 target_protocol,
                 target_path,
                 adapter_state,
                 expires_at
             )
             VALUES (
                 $1, $2, $3, $4, $5, $6, $7,
                 $8, $9, $10, $11, $12, $13, $14, $15
             )
             RETURNING session_id,
                       sandbox_id,
                       kind,
                       state,
                       reconnect_count,
                       last_reconnect_reason,
                       scopes,
                       target_port,
                       target_protocol,
                       target_path,
                       created_at,
                       updated_at,
                       expires_at"
        ))
        .bind(&session_id_string)
        .bind(input.org_id)
        .bind(input.actor_user_id)
        .bind(input.sandbox_id.as_uuid())
        .bind(input.kind.as_str())
        .bind(SessionState::Pending.as_str())
        .bind(vec![scope.as_str().to_string()])
        .bind(token.hash.as_str())
        .bind(token_expires_at)
        .bind(attach_deadline_at)
        .bind(input.target_port.map(i32::from))
        .bind(input.target_protocol.map(SessionTargetProtocol::as_str))
        .bind(input.target_path)
        .bind(Json(input.adapter_state.clone()))
        .bind(expires_at)
        .fetch_one(&mut *tx)
        .await?;

        insert_completed_session_operation_tx(
            &mut tx,
            operation_id,
            input.org_id,
            input.actor_user_id,
            session_id,
            input.sandbox_id,
            input.kind,
        )
        .await?;
        insert_idempotency_record_tx(
            &mut tx,
            input.org_id,
            input.idempotency,
            &fingerprint,
            operation_id,
        )
        .await?;
        tx.commit().await?;

        pplx_space_metrics::operations::created(
            API_PRODUCT_NATIVE.as_str(),
            OperationKind::CreateSession.as_str(),
            OperationTargetType::Session.as_str(),
        );
        pplx_space_metrics::operations::completed(
            API_PRODUCT_NATIVE.as_str(),
            OperationKind::CreateSession.as_str(),
            OperationState::Succeeded.as_str(),
        );

        PublicSessionRow::try_into_public(row, Some(token), self.data_plane_base_url.as_str())
    }

    pub async fn fetch_session(
        &self,
        org_id: OrgId,
        sandbox_id: PublicSandboxId,
        session_id: DataPlaneSessionId,
    ) -> Result<Option<PublicSession>, PublicSessionStoreError> {
        let session_id_string = session_id.to_string();
        let row = sqlx::query_as::<_, PublicSessionRow>(SESSION_SELECT_SQL)
            .bind(org_id)
            .bind(sandbox_id.as_uuid())
            .bind(&session_id_string)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| {
            PublicSessionRow::try_into_public(row, None, self.data_plane_base_url.as_str())
        })
        .transpose()
    }

    pub async fn list_sessions(
        &self,
        input: ListPublicSessions,
    ) -> Result<Vec<PublicSession>, PublicSessionStoreError> {
        let kind = input.kind.map(SessionKind::as_str);
        let rows = sqlx::query_as::<_, PublicSessionRow>(sql!(
            "SELECT session_id,
                    sandbox_id,
                    kind,
                    state,
                    reconnect_count,
                    last_reconnect_reason,
                    scopes,
                    target_port,
                    target_protocol,
                    target_path,
                    created_at,
                    updated_at,
                    expires_at
             FROM data_plane_sessions
             WHERE org_id = $1
               AND sandbox_id = $2
               AND ($3::TEXT IS NULL OR kind = $3)
               AND (
                   $4::TIMESTAMPTZ IS NULL
                   OR (created_at, session_id) < ($4::TIMESTAMPTZ, $5::TEXT)
               )
               AND (expires_at IS NULL OR expires_at > NOW())
             ORDER BY created_at DESC, session_id DESC
             LIMIT $6"
        ))
        .bind(input.org_id)
        .bind(input.sandbox_id.as_uuid())
        .bind(kind)
        .bind(input.cursor.as_ref().map(|cursor| cursor.created_at))
        .bind(
            input
                .cursor
                .as_ref()
                .map(|cursor| cursor.session_id.to_string()),
        )
        .bind(i64::from(input.limit) + 1)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                PublicSessionRow::try_into_public(row, None, self.data_plane_base_url.as_str())
            })
            .collect()
    }

    pub async fn revoke_session(
        &self,
        input: RevokePublicSession<'_>,
    ) -> Result<Option<PublicSession>, PublicSessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let prior_status = sqlx::query_scalar::<_, String>(sql!(
            "SELECT revocation_status
             FROM data_plane_sessions
             WHERE org_id = $1
               AND sandbox_id = $2
               AND session_id = $3
               AND (expires_at IS NULL OR expires_at > NOW())
             FOR UPDATE"
        ))
        .bind(input.org_id)
        .bind(input.sandbox_id.as_uuid())
        .bind(input.session_id.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(prior_status) = prior_status else {
            return Ok(None);
        };
        let transitioned = prior_status != "revoked";

        let row = sqlx::query_as::<_, PublicSessionRow>(
            sql!("UPDATE data_plane_sessions
             SET revocation_epoch = CASE WHEN $5::BOOL THEN revocation_epoch + 1 ELSE revocation_epoch END,
                 revocation_status = 'revoked',
                 revocation_reason = COALESCE(revocation_reason, $4),
                 revocation_updated_at = NOW(),
                 state = 'revoked',
                 token_hash = NULL,
                 token_expires_at = NOW(),
                 attach_deadline_at = NULL,
                 updated_at = NOW()
             WHERE org_id = $1
               AND sandbox_id = $2
               AND session_id = $3
               AND (expires_at IS NULL OR expires_at > NOW())
             RETURNING session_id,
                       sandbox_id,
                       kind,
                       state,
                       reconnect_count,
                       last_reconnect_reason,
                       scopes,
                       target_port,
                       target_protocol,
                       target_path,
                       created_at,
                       updated_at,
                       expires_at"),
        )
        .bind(input.org_id)
        .bind(input.sandbox_id.as_uuid())
        .bind(input.session_id.to_string())
        .bind(input.reason)
        .bind(transitioned)
        .fetch_one(&mut *tx)
        .await?;

        if transitioned {
            sqlx::query(sql!(
                "INSERT INTO data_plane_org_revocation_epochs (org_id, epoch, updated_at)
                 VALUES ($1, 1, NOW())
                 ON CONFLICT (org_id) DO UPDATE
                     SET epoch = data_plane_org_revocation_epochs.epoch + 1,
                         updated_at = EXCLUDED.updated_at"
            ))
            .bind(input.org_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        PublicSessionRow::try_into_public(row, None, self.data_plane_base_url.as_str()).map(Some)
    }
}

#[derive(Debug, Clone)]
pub struct CreatePublicSession<'a> {
    pub org_id: OrgId,
    pub actor_user_id: Option<UserId>,
    pub sandbox_id: PublicSandboxId,
    pub kind: SessionKind,
    pub target_port: Option<u16>,
    pub target_protocol: Option<SessionTargetProtocol>,
    pub target_path: Option<&'a str>,
    pub adapter_state: serde_json::Value,
    pub fingerprint_payload: &'a serde_json::Value,
    pub idempotency: CreatePublicSessionIdempotency<'a>,
}

#[derive(Debug, Clone)]
pub struct CreatePublicSessionIdempotency<'a> {
    pub method: HttpOperationMethod,
    pub path_template: OperationPathTemplate,
    pub key: &'a IdempotencyKey,
}

#[derive(Debug, Clone)]
pub struct ListPublicSessions {
    pub org_id: OrgId,
    pub sandbox_id: PublicSandboxId,
    pub kind: Option<SessionKind>,
    pub cursor: Option<PublicSessionCursor>,
    pub limit: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicSessionCursor {
    pub created_at: OffsetDateTime,
    pub session_id: DataPlaneSessionId,
}

#[derive(Debug, Clone, Copy)]
pub struct RevokePublicSession<'a> {
    pub org_id: OrgId,
    pub sandbox_id: PublicSandboxId,
    pub session_id: DataPlaneSessionId,
    pub reason: &'a str,
}

#[derive(Debug, Clone)]
struct DataPlaneToken {
    raw: String,
    hash: String,
}

impl DataPlaneToken {
    fn new() -> Self {
        let mut bytes = [0u8; 32];
        rand::fill(&mut bytes);
        let raw = format!("{DATA_PLANE_TOKEN_PREFIX}{}", hex::encode(bytes));
        let hash = hash_token(&raw);
        Self { raw, hash }
    }
}

#[derive(Debug, Clone)]
struct PublicSessionIdempotencyRecord {
    operation_id: Option<Uuid>,
    request_fingerprint: RequestFingerprint,
}

#[derive(Debug, FromRow)]
struct PublicSessionIdempotencyRow {
    operation_id: Option<Uuid>,
    request_fingerprint: String,
}

#[derive(Debug, FromRow)]
struct PublicSessionRow {
    session_id: String,
    sandbox_id: SandboxId,
    kind: String,
    state: String,
    reconnect_count: i64,
    last_reconnect_reason: Option<String>,
    scopes: Vec<String>,
    target_port: Option<i32>,
    target_protocol: Option<String>,
    target_path: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    expires_at: Option<OffsetDateTime>,
}

const SESSION_SELECT_SQL: &str = sql!(
    "SELECT session_id,
       sandbox_id,
       kind,
       state,
       reconnect_count,
       last_reconnect_reason,
       scopes,
       target_port,
       target_protocol,
       target_path,
       created_at,
       updated_at,
       expires_at
FROM data_plane_sessions
WHERE org_id = $1
  AND sandbox_id = $2
  AND session_id = $3
  AND (expires_at IS NULL OR expires_at > NOW())"
);

async fn find_idempotency_record(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    idempotency: CreatePublicSessionIdempotency<'_>,
) -> Result<Option<PublicSessionIdempotencyRecord>, PublicSessionStoreError> {
    // The lookup ignores an expired record, and the create that follows must
    // replace it, so it is removed here, in the transaction that inserts its
    // successor (see `delete_expired_idempotency_record_tx`). The key is
    // unique, so a live record and an expired one never coexist: this deletes
    // only when the lookup comes back empty. `NOW()` is the transaction's
    // start, so the two predicates partition the record exactly.
    let row = sqlx::query_as::<_, PublicSessionIdempotencyRow>(sql!(
        "WITH expired AS (
             DELETE FROM public_idempotency_records
             WHERE org_id = $1
               AND api_product = $2
               AND method = $3
               AND path_template = $4
               AND idempotency_key = $5
               AND expires_at <= NOW()
         )
         SELECT operation_id, request_fingerprint
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
    .bind(API_PRODUCT_NATIVE.as_str())
    .bind(idempotency.method.as_str())
    .bind(idempotency.path_template.as_str())
    .bind(idempotency.key.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    row.map(TryInto::try_into).transpose()
}

async fn lock_idempotency_key(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    idempotency: CreatePublicSessionIdempotency<'_>,
) -> Result<(), PublicSessionStoreError> {
    sqlx::query(sql!("SELECT pg_advisory_xact_lock($1)"))
        .bind(idempotency_lock_key(org_id, idempotency))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn idempotency_lock_key(org_id: OrgId, idempotency: CreatePublicSessionIdempotency<'_>) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(org_id.as_uuid().as_bytes());
    hasher.update([0]);
    hasher.update(API_PRODUCT_NATIVE.as_str().as_bytes());
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
            .expect("sha256 digest has at least eight bytes"),
    );
    i64::from_be_bytes(key)
}

async fn fetch_operation_target_session(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
) -> Result<Option<DataPlaneSessionId>, PublicSessionStoreError> {
    let value = sqlx::query_scalar::<_, String>(sql!(
        "SELECT target_id
         FROM public_operations
         WHERE operation_id = $1
           AND kind = $2
           AND target_type = $3"
    ))
    .bind(operation_id)
    .bind(OperationKind::CreateSession.as_str())
    .bind(OperationTargetType::Session.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    value
        .map(|session_id| {
            session_id
                .parse::<DataPlaneSessionId>()
                .map_err(|_| PublicSessionStoreError::InvalidStoredSessionId(session_id))
        })
        .transpose()
}

async fn refresh_attach_token_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: DataPlaneSessionId,
) -> Result<DataPlaneToken, sqlx::Error> {
    let token = DataPlaneToken::new();
    let now = OffsetDateTime::now_utc();
    let session_id_string = session_id.to_string();
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET token_hash = $2,
             token_expires_at = $3,
             attach_deadline_at = $4,
             updated_at = NOW()
         WHERE session_id = $1"
    ))
    .bind(&session_id_string)
    .bind(token.hash.as_str())
    .bind(now + DEFAULT_ATTACH_TOKEN_TTL)
    .bind(now + DEFAULT_ATTACH_DEADLINE)
    .execute(&mut **tx)
    .await?;
    Ok(token)
}

async fn fetch_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
    session_id: DataPlaneSessionId,
) -> Result<Option<PublicSessionRow>, sqlx::Error> {
    let session_id_string = session_id.to_string();
    sqlx::query_as::<_, PublicSessionRow>(SESSION_SELECT_SQL)
        .bind(org_id)
        .bind(sandbox_id.as_uuid())
        .bind(&session_id_string)
        .fetch_optional(&mut **tx)
        .await
}

async fn insert_completed_session_operation_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
    org_id: OrgId,
    actor_user_id: Option<UserId>,
    session_id: DataPlaneSessionId,
    sandbox_id: PublicSandboxId,
    kind: SessionKind,
) -> Result<(), sqlx::Error> {
    let result = PublicOperationResult::Session(PublicSessionOperationResult {
        session_id,
        sandbox_id,
        kind,
    });
    let result_json = serde_json::to_value(&result).expect("public session result serializes");

    // The operation is new, so its completion is its first event: sequence 1
    // is what `append_operation_event_tx` would compute. The event's foreign
    // key is checked at the end of the statement, after the operation row.
    sqlx::query(sql!(
        "WITH operation AS (
         INSERT INTO public_operations (
             operation_id,
             org_id,
             actor_user_id,
             api_product,
             kind,
             target_type,
             target_id,
             state,
             progress_message,
             result_json,
             started_at,
             completed_at,
             gc_after
         )
         VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8, $9, $10, NOW(), NOW(),
             NOW() + ('7 days')::INTERVAL
         )
         RETURNING operation_id
         )
         INSERT INTO public_operation_events (
             operation_id,
             sequence,
             event_type,
             event_json
         )
         SELECT operation_id, 1, $11, $12
         FROM operation"
    ))
    .bind(operation_id)
    .bind(org_id)
    .bind(actor_user_id)
    .bind(API_PRODUCT_NATIVE.as_str())
    .bind(OperationKind::CreateSession.as_str())
    .bind(OperationTargetType::Session.as_str())
    .bind(session_id.to_string())
    .bind(OperationState::Succeeded.as_str())
    .bind(SESSION_OPERATION_PROGRESS)
    .bind(Json(result_json.clone()))
    .bind(PublicOperationEventType::Completed.as_str())
    .bind(Json(serde_json::json!({
        "state": OperationState::Succeeded.as_str(),
        "result": result_json,
    })))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_idempotency_record_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    idempotency: CreatePublicSessionIdempotency<'_>,
    request_fingerprint: &RequestFingerprint,
    operation_id: Uuid,
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
             NOW() + ('24 hours')::INTERVAL
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .bind(API_PRODUCT_NATIVE.as_str())
    .bind(idempotency.method.as_str())
    .bind(idempotency.path_template.as_str())
    .bind(idempotency.key.as_str())
    .bind(request_fingerprint.as_str())
    .bind(operation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn request_fingerprint(
    payload: &serde_json::Value,
) -> Result<RequestFingerprint, PublicSessionStoreError> {
    let bytes =
        serde_json::to_vec(payload).map_err(PublicSessionStoreError::SerializeFingerprint)?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest))
        .map_err(|_| PublicSessionStoreError::InvalidStoredFingerprint("generated".to_string()))
}

fn attach_url(base_url: &str, session_id: DataPlaneSessionId, kind: SessionKind) -> String {
    let base_url = base_url.trim_end_matches('/');
    let suffix = match kind {
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch => "attach",
        SessionKind::Port | SessionKind::Protocol => "proxy",
    };
    if base_url.is_empty() {
        format!("/v1/sessions/{session_id}/{suffix}")
    } else {
        format!("{base_url}/v1/sessions/{session_id}/{suffix}")
    }
}

fn events_url(base_url: &str, session_id: DataPlaneSessionId, kind: SessionKind) -> Option<String> {
    if !matches!(kind, SessionKind::Process | SessionKind::FileWatch) {
        return None;
    }
    let base_url = base_url.trim_end_matches('/');
    let path = data_plane_session_events_path(session_id);
    if base_url.is_empty() {
        Some(path)
    } else {
        Some(format!("{base_url}{path}"))
    }
}

fn token_transport(kind: SessionKind) -> Vec<DataPlaneTokenTransport> {
    match kind {
        SessionKind::Terminal | SessionKind::Process | SessionKind::FileWatch => vec![
            DataPlaneTokenTransport::FirstFrame,
            DataPlaneTokenTransport::SecWebSocketProtocol,
            DataPlaneTokenTransport::AuthorizationHeader,
        ],
        SessionKind::Port | SessionKind::Protocol => vec![
            DataPlaneTokenTransport::AuthorizationHeader,
            DataPlaneTokenTransport::SecWebSocketProtocol,
        ],
    }
}

impl PublicSessionRow {
    fn try_into_public(
        row: Self,
        token: Option<DataPlaneToken>,
        data_plane_base_url: &str,
    ) -> Result<PublicSession, PublicSessionStoreError> {
        let id = row
            .session_id
            .parse::<DataPlaneSessionId>()
            .map_err(|_| PublicSessionStoreError::InvalidStoredSessionId(row.session_id.clone()))?;
        let kind = parse_stored_enum("kind", &row.kind)?;
        let sandbox_id = PublicSandboxId::from(row.sandbox_id);
        let scopes = row
            .scopes
            .iter()
            .map(|scope| parse_stored_enum("scope", scope))
            .collect::<Result<Vec<DataPlaneScope>, _>>()?;
        let target_port = row
            .target_port
            .map(|port| {
                u16::try_from(port)
                    .map_err(|_| PublicSessionStoreError::InvalidStoredTargetPort(port))
            })
            .transpose()?;
        let attach = token.map(|token| DataPlaneAttachInfo {
            url: attach_url(data_plane_base_url, id, kind),
            events_url: events_url(data_plane_base_url, id, kind),
            token: token.raw,
            expires_at: OffsetDateTime::now_utc() + DEFAULT_ATTACH_TOKEN_TTL,
            token_transport: token_transport(kind),
        });

        Ok(PublicSession {
            id,
            sandbox_id,
            kind,
            state: parse_stored_enum("state", &row.state)?,
            reconnect_count: u64::try_from(row.reconnect_count).map_err(|_| {
                PublicSessionStoreError::InvalidStoredReconnectCount(row.reconnect_count)
            })?,
            last_reconnect_reason: row
                .last_reconnect_reason
                .as_deref()
                .map(|reason| parse_stored_enum("last_reconnect_reason", reason))
                .transpose()?,
            scopes,
            target_port,
            target_protocol: row
                .target_protocol
                .as_deref()
                .map(|protocol| parse_stored_enum("target_protocol", protocol))
                .transpose()?,
            target_path: row.target_path,
            attach,
            created_at: row.created_at,
            updated_at: row.updated_at,
            expires_at: row.expires_at,
        })
    }
}

impl TryFrom<PublicSessionIdempotencyRow> for PublicSessionIdempotencyRecord {
    type Error = PublicSessionStoreError;

    fn try_from(row: PublicSessionIdempotencyRow) -> Result<Self, Self::Error> {
        Ok(Self {
            operation_id: row.operation_id,
            request_fingerprint: RequestFingerprint::parse(row.request_fingerprint.clone())
                .map_err(|_| {
                    PublicSessionStoreError::InvalidStoredFingerprint(row.request_fingerprint)
                })?,
        })
    }
}

#[derive(Debug, Serialize)]
struct SessionFingerprintPayload<'a> {
    sandbox_id: PublicSandboxId,
    kind: SessionKind,
    target_port: Option<u16>,
    target_protocol: Option<SessionTargetProtocol>,
    target_path: Option<&'a str>,
    request: &'a serde_json::Value,
}

pub fn session_fingerprint_payload(
    sandbox_id: PublicSandboxId,
    kind: SessionKind,
    target_port: Option<u16>,
    target_protocol: Option<SessionTargetProtocol>,
    target_path: Option<&str>,
    request: &serde_json::Value,
) -> serde_json::Value {
    serde_json::to_value(SessionFingerprintPayload {
        sandbox_id,
        kind,
        target_port,
        target_protocol,
        target_path,
        request,
    })
    .expect("serializing session fingerprint payload should not fail")
}

#[cfg(test)]
mod tests;
