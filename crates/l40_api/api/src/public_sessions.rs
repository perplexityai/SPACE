use pplx_space_accounts::{OrgId, UserId};
use pplx_space_data_plane_session_store::{
    self as data_plane_session_store, Session as PublicSessionRow,
};
use pplx_space_public_operation_store as operation_store;
use std::collections::HashMap;
use std::time::Duration;

use pplx_space_guest_process::IdempotencyKey;
use pplx_space_public_api::{
    DataPlaneAttachInfo, DataPlaneScope, DataPlaneSessionId, DataPlaneTokenTransport,
    OperationKind, OperationState, OperationTargetType, PublicOperationResult, PublicSandboxId,
    PublicSession, PublicSessionOperationResult, RequestFingerprint, SessionKind, SessionState,
    SessionTargetProtocol, data_plane_session_events_path,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::auth::hash_token;
use crate::public_operations::{ApiProduct, HttpOperationMethod, OperationPathTemplate};
use crate::public_store::{InvalidStoredEnum, parse_stored_enum};

const API_PRODUCT_NATIVE: ApiProduct = ApiProduct::Native;
/// How long a session create's idempotency record replays it.
const IDEMPOTENCY_TTL_HOURS: i64 = 24;
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
    #[error("session was revoked or has ended")]
    SessionEnded,
    #[error("database error: {0}")]
    Sql(#[from] sqlx::Error),
}

impl From<pplx_space_postgres_pool::DbError> for PublicSessionStoreError {
    fn from(error: pplx_space_postgres_pool::DbError) -> Self {
        Self::from(sqlx::Error::from(error))
    }
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

    /// Create a session, or replay the one this idempotency key created;
    /// a new session only when `admit` allows it.
    ///
    /// `admit` runs only after the idempotency lookup misses, so a retry of a
    /// request that already created its session replays it even if the state
    /// `admit` judges has changed since.
    pub async fn create_session<E>(
        &self,
        input: CreatePublicSession<'_>,
        admit: impl FnOnce() -> Result<(), E>,
    ) -> Result<Result<PublicSession, E>, PublicSessionStoreError> {
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
            // A replay must not hand out a token for a session that is revoked,
            // expired or gone. The create wrote the session, operation and key
            // record together under the key's lock, so a missing row was swept
            // by retention after it ended. An expired row means the API host's
            // clock ran behind the database's, which set the key's expiry.
            let Some(token) = refresh_attach_token_tx(&mut tx, session_id).await? else {
                return Err(PublicSessionStoreError::SessionEnded);
            };
            let row = fetch_session_tx(&mut tx, input.org_id, input.sandbox_id, session_id)
                .await?
                .ok_or(PublicSessionStoreError::MissingIdempotencyTarget)?;
            tx.commit().await?;
            return session_into_public(row, Some(token), self.data_plane_base_url.as_str())
                .map(Ok);
        }
        if let Err(refusal) = admit() {
            return Ok(Err(refusal));
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

        let scopes = vec![scope.as_str().to_string()];
        let row = data_plane_session_store::insert_session(
            &mut *tx,
            &data_plane_session_store::NewSession {
                session_id: &session_id_string,
                org_id: input.org_id,
                actor_user_id: input.actor_user_id,
                sandbox_id: input.sandbox_id.into(),
                kind: input.kind.as_str(),
                state: SessionState::Pending.as_str(),
                scopes: &scopes,
                token_hash: token.hash.as_str(),
                token_expires_at,
                attach_deadline_at,
                target_port: input.target_port.map(i32::from),
                target_protocol: input.target_protocol.map(SessionTargetProtocol::as_str),
                target_path: input.target_path,
                adapter_state: &input.adapter_state,
                expires_at,
            },
        )
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

        crate::metrics::operations::created(
            API_PRODUCT_NATIVE.as_str(),
            OperationKind::CreateSession.as_str(),
            OperationTargetType::Session.as_str(),
        );
        crate::metrics::operations::completed(
            API_PRODUCT_NATIVE.as_str(),
            OperationKind::CreateSession.as_str(),
            OperationState::Succeeded.as_str(),
        );

        session_into_public(row, Some(token), self.data_plane_base_url.as_str()).map(Ok)
    }

    pub async fn fetch_session(
        &self,
        org_id: OrgId,
        sandbox_id: PublicSandboxId,
        session_id: DataPlaneSessionId,
    ) -> Result<Option<PublicSession>, PublicSessionStoreError> {
        let session_id_string = session_id.to_string();
        let row = data_plane_session_store::live_session(
            &self.pool,
            org_id,
            sandbox_id.into(),
            &session_id_string,
        )
        .await?;

        row.map(|row| session_into_public(row, None, self.data_plane_base_url.as_str()))
            .transpose()
    }

    pub async fn list_sessions(
        &self,
        input: ListPublicSessions,
    ) -> Result<Vec<PublicSession>, PublicSessionStoreError> {
        let kind = input.kind.map(SessionKind::as_str);
        let cursor_session_id = input
            .cursor
            .as_ref()
            .map(|cursor| cursor.session_id.to_string());
        let cursor =
            input
                .cursor
                .as_ref()
                .zip(cursor_session_id.as_deref())
                .map(
                    |(cursor, session_id)| data_plane_session_store::SessionCursor {
                        created_at: cursor.created_at,
                        session_id,
                    },
                );
        let rows = data_plane_session_store::list_live_sessions(
            &self.pool,
            input.org_id,
            input.sandbox_id.into(),
            kind,
            cursor,
            i64::from(input.limit) + 1,
        )
        .await?;

        rows.into_iter()
            .map(|row| session_into_public(row, None, self.data_plane_base_url.as_str()))
            .collect()
    }

    pub async fn revoke_session(
        &self,
        input: RevokePublicSession<'_>,
    ) -> Result<Option<PublicSession>, PublicSessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let session_id = input.session_id.to_string();
        let prior_status = data_plane_session_store::lock_live_revocation_status(
            &mut *tx,
            input.org_id,
            input.sandbox_id.into(),
            &session_id,
        )
        .await?;
        let Some(prior_status) = prior_status else {
            return Ok(None);
        };
        let transitioned = prior_status != "revoked";

        // The row lock above keeps other writers off the row until the
        // transaction ends, and `NOW()` is the transaction's start in both
        // statements, so the revoke matches the row the lock read.
        let row = data_plane_session_store::revoke_live_session(
            &mut *tx,
            input.org_id,
            input.sandbox_id.into(),
            &session_id,
            input.reason,
            transitioned,
        )
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

        if transitioned {
            data_plane_session_store::advance_org_revocation_epoch(&mut *tx, input.org_id).await?;
        }

        tx.commit().await?;

        session_into_public(row, None, self.data_plane_base_url.as_str()).map(Some)
    }

    /// The guest process id the data plane bound `session_id` to, if it has
    /// bound one.
    ///
    /// Expired and revoked sessions are read too: the process can outlive
    /// its session. Session retention keeps a row bound to an id other than
    /// its session id until the sandbox is deleted, so the binding lasts as
    /// long as the process can.
    pub async fn bound_process_id(
        &self,
        org_id: OrgId,
        sandbox_id: PublicSandboxId,
        session_id: DataPlaneSessionId,
    ) -> Result<Option<String>, PublicSessionStoreError> {
        Ok(data_plane_session_store::bound_process_id(
            &self.pool,
            org_id,
            sandbox_id.into(),
            &session_id.to_string(),
        )
        .await?)
    }

    /// The sessions in `sandbox_id` the data plane bound to any of
    /// `process_ids`, keyed by the guest process id each is bound to.
    /// Expired and revoked sessions are read too, as in
    /// [`Self::bound_process_id`].
    pub async fn sessions_bound_to_processes(
        &self,
        org_id: OrgId,
        sandbox_id: PublicSandboxId,
        process_ids: &[String],
    ) -> Result<HashMap<String, DataPlaneSessionId>, PublicSessionStoreError> {
        let rows = data_plane_session_store::sessions_bound_to_processes(
            &self.pool,
            org_id,
            sandbox_id.into(),
            process_ids,
        )
        .await?;
        rows.into_iter()
            .map(|row| {
                let session_id = row
                    .session_id
                    .parse()
                    .map_err(|_| PublicSessionStoreError::InvalidStoredSessionId(row.session_id))?;
                Ok((row.backend_process_id, session_id))
            })
            .collect()
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
    let row = operation_store::idempotency::delete_expired_and_lock_live(
        &mut **tx,
        API_PRODUCT_NATIVE.as_str(),
        operation_store::idempotency::IdempotencyKeyRef {
            org_id,
            method: idempotency.method.as_str(),
            path_template: idempotency.path_template.as_str(),
            idempotency_key: idempotency.key.as_str(),
        },
    )
    .await?;

    row.map(TryInto::try_into).transpose()
}

async fn lock_idempotency_key(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    idempotency: CreatePublicSessionIdempotency<'_>,
) -> Result<(), PublicSessionStoreError> {
    operation_store::idempotency::lock_key(&mut **tx, idempotency_lock_key(org_id, idempotency))
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
    let value = pplx_space_public_operation_store::operations::target_id(
        &mut **tx,
        operation_id,
        OperationKind::CreateSession,
        OperationTargetType::Session,
    )
    .await?;

    value
        .map(|session_id| {
            session_id
                .parse::<DataPlaneSessionId>()
                .map_err(|_| PublicSessionStoreError::InvalidStoredSessionId(session_id))
        })
        .transpose()
}

/// Mint a token for a replayed session, or `None` when the session was revoked,
/// has expired or no longer exists. The row lock this takes orders the replay
/// against a concurrent revoke.
async fn refresh_attach_token_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: DataPlaneSessionId,
) -> Result<Option<DataPlaneToken>, sqlx::Error> {
    let token = DataPlaneToken::new();
    let now = OffsetDateTime::now_utc();
    let session_id_string = session_id.to_string();
    let refreshed = data_plane_session_store::refresh_live_attach_token(
        &mut **tx,
        &session_id_string,
        token.hash.as_str(),
        now + DEFAULT_ATTACH_TOKEN_TTL,
        now + DEFAULT_ATTACH_DEADLINE,
    )
    .await?;
    Ok(refreshed.then_some(token))
}

async fn fetch_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
    session_id: DataPlaneSessionId,
) -> Result<Option<PublicSessionRow>, sqlx::Error> {
    let session_id_string = session_id.to_string();
    data_plane_session_store::live_session(&mut **tx, org_id, sandbox_id.into(), &session_id_string)
        .await
        .map_err(sqlx::Error::from)
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
    // is what `operation_store::events::append` would compute.
    pplx_space_public_operation_store::operations::insert_completed_session_create(
        &mut **tx,
        pplx_space_public_operation_store::operations::CompletedSessionCreate {
            operation_id,
            org_id,
            actor_user_id,
            api_product: API_PRODUCT_NATIVE.as_str(),
            session_id: session_id.to_string(),
            progress_message: SESSION_OPERATION_PROGRESS,
            result_json: result_json.clone(),
            event_json: serde_json::json!({
                "state": OperationState::Succeeded.as_str(),
                "result": result_json,
            }),
        },
    )
    .await
    .map_err(sqlx::Error::from)
}

async fn insert_idempotency_record_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    idempotency: CreatePublicSessionIdempotency<'_>,
    request_fingerprint: &RequestFingerprint,
    operation_id: Uuid,
) -> Result<(), sqlx::Error> {
    pplx_space_public_operation_store::idempotency::insert_for_operation(
        &mut **tx,
        Uuid::now_v7(),
        API_PRODUCT_NATIVE.as_str(),
        pplx_space_public_operation_store::idempotency::IdempotencyKeyRef {
            org_id,
            method: idempotency.method.as_str(),
            path_template: idempotency.path_template.as_str(),
            idempotency_key: idempotency.key.as_str(),
        },
        request_fingerprint.as_str(),
        operation_id,
        IDEMPOTENCY_TTL_HOURS,
    )
    .await
    .map_err(sqlx::Error::from)
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

fn session_into_public(
    row: PublicSessionRow,
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
            u16::try_from(port).map_err(|_| PublicSessionStoreError::InvalidStoredTargetPort(port))
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

impl TryFrom<operation_store::idempotency::OperationKeyRow> for PublicSessionIdempotencyRecord {
    type Error = PublicSessionStoreError;

    fn try_from(row: operation_store::idempotency::OperationKeyRow) -> Result<Self, Self::Error> {
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
