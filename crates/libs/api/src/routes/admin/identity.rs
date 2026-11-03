use pplx_space_common_core::types::{OrgId, SandboxOwnerSource, UserId};

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use pplx_space_common_core::{
    sql,
    types::{KekProvider, platform_default_kek_provider},
};
use pplx_space_sandbox_store::UserSandboxRows;
use time::OffsetDateTime;
use uuid::Uuid;

use super::super::authz;
use super::support;
use super::template_store::OrgTemplateRows;
use crate::{
    auth::hash_password,
    middleware::auth::AuthUser,
    models::{
        AdminUserResponse, Cluster, CreateOrgRequest, CreateUserRequest, OrgMasterKey,
        Organization, UpdateClusterRequest, UpdateOrgRequest, UpdateUserRequest,
        UpdateUserRoleRequest, User,
    },
    routes::AppState,
    user_role::UserRole,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum IdentityAdminError {
    #[error("not authorized")]
    Forbidden,
    #[error("resource not found")]
    NotFound,
    #[error("cannot modify your own role or delete yourself")]
    SelfMutation,
    /// The user still owns rows whose foreign key to `users` restricts the
    /// delete. Ownership is not reassigned here, so those rows have to go
    /// first.
    #[error("{}", .0.message())]
    StillOwns(UserDeleteBlocker),
    /// The organization still has rows whose foreign key refuses its delete.
    #[error("{}", .0.message())]
    OrgNotEmpty(OrgDeleteBlocker),
    /// Another user already has the email (`users_email_key`).
    #[error("email already in use")]
    EmailInUse,
    #[error("{0}")]
    InvalidRequest(&'static str),
    /// A request field failed validation; the body names the field.
    #[error("{message}")]
    InvalidField {
        field: &'static str,
        message: &'static str,
    },
    #[error("internal server error")]
    Internal(#[source] sqlx::Error),
    #[error("internal server error")]
    Password(#[from] bcrypt::BcryptError),
    /// A statement lost a race for a lock. `status` and `message` come from
    /// [`support::contention_disposition`] so this surface answers exactly as
    /// the handlers that return `support::db_error` do — 503 for `55P03`, 409
    /// for a serialization failure or deadlock.
    ///
    /// Every statement here can reach it, not only `set_org_master_key`'s
    /// explicit `SELECT ... FOR UPDATE` on `organizations`: the password and
    /// role writes take row locks, and a plain admin `SELECT` blocks behind
    /// DDL on the table it reads.
    #[error("{message}")]
    Contended {
        status: StatusCode,
        message: &'static str,
    },
}

/// Classify rather than derive `#[from]`: the derive would send every SQLSTATE
/// to `Internal`, and a lost lock race is not an internal failure.
impl From<sqlx::Error> for IdentityAdminError {
    fn from(error: sqlx::Error) -> Self {
        // `ReadOnly` even though most of this surface writes. This is a blanket
        // conversion covering both the master-key write (which takes a
        // `SELECT ... FOR UPDATE` on `organizations`) and plain admin reads, so
        // one scope has to answer for both. `Writing` would tell a read that
        // its own write raced another admin write, which is a false statement
        // to the caller; `ReadOnly` only withholds the write-specific 409 and
        // answers every contention SQLSTATE with the retryable 503. Both are
        // retryable, so the cost of under-claiming is a less specific status,
        // not a wrong disposition — and that is the direction to err in.
        match support::contention_disposition(support::ContentionScope::ReadOnly, &error) {
            Some((status, message)) => Self::Contended { status, message },
            None => Self::Internal(error),
        }
    }
}

impl IdentityAdminError {
    const fn status(&self) -> StatusCode {
        match self {
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::SelfMutation | Self::StillOwns(_) | Self::OrgNotEmpty(_) | Self::EmailInUse => {
                StatusCode::CONFLICT
            }
            Self::InvalidRequest(_) | Self::InvalidField { .. } => StatusCode::BAD_REQUEST,
            Self::Internal(_) | Self::Password(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Contended { status, .. } => *status,
        }
    }

    /// A stable machine-readable reason for the 409s callers must tell
    /// apart. It is not `code`, which elsewhere in `/admin/api/v1` is the
    /// status category. The other variants have none; only
    /// [`Self::InvalidField`] adds a `field` to the message.
    const fn reason(&self) -> Option<&'static str> {
        match self {
            Self::SelfMutation => Some("self_mutation"),
            Self::StillOwns(blocker) => Some(blocker.reason()),
            Self::OrgNotEmpty(blocker) => Some(blocker.reason()),
            Self::EmailInUse => Some("email_in_use"),
            Self::Forbidden
            | Self::NotFound
            | Self::InvalidRequest(_)
            | Self::InvalidField { .. }
            | Self::Internal(_)
            | Self::Password(_)
            | Self::Contended { .. } => None,
        }
    }

    /// A failed `users` insert or update: a unique violation on
    /// `users_email_key` is [`Self::EmailInUse`], anything else converts as
    /// usual.
    fn from_user_write(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &error
            && db.is_unique_violation()
            && db.constraint() == Some("users_email_key")
        {
            return Self::EmailInUse;
        }
        Self::from(error)
    }
}

impl IntoResponse for IdentityAdminError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut body = serde_json::Map::new();
        body.insert("error".to_string(), self.to_string().into());
        if let Some(reason) = self.reason() {
            body.insert("reason".to_string(), reason.into());
        }
        if let Self::InvalidField { field, .. } = &self {
            body.insert("field".to_string(), (*field).into());
        }
        let retained_until = match &self {
            Self::StillOwns(UserDeleteBlocker::DeletedSandboxes { retained_until }) => {
                Some(*retained_until)
            }
            Self::OrgNotEmpty(OrgDeleteBlocker::DeletedTemplates { retained_until }) => {
                *retained_until
            }
            _ => None,
        };
        if let Some(retained_until) = retained_until {
            body.insert(
                "retained_until".to_string(),
                format_rfc3339(retained_until).into(),
            );
        }
        (status, Json(body)).into_response()
    }
}

/// A row kind whose foreign key to `users` has no `ON DELETE` action, so it
/// refuses the user's delete while any such row exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UserDeleteBlocker {
    /// `sandboxes.user_id`, which includes sandboxes still being torn down.
    Sandboxes,
    /// `sandboxes.user_id` held only by deleted sandboxes, whose rows
    /// tombstone retention keeps until about `retained_until`.
    DeletedSandboxes { retained_until: OffsetDateTime },
    /// `sandbox_archives.user_id` (`ON DELETE RESTRICT`).
    ArchivedSandboxes,
}

impl UserDeleteBlocker {
    /// The blocker named by a foreign-key violation from `DELETE FROM users`,
    /// or `None` for any other database error.
    fn from_delete_error(error: &sqlx::Error) -> Option<Self> {
        let sqlx::Error::Database(db) = error else {
            return None;
        };
        if !db.is_foreign_key_violation() {
            return None;
        }
        match db.constraint()? {
            "sandboxes_user_id_fkey" => Some(Self::Sandboxes),
            "sandbox_archives_user_id_fkey" => Some(Self::ArchivedSandboxes),
            _ => None,
        }
    }

    /// Refine [`Self::Sandboxes`] to [`Self::DeletedSandboxes`] when every
    /// sandbox row still holding the user is already deleted, so the operator
    /// is told to wait out retention rather than delete them again.
    async fn refine(self, state: &AppState, user_id: UserId) -> Result<Self, IdentityAdminError> {
        if self != Self::Sandboxes {
            return Ok(self);
        }
        let rows = pplx_space_sandbox_store::user_sandbox_rows(&state.pool, user_id)
            .await
            .map_err(
                |pplx_space_sandbox_store::SandboxStoreError::Database(error)| {
                    IdentityAdminError::from(error)
                },
            )?;
        Ok(match rows {
            UserSandboxRows::DeletedOnly { retained_until } => {
                Self::DeletedSandboxes { retained_until }
            }
            // `None`: the rows went between the delete and this read; the
            // operator's retry then succeeds.
            UserSandboxRows::Live | UserSandboxRows::None => Self::Sandboxes,
        })
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Sandboxes => "user_owns_sandboxes",
            Self::DeletedSandboxes { .. } => "user_owns_retained_deleted_sandboxes",
            Self::ArchivedSandboxes => "user_owns_archived_sandboxes",
        }
    }

    fn message(self) -> String {
        match self {
            Self::Sandboxes => {
                "user still owns sandboxes; delete them before deleting the user".to_string()
            }
            Self::DeletedSandboxes { retained_until } => format!(
                "user's sandboxes are all deleted, but their records are kept for retention \
                 until about {}; retry the delete after that",
                format_rfc3339(retained_until)
            ),
            Self::ArchivedSandboxes => "user still owns archived sandboxes; they must be purged \
                                        before the user can be deleted"
                .to_string(),
        }
    }
}

/// A row kind whose foreign key refuses an organization's delete: one that
/// references `organizations` with no `ON DELETE` action or `RESTRICT`, or
/// that restricts the delete of a row the organization's delete cascades to.
/// API keys, credentials, operations and the other rows that reference
/// `organizations` with `ON DELETE CASCADE` go with the organization, and its
/// child organizations are detached (`parent_id` is set to `NULL`).
///
/// When several kinds remain, the error names the first one the database
/// checks; the next delete names the next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OrgDeleteBlocker {
    /// `users.org_id`.
    Users,
    /// `sandboxes.org_id`, which includes deleted sandboxes kept for
    /// retention. The encrypted-field and volume-encryption rows of the
    /// organization's sandboxes and snapshots also restrict the delete of the
    /// master key the organization's delete cascades to.
    Sandboxes,
    /// `sandbox_archives.org_id` (`ON DELETE RESTRICT`).
    ArchivedSandboxes,
    /// `templates.org_id`, and the encrypted-field rows of the organization's
    /// templates and template releases through its master key, while at least
    /// one of its templates is not deleted.
    Templates,
    /// The same rows when every template is deleted: tombstone retention
    /// keeps a deleted template's row, which the template routes no longer
    /// show, until about `retained_until`, or with no time to name while a
    /// node still caches it.
    DeletedTemplates {
        retained_until: Option<OffsetDateTime>,
    },
    /// `snapshot_storage_location_encrypted_fields_kek_fkey` once the
    /// organization has no sandbox rows left: a snapshot storage location its
    /// deleted snapshots left behind, still encrypted under its master key.
    /// The snapshot storage sweeper does not remove encrypted locations, so
    /// neither deleting sandboxes nor waiting clears it.
    OrphanedSnapshotStorage,
    /// `node_groups.org_id`.
    NodeGroups,
    /// `clusters.managing_org_id` (`ON DELETE RESTRICT`). The handler refuses
    /// the managing organization before deleting; this is the managing
    /// organization changing between that check and the delete.
    ManagesCluster,
}

impl OrgDeleteBlocker {
    /// The blocker named by a foreign-key violation from
    /// `DELETE FROM organizations`, or `None` for any other database error.
    /// [`Self::refine`] then tells apart the cases the constraint alone does
    /// not.
    fn from_delete_error(error: &sqlx::Error) -> Option<Self> {
        let sqlx::Error::Database(db) = error else {
            return None;
        };
        if !db.is_foreign_key_violation() {
            return None;
        }
        match db.constraint()? {
            "users_org_id_fkey" => Some(Self::Users),
            "sandboxes_org_id_fkey"
            | "sandbox_encrypted_fields_master_key_id_org_id_fkey"
            | "sandbox_volume_encryption_master_key_id_org_id_fkey" => Some(Self::Sandboxes),
            "snapshot_storage_location_encrypted_fields_kek_fkey" => {
                Some(Self::OrphanedSnapshotStorage)
            }
            "sandbox_archives_org_id_fkey" => Some(Self::ArchivedSandboxes),
            "templates_org_id_fkey"
            | "template_encrypted_fields_master_key_id_org_id_fkey"
            | "template_release_storage_location_encrypted_fields_kek_fkey" => {
                Some(Self::Templates)
            }
            "node_groups_org_id_fkey" => Some(Self::NodeGroups),
            "clusters_managing_org_id_fkey" => Some(Self::ManagesCluster),
            _ => None,
        }
    }

    /// Refine [`Self::Templates`] to [`Self::DeletedTemplates`] when every
    /// template row still holding the organization is deleted, so the
    /// operator is told to wait out retention rather than delete templates
    /// the template routes no longer show. Refine
    /// [`Self::OrphanedSnapshotStorage`] to [`Self::Sandboxes`] while the
    /// organization still has sandbox rows: the location may belong to one
    /// of their snapshots, and they have to go first in any case.
    async fn refine(self, state: &AppState, org_id: OrgId) -> Result<Self, IdentityAdminError> {
        match self {
            Self::Templates => Ok(
                match super::template_store::org_template_rows(&state.pool, org_id).await? {
                    OrgTemplateRows::DeletedOnly { retained_until } => {
                        Self::DeletedTemplates { retained_until }
                    }
                    // `None`: the rows went between the delete and this read;
                    // the operator's retry then succeeds or names the next.
                    OrgTemplateRows::Live | OrgTemplateRows::None => Self::Templates,
                },
            ),
            Self::OrphanedSnapshotStorage => {
                let has_sandboxes =
                    pplx_space_sandbox_store::org_has_sandbox_rows(&state.pool, org_id)
                        .await
                        .map_err(
                            |pplx_space_sandbox_store::SandboxStoreError::Database(error)| {
                                IdentityAdminError::from(error)
                            },
                        )?;
                Ok(if has_sandboxes { Self::Sandboxes } else { self })
            }
            Self::Users
            | Self::Sandboxes
            | Self::ArchivedSandboxes
            | Self::DeletedTemplates { .. }
            | Self::NodeGroups
            | Self::ManagesCluster => Ok(self),
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Users => "org_has_users",
            Self::Sandboxes => "org_has_sandboxes",
            Self::ArchivedSandboxes => "org_has_archived_sandboxes",
            Self::Templates => "org_has_templates",
            Self::DeletedTemplates { .. } => "org_has_retained_deleted_templates",
            Self::OrphanedSnapshotStorage => "org_has_orphaned_snapshot_storage",
            Self::NodeGroups => "org_has_node_groups",
            Self::ManagesCluster => "org_manages_cluster",
        }
    }

    fn message(self) -> String {
        match self {
            Self::Users => "organization still has users; delete them or move them to another \
                            organization before deleting the organization"
                .to_string(),
            Self::Sandboxes => "organization still has sandboxes, including deleted ones whose \
                                records are kept for retention; delete them, or wait out \
                                retention, before deleting the organization"
                .to_string(),
            Self::ArchivedSandboxes => "organization still has archived sandboxes; they must be \
                                        purged before the organization can be deleted"
                .to_string(),
            Self::Templates => "organization still owns templates; delete them before deleting \
                                the organization"
                .to_string(),
            Self::DeletedTemplates {
                retained_until: Some(retained_until),
            } => format!(
                "organization's templates are all deleted, but their records are kept for \
                 retention until about {}; retry the delete after that",
                format_rfc3339(retained_until)
            ),
            Self::DeletedTemplates {
                retained_until: None,
            } => "organization's templates are all deleted, but their records are kept until \
                  retention ends and no node still caches them; retry the delete later"
                .to_string(),
            Self::OrphanedSnapshotStorage => "organization has no sandboxes left, but its master \
                                              key still encrypts snapshot storage locations its \
                                              deleted snapshots left behind, which the snapshot \
                                              storage sweeper does not remove; they must be \
                                              purged before the organization can be deleted"
                .to_string(),
            Self::NodeGroups => "organization still owns node groups; release each one with \
                                 :assign-org and a null org_id, or delete it once it has no \
                                 nodes (relabel or retire them first), before deleting the \
                                 organization"
                .to_string(),
            Self::ManagesCluster => "organization manages the cluster; make another \
                                     organization the managing organization before deleting it"
                .to_string(),
        }
    }
}

fn format_rfc3339(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

pub(super) async fn list_orgs(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> Result<Json<Vec<Organization>>, IdentityAdminError> {
    if is_managing_org_admin(&state, &actor).await? {
        return Ok(Json(
            sqlx::query_as::<_, Organization>(sql!(
                "SELECT * FROM organizations ORDER BY name, id"
            ))
            .fetch_all(&state.pool)
            .await?,
        ));
    }

    Ok(Json(
        sqlx::query_as::<_, Organization>(sql!("SELECT * FROM organizations WHERE id = $1"))
            .bind(actor.org_id)
            .fetch_all(&state.pool)
            .await?,
    ))
}

pub(super) async fn create_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(request): Json<CreateOrgRequest>,
) -> Result<(StatusCode, Json<Organization>), IdentityAdminError> {
    require_managing_org_admin(&state, &actor).await?;
    validate_branding(
        request.theme_primary.as_deref(),
        request.theme_secondary.as_deref(),
        request.logo_url.as_deref(),
    )?;
    let org = sqlx::query_as::<_, Organization>(sql!(
        "INSERT INTO organizations (
             id, name, slug, parent_id, theme_primary, theme_secondary, logo_url
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING *"
    ))
    .bind(Uuid::now_v7())
    .bind(request.name)
    .bind(request.slug)
    .bind(request.parent_id)
    .bind(request.theme_primary)
    .bind(request.theme_secondary)
    .bind(request.logo_url)
    .fetch_one(&state.pool)
    .await?;

    Ok((StatusCode::CREATED, Json(org)))
}

pub(super) async fn get_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
) -> Result<Json<Organization>, IdentityAdminError> {
    require_org_visible(&state, &actor, org_id).await?;
    let org = sqlx::query_as::<_, Organization>(sql!("SELECT * FROM organizations WHERE id = $1"))
        .bind(org_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(IdentityAdminError::NotFound)?;
    Ok(Json(org))
}

pub(super) async fn update_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
    Json(request): Json<UpdateOrgRequest>,
) -> Result<Json<Organization>, IdentityAdminError> {
    require_org_admin(&state, &actor, org_id).await?;
    let (theme_primary_present, theme_primary) = nullable_patch_bind(request.theme_primary);
    let (theme_secondary_present, theme_secondary) = nullable_patch_bind(request.theme_secondary);
    let (logo_url_present, logo_url) = nullable_patch_bind(request.logo_url);
    let (snapshot_base_object_storage_uri_present, snapshot_base_object_storage_uri) =
        nullable_patch_bind(request.snapshot_base_object_storage_uri);

    // The KEK existence check reads the credential-fenced org_master_keys
    // table, so it runs on the credentials lane ahead of the org-update
    // transaction rather than inside it. Key provisioning is owned by the
    // credential authority; this API only checks whether an active key uses a
    // supported authority provider or the configured backend can provision
    // one lazily.
    if request.byok_enabled == Some(true) {
        ensure_byok_enable_allowed(
            active_kek_provider(&state.credentials_pool, org_id).await?,
            platform_default_kek_provider(),
        )?;
    }
    let mut tx = state.pool.begin().await?;
    // Branding is validated only where the patch changes it: the console
    // re-sends every branding field on save, and a value stored before
    // validation existed must not block an edit to another field. Locked so
    // the comparison holds until the update.
    let stored = sqlx::query_as::<_, StoredBranding>(sql!(
        "SELECT theme_primary, theme_secondary, logo_url
         FROM organizations
         WHERE id = $1
         FOR UPDATE"
    ))
    .bind(org_id)
    .fetch_optional(tx.as_mut())
    .await?
    .ok_or(IdentityAdminError::NotFound)?;
    validate_branding(
        changed_value(
            theme_primary_present,
            theme_primary.as_deref(),
            stored.theme_primary.as_deref(),
        ),
        changed_value(
            theme_secondary_present,
            theme_secondary.as_deref(),
            stored.theme_secondary.as_deref(),
        ),
        changed_value(
            logo_url_present,
            logo_url.as_deref(),
            stored.logo_url.as_deref(),
        ),
    )?;
    let org = sqlx::query_as::<_, Organization>(sql!(
        "UPDATE organizations
         SET name = COALESCE($2, name),
             slug = COALESCE($3, slug),
             theme_primary = CASE WHEN $4 THEN $5 ELSE theme_primary END,
             theme_secondary = CASE WHEN $6 THEN $7 ELSE theme_secondary END,
             logo_url = CASE WHEN $8 THEN $9 ELSE logo_url END,
             snapshot_base_object_storage_uri =
                 CASE WHEN $10 THEN $11 ELSE snapshot_base_object_storage_uri END,
             byok_enabled = COALESCE($12, byok_enabled),
             updated_at = NOW()
         WHERE id = $1
         RETURNING *"
    ))
    .bind(org_id)
    .bind(request.name)
    .bind(request.slug)
    .bind(theme_primary_present)
    .bind(theme_primary)
    .bind(theme_secondary_present)
    .bind(theme_secondary)
    .bind(logo_url_present)
    .bind(logo_url)
    .bind(snapshot_base_object_storage_uri_present)
    .bind(snapshot_base_object_storage_uri)
    .bind(request.byok_enabled)
    .fetch_optional(tx.as_mut())
    .await?
    .ok_or(IdentityAdminError::NotFound)?;
    tx.commit().await?;
    Ok(Json(org))
}

/// BYOK may only be switched on for an org that can obtain an active master
/// key: the flag stamps `volume_encryption` on new sandboxes at create, and
/// credmgr refuses to mint volume DEKs without an active KEK — a keyless org
/// toggled on would create encrypted sandboxes that can never start.
///
/// An org already holding an active KEK is allowed only when the credential
/// authority supports that provider. In particular, a historical AWS KMS row
/// cannot enable BYOK after that backend has been removed: the row prevents
/// lazy provisioning and would fail when the first volume DEK is issued. A
/// keyless org is allowed only when the platform default provisions its KEK
/// lazily on first use (intermediate-key-broker, minted by the credmgr authority
/// on the org's first Encrypt / sandbox volume DEK).
fn ensure_byok_enable_allowed(
    active_kek_provider: Option<KekProvider>,
    platform_default_kek_provider: Option<KekProvider>,
) -> Result<(), IdentityAdminError> {
    match active_kek_provider {
        Some(KekProvider::InMemory | KekProvider::IntermediateKeyBroker) => Ok(()),
        Some(KekProvider::AwsKms) => Err(IdentityAdminError::InvalidRequest(
            "cannot enable BYOK: the organization's active master key provider is not supported \
             by the credential authority",
        )),
        None if platform_default_kek_provider.is_some_and(KekProvider::provisions_lazily) => Ok(()),
        None => Err(IdentityAdminError::InvalidRequest(
            "cannot enable BYOK: the organization has no active master key and \
             the platform has no lazy key provider",
        )),
    }
}

async fn active_kek_provider(
    credentials_pool: &pplx_space_common_svc::sql::CredentialsPool,
    org_id: OrgId,
) -> Result<Option<KekProvider>, IdentityAdminError> {
    let provider: Option<String> = sqlx::query_scalar(sql!(
        "SELECT provider FROM org_master_keys WHERE org_id = $1 AND retired_at IS NULL"
    ))
    .bind(org_id)
    .fetch_optional(credentials_pool.pool())
    .await?;
    parse_active_kek_provider(provider)
}

fn parse_active_kek_provider(
    provider: Option<String>,
) -> Result<Option<KekProvider>, IdentityAdminError> {
    provider
        .map(|provider| {
            KekProvider::parse(&provider).map_err(|_| {
                IdentityAdminError::InvalidRequest(
                    "cannot enable BYOK: the organization's active master key provider is not \
                     supported by the credential authority",
                )
            })
        })
        .transpose()
}

/// Active (non-retired) master key for the org, or `null` when the org
/// has none yet (the credential seal path lazily mints one from the
/// platform-default env config on first use).
pub(super) async fn get_org_master_key(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
) -> Result<Json<Option<OrgMasterKey>>, IdentityAdminError> {
    require_org_visible(&state, &actor, org_id).await?;
    require_org_exists(&state, org_id).await?;
    let key = sqlx::query_as::<_, OrgMasterKey>(sql!(
        "SELECT * FROM org_master_keys WHERE org_id = $1 AND retired_at IS NULL"
    ))
    .bind(org_id)
    .fetch_optional(state.credentials_pool.pool())
    .await?;
    Ok(Json(key))
}

pub(super) async fn delete_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
) -> Result<StatusCode, IdentityAdminError> {
    require_managing_org_admin(&state, &actor).await?;
    let cluster = fetch_cluster(&state).await?;
    if org_id == cluster.managing_org_id {
        return Err(IdentityAdminError::Forbidden);
    }
    let result = sqlx::query(sql!("DELETE FROM organizations WHERE id = $1"))
        .bind(org_id)
        .execute(&state.pool)
        .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return Err(match OrgDeleteBlocker::from_delete_error(&error) {
                Some(blocker) => {
                    IdentityAdminError::OrgNotEmpty(blocker.refine(&state, org_id).await?)
                }
                None => IdentityAdminError::from(error),
            });
        }
    };
    if result.rows_affected() == 0 {
        return Err(IdentityAdminError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_users_for_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
) -> Result<Json<Vec<AdminUserResponse>>, IdentityAdminError> {
    require_org_visible(&state, &actor, org_id).await?;
    let users = sqlx::query_as::<_, User>(sql!(
        "SELECT * FROM users WHERE org_id = $1 ORDER BY email ASC, id ASC"
    ))
    .bind(org_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(users.into_iter().map(Into::into).collect()))
}

pub(super) async fn create_user_for_org(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(org_id): Path<OrgId>,
    Json(request): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<AdminUserResponse>), IdentityAdminError> {
    if !authz::can_create_user_with_role(&state.pool, &actor, org_id, request.role).await? {
        return Err(IdentityAdminError::Forbidden);
    }
    let user = sqlx::query_as::<_, User>(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING *"
    ))
    .bind(Uuid::now_v7())
    .bind(request.email)
    .bind(hash_password(&request.password)?)
    .bind(request.display_name)
    .bind(org_id)
    .bind(request.role)
    .fetch_one(&state.pool)
    .await
    .map_err(IdentityAdminError::from_user_write)?;

    Ok((StatusCode::CREATED, Json(user.into())))
}

pub(super) async fn get_user(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(user_id): Path<UserId>,
) -> Result<Json<AdminUserResponse>, IdentityAdminError> {
    let user = fetch_user(&state, user_id).await?;
    require_org_visible(&state, &actor, user.org_id).await?;
    Ok(Json(user.into()))
}

pub(super) async fn update_user(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(user_id): Path<UserId>,
    Json(request): Json<UpdateUserRequest>,
) -> Result<Json<AdminUserResponse>, IdentityAdminError> {
    let target = fetch_user(&state, user_id).await?;
    if !authz::can_update_user_profile(&state.pool, &actor, &target).await? {
        return Err(IdentityAdminError::Forbidden);
    }
    if request.password.is_some() && target.owner_source == SandboxOwnerSource::FederatedIdentity {
        return Err(IdentityAdminError::InvalidRequest(
            "federated users cannot have local passwords",
        ));
    }
    let password_changed = request.password.is_some();
    let password_hash = match request.password {
        Some(password) => Some(hash_password(&password)?),
        None => None,
    };
    let mut tx = state.pool.begin().await?;
    let user = sqlx::query_as::<_, User>(sql!(
        "UPDATE users
         SET email = COALESCE($2, email),
             password_hash = COALESCE($3, password_hash),
             display_name = COALESCE($4, display_name),
             updated_at = NOW()
         WHERE id = $1
         RETURNING *"
    ))
    .bind(user_id)
    .bind(request.email)
    .bind(password_hash)
    .bind(request.display_name)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(IdentityAdminError::from_user_write)?
    .ok_or(IdentityAdminError::NotFound)?;
    if password_changed {
        sqlx::query(sql!("DELETE FROM sessions WHERE user_id = $1"))
            .bind(user_id)
            .execute(tx.as_mut())
            .await?;
    }
    tx.commit().await?;
    Ok(Json(user.into()))
}

pub(super) async fn update_user_role(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(user_id): Path<UserId>,
    Json(request): Json<UpdateUserRoleRequest>,
) -> Result<Json<AdminUserResponse>, IdentityAdminError> {
    if user_id == actor.id {
        return Err(IdentityAdminError::SelfMutation);
    }
    let target = fetch_user(&state, user_id).await?;
    if !authz::can_update_user_role(&state.pool, &actor, &target, request.role).await? {
        return Err(IdentityAdminError::Forbidden);
    }
    let user = sqlx::query_as::<_, User>(sql!(
        "UPDATE users SET role = $2, updated_at = NOW() WHERE id = $1 RETURNING *"
    ))
    .bind(user_id)
    .bind(request.role)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(IdentityAdminError::NotFound)?;
    Ok(Json(user.into()))
}

pub(super) async fn delete_user(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(user_id): Path<UserId>,
) -> Result<StatusCode, IdentityAdminError> {
    if user_id == actor.id {
        return Err(IdentityAdminError::SelfMutation);
    }
    let target = fetch_user(&state, user_id).await?;
    require_org_admin(&state, &actor, target.org_id).await?;
    let result = sqlx::query(sql!("DELETE FROM users WHERE id = $1"))
        .bind(user_id)
        .execute(&state.pool)
        .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return Err(match UserDeleteBlocker::from_delete_error(&error) {
                Some(blocker) => {
                    IdentityAdminError::StillOwns(blocker.refine(&state, user_id).await?)
                }
                None => IdentityAdminError::from(error),
            });
        }
    };
    if result.rows_affected() == 0 {
        return Err(IdentityAdminError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn get_cluster(
    State(state): State<AppState>,
) -> Result<Json<Cluster>, IdentityAdminError> {
    Ok(Json(fetch_cluster(&state).await?))
}

pub(super) async fn update_cluster(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(request): Json<UpdateClusterRequest>,
) -> Result<Json<Cluster>, IdentityAdminError> {
    require_managing_org_admin(&state, &actor).await?;
    let cluster = sqlx::query_as::<_, Cluster>(sql!(
        "UPDATE clusters
         SET managing_org_id = $1, updated_at = NOW()
         WHERE singleton
         RETURNING *"
    ))
    .bind(request.managing_org_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(IdentityAdminError::NotFound)?;
    Ok(Json(cluster))
}

async fn fetch_user(state: &AppState, user_id: UserId) -> Result<User, IdentityAdminError> {
    sqlx::query_as::<_, User>(sql!("SELECT * FROM users WHERE id = $1"))
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(IdentityAdminError::NotFound)
}

async fn require_org_exists(state: &AppState, org_id: OrgId) -> Result<(), IdentityAdminError> {
    let exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM organizations WHERE id = $1)"
    ))
    .bind(org_id)
    .fetch_one(&state.pool)
    .await?;
    if exists {
        return Ok(());
    }
    Err(IdentityAdminError::NotFound)
}

async fn fetch_cluster(state: &AppState) -> Result<Cluster, IdentityAdminError> {
    sqlx::query_as::<_, Cluster>(sql!("SELECT * FROM clusters WHERE singleton"))
        .fetch_optional(&state.pool)
        .await?
        .ok_or(IdentityAdminError::NotFound)
}

async fn require_org_visible(
    state: &AppState,
    actor: &User,
    org_id: OrgId,
) -> Result<(), IdentityAdminError> {
    if actor.org_id == org_id || is_managing_org_admin(state, actor).await? {
        return Ok(());
    }
    Err(IdentityAdminError::Forbidden)
}

async fn require_org_admin(
    state: &AppState,
    actor: &User,
    org_id: OrgId,
) -> Result<(), IdentityAdminError> {
    if actor.role == UserRole::OrganizationAdmin
        && (actor.org_id == org_id || is_managing_org_admin(state, actor).await?)
    {
        return Ok(());
    }
    Err(IdentityAdminError::Forbidden)
}

async fn require_managing_org_admin(
    state: &AppState,
    actor: &User,
) -> Result<(), IdentityAdminError> {
    if is_managing_org_admin(state, actor).await? {
        return Ok(());
    }
    Err(IdentityAdminError::Forbidden)
}

async fn is_managing_org_admin(state: &AppState, actor: &User) -> Result<bool, IdentityAdminError> {
    Ok(authz::is_managing_org_admin(&state.pool, actor).await?)
}

/// Check an organization's branding before it is stored: the console renders
/// the colors as CSS and the logo as an image source.
fn validate_branding(
    theme_primary: Option<&str>,
    theme_secondary: Option<&str>,
    logo_url: Option<&str>,
) -> Result<(), IdentityAdminError> {
    for (field, value) in [
        ("theme_primary", theme_primary),
        ("theme_secondary", theme_secondary),
    ] {
        if value.is_some_and(|value| !is_hex_color(value)) {
            return Err(IdentityAdminError::InvalidField {
                field,
                message: "theme colors must be #rgb or #rrggbb hex colors",
            });
        }
    }
    if logo_url.is_some_and(|value| !is_logo_url(value)) {
        return Err(IdentityAdminError::InvalidField {
            field: "logo_url",
            message: "logo_url must be an absolute https URL; http is allowed only for localhost",
        });
    }
    Ok(())
}

fn is_hex_color(value: &str) -> bool {
    value.strip_prefix('#').is_some_and(|digits| {
        matches!(digits.len(), 3 | 6) && digits.chars().all(|c| c.is_ascii_hexdigit())
    })
}

/// An absolute `https` URL with a host and no credentials, or `http` on a
/// loopback host for local development.
fn is_logo_url(value: &str) -> bool {
    let Ok(uri) = value.parse::<axum::http::Uri>() else {
        return false;
    };
    let Some(authority) = uri.authority() else {
        return false;
    };
    if authority.as_str().contains('@') || authority.host().is_empty() {
        return false;
    }
    match uri.scheme_str() {
        Some("https") => true,
        Some("http") => matches!(authority.host(), "localhost" | "127.0.0.1" | "[::1]"),
        _ => false,
    }
}

#[derive(sqlx::FromRow)]
struct StoredBranding {
    theme_primary: Option<String>,
    theme_secondary: Option<String>,
    logo_url: Option<String>,
}

/// The value a patch field sets, when it sets one other than the stored
/// value; clearing and leaving a field alone need no validation.
fn changed_value<'a>(
    present: bool,
    patched: Option<&'a str>,
    stored: Option<&str>,
) -> Option<&'a str> {
    patched.filter(|value| present && Some(*value) != stored)
}

fn nullable_patch_bind<T>(patch: Option<crate::models::NullablePatch<T>>) -> (bool, Option<T>) {
    match patch {
        Some(crate::models::NullablePatch::Set(value)) => (true, Some(value)),
        Some(crate::models::NullablePatch::Clear) => (true, None),
        None => (false, None),
    }
}

#[cfg(test)]
mod tests;
