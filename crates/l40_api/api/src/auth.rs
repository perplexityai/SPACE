use pplx_space_accounts::OrgId;

use crate::api_surface::ApiSurface;
use pplx_space_sql_locator::sql;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;

use crate::models::{ApiKey, ConsoleSignIn, Organization, User};
use pplx_space_accounts_store::{api_keys, organizations, users};

pub struct ApiKeyAuthContext {
    pub api_key: ApiKey,
    pub org: Organization,
    /// The user the key acts as: its `created_by`, which must be a user of the
    /// key's org, or the org's oldest user when `created_by` is NULL (the
    /// recorded user was deleted, or the key predates recording one). `None`
    /// when `created_by` names a user outside the key's org, or the org has no
    /// users.
    pub actor: Option<User>,
}

/// bcrypt's work factor for stored passwords. This crate's tests use the
/// minimum, 4: a cost-12 hash took 0.3-0.5 s on a loaded CI runner, and the
/// tests that create users or sign in spent most of their time on it.
/// `verify_password` reads the cost from the stored hash, so the same code
/// paths run either way.
const PASSWORD_HASH_COST: u32 = if cfg!(test) { 4 } else { 12 };

pub fn hash_password(password: &str) -> Result<String, bcrypt::BcryptError> {
    bcrypt::hash(password, PASSWORD_HASH_COST)
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, bcrypt::BcryptError> {
    if hash == "!" {
        return Ok(false);
    }
    bcrypt::verify(password, hash)
}

/// Password logins that may verify a password at once in this process.
/// A cost-12 verification is hundreds of milliseconds of CPU, so it runs on
/// the blocking pool rather than an async worker, and this bound keeps a
/// flood of login attempts to at most half the cores. Further logins wait
/// for a slot before they open a database transaction, so waiting holds no
/// connection or row lock, and requests other than logins are not delayed.
static PASSWORD_VERIFICATIONS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(password_verification_slots()));

fn password_verification_slots() -> usize {
    std::thread::available_parallelism().map_or(1, |cores| cores.get().div_ceil(2))
}

/// The password verifier could not run: its slot pool was closed or the
/// blocking task failed. Neither happens in normal operation.
#[derive(Debug, thiserror::Error)]
#[error("password verification could not run")]
pub(crate) struct VerifierUnavailable;

/// Wait for a password-verification slot; see [`PASSWORD_VERIFICATIONS`].
pub(crate) async fn password_verification_slot()
-> Result<tokio::sync::SemaphorePermit<'static>, VerifierUnavailable> {
    PASSWORD_VERIFICATIONS
        .acquire()
        .await
        .map_err(|_| VerifierUnavailable)
}

/// [`verify_password`] on the blocking pool, off the async workers. The slot
/// moves into the blocking task and is released only when the verification
/// ends, so a cancelled login cannot free it while bcrypt still runs.
pub(crate) async fn verify_password_blocking(
    password: String,
    hash: String,
    slot: tokio::sync::SemaphorePermit<'static>,
) -> Result<Result<bool, bcrypt::BcryptError>, VerifierUnavailable> {
    run_in_slot(slot, move || verify_password(&password, &hash)).await
}

/// Runs `work` on the blocking pool while it holds `slot`, releasing the slot
/// only when `work` returns.
async fn run_in_slot<T: Send + 'static>(
    slot: tokio::sync::SemaphorePermit<'static>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, VerifierUnavailable> {
    tokio::task::spawn_blocking(move || {
        let output = work();
        drop(slot);
        output
    })
    .await
    .map_err(|_| VerifierUnavailable)
}

/// Returns (raw_token, token_hash). Send raw_token to the client; store token_hash in the DB.
pub fn generate_session_token() -> (String, String) {
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes);
    let raw_token = hex::encode(bytes);
    let token_hash = hash_token(&raw_token);
    (raw_token, token_hash)
}

pub fn hash_token(raw_token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    hex::encode(hasher.finalize())
}

pub async fn get_session_user(
    pool: &PgPool,
    raw_token: &str,
) -> Result<Option<(User, Organization)>, sqlx::Error> {
    let token_hash = hash_token(raw_token);

    let row = sqlx::query_as::<_, User>(sql!(
        "SELECT u.* FROM users u
         JOIN sessions s ON s.user_id = u.id
         WHERE s.token_hash = $1 AND s.expires_at > NOW()"
    ))
    .bind(&token_hash)
    .fetch_optional(pool)
    .await?;

    let Some(user) = row else {
        return Ok(None);
    };
    if !user.password_login_configured() {
        return Ok(None);
    }

    let org = organizations::get_organization(pool, user.org_id)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

    Ok(Some((user, org)))
}

/// Resolve the local SPACE user an external console identity maps to.
///
/// The email is matched exactly against `users.email`; nothing is created or
/// changed. Only users eligible for console sign-in qualify (see
/// [`ConsoleSignIn::external_console_sign_in_allowed`]). The user's own role and
/// organization authorize the request afterwards, exactly as for a password
/// session.
pub async fn get_external_console_user(
    pool: &PgPool,
    email: &str,
) -> Result<Option<(User, Organization)>, sqlx::Error> {
    let row = users::user_by_email(pool, email).await?;

    let Some(user) = row else {
        return Ok(None);
    };
    if !user.external_console_sign_in_allowed() {
        return Ok(None);
    }

    let org = organizations::get_organization(pool, user.org_id)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

    Ok(Some((user, org)))
}

/// How stale `api_keys.last_used_at` may get before a use of the key records
/// a new time. A use within this window of the recorded one writes nothing,
/// so an active key costs at most one `UPDATE` per window, not one per
/// request.
pub(crate) const API_KEY_LAST_USED_GRANULARITY: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

/// Resolve a service credential for one surface.
///
/// The surface is part of the lookup rather than a check the caller applies
/// afterwards, so a credential issued for one surface cannot authenticate on
/// another even if a future caller forgets to ask. A key that exists but is
/// not scoped to `surface` is indistinguishable from no key at all, which is
/// the right answer for an auth boundary.
///
/// A successful lookup records the key's use; see [`record_api_key_use`].
pub async fn get_api_key_org(
    pool: &PgPool,
    raw_api_key: &str,
    surface: ApiSurface,
) -> Result<Option<ApiKeyAuthContext>, sqlx::Error> {
    let key_hash = hash_token(raw_api_key);

    let Some(auth) = api_keys::api_key_auth_by_hash(pool, &key_hash, surface).await? else {
        return Ok(None);
    };
    let context = ApiKeyAuthContext {
        api_key: auth.api_key,
        org: auth.org,
        actor: auth.actor,
    };
    record_api_key_use(pool, &context.api_key).await;
    Ok(Some(context))
}

/// Advance the key's `last_used_at` when it is older than
/// [`API_KEY_LAST_USED_GRANULARITY`], best effort: a failure is logged and
/// never fails the request it is recording.
///
/// Only lookups through this API record a use. The ECW data-plane route
/// resolver checks a key's hash on its own and records nothing, so a key used
/// only for guest traffic keeps its last API use.
///
/// The value the lookup just read decides whether to write at all, so most
/// requests issue no statement. The `UPDATE` repeats the check against the
/// database clock and skips a row another request is already updating, so
/// concurrent uses of one key neither wait on each other nor write twice. It
/// also skips a key revoked since the lookup read it.
async fn record_api_key_use(pool: &PgPool, api_key: &ApiKey) {
    let fresh = api_key.last_used_at.is_some_and(|last_used_at| {
        OffsetDateTime::now_utc() - last_used_at < API_KEY_LAST_USED_GRANULARITY
    });
    if fresh {
        return;
    }
    let result = sqlx::query(sql!(
        "UPDATE api_keys
            SET last_used_at = NOW()
          WHERE id = (
              SELECT id
              FROM api_keys
              WHERE id = $1
                AND revoked_at IS NULL
                AND (last_used_at IS NULL
                     OR last_used_at < NOW() - make_interval(secs => $2))
              FOR UPDATE SKIP LOCKED
          )"
    ))
    .bind(api_key.id)
    .bind(API_KEY_LAST_USED_GRANULARITY.as_secs_f64())
    .execute(pool)
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, api_key_id = %api_key.id, "failed to record api key use");
    }
}

pub(crate) async fn org_has_local_owner(pool: &PgPool, org_id: OrgId) -> Result<bool, sqlx::Error> {
    organizations::organization_has_local_owner(pool, org_id).await
}

pub async fn api_key_belongs_to_org(
    pool: &PgPool,
    raw_api_key: &str,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    Ok(get_api_key_org(pool, raw_api_key, ApiSurface::Ecw)
        .await?
        .is_some_and(|context| context.org.id == org_id))
}

#[cfg(test)]
#[path = "auth/surface_scope_tests.rs"]
mod surface_scope_tests;

#[cfg(test)]
#[path = "auth/api_key_use_tests.rs"]
mod api_key_use_tests;

#[cfg(test)]
#[path = "auth/password_verification_tests.rs"]
mod password_verification_tests;
