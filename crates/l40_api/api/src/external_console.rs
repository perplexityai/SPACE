//! Optional external console identity.
//!
//! A deployment can place the console behind an identity-aware proxy that
//! authenticates the browser and attaches a signed assertion to every request
//! it forwards. When the API is started with an
//! [`ExternalConsoleAuthenticator`], console browser requests authenticate
//! with that assertion instead of a SPACE password session:
//!
//! - a console request is one that carries [`CONSOLE_REQUEST_HEADER`], which
//!   the console sends on every request in this mode. Console requests are
//!   authenticated by the assertion alone: no assertion is a 401, and a
//!   bearer credential sent alongside is never used (console routes ignore
//!   it; the native API refuses the combination). Requests without the
//!   header are machine requests; they authenticate with a service credential
//!   as before and their assertion, if the proxy added one, is not consulted.
//!   A client therefore cannot gain anything by adding or omitting the
//!   header: with it, it needs an admitted assertion; without it, a service
//!   credential;
//! - the authenticator alone decides whether the assertion is genuine and
//!   whether policy admits the identity; this crate never reads an unsigned
//!   identity header and holds no provider-specific policy;
//! - the verified email maps to an existing local SPACE user, whose own role
//!   and organization authorize the request exactly as a password session
//!   would. Nothing is provisioned and no role is granted;
//! - password login, password sessions and session revocation are disabled
//!   everywhere, so an old session token cannot bypass the mode;
//! - an assertion that is present but rejected never falls back to another
//!   credential;
//! - state-changing requests must come from a trusted console origin and carry
//!   [`CONSOLE_REQUEST_HEADER`], because the assertion is ambient: the proxy
//!   attaches it to whatever the browser sends. This also relies on the API
//!   never allowing credentialed cross-origin requests: its CORS policy sets
//!   no `Access-Control-Allow-Credentials`, so the preflight a cross-site page
//!   needs for the console header fails whenever it asks the browser to send
//!   the proxy's cookie.
//!
//! Without an authenticator nothing here runs and password authentication is
//! unchanged.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::http::{HeaderMap, Method};
use serde::Serialize;
use sqlx::PgPool;

use crate::models::{Organization, User};
use crate::routes::public::error::PublicApiError;

/// Comma-separated exact origins (`scheme://host[:port]`) the console is
/// served from. Required when an authenticator is installed.
pub const TRUSTED_ORIGINS_ENV: &str = "SPACE_EXTERNAL_CONSOLE_TRUSTED_ORIGINS";
/// Name the console shows for the sign-in provider. Optional.
pub const PROVIDER_NAME_ENV: &str = "SPACE_EXTERNAL_CONSOLE_PROVIDER_NAME";
/// Where the console sends the browser to end the provider session: a
/// same-origin absolute path or an `https` URL. Optional.
pub const LOGOUT_URL_ENV: &str = "SPACE_EXTERNAL_CONSOLE_LOGOUT_URL";

/// Header every console request sends in external mode, with the value `1`.
/// It marks the request as a console request, authenticated by the assertion.
/// A cross-site page cannot add a custom header without a CORS preflight, so
/// its presence on a state-changing request is also part of the CSRF defence.
pub const CONSOLE_REQUEST_HEADER: &str = "x-space-console-request";

const DEFAULT_PROVIDER_NAME: &str = "Single sign-on";
const MAX_PROVIDER_NAME_LEN: usize = 64;
const MAX_LOGOUT_URL_LEN: usize = 2048;
const MAX_TRUSTED_ORIGINS: usize = 16;

/// Identity an [`ExternalConsoleAuthenticator`] verified and admitted for one
/// request.
#[derive(Clone, PartialEq, Eq)]
pub struct ExternalConsoleIdentity {
    email: String,
}

impl ExternalConsoleIdentity {
    /// The verified email. It is matched exactly against `users.email`.
    pub fn new(email: impl Into<String>) -> Self {
        Self {
            email: email.into(),
        }
    }

    pub fn email(&self) -> &str {
        &self.email
    }
}

// Emails are personal data rather than credentials, but nothing needs them in
// a log line: callers that log an identity name the user id instead.
impl fmt::Debug for ExternalConsoleIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalConsoleIdentity")
            .field("email", &"<redacted>")
            .finish()
    }
}

/// Why an authenticator refused an assertion that was present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalConsoleRejection {
    /// The assertion is malformed, forged, expired, not yet valid, issued by
    /// another issuer, scoped to another audience, or lacks an identity. The
    /// browser has to sign in again.
    Unauthenticated,
    /// The assertion is genuine, but policy does not admit the identity.
    Forbidden,
    /// The authenticator cannot decide right now, for example because its
    /// signing keys or policy are unavailable. Treated as a refusal.
    Unavailable,
}

/// What an authenticator found on one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalConsoleAssertion {
    /// The request carries no assertion for this authenticator.
    Absent,
    /// The assertion is genuine and policy admits the identity.
    Verified(ExternalConsoleIdentity),
    /// The request carries an assertion that was refused.
    Rejected(ExternalConsoleRejection),
}

/// Future returned by [`ExternalConsoleAuthenticator::authenticate`].
pub type AuthenticateFuture<'a> =
    Pin<Box<dyn Future<Output = ExternalConsoleAssertion> + Send + 'a>>;

/// Verifies the assertion an identity-aware proxy attaches to console
/// requests and applies the deployment's admission policy.
///
/// Implementations must only return [`ExternalConsoleAssertion::Verified`]
/// for a cryptographically verified assertion, and must return
/// [`ExternalConsoleAssertion::Absent`] only when the request carries no
/// assertion at all: anything present but unusable is a rejection.
///
/// The API awaits the result inline on every console request and applies no
/// timeout of its own, so implementations must bound their own latency, for
/// example by timing out key fetches and answering
/// [`ExternalConsoleRejection::Unavailable`] instead of waiting.
pub trait ExternalConsoleAuthenticator: Send + Sync + 'static {
    fn authenticate<'a>(&'a self, headers: &'a HeaderMap) -> AuthenticateFuture<'a>;
}

/// External console settings the API reads from its own environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalConsoleSettings {
    provider_name: String,
    logout_url: Option<String>,
    trusted_origins: Vec<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExternalConsoleConfigError {
    #[error("{0} is not valid unicode")]
    NotUnicode(&'static str),
    #[error("{TRUSTED_ORIGINS_ENV} must name at least one console origin")]
    NoTrustedOrigins,
    #[error("{TRUSTED_ORIGINS_ENV} names more than {MAX_TRUSTED_ORIGINS} origins")]
    TooManyTrustedOrigins,
    #[error(
        "{TRUSTED_ORIGINS_ENV} entry {0:?} is not an exact origin of the form scheme://host[:port]"
    )]
    InvalidTrustedOrigin(String),
    #[error("{PROVIDER_NAME_ENV} must be 1 to {MAX_PROVIDER_NAME_LEN} printable characters")]
    InvalidProviderName,
    #[error("{LOGOUT_URL_ENV} must be a same-origin absolute path or an https URL")]
    InvalidLogoutUrl,
}

impl ExternalConsoleSettings {
    pub fn new(
        trusted_origins: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, ExternalConsoleConfigError> {
        let mut origins = Vec::new();
        for origin in trusted_origins {
            let origin = origin.as_ref().trim();
            if origin.is_empty() {
                continue;
            }
            if !is_exact_origin(origin) {
                return Err(ExternalConsoleConfigError::InvalidTrustedOrigin(
                    origin.to_owned(),
                ));
            }
            if !origins.iter().any(|known| known == origin) {
                origins.push(origin.to_owned());
            }
        }
        if origins.is_empty() {
            return Err(ExternalConsoleConfigError::NoTrustedOrigins);
        }
        if origins.len() > MAX_TRUSTED_ORIGINS {
            return Err(ExternalConsoleConfigError::TooManyTrustedOrigins);
        }
        Ok(Self {
            provider_name: DEFAULT_PROVIDER_NAME.to_owned(),
            logout_url: None,
            trusted_origins: origins,
        })
    }

    pub fn with_provider_name(
        mut self,
        provider_name: impl Into<String>,
    ) -> Result<Self, ExternalConsoleConfigError> {
        let provider_name = provider_name.into();
        let provider_name = provider_name.trim();
        if provider_name.is_empty()
            || provider_name.chars().count() > MAX_PROVIDER_NAME_LEN
            || provider_name.chars().any(char::is_control)
        {
            return Err(ExternalConsoleConfigError::InvalidProviderName);
        }
        self.provider_name = provider_name.to_owned();
        Ok(self)
    }

    pub fn with_logout_url(
        mut self,
        logout_url: impl Into<String>,
    ) -> Result<Self, ExternalConsoleConfigError> {
        let logout_url = logout_url.into();
        let logout_url = logout_url.trim();
        if !is_valid_logout_url(logout_url) {
            return Err(ExternalConsoleConfigError::InvalidLogoutUrl);
        }
        self.logout_url = Some(logout_url.to_owned());
        Ok(self)
    }

    pub fn from_env() -> Result<Self, ExternalConsoleConfigError> {
        Self::from_vars(
            std::env::var(TRUSTED_ORIGINS_ENV),
            std::env::var(PROVIDER_NAME_ENV),
            std::env::var(LOGOUT_URL_ENV),
        )
    }

    fn from_vars(
        trusted_origins: Result<String, std::env::VarError>,
        provider_name: Result<String, std::env::VarError>,
        logout_url: Result<String, std::env::VarError>,
    ) -> Result<Self, ExternalConsoleConfigError> {
        let value = |name, value: Result<String, std::env::VarError>| match value {
            Ok(value) => Ok(value),
            Err(std::env::VarError::NotPresent) => Ok(String::new()),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(ExternalConsoleConfigError::NotUnicode(name))
            }
        };
        let trusted_origins = value(TRUSTED_ORIGINS_ENV, trusted_origins)?;
        let provider_name = value(PROVIDER_NAME_ENV, provider_name)?;
        let logout_url = value(LOGOUT_URL_ENV, logout_url)?;

        let mut settings = Self::new(trusted_origins.split(','))?;
        if !provider_name.trim().is_empty() {
            settings = settings.with_provider_name(provider_name)?;
        }
        if !logout_url.trim().is_empty() {
            settings = settings.with_logout_url(logout_url)?;
        }
        Ok(settings)
    }

    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    pub fn logout_url(&self) -> Option<&str> {
        self.logout_url.as_deref()
    }

    pub fn trusted_origins(&self) -> &[String] {
        &self.trusted_origins
    }
}

/// `scheme://host[:port]` exactly as a browser sends it in `Origin`: `http`
/// or `https`, a lowercase host, no userinfo, path, query, fragment or
/// trailing slash.
fn is_exact_origin(origin: &str) -> bool {
    let Some((scheme, authority)) = origin.split_once("://") else {
        return false;
    };
    if scheme != "https" && scheme != "http" {
        return false;
    }
    if authority.is_empty()
        || authority.contains(['/', '?', '#', '@', '\\'])
        || authority
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
        || authority != authority.to_ascii_lowercase()
    {
        return false;
    }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if !authority.ends_with(']') => {
            if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) {
                return false;
            }
            host
        }
        _ => authority,
    };
    !host.is_empty()
}

fn is_valid_logout_url(url: &str) -> bool {
    if url.is_empty()
        || url.len() > MAX_LOGOUT_URL_LEN
        || url.chars().any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return false;
    }
    if let Some(rest) = url.strip_prefix('/') {
        // `//host` and `/\host` are protocol-relative in a browser.
        return !rest.starts_with('/') && !rest.starts_with('\\');
    }
    let Some(rest) = url.strip_prefix("https://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    !authority.is_empty() && !authority.contains('@')
}

/// The installed authenticator and its settings, shared by every gate.
#[derive(Clone)]
pub struct ExternalConsole {
    authenticator: Arc<dyn ExternalConsoleAuthenticator>,
    settings: Arc<ExternalConsoleSettings>,
}

impl fmt::Debug for ExternalConsole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalConsole")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// How a console gate resolved a request in external mode.
pub(crate) enum ExternalConsoleAuth {
    /// Not a console request: the console gates refuse; the native gate may
    /// still accept a service credential.
    NotConsole,
    /// The verified identity's SPACE user and organization.
    Principal(Box<(User, Organization)>),
    /// Refused. The response is final; no other credential is consulted.
    Denied(PublicApiError),
}

/// Auth-mode discovery body served at `GET /auth/v1/config`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ConsoleAuthConfig {
    Password,
    External {
        provider_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        logout_url: Option<String>,
    },
}

impl ExternalConsole {
    pub fn new(
        authenticator: Arc<dyn ExternalConsoleAuthenticator>,
        settings: ExternalConsoleSettings,
    ) -> Self {
        Self {
            authenticator,
            settings: Arc::new(settings),
        }
    }

    pub fn settings(&self) -> &ExternalConsoleSettings {
        &self.settings
    }

    pub(crate) fn auth_config(&self) -> ConsoleAuthConfig {
        ConsoleAuthConfig::External {
            provider_name: self.settings.provider_name.clone(),
            logout_url: self.settings.logout_url.clone(),
        }
    }

    /// Resolve a console browser request: verify the assertion, enforce the
    /// CSRF rule for state-changing methods, and map the identity to an
    /// existing local SPACE user.
    pub(crate) async fn resolve(
        &self,
        pool: &PgPool,
        method: &Method,
        headers: &HeaderMap,
    ) -> ExternalConsoleAuth {
        if !is_console_request(headers) {
            return ExternalConsoleAuth::NotConsole;
        }
        let identity = match self.authenticator.authenticate(headers).await {
            ExternalConsoleAssertion::Absent => {
                ConsoleDecision::MissingAssertion.record();
                return ExternalConsoleAuth::Denied(PublicApiError::unauthorized(
                    "Console sign-in is required",
                ));
            }
            ExternalConsoleAssertion::Rejected(rejection) => {
                ConsoleDecision::Rejected(rejection).record();
                return ExternalConsoleAuth::Denied(rejection_error(rejection));
            }
            ExternalConsoleAssertion::Verified(identity) => identity,
        };

        if let Err(refusal) = self.check_request_origin(method, headers) {
            ConsoleDecision::CsrfRejected.record();
            tracing::warn!(
                %method,
                reason = refusal.as_str(),
                "rejected state-changing external console request from an untrusted context"
            );
            return ExternalConsoleAuth::Denied(PublicApiError::forbidden(
                "Request origin is not a trusted console origin",
            ));
        }

        match crate::auth::get_external_console_user(pool, identity.email()).await {
            Ok(Some(principal)) => {
                ConsoleDecision::Accepted.record();
                ExternalConsoleAuth::Principal(Box::new(principal))
            }
            Ok(None) => {
                ConsoleDecision::NoUser.record();
                tracing::warn!("external console identity has no eligible SPACE user");
                ExternalConsoleAuth::Denied(PublicApiError::forbidden(
                    "No SPACE user is provisioned for this identity",
                ))
            }
            Err(error) => {
                ConsoleDecision::LookupFailed.record();
                if pplx_space_common_svc::sql::contention::is_lock_contention(&error) {
                    tracing::warn!(%error, "external console user lookup lost a lock race; retryable");
                    return ExternalConsoleAuth::Denied(PublicApiError::lock_contention());
                }
                tracing::error!(%error, "external console user lookup failed");
                ExternalConsoleAuth::Denied(PublicApiError::internal())
            }
        }
    }

    /// CSRF rule for an ambient credential. Safe methods pass; every other
    /// method needs an `Origin` that exactly matches a trusted console origin.
    /// The console request header, which a cross-site page cannot add without
    /// a preflight, is already required of every console request.
    fn check_request_origin(
        &self,
        method: &Method,
        headers: &HeaderMap,
    ) -> Result<(), OriginRefusal> {
        if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
            return Ok(());
        }
        let Some(origin) = headers.get(axum::http::header::ORIGIN) else {
            return Err(OriginRefusal::Missing);
        };
        let Ok(origin) = origin.to_str() else {
            return Err(OriginRefusal::Malformed);
        };
        if !self
            .settings
            .trusted_origins
            .iter()
            .any(|trusted| trusted == origin)
        {
            return Err(OriginRefusal::Untrusted);
        }
        Ok(())
    }
}

/// Why a state-changing console request failed the origin check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OriginRefusal {
    Missing,
    Malformed,
    Untrusted,
}

impl OriginRefusal {
    fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing Origin",
            Self::Malformed => "malformed Origin",
            Self::Untrusted => "untrusted Origin",
        }
    }
}

/// The outcome recorded once for every console request resolved in external
/// mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsoleDecision {
    Accepted,
    MissingAssertion,
    Rejected(ExternalConsoleRejection),
    CsrfRejected,
    NoUser,
    LookupFailed,
}

impl ConsoleDecision {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::MissingAssertion => "missing_assertion",
            Self::Rejected(ExternalConsoleRejection::Unauthenticated) => "unauthenticated",
            Self::Rejected(ExternalConsoleRejection::Forbidden) => "forbidden",
            Self::Rejected(ExternalConsoleRejection::Unavailable) => "unavailable",
            Self::CsrfRejected => "csrf_rejected",
            Self::NoUser => "no_user",
            Self::LookupFailed => "lookup_failed",
        }
    }

    fn record(self) {
        pplx_space_metrics::auth::external_console_decision(self.as_str());
    }
}

/// Whether the request carries `CONSOLE_REQUEST_HEADER: 1`, exactly once.
pub(crate) fn is_console_request(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(CONSOLE_REQUEST_HEADER).iter();
    matches!((values.next(), values.next()), (Some(value), None) if value.as_bytes() == b"1")
}

fn rejection_error(rejection: ExternalConsoleRejection) -> PublicApiError {
    match rejection {
        ExternalConsoleRejection::Unauthenticated => {
            PublicApiError::unauthorized("Console sign-in is missing, invalid, or expired")
        }
        ExternalConsoleRejection::Forbidden => {
            PublicApiError::forbidden("This identity is not allowed to use the console")
        }
        ExternalConsoleRejection::Unavailable => {
            PublicApiError::service_unavailable("Console sign-in cannot be verified right now")
        }
    }
}

#[cfg(test)]
mod tests;
