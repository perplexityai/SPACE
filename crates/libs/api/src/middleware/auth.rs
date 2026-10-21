use pplx_space_common_core::types::OrgId;

use crate::api_surface::ApiSurface;
use crate::e2b::errors::E2bApiError;
use crate::federated_identity::{
    FederatedOwner, federated_owner_from_headers, owner_headers_present,
};
use crate::models::{ApiKey, Organization, User};
use crate::routes::AppState;
use crate::routes::public::error::PublicApiError;
use axum::{
    extract::{Request, State},
    http::HeaderMap,
    middleware::Next,
    response::{IntoResponse, Response},
};
use pplx_space_public_api::routes::{self as routes, NativeOperation};

/// Authenticated user, inserted into request extensions by the auth middleware.
#[derive(Debug, Clone)]
pub struct AuthUser(pub User);

/// Authenticated user's organization, inserted into request extensions by the auth middleware.
#[derive(Debug, Clone)]
pub struct AuthOrg(pub Organization);

/// Authenticated service credential, inserted into request extensions by the
/// ECW auth middleware and by [`require_native_auth`].
#[derive(Debug, Clone)]
pub struct AuthApiKey(pub ApiKey);

/// Actor resolved alongside the API key. `None` when the key has
/// `created_by` set but the user no longer exists, or when the org has no
/// users at all. Handlers that need an actor are responsible for deciding
/// whether the missing-actor case is fatal and returning a domain error.
#[derive(Debug, Clone)]
pub struct AuthEcwActor(pub Option<User>);

/// Authenticated caller identity for the ECW request-complete log line.
/// Inserted into *response* extensions by [`require_e2b_api_key`] because
/// the request-logging middleware wraps the auth layer and therefore only
/// sees auth-derived context on the response side. Carries opaque ids only
/// — never the API key material itself.
#[derive(Debug, Clone, Copy)]
pub struct EcwCallerIdentity {
    pub api_key_id: uuid::Uuid,
    pub org_id: OrgId,
}

/// Upstream owner asserted by a trusted federated integration, inserted into
/// request extensions only when federated identity is enabled and the request
/// authenticated with a trusted federated API key.
#[derive(Debug, Clone)]
pub struct AuthFederatedOwner(pub FederatedOwner);

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty())
}

fn api_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
}

pub async fn require_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let pool = &state.pool;
    let token = bearer_token(request.headers());

    let Some(token) = token else {
        return crate::routes::public::error::PublicApiError::unauthorized(
            "Missing or invalid Authorization header",
        )
        .into_response();
    };

    match crate::auth::get_session_user(pool, token).await {
        Ok(Some((user, org))) => {
            request.extensions_mut().insert(AuthUser(user));
            request.extensions_mut().insert(AuthOrg(org));
            next.run(request).await
        }
        Ok(None) => {
            crate::routes::public::error::PublicApiError::unauthorized("Invalid or expired session")
                .into_response()
        }
        // Classified rather than a bare `internal()`: this gate runs before
        // every authenticated console request, so a lock race on the session
        // lookup would 500 the request before the handler's own 503 could ever
        // be reached.
        Err(error) => crate::routes::public::error::PublicApiError::from(error).into_response(),
    }
}

/// The native operations a service credential may reach while it has no
/// effective owner.
///
/// Deny by default. Most native mutations authorize by organization alone --
/// `files.rs`, `processes.rs` and `ports.rs` take no `AuthUser` at all -- so
/// attaching `AuthOrg` to a service credential would otherwise let it write
/// files, kill processes and open ports on any sandbox in the organization,
/// with no record of whom it acted for.
///
/// Only explicitly supported non-mutating operations belong here. Absence is
/// intentional: adding reads such as `LIST_PORTS`, `GET_SANDBOX_STATS`,
/// `LIST_SNAPSHOTS`, `GET_SESSION`, `LIST_OPERATION_EVENTS` or `LIST_TEMPLATES`
/// requires an authorization review rather than assuming every read is safe.
///
/// Note that "read-only" is not derivable from the HTTP method: `files:stat`,
/// `files:list` and `files:read` are all POST.
///
/// `no_allowlisted_operation_shares_a_mount_with_a_refused_one` guards the
/// invariant that makes "a route added later is unreachable until listed"
/// true -- see that test before adding an entry in an `:action` group.
const SERVICE_CREDENTIAL_OPERATIONS: &[NativeOperation] = &[
    routes::GET_SANDBOX,
    routes::LIST_SANDBOXES,
    routes::GET_OPERATION,
    routes::LIST_PROCESSES,
    routes::GET_PROCESS,
    routes::STAT_FILE,
    routes::LIST_FILES,
    routes::READ_FILE,
];

/// Set by [`owner_not_required`]; a client cannot set it.
#[derive(Clone, Copy, Debug)]
struct OwnerNotRequired;

/// Layered outside [`require_native_auth`] on a route that needs a native
/// credential but no effective owner, so a service credential with no actor
/// may call it whatever its path.
pub(crate) async fn owner_not_required(mut request: Request, next: Next) -> Response {
    request.extensions_mut().insert(OwnerNotRequired);
    next.run(request).await
}

/// Absent or unrecognised routing information denies, so a request this
/// cannot classify is refused rather than allowed.
fn service_credential_may_call(request: &Request) -> bool {
    let Some(matched) = request.extensions().get::<axum::extract::MatchedPath>() else {
        return false;
    };
    // The public router is nested under both `/v1` and `/api/v1`, and
    // `MatchedPath` carries whichever mount served the request. Compare
    // mount-relative, which is also the form the router registers.
    let full = matched.as_str();
    let Some(relative) = full
        .strip_prefix(crate::routes::PUBLIC_API_SHARED_ROOT)
        .or_else(|| full.strip_prefix("/v1"))
    else {
        return false;
    };
    // `method_filter` serves HEAD from every GET route, so classify it as GET
    // rather than leaving HEAD 403 while GET is 200.
    let method = if request.method() == axum::http::Method::HEAD {
        axum::http::Method::GET
    } else {
        request.method().clone()
    };
    SERVICE_CREDENTIAL_OPERATIONS.iter().any(|operation| {
        operation.relative_runtime_path() == relative
            && method
                .as_str()
                .eq_ignore_ascii_case(operation.method().openapi_name())
    })
}

/// Resolve the owner a service credential claims to act for.
///
/// Mirrors the ECW ingress rules rather than inventing native ones: only a
/// key marked trusted may assert an owner, and an assertion that is present
/// but unusable is refused loudly instead of ignored. Ignoring it would run
/// the request as the credential's own organization, which is exactly the
/// mis-attribution this exists to prevent.
fn native_federated_owner(
    state: &AppState,
    context: &crate::auth::ApiKeyAuthContext,
    headers: &axum::http::HeaderMap,
) -> Result<Option<crate::federated_identity::FederatedOwner>, PublicApiError> {
    let federated = &state.federated_identity;
    let asserted = crate::federated_identity::owner_headers_present(headers);

    if !federated.enabled {
        // With the feature off an assertion cannot be validated, so a request
        // carrying one must not proceed as though it had none.
        if asserted {
            tracing::warn!("rejected native owner assertion while federated identity is disabled");
            return Err(PublicApiError::forbidden(
                "owner assertions are not accepted",
            ));
        }
        return Ok(None);
    }

    if !asserted {
        return Ok(None);
    }

    if !context.api_key.federated_owner_assertions_enabled {
        tracing::warn!(
            api_key_id = %context.api_key.id,
            "rejected owner assertion on a native credential not trusted to make one"
        );
        return Err(PublicApiError::forbidden(
            "this credential may not assert an owner",
        ));
    }

    match crate::federated_identity::federated_owner_from_headers(federated, headers) {
        Ok(owner) => Ok(Some(owner)),
        Err(error) => {
            tracing::warn!(
                %error,
                api_key_id = %context.api_key.id,
                "rejected native request with an invalid owner tuple"
            );
            Err(PublicApiError::forbidden("invalid owner assertion"))
        }
    }
}

fn federated_identity_error(
    error: crate::federated_identity::ResolvePrincipalError,
) -> PublicApiError {
    use crate::federated_identity::ResolvePrincipalError;
    match error {
        ResolvePrincipalError::Lookup(error) | ResolvePrincipalError::Reload(error) => error.into(),
        ResolvePrincipalError::Materialize(error) => {
            if error.is_lock_contention() {
                tracing::warn!(%error, "database statement lost a lock race; retryable");
                PublicApiError::lock_contention()
            } else {
                tracing::warn!(%error, "federated identity resolution failed");
                PublicApiError::internal()
            }
        }
    }
}

/// Native auth: a user session, or a service credential scoped to native.
///
/// Separate from [`require_auth`] rather than folded into it, because that
/// middleware also guards the session-management routes under `/auth/v1`. A
/// service credential has no business reading `/auth/v1/cluster` or asking
/// `/auth/v1/session` to delete a session it does not have, so the fallback
/// belongs to the surface that needs it.
pub async fn require_native_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let pool = &state.pool;

    let Some(token) = bearer_token(request.headers()) else {
        return crate::routes::public::error::PublicApiError::unauthorized(
            "Missing or invalid Authorization header",
        )
        .into_response();
    };

    match crate::auth::get_session_user(pool, token).await {
        Ok(Some((user, org))) => {
            if owner_headers_present(request.headers()) {
                return PublicApiError::forbidden("sessions may not assert an owner")
                    .into_response();
            }
            request.extensions_mut().insert(AuthUser(user));
            request.extensions_mut().insert(AuthOrg(org));
            return next.run(request).await;
        }
        Ok(None) => {}
        Err(error) => {
            return crate::routes::public::error::PublicApiError::from(error).into_response();
        }
    }

    // A backend service has no session, so the same bearer slot also carries a
    // service credential. Sessions are tried first because they are the common
    // case; only a miss costs the second lookup.
    match crate::auth::get_api_key_org(pool, token, ApiSurface::Native).await {
        Ok(Some(mut context)) => {
            let federated_owner = match native_federated_owner(&state, &context, request.headers())
            {
                Ok(owner) => owner,
                Err(error) => return error.into_response(),
            };

            // Native handlers receive the effective org and actor, never the
            // integration key's tenant. The public router additionally enforces
            // subject scope before any sandbox/session/operation handler runs.
            if let Some(owner) = &federated_owner {
                match crate::federated_identity::resolve_principal(pool, owner)
                    .await
                    .map_err(federated_identity_error)
                {
                    Ok((user, org)) => {
                        context.actor = Some(user);
                        context.org = org;
                    }
                    Err(error) => return error.into_response(),
                }
            }
            let actor = context.actor.clone();
            if actor.is_none()
                && request.extensions().get::<OwnerNotRequired>().is_none()
                && !service_credential_may_call(&request)
            {
                return crate::routes::public::error::PublicApiError::forbidden(
                    "this operation requires an effective owner; a service credential cannot call it",
                )
                .into_response();
            }
            if let Some(actor) = actor {
                request.extensions_mut().insert(AuthUser(actor));
            }
            request.extensions_mut().insert(AuthApiKey(context.api_key));
            request.extensions_mut().insert(AuthOrg(context.org));
            if let Some(owner) = federated_owner {
                request.extensions_mut().insert(AuthFederatedOwner(owner));
            }
            next.run(request).await
        }
        Ok(None) => crate::routes::public::error::PublicApiError::unauthorized(
            "Invalid or expired credentials",
        )
        .into_response(),
        Err(error) => {
            tracing::error!(%error, "native service-credential lookup failed");
            crate::routes::public::error::PublicApiError::from(error).into_response()
        }
    }
}

pub async fn require_e2b_api_key(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let pool = &state.pool;
    let key = api_key(request.headers());

    let Some(key) = key else {
        return E2bApiError::unauthorized().into_response();
    };

    match crate::auth::get_api_key_org(pool, key, ApiSurface::Ecw).await {
        Ok(Some(context)) => {
            let identity = EcwCallerIdentity {
                api_key_id: context.api_key.id,
                org_id: context.org.id,
            };
            let federated = &state.federated_identity;
            let mut federated_owner = None;
            if federated.enabled {
                let has_owner_headers = owner_headers_present(request.headers());
                if context.api_key.federated_owner_assertions_enabled && has_owner_headers {
                    match federated_owner_from_headers(federated, request.headers()) {
                        Ok(owner) => federated_owner = Some(owner),
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                api_key_id = %context.api_key.id,
                                "rejected trusted federated ECW request with invalid owner tuple"
                            );
                            return E2bApiError::unauthorized().into_response();
                        }
                    }
                } else if has_owner_headers {
                    // Owner assertions are authorization input only on the
                    // trusted federated integration; reject spoof attempts loudly
                    // instead of silently ignoring the headers.
                    tracing::warn!(
                        api_key_id = %context.api_key.id,
                        "rejected owner assertion headers on untrusted ECW path"
                    );
                    return E2bApiError::unauthorized().into_response();
                }
            }

            request.extensions_mut().insert(AuthApiKey(context.api_key));
            request.extensions_mut().insert(AuthOrg(context.org));
            request.extensions_mut().insert(AuthEcwActor(context.actor));
            if let Some(owner) = federated_owner {
                request.extensions_mut().insert(AuthFederatedOwner(owner));
            }
            let mut response = next.run(request).await;
            response.extensions_mut().insert(identity);
            response
        }
        Ok(None) => E2bApiError::unauthorized().into_response(),
        // Classified for the same reason as `require_auth` above, and this
        // gate matters more: it runs before *every* ECW handler, so a lock
        // race on the api-key/org lookup 500s the request before any of the
        // handler-level contention mappings can be reached.
        Err(error) => {
            if pplx_space_common_svc::sql::contention::is_lock_contention(&error) {
                tracing::warn!(%error, "ecw api-key lookup lost a lock race; retryable");
                return E2bApiError::lock_contention().into_response();
            }
            tracing::error!(%error, "ecw api-key lookup failed");
            E2bApiError::internal("Internal server error").into_response()
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod federated_ingress_tests;

#[cfg(test)]
mod native_service_key_tests;

#[cfg(test)]
mod native_federated_owner_tests;
